from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from ...core.coercion import node_id_or_none
from ...core.coercion import node_key as _node_key
from ...core.protocol import list_payload
from ...core.topology import TopologyNode
from .motor import (
    MOTOR_TARGET_ANGLE_PROP,
    MOTOR_TARGET_POSITION_PROP,
)

PENDING_WRITE_REPORT_GRACE = 5.0
PENDING_WRITE_QUIET_WINDOW = 2.5


@dataclass(frozen=True)
class PendingWrite:
    node_id: str | int
    prop: str
    write_id: int
    target: Any
    held: Any
    held_present: bool
    created_at: float
    transition_delay: float = 0.0
    not_before: float | None = None
    matched_since: float | None = None
    refreshing: bool = False


@dataclass(frozen=True)
class PendingRefresh:
    node_id: str | int
    write_ids: Mapping[str, int]


@dataclass(frozen=True)
class PendingTickResult:
    visible_affected: set[str | int]
    refreshes: tuple[PendingRefresh, ...]


class PendingWriteTracker:
    """Latch HA-visible properties until gateway observations settle."""

    def __init__(
        self,
        *,
        report_grace: float = PENDING_WRITE_REPORT_GRACE,
        quiet_window: float = PENDING_WRITE_QUIET_WINDOW,
    ) -> None:
        self.report_grace = max(0.0, report_grace)
        self.quiet_window = max(0.0, quiet_window)
        self._pending: dict[str, dict[str, PendingWrite]] = {}

    def prepare_writes(
        self,
        write_id: int,
        props_by_node: Mapping[str | int, Mapping[str, Any]],
        *,
        nodes: Mapping[str | int, TopologyNode],
        now: float,
        transition_delays: Mapping[str | int, Mapping[str, float]] | None = None,
    ) -> set[str | int]:
        affected: set[str | int] = set()
        for node_id, props in props_by_node.items():
            node_key = _node_key(node_id)
            if node_key is None:
                continue
            node = nodes.get(node_id)
            if node is None:
                continue
            pending_by_prop = self._pending.setdefault(node_key, {})
            delays = _mapping_for_node(transition_delays, node_id)
            for prop, target in _trackable_property_props(props).items():
                previous = pending_by_prop.get(prop)
                held = previous.held if previous is not None else node.params.get(prop)
                held_present = previous.held_present if previous is not None else prop in node.params
                pending_by_prop[prop] = PendingWrite(
                    node_id=node_id,
                    prop=prop,
                    write_id=write_id,
                    target=target,
                    held=held,
                    held_present=held_present,
                    created_at=now,
                    transition_delay=max(0.0, float(delays.get(prop, 0.0))),
                )
                affected.add(node_id)
        return affected

    def accept_writes(self, write_ids: Iterable[int], *, now: float) -> set[str | int]:
        wanted = set(write_ids)
        affected: set[str | int] = set()
        for node_key, pending_by_prop in list(self._pending.items()):
            for prop, pending in list(pending_by_prop.items()):
                if pending.write_id not in wanted or pending.not_before is not None:
                    continue
                pending_by_prop[prop] = replace(pending, not_before=now + pending.transition_delay)
                affected.add(pending.node_id)
            if not pending_by_prop:
                self._pending.pop(node_key, None)
        return affected

    def fail_writes(self, write_ids: Iterable[int]) -> set[str | int]:
        wanted = set(write_ids)
        return self._remove_where(lambda pending: pending.write_id in wanted)

    def capture_write_ids(self) -> dict[str | int, dict[str, int]]:
        captured: dict[str | int, dict[str, int]] = {}
        for pending_by_prop in self._pending.values():
            for pending in pending_by_prop.values():
                captured.setdefault(pending.node_id, {})[pending.prop] = pending.write_id
        return captured

    def filter_stale_pull(
        self,
        message: Mapping[str, Any],
        captured_write_ids: Mapping[str | int, Mapping[str, int]] | None,
    ) -> Mapping[str, Any]:
        if captured_write_ids is None:
            return message
        result = dict(message)
        for collection in ("nodes", "groups"):
            raw_items = message.get(collection)
            if not isinstance(raw_items, list):
                continue
            filtered_items: list[Any] = []
            for raw_item in raw_items:
                if not isinstance(raw_item, Mapping):
                    filtered_items.append(raw_item)
                    continue
                item = dict(raw_item)
                node_id = node_id_or_none(item.get("id"))
                params = item.get("params")
                if node_id is None or not isinstance(params, Mapping):
                    filtered_items.append(item)
                    continue
                captured = _mapping_for_node(captured_write_ids, node_id)
                pending = self._pending_for_node(node_id)
                item["params"] = {
                    prop: value
                    for prop, value in params.items()
                    if not isinstance(prop, str) or prop not in pending or captured.get(prop) == pending[prop].write_id
                }
                filtered_items.append(item)
            result[collection] = filtered_items
        return result

    def apply_observation(self, message: Mapping[str, Any], *, now: float) -> set[str | int]:
        affected: set[str | int] = set()
        for item in _authoritative_property_items(message):
            node_id = node_id_or_none(item.get("id"))
            params = item.get("params")
            if node_id is None or not isinstance(params, Mapping):
                continue
            pending_by_prop = self._pending_for_node(node_id)
            for prop, observed in params.items():
                if not isinstance(prop, str):
                    continue
                pending = pending_by_prop.get(prop)
                if pending is None or pending.not_before is None:
                    continue
                matched_since = pending.matched_since
                if observed == pending.target and now >= pending.not_before:
                    if matched_since is None:
                        pending_by_prop[prop] = replace(pending, matched_since=now)
                        affected.add(pending.node_id)
                elif matched_since is not None:
                    pending_by_prop[prop] = replace(pending, matched_since=None)
                    affected.add(pending.node_id)
        return affected

    def complete_refresh(
        self,
        refresh: PendingRefresh,
        message: Mapping[str, Any] | None,
        *,
        failed: bool,
        now: float,
    ) -> set[str | int]:
        observed = {} if message is None else _observed_params(message, refresh.node_id)
        pending_by_prop = self._pending_for_node(refresh.node_id)
        changed = False
        for prop, write_id in refresh.write_ids.items():
            pending = pending_by_prop.get(prop)
            if pending is None or pending.write_id != write_id or not pending.refreshing:
                continue
            if failed or prop not in observed or observed[prop] != pending.target:
                pending_by_prop.pop(prop, None)
                changed = True
                continue
            if pending.not_before is not None and now >= pending.not_before:
                pending_by_prop[prop] = replace(pending, matched_since=now, refreshing=False)
        self._drop_empty_node(refresh.node_id)
        return {refresh.node_id} if changed else set()

    def tick(self, *, now: float) -> PendingTickResult:
        visible_affected: set[str | int] = set()
        refreshes_by_node: dict[str, tuple[str | int, dict[str, int]]] = {}
        for node_key, pending_by_prop in list(self._pending.items()):
            for prop, pending in list(pending_by_prop.items()):
                if pending.not_before is None:
                    continue
                if pending.matched_since is not None:
                    if now >= pending.matched_since + self.quiet_window:
                        pending_by_prop.pop(prop, None)
                        visible_affected.add(pending.node_id)
                    continue
                if pending.refreshing or now < pending.not_before + self.report_grace:
                    continue
                pending_by_prop[prop] = replace(pending, refreshing=True)
                current = refreshes_by_node.setdefault(node_key, (pending.node_id, {}))
                current[1][prop] = pending.write_id
            if not pending_by_prop:
                self._pending.pop(node_key, None)

        refreshes = tuple(
            PendingRefresh(node_id=node_id, write_ids=dict(write_ids))
            for node_id, write_ids in refreshes_by_node.values()
        )
        return PendingTickResult(visible_affected=visible_affected, refreshes=refreshes)

    def next_deadline(self, *, now: float) -> float | None:
        deadlines: list[float] = []
        for pending in self._iter_pending():
            if pending.not_before is None:
                continue
            if pending.matched_since is not None:
                deadlines.append(pending.matched_since + self.quiet_window)
            elif not pending.refreshing:
                deadlines.append(pending.not_before + self.report_grace)
        return min(deadlines) if deadlines else None

    def project_visible(self, node: TopologyNode) -> TopologyNode:
        pending_by_prop = self._pending_for_node(node.id)
        params = dict(node.params)
        for prop, pending in pending_by_prop.items():
            if pending.held_present:
                params[prop] = pending.held
            else:
                params.pop(prop, None)
        return node if params == node.params else replace(node, params=params)

    def clear_all(self) -> set[str | int]:
        affected = {pending.node_id for pending in self._iter_pending()}
        self._pending.clear()
        return affected

    def has_pending(self, node_id: str | int, props: Iterable[str] | None = None) -> bool:
        pending = self._pending_for_node(node_id)
        if props is None:
            return bool(pending)
        return any(prop in pending for prop in props)

    def diagnostics(self, *, now: float) -> dict[str, Any]:
        properties = [
            {
                "node_id": str(pending.node_id),
                "property": pending.prop,
                "write_id": pending.write_id,
                "target": pending.target,
                "held": pending.held,
                "age": round(max(0.0, now - pending.created_at), 3),
                "accepted": pending.not_before is not None,
                "matched": pending.matched_since is not None,
                "refreshing": pending.refreshing,
            }
            for pending in sorted(self._iter_pending(), key=lambda item: item.write_id)
        ]
        return {
            "count": len(properties),
            "properties": properties,
        }

    def _pending_for_node(self, node_id: str | int) -> dict[str, PendingWrite]:
        node_key = _node_key(node_id)
        if node_key is None:
            return {}
        return self._pending.get(node_key, {})

    def _drop_empty_node(self, node_id: str | int) -> None:
        node_key = _node_key(node_id)
        if node_key is not None and not self._pending.get(node_key):
            self._pending.pop(node_key, None)

    def _iter_pending(self) -> Iterable[PendingWrite]:
        for pending_by_prop in self._pending.values():
            yield from pending_by_prop.values()

    def _remove_where(self, predicate: Any) -> set[str | int]:
        affected: set[str | int] = set()
        for node_key, pending_by_prop in list(self._pending.items()):
            for prop, pending in list(pending_by_prop.items()):
                if predicate(pending):
                    pending_by_prop.pop(prop, None)
                    affected.add(pending.node_id)
            if not pending_by_prop:
                self._pending.pop(node_key, None)
        return affected


def _authoritative_property_items(message: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    yield from list_payload(message, "nodes")
    yield from list_payload(message, "groups")


def _mapping_for_node(
    mapping: Mapping[str | int, Mapping[str, Any]] | None,
    node_id: str | int,
) -> Mapping[str, Any]:
    if mapping is None:
        return {}
    direct = mapping.get(node_id)
    if direct is not None:
        return direct
    node_key = _node_key(node_id)
    if node_key is None:
        return {}
    for candidate_id, value in mapping.items():
        if _node_key(candidate_id) == node_key:
            return value
    return {}


def _observed_params(message: Mapping[str, Any], node_id: str | int) -> Mapping[str, Any]:
    wanted = _node_key(node_id)
    for item in _authoritative_property_items(message):
        if _node_key(item.get("id")) != wanted:
            continue
        params = item.get("params")
        return params if isinstance(params, Mapping) else {}
    return {}


def _trackable_property_props(props: Mapping[str, Any]) -> dict[str, Any]:
    return {
        prop: value
        for prop, value in props.items()
        if isinstance(prop, str) and prop not in {MOTOR_TARGET_POSITION_PROP, MOTOR_TARGET_ANGLE_PROP}
    }
