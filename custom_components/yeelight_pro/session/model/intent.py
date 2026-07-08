from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from ...core.coercion import int_or_none as _int_or_none
from ...core.coercion import node_id_or_none
from ...core.coercion import node_key as _node_key
from ...core.protocol import list_payload
from ...core.topology import TopologyNode
from .motor import (
    MOTOR_TARGET_ANGLE_PROP,
    MOTOR_TARGET_POSITION_PROP,
    MOTOR_TRACKING_TTL,
    MotorAxisTrack,
    MotorStateTracker,
    MotorTargetIntent,
)

COMMAND_INTENT_TTL = 5.0


@dataclass(frozen=True)
class PendingPropertyIntent:
    node_id: str | int
    prop: str
    value: Any
    created_at: float
    updated_at: float
    expires_at: float
    generation: int


class PropertyIntentTracker:
    """Tracks ACKed property targets until matching authoritative state arrives."""

    def __init__(self, *, ttl: float = COMMAND_INTENT_TTL) -> None:
        self.ttl = ttl
        self._generation = 0
        self._pending: dict[str, dict[str, PendingPropertyIntent]] = {}

    def record(
        self,
        node_id: str | int,
        props: Mapping[str, Any],
        *,
        now: float,
        current_params: Mapping[str, Any] | None = None,
    ) -> set[str | int]:
        del current_params
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        pending_by_prop = self._pending.setdefault(normalized, {})
        changed = False
        expires_at = now + self.ttl
        for prop, value in props.items():
            if not isinstance(prop, str):
                continue
            self._generation += 1
            pending_by_prop[prop] = PendingPropertyIntent(
                node_id=node_id,
                prop=prop,
                value=value,
                created_at=now,
                updated_at=now,
                expires_at=expires_at,
                generation=self._generation,
            )
            changed = True
        if not pending_by_prop:
            self._pending.pop(normalized, None)
        return {node_id} if changed else set()

    def apply_authoritative_node(
        self,
        node_id: str | int,
        params: Mapping[str, Any],
        *,
        now: float,
    ) -> set[str | int]:
        del now
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        pending_by_prop = self._pending.get(normalized)
        if pending_by_prop is None:
            return set()
        changed = False
        for prop, value in params.items():
            if not isinstance(prop, str):
                continue
            pending = pending_by_prop.get(prop)
            if pending is None:
                continue
            if value == pending.value:
                pending_by_prop.pop(prop, None)
                changed = True
        if not pending_by_prop:
            self._pending.pop(normalized, None)
        return {node_id} if changed else set()

    def project_visible(self, node: TopologyNode) -> TopologyNode:
        normalized = _node_key(node.id)
        if normalized is None:
            return node
        pending_by_prop = self._pending.get(normalized)
        if pending_by_prop is None:
            return node
        params = dict(node.params)
        for prop, pending in pending_by_prop.items():
            params[prop] = pending.value
        if params == node.params:
            return node
        return replace(node, params=params)

    def expire_pending(self, *, now: float) -> tuple[PendingPropertyIntent, ...]:
        expired: list[PendingPropertyIntent] = []
        for node_key, pending_by_prop in list(self._pending.items()):
            for prop, pending in list(pending_by_prop.items()):
                if pending.expires_at > now:
                    continue
                pending_by_prop.pop(prop, None)
                expired.append(pending)
            if not pending_by_prop:
                self._pending.pop(node_key, None)
        expired.sort(key=lambda pending: pending.generation)
        return tuple(expired)

    def clear_props(self, node_id: str | int, props: Iterable[str]) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        pending_by_prop = self._pending.get(normalized)
        if pending_by_prop is None:
            return set()
        changed = False
        for prop in props:
            if isinstance(prop, str):
                changed = pending_by_prop.pop(prop, None) is not None or changed
        if not pending_by_prop:
            self._pending.pop(normalized, None)
        return {node_id} if changed else set()

    def clear_node(self, node_id: str | int) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        pending_by_prop = self._pending.pop(normalized, None)
        if pending_by_prop is None:
            return set()
        return {pending.node_id for pending in pending_by_prop.values()}

    def clear_missing_nodes(self, node_ids: Iterable[str | int]) -> set[str | int]:
        known = {_node_key(node_id) for node_id in node_ids}
        known.discard(None)
        affected: set[str | int] = set()
        for node_key, pending_by_prop in list(self._pending.items()):
            if node_key in known:
                continue
            self._pending.pop(node_key, None)
            affected.update(pending.node_id for pending in pending_by_prop.values())
        return affected

    def clear_all(self) -> set[str | int]:
        affected = {pending.node_id for pending in self._iter_pending()}
        self._pending.clear()
        return affected

    def next_expiration(self, *, now: float) -> float | None:
        expirations = [pending.expires_at for pending in self._iter_pending() if pending.expires_at > now]
        return min(expirations) if expirations else None

    def has_pending(self, node_id: str | int, props: Iterable[str] | None = None) -> bool:
        normalized = _node_key(node_id)
        if normalized is None:
            return False
        pending_by_prop = self._pending.get(normalized)
        if pending_by_prop is None:
            return False
        if props is None:
            return bool(pending_by_prop)
        return any(prop in pending_by_prop for prop in props)

    def diagnostics(self, *, now: float) -> list[dict[str, Any]]:
        return [
            {
                "node_id": str(pending.node_id),
                "property": pending.prop,
                "age": round(max(0.0, now - pending.created_at), 3),
                "inactive_for": round(max(0.0, now - pending.updated_at), 3),
                "expires_in": round(max(0.0, pending.expires_at - now), 3),
                "generation": pending.generation,
            }
            for pending in sorted(self._iter_pending(), key=lambda item: item.generation)
        ]

    def _iter_pending(self) -> Iterable[PendingPropertyIntent]:
        for pending_by_prop in self._pending.values():
            yield from pending_by_prop.values()


class MotorIntentTracker:
    """Adapter around motor tracking so registry owns all in-flight command state."""

    def __init__(self, *, ttl: float = MOTOR_TRACKING_TTL) -> None:
        self.tracker = MotorStateTracker(ttl=ttl)

    def record_targets(
        self,
        targets: Iterable[MotorTargetIntent],
        *,
        nodes: Mapping[str | int, TopologyNode],
        now: float,
    ) -> set[str | int]:
        affected: set[str | int] = set()
        for target in targets:
            node = nodes.get(target.node_id)
            current_value = _int_or_none(node.params.get(target.current_prop)) if node is not None else None
            affected.update(self.tracker.set_target(target, current_value=current_value, now=now))
        return affected

    def apply_authoritative_message(
        self,
        message: Mapping[str, Any],
        nodes: Mapping[str | int, TopologyNode],
        *,
        now: float,
    ) -> set[str | int]:
        return self.tracker.apply_authoritative_message(message, nodes, now=now)

    def clear_node(self, node_id: str | int) -> set[str | int]:
        return self.tracker.clear_node(node_id)

    def clear_missing_nodes(self, node_ids: Iterable[str | int]) -> set[str | int]:
        return self.tracker.clear_missing_nodes(node_ids)

    def clear_all(self) -> set[str | int]:
        return self.tracker.clear_all()

    def expire_pending(self, *, now: float) -> tuple[MotorAxisTrack, ...]:
        return self.tracker.expire_pending(now=now)

    def next_expiration(self, *, now: float) -> float | None:
        return self.tracker.next_expiration(now=now)

    def project_visible(self, node: TopologyNode) -> TopologyNode:
        return self.tracker.visible_node(node)

    def has_tracking(self, node_id: str | int) -> bool:
        return self.tracker.has_tracking(node_id)

    def diagnostics(self, *, now: float) -> dict[str, Any]:
        return self.tracker.diagnostics(now=now)


@dataclass(frozen=True)
class ExpiredIntent:
    node_id: str | int
    props: tuple[str, ...]


class CommandIntentRegistry:
    """Registry-owned command intent state for visible-state projection."""

    def __init__(self, *, ttl: float = COMMAND_INTENT_TTL, motor_tracking_ttl: float = MOTOR_TRACKING_TTL) -> None:
        self.properties = PropertyIntentTracker(ttl=ttl)
        self.motor = MotorIntentTracker(ttl=motor_tracking_ttl)

    def record_property_intents(
        self,
        props_by_node: Mapping[str | int, Mapping[str, Any]],
        *,
        nodes: Mapping[str | int, TopologyNode],
        now: float,
    ) -> set[str | int]:
        affected: set[str | int] = set()
        for node_id, props in props_by_node.items():
            node = nodes.get(node_id)
            current_params = node.params if node is not None else {}
            affected.update(
                self.properties.record(
                    node_id,
                    _trackable_property_props(props),
                    now=now,
                    current_params=current_params,
                )
            )
        return affected

    def record_motor_targets(
        self,
        targets: Iterable[MotorTargetIntent],
        *,
        nodes: Mapping[str | int, TopologyNode],
        now: float,
    ) -> set[str | int]:
        return self.motor.record_targets(targets, nodes=nodes, now=now)

    def apply_authoritative_message(
        self,
        message: Mapping[str, Any],
        *,
        nodes: Mapping[str | int, TopologyNode],
        now: float,
    ) -> set[str | int]:
        affected: set[str | int] = set()
        for item in _authoritative_property_items(message):
            node_id = _item_id(item)
            params = item.get("params")
            if node_id is None or not isinstance(params, Mapping):
                continue
            affected.update(self.properties.apply_authoritative_node(node_id, params, now=now))
        affected.update(self.motor.apply_authoritative_message(message, nodes, now=now))
        return affected

    def expire_pending(self, *, now: float) -> tuple[ExpiredIntent, ...]:
        expired: dict[str, tuple[str | int, set[str]]] = {}
        for pending in self.properties.expire_pending(now=now):
            key = _node_key(pending.node_id)
            if key is None:
                continue
            current = expired.setdefault(key, (pending.node_id, set()))
            current[1].add(pending.prop)
        for track in self.motor.expire_pending(now=now):
            key = _node_key(track.node_id)
            if key is None:
                continue
            current = expired.setdefault(key, (track.node_id, set()))
            current[1].update({track.current_prop, track.target_prop})
        return tuple(ExpiredIntent(node_id=node_id, props=tuple(sorted(props))) for node_id, props in expired.values())

    def project_visible(self, node: TopologyNode) -> TopologyNode:
        visible = self.properties.project_visible(node)
        return self.motor.project_visible(visible)

    def clear_node(self, node_id: str | int) -> set[str | int]:
        return self.properties.clear_node(node_id) | self.motor.clear_node(node_id)

    def clear_missing_nodes(self, node_ids: Iterable[str | int]) -> set[str | int]:
        return self.properties.clear_missing_nodes(node_ids) | self.motor.clear_missing_nodes(node_ids)

    def clear_all(self) -> set[str | int]:
        return self.properties.clear_all() | self.motor.clear_all()

    def next_expiration(self, *, now: float) -> float | None:
        expirations = [
            value
            for value in (
                self.properties.next_expiration(now=now),
                self.motor.next_expiration(now=now),
            )
            if value is not None
        ]
        return min(expirations) if expirations else None

    def has_pending(self, node_id: str | int, props: Iterable[str] | None = None) -> bool:
        return self.properties.has_pending(node_id, props) or (props is None and self.motor.has_tracking(node_id))

    def diagnostics(self, *, now: float) -> dict[str, Any]:
        property_entries = self.properties.diagnostics(now=now)
        return {
            "count": len(property_entries),
            "properties": property_entries,
            "motor": self.motor.diagnostics(now=now),
        }


def _item_id(item: Mapping[str, Any]) -> str | int | None:
    return node_id_or_none(item.get("id"))


def _authoritative_property_items(message: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    yield from list_payload(message, "nodes")
    yield from list_payload(message, "groups")


def _trackable_property_props(props: Mapping[str, Any]) -> dict[str, Any]:
    return {
        prop: value
        for prop, value in props.items()
        if isinstance(prop, str) and prop not in {MOTOR_TARGET_POSITION_PROP, MOTOR_TARGET_ANGLE_PROP}
    }
