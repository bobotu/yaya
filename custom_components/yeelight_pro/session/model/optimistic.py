from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from ...core.topology import TopologyNode

OPTIMISTIC_STATE_TTL = 5.0


@dataclass(frozen=True)
class PendingOverlay:
    node_id: str | int
    prop: str
    value: Any
    created_at: float
    expires_at: float
    generation: int


class OptimisticStateOverlay:
    def __init__(self, *, ttl: float = OPTIMISTIC_STATE_TTL) -> None:
        self.ttl = ttl
        self._generation = 0
        self._pending: dict[str, dict[str, PendingOverlay]] = {}

    def set_props(self, node_id: str | int, props: Mapping[str, Any], *, now: float) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        added = False
        pending_by_prop = self._pending.setdefault(normalized, {})
        for prop, value in props.items():
            if not isinstance(prop, str):
                continue
            self._generation += 1
            pending_by_prop[prop] = PendingOverlay(
                node_id=node_id,
                prop=prop,
                value=value,
                created_at=now,
                expires_at=now + self.ttl,
                generation=self._generation,
            )
            added = True
        return {node_id} if added else set()

    def reconcile_node_props(
        self,
        node_id: str | int,
        props: Mapping[str, Any],
    ) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        pending_by_prop = self._pending.get(normalized)
        if pending_by_prop is None:
            return set()
        changed = False
        for prop in props:
            if not isinstance(prop, str):
                continue
            changed = pending_by_prop.pop(prop, None) is not None or changed
        if not pending_by_prop:
            self._pending.pop(normalized, None)
        return {node_id} if changed else set()

    def clear_props(self, node_id: str | int, props: Iterable[str]) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        pending_by_prop = self._pending.get(normalized)
        if pending_by_prop is None:
            return set()
        changed = False
        for prop in props:
            changed = pending_by_prop.pop(prop, None) is not None or changed
        if not pending_by_prop:
            self._pending.pop(normalized, None)
        return {node_id} if changed else set()

    def expire(self, *, now: float) -> set[str | int]:
        affected: set[str | int] = set()
        for node_key, pending_by_prop in list(self._pending.items()):
            for prop, pending in list(pending_by_prop.items()):
                if pending.expires_at > now:
                    continue
                pending_by_prop.pop(prop, None)
                affected.add(pending.node_id)
            if not pending_by_prop:
                self._pending.pop(node_key, None)
        return affected

    def next_expiration(self, *, now: float) -> float | None:
        expirations = [pending.expires_at for pending in self._iter_pending() if pending.expires_at > now]
        return min(expirations) if expirations else None

    def clear_all(self) -> set[str | int]:
        affected = {pending.node_id for pending in self._iter_pending()}
        self._pending.clear()
        return affected

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

    def visible_node(self, node: TopologyNode, *, now: float | None = None) -> TopologyNode:
        normalized = _node_key(node.id)
        if normalized is None:
            return node
        pending_by_prop = self._pending.get(normalized)
        if pending_by_prop is None:
            return node
        params = dict(node.params)
        for prop, pending in pending_by_prop.items():
            if now is not None and pending.expires_at <= now:
                continue
            params[prop] = pending.value
        if params == node.params:
            return node
        return replace(node, params=params)

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

    def diagnostics(self, *, now: float) -> dict[str, Any]:
        entries = [
            {
                "node_id": str(pending.node_id),
                "property": pending.prop,
                "age": round(max(0.0, now - pending.created_at), 3),
                "expires_in": round(max(0.0, pending.expires_at - now), 3),
                "generation": pending.generation,
            }
            for pending in sorted(self._iter_pending(), key=lambda item: item.generation)
        ]
        return {
            "count": len(entries),
            "entries": entries,
        }

    def _iter_pending(self) -> Iterable[PendingOverlay]:
        for pending_by_prop in self._pending.values():
            yield from pending_by_prop.values()


def _node_key(node_id: object) -> str | None:
    if isinstance(node_id, bool) or not isinstance(node_id, (str, int)):
        return None
    return str(node_id)
