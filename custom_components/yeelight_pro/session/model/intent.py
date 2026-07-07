from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from ...core.topology import DeviceType, TopologyNode
from .motor import (
    MOTOR_TARGET_ANGLE_PROP,
    MOTOR_TARGET_POSITION_PROP,
    MOTOR_TRACKING_TTL,
    MotorAxisTrack,
    MotorStateTracker,
    MotorTargetIntent,
)

COMMAND_INTENT_TTL = 5.0
LIGHT_INTENT_PROPS = frozenset({"p", "l", "ct", "c"})
LIGHT_DEVICE_TYPES = frozenset(
    {
        int(DeviceType.LIGHT_SWITCHABLE),
        int(DeviceType.LIGHT_BRIGHTNESS),
        int(DeviceType.LIGHT_TEMPERATURE),
        int(DeviceType.LIGHT_COLOR),
        int(DeviceType.LAMP_DFT),
    }
)


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
        ttl: float | None = None,
    ) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        current_params = current_params or {}
        pending_by_prop = self._pending.setdefault(normalized, {})
        changed = False
        expires_at = now + (self.ttl if ttl is None else max(0.0, ttl))
        for prop, value in props.items():
            if not isinstance(prop, str):
                continue
            if current_params.get(prop) == value:
                changed = pending_by_prop.pop(prop, None) is not None or changed
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
        for prop, _value in params.items():
            if not isinstance(prop, str):
                continue
            pending = pending_by_prop.get(prop)
            if pending is None:
                continue
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


class LightIntentTracker(PropertyIntentTracker):
    """Projects HA-friendly light targets while gateway transition updates are in flight."""

    def apply_authoritative_node(
        self,
        node_id: str | int,
        params: Mapping[str, Any],
        *,
        now: float,
    ) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        pending_by_prop = self._pending.get(normalized)
        if pending_by_prop is None:
            return set()

        affected: set[str | int] = set()
        saw_transition_progress = any(prop in LIGHT_INTENT_PROPS for prop in params)
        for prop, value in params.items():
            if not isinstance(prop, str):
                continue
            pending = pending_by_prop.get(prop)
            if pending is None:
                continue
            if value == pending.value:
                pending_by_prop.pop(prop, None)
            else:
                pending_by_prop[prop] = replace(pending, updated_at=now, expires_at=now + self.ttl)
            affected.add(pending.node_id)

        if pending_by_prop and saw_transition_progress:
            for prop, pending in list(pending_by_prop.items()):
                if prop not in LIGHT_INTENT_PROPS:
                    continue
                pending_by_prop[prop] = replace(pending, updated_at=now, expires_at=now + self.ttl)
                affected.add(pending.node_id)

        if not pending_by_prop:
            self._pending.pop(normalized, None)
        return affected


class GenericIntentTracker(PropertyIntentTracker):
    """Exact-match property intent tracker for ordinary device properties."""


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
        self.light = LightIntentTracker(ttl=ttl)
        self.generic = GenericIntentTracker(ttl=ttl)
        self.motor = MotorIntentTracker(ttl=motor_tracking_ttl)

    def record_property_intents(
        self,
        props_by_node: Mapping[str | int, Mapping[str, Any]],
        *,
        nodes: Mapping[str | int, TopologyNode],
        now: float,
        ttl_by_node: Mapping[str | int, float] | None = None,
    ) -> set[str | int]:
        affected: set[str | int] = set()
        for node_id, props in props_by_node.items():
            light_props, generic_props = self._partition_props(node_id, props, nodes)
            node = nodes.get(node_id)
            current_params = node.params if node is not None else {}
            ttl = ttl_by_node.get(node_id) if ttl_by_node is not None else None
            affected.update(self.light.record(node_id, light_props, now=now, current_params=current_params, ttl=ttl))
            affected.update(
                self.generic.record(node_id, generic_props, now=now, current_params=current_params, ttl=ttl)
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
        raw_nodes = message.get("nodes")
        if isinstance(raw_nodes, list):
            for item in raw_nodes:
                if not isinstance(item, Mapping):
                    continue
                node_id = _item_id(item)
                params = item.get("params")
                if node_id is None or not isinstance(params, Mapping):
                    continue
                affected.update(self.light.apply_authoritative_node(node_id, params, now=now))
                affected.update(self.generic.apply_authoritative_node(node_id, params, now=now))
        affected.update(self.motor.apply_authoritative_message(message, nodes, now=now))
        return affected

    def expire_pending(self, *, now: float) -> tuple[ExpiredIntent, ...]:
        expired: dict[str, tuple[str | int, set[str]]] = {}
        for tracker in (self.light, self.generic):
            for pending in tracker.expire_pending(now=now):
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
        visible = self.generic.project_visible(node)
        visible = self.light.project_visible(visible)
        return self.motor.project_visible(visible)

    def clear_node(self, node_id: str | int) -> set[str | int]:
        return self.light.clear_node(node_id) | self.generic.clear_node(node_id) | self.motor.clear_node(node_id)

    def clear_missing_nodes(self, node_ids: Iterable[str | int]) -> set[str | int]:
        return (
            self.light.clear_missing_nodes(node_ids)
            | self.generic.clear_missing_nodes(node_ids)
            | self.motor.clear_missing_nodes(node_ids)
        )

    def clear_all(self) -> set[str | int]:
        return self.light.clear_all() | self.generic.clear_all() | self.motor.clear_all()

    def next_expiration(self, *, now: float) -> float | None:
        expirations = [
            value
            for value in (
                self.light.next_expiration(now=now),
                self.generic.next_expiration(now=now),
                self.motor.next_expiration(now=now),
            )
            if value is not None
        ]
        return min(expirations) if expirations else None

    def has_pending(self, node_id: str | int, props: Iterable[str] | None = None) -> bool:
        return (
            self.light.has_pending(node_id, props)
            or self.generic.has_pending(node_id, props)
            or (props is None and self.motor.has_tracking(node_id))
        )

    def diagnostics(self, *, now: float) -> dict[str, Any]:
        light_entries = self.light.diagnostics(now=now)
        generic_entries = self.generic.diagnostics(now=now)
        return {
            "count": len(light_entries) + len(generic_entries),
            "light": light_entries,
            "generic": generic_entries,
            "motor": self.motor.diagnostics(now=now),
        }

    def _partition_props(
        self,
        node_id: str | int,
        props: Mapping[str, Any],
        nodes: Mapping[str | int, TopologyNode],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        node = nodes.get(node_id)
        is_light = node is not None and node.type in LIGHT_DEVICE_TYPES
        light_props: dict[str, Any] = {}
        generic_props: dict[str, Any] = {}
        for prop, value in props.items():
            if not isinstance(prop, str):
                continue
            if is_light and prop in LIGHT_INTENT_PROPS:
                light_props[prop] = value
            elif prop not in {MOTOR_TARGET_POSITION_PROP, MOTOR_TARGET_ANGLE_PROP}:
                generic_props[prop] = value
        return light_props, generic_props


def _item_id(item: Mapping[str, Any]) -> str | int | None:
    item_id = item.get("id")
    return item_id if isinstance(item_id, (str, int)) and not isinstance(item_id, bool) else None


def _node_key(node_id: object) -> str | None:
    if isinstance(node_id, bool) or not isinstance(node_id, (str, int)):
        return None
    return str(node_id)


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None
