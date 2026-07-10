from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from ..core.coercion import int_or_none as _int_or_none
from ..core.coercion import node_id_or_none
from ..core.coercion import node_key as _node_key
from ..core.topology import TopologyNode
from ..core.updates import PropertyChange

MOTOR_TRACKING_TTL = 120.0

MOTOR_CURRENT_POSITION_PROP = "cp"
MOTOR_TARGET_POSITION_PROP = "tp"
MOTOR_CURRENT_ANGLE_PROP = "cra"
MOTOR_TARGET_ANGLE_PROP = "tra"

MOTOR_TRACKING_TARGET_POSITION = "__yeelight_pro_motor_target_position"
MOTOR_TRACKING_POSITION_MOTION = "__yeelight_pro_motor_position_motion"
MOTOR_TRACKING_TARGET_ANGLE = "__yeelight_pro_motor_target_angle"
MOTOR_TRACKING_ANGLE_MOTION = "__yeelight_pro_motor_angle_motion"
MOTOR_TRACKING_ASSUMED = "__yeelight_pro_motor_assumed"

MOTOR_MOTION_OPENING = "opening"
MOTOR_MOTION_CLOSING = "closing"


@dataclass(frozen=True)
class MotorTarget:
    node_id: str | int
    current_prop: str
    target_prop: str
    target_value: int


@dataclass(frozen=True)
class MotorAxisTrack:
    node_id: str | int
    current_prop: str
    target_prop: str
    target_value: int
    assumed: bool
    created_at: float
    updated_at: float
    expires_at: float


class MotorStateTracker:
    """Tracks slow motor movement without projecting final position as current."""

    def __init__(self, *, ttl: float = MOTOR_TRACKING_TTL) -> None:
        self.ttl = ttl
        self._tracking: dict[str, dict[str, MotorAxisTrack]] = {}

    def set_target(
        self,
        target: MotorTarget,
        *,
        current_value: int | None,
        now: float,
        assumed: bool = True,
    ) -> set[str | int]:
        normalized = _node_key(target.node_id)
        if normalized is None:
            return set()
        if current_value == target.target_value:
            return self.clear_axis(target.node_id, target.target_prop)
        tracks = self._tracking.setdefault(normalized, {})
        tracks[target.target_prop] = MotorAxisTrack(
            node_id=target.node_id,
            current_prop=target.current_prop,
            target_prop=target.target_prop,
            target_value=target.target_value,
            assumed=assumed,
            created_at=now,
            updated_at=now,
            expires_at=now + self.ttl,
        )
        return {target.node_id}

    def apply_authoritative_changes(
        self,
        changes: Iterable[PropertyChange],
        nodes: Mapping[str | int, TopologyNode],
        *,
        now: float,
    ) -> set[str | int]:
        affected: set[str | int] = set()
        for change in changes:
            affected.update(self.apply_authoritative_node(change.after.id, change.update.get("params"), nodes, now=now))
        return affected

    def apply_authoritative_message(
        self,
        message: Mapping[str, Any],
        nodes: Mapping[str | int, TopologyNode],
        *,
        now: float,
    ) -> set[str | int]:
        affected: set[str | int] = set()
        raw_nodes = message.get("nodes")
        if not isinstance(raw_nodes, list):
            return affected
        for item in raw_nodes:
            if not isinstance(item, Mapping):
                continue
            node_id = _item_id(item)
            if node_id is None:
                continue
            affected.update(self.apply_authoritative_node(node_id, item.get("params"), nodes, now=now))
        return affected

    def apply_authoritative_node(
        self,
        node_id: str | int,
        params: object,
        nodes: Mapping[str | int, TopologyNode],
        *,
        now: float,
    ) -> set[str | int]:
        if not isinstance(params, Mapping):
            return set()
        affected: set[str | int] = set()
        node = nodes.get(node_id)
        current_params = node.params if node is not None else {}
        for axis in _AXES:
            target_value = _int_or_none(params.get(axis.target_prop))
            if target_value is not None:
                current_value = _int_or_none(current_params.get(axis.current_prop))
                affected.update(
                    self.set_target(
                        MotorTarget(
                            node_id=node_id,
                            current_prop=axis.current_prop,
                            target_prop=axis.target_prop,
                            target_value=target_value,
                        ),
                        current_value=current_value,
                        now=now,
                        assumed=False,
                    )
                )
            if axis.current_prop in params:
                affected.update(self._refresh_axis_from_current(node_id, axis.target_prop, current_params, now=now))
        return affected

    def clear_axis(self, node_id: str | int, target_prop: str) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        tracks = self._tracking.get(normalized)
        if tracks is None:
            return set()
        changed = tracks.pop(target_prop, None) is not None
        if not tracks:
            self._tracking.pop(normalized, None)
        return {node_id} if changed else set()

    def clear_node(self, node_id: str | int) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        tracks = self._tracking.pop(normalized, None)
        if tracks is None:
            return set()
        return {track.node_id for track in tracks.values()}

    def clear_missing_nodes(self, node_ids: Iterable[str | int]) -> set[str | int]:
        known = {_node_key(node_id) for node_id in node_ids}
        known.discard(None)
        affected: set[str | int] = set()
        for node_key, tracks in list(self._tracking.items()):
            if node_key in known:
                continue
            self._tracking.pop(node_key, None)
            affected.update(track.node_id for track in tracks.values())
        return affected

    def clear_all(self) -> set[str | int]:
        affected = {track.node_id for tracks in self._tracking.values() for track in tracks.values()}
        self._tracking.clear()
        return affected

    def expire_pending(self, *, now: float) -> tuple[MotorAxisTrack, ...]:
        expired: list[MotorAxisTrack] = []
        for node_key, tracks in list(self._tracking.items()):
            for target_prop, track in list(tracks.items()):
                if track.expires_at > now:
                    continue
                tracks.pop(target_prop, None)
                expired.append(track)
            if not tracks:
                self._tracking.pop(node_key, None)
        return tuple(expired)

    def next_expiration(self, *, now: float) -> float | None:
        expirations = [
            track.expires_at
            for tracks in self._tracking.values()
            for track in tracks.values()
            if track.expires_at > now
        ]
        return min(expirations) if expirations else None

    def visible_node(self, node: TopologyNode) -> TopologyNode:
        normalized = _node_key(node.id)
        if normalized is None:
            return node
        tracks = self._tracking.get(normalized)
        if not tracks:
            return node
        params = dict(node.params)
        assumed = False
        for track in tracks.values():
            current_value = _int_or_none(node.params.get(track.current_prop))
            if current_value == track.target_value:
                continue
            target_key, motion_key = _visible_keys(track.target_prop)
            params[target_key] = track.target_value
            motion = _motion(current_value, track.target_value)
            if motion is not None:
                params[motion_key] = motion
            assumed = assumed or track.assumed
        if assumed:
            params[MOTOR_TRACKING_ASSUMED] = True
        if params == node.params:
            return node
        return replace(node, params=params)

    def has_tracking(self, node_id: str | int) -> bool:
        normalized = _node_key(node_id)
        return normalized is not None and normalized in self._tracking

    def diagnostics(self, *, now: float) -> dict[str, Any]:
        entries = []
        for tracks in self._tracking.values():
            for track in tracks.values():
                entries.append(
                    {
                        "node_id": str(track.node_id),
                        "current_property": track.current_prop,
                        "target_property": track.target_prop,
                        "target": track.target_value,
                        "assumed": track.assumed,
                        "age": round(max(0.0, now - track.created_at), 3),
                        "expires_in": round(max(0.0, track.expires_at - now), 3),
                    }
                )
        return {"count": len(entries), "entries": entries}

    def signature(self) -> tuple[tuple[str, tuple[tuple[str, str, int, bool], ...]], ...]:
        return tuple(
            sorted(
                (
                    node_key,
                    tuple(
                        sorted(
                            (
                                track.current_prop,
                                track.target_prop,
                                track.target_value,
                                track.assumed,
                            )
                            for track in tracks.values()
                        )
                    ),
                )
                for node_key, tracks in self._tracking.items()
            )
        )

    def _refresh_axis_from_current(
        self,
        node_id: str | int,
        target_prop: str,
        current_params: Mapping[str, Any],
        *,
        now: float,
    ) -> set[str | int]:
        normalized = _node_key(node_id)
        if normalized is None:
            return set()
        tracks = self._tracking.get(normalized)
        if tracks is None:
            return set()
        track = tracks.get(target_prop)
        if track is None:
            return set()
        current_value = _int_or_none(current_params.get(track.current_prop))
        if current_value == track.target_value:
            return self.clear_axis(node_id, target_prop)
        tracks[target_prop] = replace(track, updated_at=now, expires_at=now + self.ttl)
        return {track.node_id}


@dataclass(frozen=True)
class _Axis:
    current_prop: str
    target_prop: str


_AXES = (
    _Axis(MOTOR_CURRENT_POSITION_PROP, MOTOR_TARGET_POSITION_PROP),
    _Axis(MOTOR_CURRENT_ANGLE_PROP, MOTOR_TARGET_ANGLE_PROP),
)


def _visible_keys(target_prop: str) -> tuple[str, str]:
    if target_prop == MOTOR_TARGET_ANGLE_PROP:
        return MOTOR_TRACKING_TARGET_ANGLE, MOTOR_TRACKING_ANGLE_MOTION
    return MOTOR_TRACKING_TARGET_POSITION, MOTOR_TRACKING_POSITION_MOTION


def _motion(current_value: int | None, target_value: int) -> str | None:
    if current_value is None or current_value == target_value:
        return None
    return MOTOR_MOTION_OPENING if target_value > current_value else MOTOR_MOTION_CLOSING


def _item_id(item: Mapping[str, Any]) -> str | int | None:
    return node_id_or_none(item.get("id"))
