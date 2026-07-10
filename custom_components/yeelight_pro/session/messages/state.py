from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from ...core.updates import PropertyChange
from ..model.motor import MotorTargetIntent
from ..model.pending import PendingRefresh
from .enums import FullSyncSource, StateChangeReason
from .public import SessionStatusChanged


@dataclass(frozen=True)
class SyncCompletedEvent:
    source: FullSyncSource | None


@dataclass(frozen=True)
class AuthoritativeStateChangedEvent:
    reason: StateChangeReason
    message: Mapping[str, Any]
    changes: tuple[PropertyChange, ...] = ()


@dataclass(frozen=True)
class RefreshNodeRequestedEvent:
    node_id: str | int
    node_type: int | None = None
    write_ids: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class PreparePendingWritesCommand:
    write_id: int
    props_by_node: Mapping[str | int, Mapping[str, Any]]
    transition_delays: Mapping[str | int, Mapping[str, float]] = field(default_factory=dict)


@dataclass(frozen=True)
class AcceptPendingWritesCommand:
    write_ids: tuple[int, ...]
    motor_targets: tuple[MotorTargetIntent, ...] = ()
    motor_stops: tuple[str | int, ...] = ()


@dataclass(frozen=True)
class FailPendingWritesCommand:
    write_ids: tuple[int, ...]


@dataclass(frozen=True)
class PendingWritesTickCommand:
    pass


@dataclass(frozen=True)
class ResolvePendingRefreshCommand:
    refresh: PendingRefresh
    response: Mapping[str, Any] | None
    failed: bool


@dataclass(frozen=True)
class CaptureWriteWatermarkCommand:
    pass


@dataclass(frozen=True)
class ApplyTopologyCommand:
    payload: Mapping[str, Any]
    reason: StateChangeReason
    message: Mapping[str, Any]
    replace: bool = True
    captured_write_ids: Mapping[str | int, Mapping[str, int]] | None = None


@dataclass(frozen=True)
class ApplyPropertiesCommand:
    payload: Mapping[str, Any]
    reason: StateChangeReason
    captured_write_ids: Mapping[str | int, Mapping[str, int]] | None = None


@dataclass(frozen=True)
class ApplyGenericStateMessageCommand:
    payload: Mapping[str, Any]
    reason: StateChangeReason


@dataclass(frozen=True)
class ApplyGroupsCommand:
    payload: Mapping[str, Any]
    reason: StateChangeReason | None = None
    captured_write_ids: Mapping[str | int, Mapping[str, int]] | None = None


@dataclass(frozen=True)
class ApplyRoomsCommand:
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class ApplyScenesCommand:
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class AppliedPropertiesResult:
    changes: tuple[PropertyChange, ...]
    full_property_coverage: bool


DeviceStateActorMessage: TypeAlias = (
    ApplyTopologyCommand
    | ApplyPropertiesCommand
    | ApplyGenericStateMessageCommand
    | ApplyGroupsCommand
    | ApplyRoomsCommand
    | ApplyScenesCommand
    | PreparePendingWritesCommand
    | AcceptPendingWritesCommand
    | FailPendingWritesCommand
    | PendingWritesTickCommand
    | ResolvePendingRefreshCommand
    | CaptureWriteWatermarkCommand
    | SyncCompletedEvent
    | SessionStatusChanged
)
