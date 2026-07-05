from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

from ...core.updates import PropertyChange
from .enums import FullSyncSource, StateChangeReason
from .public import SessionStatusChanged


@dataclass(frozen=True)
class SyncStartedEvent:
    reason: StateChangeReason


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


@dataclass(frozen=True)
class ApplyOptimisticPropsCommand:
    props_by_node: Mapping[str | int, Mapping[str, Any]]


@dataclass(frozen=True)
class ApplyTopologyCommand:
    payload: Mapping[str, Any]
    reason: StateChangeReason
    message: Mapping[str, Any]


@dataclass(frozen=True)
class ApplyPropertiesCommand:
    payload: Mapping[str, Any]
    reason: StateChangeReason


@dataclass(frozen=True)
class ApplyGenericStateMessageCommand:
    payload: Mapping[str, Any]
    reason: StateChangeReason


@dataclass(frozen=True)
class ApplyGroupsCommand:
    payload: Mapping[str, Any]


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


@dataclass(frozen=True)
class ExpireOptimisticStateCommand:
    pass


DeviceStateActorMessage: TypeAlias = (
    ApplyTopologyCommand
    | ApplyPropertiesCommand
    | ApplyGenericStateMessageCommand
    | ApplyGroupsCommand
    | ApplyRoomsCommand
    | ApplyScenesCommand
    | ApplyOptimisticPropsCommand
    | SyncStartedEvent
    | SyncCompletedEvent
    | SessionStatusChanged
    | ExpireOptimisticStateCommand
)
