from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TypeAlias

from ..core.events import GatewayEvent
from ..core.updates import PropertyChange
from .status import GatewaySessionState


class StateChangeReason(StrEnum):
    MANUAL_SYNC = "manual sync"
    SYNC_COMPLETE = "sync complete"
    TOPOLOGY_SYNC = "topology sync"
    TOPOLOGY_PUSH = "topology push"
    PROPERTY_PUSH = "property push"
    GENERIC_PUSH = "generic push"
    STATE_READBACK = "state readback"
    POLL_FULL_PROPERTIES = "poll full properties"
    WRITE_SUPERSEDED = "write superseded"
    WRITE_ACCEPTED = "write accepted"
    WRITE_FAILED = "write failed"
    WRITE_EXPIRED = "write expired"
    MOTOR_TRACKING_EXPIRED = "motor tracking expired"
    SESSION_RESET = "session reset"


class FullSyncSource(StrEnum):
    POLL = "poll"
    PUSH = "push"


class SyntheticSessionMethod(StrEnum):
    SYNC_TOPOLOGY = "gateway_sync.topology"
    SYNC_COMPLETE = "gateway_sync.complete"
    STATE_READBACK = "gateway_state.readback"
    WRITE_SUPERSEDED = "gateway_write.superseded"
    WRITE_ACCEPTED = "gateway_write.accepted"
    WRITE_FAILED = "gateway_write.failed"
    WRITE_EXPIRED = "gateway_write.expired"
    MOTOR_TRACKING_EXPIRED = "gateway_motor.expired"
    SESSION_RESET = "gateway_session.reset"


@dataclass(frozen=True)
class SessionStatusChanged:
    previous: GatewaySessionState
    current: GatewaySessionState
    error: BaseException | None = None


@dataclass(frozen=True)
class VisibleStateChanged:
    reason: StateChangeReason
    message: Mapping[str, Any]
    changes: tuple[PropertyChange, ...] = ()


@dataclass(frozen=True)
class GatewayEventReceived:
    event: GatewayEvent


SessionEvent: TypeAlias = SessionStatusChanged | VisibleStateChanged | GatewayEventReceived
