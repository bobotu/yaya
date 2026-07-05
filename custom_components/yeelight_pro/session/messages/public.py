from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

from ...core.events import GatewayEvent
from ...core.updates import PropertyChange
from ..model.status import GatewaySessionState
from .enums import StateChangeReason


@dataclass(frozen=True)
class SessionStatusChanged:
    previous: GatewaySessionState
    current: GatewaySessionState
    error: BaseException | None = None


@dataclass(frozen=True)
class StateSnapshotChanged:
    reason: StateChangeReason
    message: Mapping[str, Any]
    changes: tuple[PropertyChange, ...] = ()


@dataclass(frozen=True)
class GatewayEventReceived:
    event: GatewayEvent


SessionEvent: TypeAlias = SessionStatusChanged | StateSnapshotChanged | GatewayEventReceived
