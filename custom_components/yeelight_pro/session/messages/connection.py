from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from ..actors.base import ActorRef

WriteCallback = Callable[[], None]


@dataclass(frozen=True)
class ConnectConnectionCommand:
    pass


@dataclass(frozen=True)
class StartConnectionCommand:
    connection_ref: ActorRef[ConnectionActorMessage]


@dataclass(frozen=True)
class CloseConnectionCommand:
    pass


@dataclass(frozen=True)
class GatewayRpcRequest:
    method: str
    payload: Mapping[str, Any] | None = None
    on_written: WriteCallback | None = None
    timeout: float | None = None


@dataclass(frozen=True)
class RpcPushCommand:
    message: Mapping[str, Any]


@dataclass(frozen=True)
class ReconnectFailedEvent:
    error: BaseException


@dataclass(frozen=True)
class ConnectionOnlineEvent:
    epoch: int


@dataclass(frozen=True)
class ConnectionLostEvent:
    epoch: int
    error: BaseException | None = None


@dataclass(frozen=True)
class RpcPushEvent:
    epoch: int
    message: Mapping[str, Any]


ConnectionActorMessage: TypeAlias = (
    ConnectConnectionCommand
    | StartConnectionCommand
    | CloseConnectionCommand
    | GatewayRpcRequest
    | RpcPushCommand
    | ReconnectFailedEvent
)
ConnectionSessionEvent: TypeAlias = ConnectionLostEvent | ConnectionOnlineEvent | RpcPushEvent
