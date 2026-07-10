from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, TypeAlias

from ..gateway.exceptions import ConnectionClosed, YeelightProError
from .actor import Actor, ActorRef, create_actor_task
from .rpc import GatewayRPC

JSONDict = dict[str, Any]
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
SessionMessageSink = Callable[[ConnectionSessionEvent], Awaitable[None]]
_LOGGER = logging.getLogger(__name__)


class ReconnectWorker:
    def __init__(self, rpc: GatewayRPC, connection_ref: ActorRef[ConnectionActorMessage]) -> None:
        self.rpc = rpc
        self.connection_ref = connection_ref
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True

    async def run(self) -> None:
        while not self.stopped:
            try:
                await self.rpc.wait_closed()
                if self.stopped:
                    return
                error = self.rpc.last_disconnect_error or ConnectionClosed("gateway connection closed")
                await self.connection_ref.tell(ReconnectFailedEvent(error=error))
                await asyncio.sleep(self.rpc.reconnect_delay)
                if not self.stopped:
                    await self.connection_ref.ask(ConnectConnectionCommand())
            except asyncio.CancelledError:
                raise
            except (OSError, TimeoutError, YeelightProError) as exc:
                _LOGGER.debug("Yeelight Pro gateway reconnect attempt failed: %s", exc)
                await self.connection_ref.tell(ReconnectFailedEvent(error=exc))
                await asyncio.sleep(self.rpc.reconnect_delay)


class ConnectionActor(Actor[ConnectionActorMessage]):
    """Mailbox actor that owns RPC lifecycle and reconnect supervision."""

    def __init__(self, rpc: GatewayRPC) -> None:
        super().__init__(f"yeelight-pro-connection-{rpc.host}:{rpc.port}")
        self.rpc = rpc
        self._session_sink: SessionMessageSink | None = None
        self._runner: asyncio.Task[None] | None = None
        self._worker: ReconnectWorker | None = None
        self._stopped = True
        self._epoch = 0
        self._remove_push_listener: Callable[[], None] | None = None

    def bind_push_listener(self, connection_ref: ActorRef[ConnectionActorMessage]) -> None:
        if self._remove_push_listener is not None:
            return

        async def handle_push(message: Mapping[str, Any]) -> None:
            await connection_ref.tell(RpcPushCommand(message=message))

        self._remove_push_listener = self.rpc.add_push_listener(handle_push)

    def set_session_sink(self, sink: SessionMessageSink) -> None:
        self._session_sink = sink

    @property
    def is_connected(self) -> bool:
        return self.rpc.is_connected

    @property
    def last_disconnect_error(self) -> BaseException | None:
        return self.rpc.last_disconnect_error

    @property
    def reconnect_delay(self) -> float:
        return self.rpc.reconnect_delay

    async def shutdown(self) -> None:
        if self._remove_push_listener is not None:
            self._remove_push_listener()
            self._remove_push_listener = None
        await super().close()

    async def wait_closed(self) -> None:
        await self.rpc.wait_closed()

    async def handle(self, message: ConnectionActorMessage) -> Any:
        if isinstance(message, ConnectConnectionCommand):
            await self._connect_now()
            return None
        if isinstance(message, StartConnectionCommand):
            self._stopped = False
            await self._connect_now()
            if self._runner is None or self._runner.done():
                self._worker = ReconnectWorker(self.rpc, message.connection_ref)
                self._runner = create_actor_task(
                    self._worker.run(),
                    name=f"yeelight-pro-connection-supervisor-{self.rpc.host}:{self.rpc.port}",
                )
            return None
        if isinstance(message, CloseConnectionCommand):
            await self._close_now()
            return None
        if isinstance(message, GatewayRpcRequest):
            return await self.rpc.request(
                message.method,
                message.payload,
                on_written=message.on_written,
                timeout=message.timeout,
            )
        if isinstance(message, RpcPushCommand):
            await self._send_session_message(RpcPushEvent(epoch=self._epoch, message=message.message))
            return None
        if isinstance(message, ReconnectFailedEvent):
            if not self._stopped:
                await self._send_session_message(ConnectionLostEvent(epoch=self._epoch, error=message.error))
            return None
        raise TypeError(f"unsupported connection message: {type(message).__name__}")

    async def _connect_now(self) -> None:
        _LOGGER.debug("Yeelight Pro connection actor connecting: current_epoch=%s", self._epoch)
        await self.rpc.connect()
        self._epoch += 1
        _LOGGER.debug("Yeelight Pro connection actor connected: epoch=%s", self._epoch)
        await self._send_session_message(ConnectionOnlineEvent(epoch=self._epoch))

    async def _close_now(self) -> None:
        self._stopped = True
        if self._worker is not None:
            self._worker.stop()
            self._worker = None
        if self._runner is not None and self._runner is not asyncio.current_task():
            self._runner.cancel()
            with suppress(asyncio.CancelledError):
                await self._runner
            self._runner = None
        await self.rpc.close()
        _LOGGER.debug(
            "Yeelight Pro connection actor closed: epoch=%s error=%s",
            self._epoch,
            repr(self.rpc.last_disconnect_error),
        )
        await self._send_session_message(ConnectionLostEvent(epoch=self._epoch, error=self.rpc.last_disconnect_error))

    async def _send_session_message(self, message: ConnectionSessionEvent) -> None:
        if self._session_sink is not None:
            _LOGGER.debug("Yeelight Pro connection session event: %s", _session_event_summary(message))
            await self._session_sink(message)


def _session_event_summary(message: ConnectionSessionEvent) -> dict[str, Any]:
    summary: dict[str, Any] = {"type": type(message).__name__}
    epoch = getattr(message, "epoch", None)
    if epoch is not None:
        summary["epoch"] = epoch
    error = getattr(message, "error", None)
    if error is not None:
        summary["error"] = repr(error)
    push = getattr(message, "message", None)
    if isinstance(push, Mapping):
        summary["method"] = push.get("method")
        summary["id"] = push.get("id")
        nodes = push.get("nodes")
        if isinstance(nodes, list):
            summary["node_count"] = len(nodes)
    return summary
