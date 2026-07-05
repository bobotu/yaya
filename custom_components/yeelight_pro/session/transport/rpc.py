from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from ...core.const import DEFAULT_VERSION, GATEWAY_CONTROL_PORT
from ...core.exceptions import (
    ConnectionClosed,
    GatewayErrorResponse,
    ProtocolFrameTooLarge,
    RequestTimeout,
    YeelightProError,
)
from ...core.protocol import build_request, parse_line
from ..actors.base import create_actor_task

JSONDict = dict[str, Any]
PushListener = Callable[[Mapping[str, Any]], Awaitable[None] | None]
WriteCallback = Callable[[], None]
MAX_RPC_FRAME_BYTES = 16 * 1024 * 1024
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _QueuedRequest:
    request_id: int
    method: str
    payload: Mapping[str, Any] | None
    future: asyncio.Future[JSONDict]
    on_written: WriteCallback | None = None


class GatewayRPC:
    """Low-level Yeelight Pro line-delimited JSON RPC client."""

    def __init__(
        self,
        host: str,
        *,
        port: int = GATEWAY_CONTROL_PORT,
        version: str = DEFAULT_VERSION,
        request_timeout: float = 5.0,
        reconnect_delay: float = 2.0,
    ) -> None:
        self.host = host
        self.port = port
        self.version = version
        self.request_timeout = request_timeout
        self.reconnect_delay = reconnect_delay
        self.close_timeout = 1.0
        self.max_frame_bytes = MAX_RPC_FRAME_BYTES

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._dispatch_task: asyncio.Task[None] | None = None
        self._write_queue: asyncio.Queue[_QueuedRequest] | None = None
        self._push_queue: asyncio.Queue[Mapping[str, Any]] | None = None
        self._pending: dict[int, asyncio.Future[JSONDict]] = {}
        self._listeners: list[PushListener] = []
        self._next_id = 0
        self._closing = False
        self._disconnected = asyncio.Event()
        self._disconnected.set()
        self.last_disconnect_error: BaseException | None = None

    async def __aenter__(self) -> GatewayRPC:
        await self.connect()
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.close()

    @property
    def is_connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    def add_push_listener(self, listener: PushListener) -> Callable[[], None]:
        self._listeners.append(listener)

        def remove() -> None:
            with suppress(ValueError):
                self._listeners.remove(listener)

        return remove

    async def connect(self) -> None:
        if self.is_connected:
            return
        await self._shutdown_connection(ConnectionClosed("reconnecting"), record_error=False)
        self._closing = False
        self.last_disconnect_error = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.host,
                    self.port,
                    limit=self.max_frame_bytes,
                ),
                timeout=self.request_timeout,
            )
        except TimeoutError as exc:
            self._mark_disconnected(RequestTimeout(f"timed out connecting to {self.host}:{self.port}"))
            raise RequestTimeout(f"timed out connecting to {self.host}:{self.port}") from exc
        self._reader = reader
        self._writer = writer
        self._write_queue = asyncio.Queue()
        self._push_queue = asyncio.Queue()
        self._disconnected.clear()
        self._reader_task = create_actor_task(
            self._read_loop(reader, writer, self._push_queue),
            name=f"yeelight-pro-rpc-{self.host}:{self.port}",
        )
        self._writer_task = create_actor_task(
            self._write_loop(self._write_queue, writer),
            name=f"yeelight-pro-rpc-writer-{self.host}:{self.port}",
        )
        self._dispatch_task = create_actor_task(
            self._dispatch_loop(self._push_queue),
            name=f"yeelight-pro-rpc-dispatch-{self.host}:{self.port}",
        )

    async def close(self) -> None:
        self._closing = True
        await self._shutdown_connection(ConnectionClosed("client closed"))

    async def wait_closed(self) -> None:
        await self._disconnected.wait()

    async def request(
        self,
        method: str,
        payload: Mapping[str, Any] | None = None,
        *,
        on_written: WriteCallback | None = None,
        timeout: float | None = None,
    ) -> JSONDict:
        if self._writer is None or self._writer.is_closing() or self._writer_task is None:
            raise ConnectionClosed("gateway is not connected")
        write_queue = self._write_queue
        if write_queue is None:
            raise ConnectionClosed("gateway is not connected")

        request_id = self._allocate_request_id()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[JSONDict] = loop.create_future()
        self._pending[request_id] = future
        write_queue.put_nowait(_QueuedRequest(request_id, method, payload, future, on_written))

        try:
            return await asyncio.wait_for(future, timeout or self.request_timeout)
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            timeout_error = RequestTimeout(f"timed out waiting for {method}")
            await self._shutdown_connection(timeout_error)
            raise timeout_error from exc
        except (ConnectionError, OSError) as exc:
            self._pending.pop(request_id, None)
            connection_error = ConnectionClosed(str(exc))
            await self._shutdown_connection(connection_error)
            raise connection_error from exc

    async def _write_loop(
        self,
        write_queue: asyncio.Queue[_QueuedRequest],
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                queued = await write_queue.get()
                if queued.future.done():
                    self._pending.pop(queued.request_id, None)
                    continue
                if writer.is_closing():
                    error = ConnectionClosed("gateway is not connected")
                    self._pending.pop(queued.request_id, None)
                    if not queued.future.done():
                        queued.future.set_exception(error)
                    continue

                wire_payload = build_request(
                    queued.method,
                    request_id=queued.request_id,
                    payload=queued.payload,
                    version=self.version,
                )
                try:
                    writer.write(wire_payload)
                    await writer.drain()
                    if queued.on_written is not None:
                        try:
                            queued.on_written()
                        except Exception:  # noqa: BLE001 - callbacks must not kill the writer task.
                            _LOGGER.exception("Gateway RPC write callback failed for %s", queued.method)
                except (ConnectionError, OSError) as exc:
                    error = ConnectionClosed(str(exc))
                    self._pending.pop(queued.request_id, None)
                    if not queued.future.done():
                        queued.future.set_exception(error)
                    await self._shutdown_connection(error)
                    return
        except asyncio.CancelledError:
            raise

    async def _read_loop(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        push_queue: asyncio.Queue[Mapping[str, Any]],
    ) -> None:
        try:
            while True:
                try:
                    line = await reader.readuntil(b"\r\n")
                except asyncio.IncompleteReadError as exc:
                    raise ConnectionClosed("gateway closed the connection") from exc
                except asyncio.LimitOverrunError as exc:
                    raise ProtocolFrameTooLarge(f"gateway message exceeded {self.max_frame_bytes} bytes") from exc
                if line == b"":
                    raise ConnectionClosed("gateway closed the connection")

                message = parse_line(line)
                request_id = message.get("id")
                if isinstance(request_id, int) and request_id in self._pending:
                    future = self._pending.pop(request_id)
                    if not future.done():
                        if message.get("result") == "error":
                            future.set_exception(GatewayErrorResponse(str(message.get("data", message))))
                        else:
                            future.set_result(message)
                    continue

                push_queue.put_nowait(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - wake all pending callers.
            await self._shutdown_connection(exc)

    async def _dispatch_loop(self, push_queue: asyncio.Queue[Mapping[str, Any]]) -> None:
        try:
            while True:
                message = await push_queue.get()
                await self._dispatch_push(message)
        except asyncio.CancelledError:
            raise

    async def _dispatch_push(self, message: Mapping[str, Any]) -> None:
        for listener in list(self._listeners):
            try:
                result = listener(message)
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001 - listeners must not kill push dispatch.
                _LOGGER.exception("Gateway RPC push listener failed")

    def _mark_disconnected(self, exc: BaseException, *, record_error: bool = True) -> None:
        self._reader = None
        self._writer = None
        if record_error and (not self._disconnected.is_set() or self.last_disconnect_error is None):
            self.last_disconnect_error = exc
        self._disconnected.set()
        for request_id, future in list(self._pending.items()):
            self._pending.pop(request_id, None)
            if not future.done():
                if isinstance(exc, YeelightProError):
                    future.set_exception(exc)
                else:
                    future.set_exception(ConnectionClosed(str(exc)))

    async def _shutdown_connection(self, exc: BaseException, *, record_error: bool = True) -> None:
        writer = self._writer
        tasks = (self._reader_task, self._writer_task, self._dispatch_task)
        self._mark_disconnected(exc, record_error=record_error)

        if writer is not None and not writer.is_closing():
            await self._close_writer(writer)

        for task in tasks:
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        self._reader_task = None
        self._writer_task = None
        self._dispatch_task = None
        self._write_queue = None
        self._push_queue = None

    async def _close_writer(self, writer: asyncio.StreamWriter) -> None:
        if not writer.is_closing():
            writer.close()
        with suppress(ConnectionError, OSError, TimeoutError):
            await asyncio.wait_for(writer.wait_closed(), timeout=self.close_timeout)

    def _allocate_request_id(self) -> int:
        self._next_id += 1
        return self._next_id
