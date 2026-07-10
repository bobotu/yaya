from __future__ import annotations

import asyncio
import contextvars
import logging
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

_LOGGER = logging.getLogger(__name__)
_current_actor: contextvars.ContextVar[Actor | None] = contextvars.ContextVar(
    "yeelight_pro_current_actor",
    default=None,
)
MessageT = TypeVar("MessageT")


@dataclass(slots=True)
class _Envelope(Generic[MessageT]):
    message: MessageT
    future: asyncio.Future[Any] | None = None


class ActorClosed(RuntimeError):
    """Raised when a message is sent to an actor after it has been closed."""


class ActorReentrancyError(RuntimeError):
    """Raised when an actor waits for a response from its own mailbox."""


class ActorRef(Generic[MessageT]):
    """External mailbox endpoint for an actor."""

    def __init__(self, actor: Actor[MessageT]) -> None:
        self._actor = actor

    async def ask(self, message: MessageT) -> Any:
        if _current_actor.get() is self._actor:
            raise ActorReentrancyError(f"actor {self._actor.name} cannot ask itself")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        await self._actor._enqueue(_Envelope(message=message, future=future))
        return await future

    async def tell(self, message: MessageT) -> None:
        if _current_actor.get() is self._actor:
            raise ActorReentrancyError(f"actor {self._actor.name} cannot tell itself through its ref; use defer")
        await self._actor._enqueue(_Envelope(message=message))


def create_actor_task(coro: Any, *, name: str) -> asyncio.Task[Any]:
    """Create a background task that does not inherit the caller's actor context."""
    context = contextvars.copy_context()
    context.run(_current_actor.set, None)
    return asyncio.create_task(coro, name=name, context=context)


class _DeferredMessageWorker(Generic[MessageT]):
    def __init__(
        self,
        mailbox: asyncio.Queue[_Envelope[MessageT] | None],
        message: MessageT,
        delay: float,
    ) -> None:
        self._mailbox = mailbox
        self._message = message
        self._delay = delay

    async def run(self) -> None:
        await asyncio.sleep(self._delay)
        await self._mailbox.put(_Envelope(message=self._message))


class Actor(Generic[MessageT]):
    """Single-mailbox async actor."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._mailbox: asyncio.Queue[_Envelope[MessageT] | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    def _start_actor(self) -> None:
        if self._closed:
            raise ActorClosed(f"actor {self.name} is closed")
        if self._task is None or self._task.done():
            self._task = create_actor_task(self._run(), name=self.name)

    @property
    def closed(self) -> bool:
        return self._closed

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._mailbox.put(None)
        if self._task is not None and self._task is not asyncio.current_task():
            try:
                await asyncio.wait_for(self._task, timeout=1.0)
            except TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            finally:
                self._fail_queued(ActorClosed(f"actor {self.name} closed"))
                self._task = None

    async def defer(self, message: MessageT) -> None:
        """Queue a future message from inside this actor's current handler."""
        if _current_actor.get() is not self:
            raise ActorReentrancyError(f"actor {self.name} can only defer from inside its mailbox")
        await self._mailbox.put(_Envelope(message=message))

    def defer_later(self, delay: float, message: MessageT, *, name: str) -> asyncio.Task[None]:
        """Schedule a future message without exposing this actor's ref to the handler."""
        if _current_actor.get() is not self:
            raise ActorReentrancyError(f"actor {self.name} can only defer from inside its mailbox")
        return create_actor_task(
            _DeferredMessageWorker(self._mailbox, message, delay).run(),
            name=name,
        )

    async def _enqueue(self, envelope: _Envelope[MessageT]) -> None:
        self._start_actor()
        await self._mailbox.put(envelope)

    async def handle(self, message: MessageT) -> Any:
        raise NotImplementedError

    async def _run(self) -> None:
        token = _current_actor.set(self)
        try:
            while True:
                envelope = await self._mailbox.get()
                if envelope is None:
                    return
                try:
                    result = await self.handle(envelope.message)
                except Exception as exc:  # noqa: BLE001 - actor must fail asks, not the runtime loop.
                    if envelope.future is not None and not envelope.future.done():
                        envelope.future.set_exception(exc)
                    else:
                        _LOGGER.exception(
                            "Yeelight Pro actor %s failed handling %s",
                            self.name,
                            type(envelope.message).__name__,
                        )
                    continue
                if envelope.future is not None and not envelope.future.done():
                    envelope.future.set_result(result)
        finally:
            _current_actor.reset(token)

    def _fail_queued(self, exc: BaseException) -> None:
        while True:
            try:
                envelope = self._mailbox.get_nowait()
            except asyncio.QueueEmpty:
                return
            if envelope is not None and envelope.future is not None and not envelope.future.done():
                envelope.future.set_exception(exc)
