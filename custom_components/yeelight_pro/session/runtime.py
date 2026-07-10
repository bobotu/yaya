from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..core.coercion import int_or_none as _int_or_none
from ..core.coercion import node_key as _node_key
from ..core.commands import MotorAction, NodeCommand, NodeSet
from ..core.events import iter_gateway_events
from ..core.exceptions import ProtocolError, YeelightProError
from ..core.protocol import GatewayMethod, list_payload
from ..core.topology import NodeId, NodeType, TopologyNode
from ..core.updates import PropertyChange
from .actor import Actor, ActorClosed, ActorRef, create_actor_task
from .connection import (
    ConnectConnectionCommand,
    ConnectionActorMessage,
    ConnectionLostEvent,
    ConnectionOnlineEvent,
    GatewayRpcRequest,
    RpcPushEvent,
)
from .events import (
    FullSyncSource,
    GatewayEventReceived,
    SessionStatusChanged,
    StateChangeReason,
    SyntheticSessionMethod,
    VisibleStateChanged,
)
from .motor import (
    MOTOR_CURRENT_ANGLE_PROP,
    MOTOR_CURRENT_POSITION_PROP,
    MOTOR_TARGET_ANGLE_PROP,
    MOTOR_TARGET_POSITION_PROP,
    MOTOR_TRACKING_TTL,
    MotorStateTracker,
    MotorTarget,
)
from .state import StateResult, StateStore
from .status import GatewaySessionState

JSONDict = dict[str, Any]
SessionStatusListener = Callable[[SessionStatusChanged], Awaitable[None] | None]
StateListener = Callable[[VisibleStateChanged], Awaitable[None] | None]
PropertyListener = Callable[[PropertyChange], Awaitable[None] | None]
GatewayEventListener = Callable[[GatewayEventReceived], Awaitable[None] | None]
_LOGGER = logging.getLogger(__name__)
_MAX_LOG_ITEMS = 20
_LIGHT_IMPLICIT_ON_PROPERTIES = frozenset({"l", "ct", "c", "angle"})

DEFAULT_STATE_READBACK_DELAY = 6.0
DEFAULT_STATE_DEADLINE = 10.0


@dataclass(frozen=True)
class _SyncOptions:
    include_groups: bool = False
    include_rooms: bool = False
    include_scenes: bool = False


@dataclass(frozen=True)
class _ConfigureAutoSync:
    options: _SyncOptions


@dataclass(frozen=True)
class _DisableAutoSync:
    pass


@dataclass(frozen=True)
class _SetSessionState:
    state: GatewaySessionState
    error: BaseException | None = None


@dataclass(frozen=True)
class _Connect:
    pass


@dataclass(frozen=True)
class _Sync:
    options: _SyncOptions


@dataclass(frozen=True)
class _FullPropertyTimeout:
    sync_id: int
    options: _SyncOptions


@dataclass(frozen=True)
class _ReadNode:
    node_id: NodeId


@dataclass(frozen=True)
class _WriteRequest:
    request_id: int
    commands: tuple[NodeCommand | NodeSet, ...]
    state_targets: Mapping[NodeId, Mapping[str, Any]] | None
    future: asyncio.Future[JSONDict]


@dataclass(frozen=True)
class _FlushWrites:
    pass


@dataclass(frozen=True)
class _DrainWrites:
    pass


@dataclass(frozen=True)
class _ReadbackBatch:
    batch_id: int


@dataclass(frozen=True)
class _ExpireWrites:
    pass


@dataclass(frozen=True)
class _ExpireMotors:
    pass


class GatewaySession(Actor[Any]):
    """Serialized gateway lifecycle, command queue, and observed-state owner."""

    def __init__(
        self,
        *,
        connection_ref: ActorRef[ConnectionActorMessage],
        set_prop_batch_delay: float = 0.01,
        state_readback_delay: float = DEFAULT_STATE_READBACK_DELAY,
        state_deadline: float = DEFAULT_STATE_DEADLINE,
        motor_tracking_ttl: float = MOTOR_TRACKING_TTL,
    ) -> None:
        super().__init__("yeelight-pro-session")
        self.ref: ActorRef[Any] = ActorRef(self)
        self.connection_ref = connection_ref
        self.store = StateStore()
        self.motor = MotorStateTracker(ttl=motor_tracking_ttl)
        self.session_state = GatewaySessionState.DISCONNECTED
        self.last_full_sync_at: datetime | None = None
        self.last_full_sync_source: FullSyncSource | None = None
        self.full_prop_timeout = 5.0

        self._batch_delay = max(0.0, set_prop_batch_delay)
        self._state_readback_delay = max(0.0, state_readback_delay)
        self._state_deadline = max(self._state_readback_delay, state_deadline)
        self._pending_writes: list[_WriteRequest] = []
        self._write_flush_task: asyncio.Task[None] | None = None
        self._write_deadline_task: asyncio.Task[None] | None = None
        self._readback_tasks: dict[int, asyncio.Task[None]] = {}
        self._motor_expiry_task: asyncio.Task[None] | None = None
        self._next_request_id = 0
        self._closing = False

        self._auto_sync = False
        self._sync_options = _SyncOptions()
        self._ready_waiter: asyncio.Future[None] | None = None
        self._ready_error: BaseException | None = None
        self._connection_epoch = 0
        self._sync_id = 0
        self._sync_waiter: asyncio.Future[None] | None = None
        self._sync_timeout_task: asyncio.Task[None] | None = None
        self._sync_options_by_id: dict[int, _SyncOptions] = {}

        self._status_listeners: list[SessionStatusListener] = []
        self._state_listeners: list[StateListener] = []
        self._property_listeners: list[PropertyListener] = []
        self._event_listeners: list[GatewayEventListener] = []
        self._suppressed_snapshot_counts: dict[str, int] = {}

    async def configure_auto_sync(
        self,
        *,
        include_groups: bool,
        include_rooms: bool,
        include_scenes: bool,
    ) -> None:
        await self.ref.ask(
            _ConfigureAutoSync(
                _SyncOptions(
                    include_groups=include_groups,
                    include_rooms=include_rooms,
                    include_scenes=include_scenes,
                )
            )
        )

    async def disable_auto_sync(self) -> None:
        await self.ref.ask(_DisableAutoSync())

    async def set_session_state(
        self,
        state: GatewaySessionState,
        error: BaseException | None = None,
    ) -> None:
        await self.ref.ask(_SetSessionState(state, error))

    async def connect(self) -> None:
        await self.ref.ask(_Connect())

    async def sync(
        self,
        *,
        include_groups: bool,
        include_rooms: bool,
        include_scenes: bool,
    ) -> None:
        waiter = await self.ref.ask(
            _Sync(
                _SyncOptions(
                    include_groups=include_groups,
                    include_rooms=include_rooms,
                    include_scenes=include_scenes,
                )
            )
        )
        await waiter

    async def read_node(self, node_id: NodeId) -> JSONDict:
        return await self.ref.ask(_ReadNode(node_id))

    async def submit_commands(
        self,
        commands: Iterable[NodeCommand | NodeSet],
        *,
        state_targets: Mapping[NodeId, Mapping[str, Any]] | None,
    ) -> JSONDict:
        command_tuple = tuple(commands)
        if not command_tuple:
            raise ValueError("submit_commands requires at least one node command")
        _validate_command_batch(command_tuple)
        if self._closing or self.closed:
            raise ActorClosed("gateway session is closed")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[JSONDict] = loop.create_future()
        self._next_request_id += 1
        await self.ref.ask(
            _WriteRequest(
                request_id=self._next_request_id,
                commands=command_tuple,
                state_targets=state_targets,
                future=future,
            )
        )
        return await future

    async def drain_writes(self) -> None:
        await self.ref.ask(_DrainWrites())

    async def wait_ready(self) -> None:
        if self.session_state == GatewaySessionState.READY:
            return
        if self._ready_error is not None:
            raise self._ready_error
        if self._ready_waiter is None or self._ready_waiter.done():
            self._ready_waiter = asyncio.get_running_loop().create_future()
        await self._ready_waiter

    def visible_node(self, node_id: NodeId) -> TopologyNode | None:
        node = _mapping_node(self.store.nodes, node_id)
        return None if node is None else self.motor.visible_node(node)

    def visible_nodes(self) -> list[TopologyNode]:
        return [self.motor.visible_node(node) for node in self.store.nodes.values()]

    def has_pending_write(self, node_id: NodeId, props: Iterable[str] | None = None) -> bool:
        return self.store.has_pending(node_id, props)

    def write_diagnostics(self) -> dict[str, Any]:
        return self.store.diagnostics(now=asyncio.get_running_loop().time())

    def motor_diagnostics(self) -> dict[str, Any]:
        return self.motor.diagnostics(now=asyncio.get_running_loop().time())

    def add_status_listener(self, listener: SessionStatusListener) -> Callable[[], None]:
        return _add_listener(self._status_listeners, listener)

    def add_state_listener(self, listener: StateListener) -> Callable[[], None]:
        return _add_listener(self._state_listeners, listener)

    def add_property_listener(self, listener: PropertyListener) -> Callable[[], None]:
        return _add_listener(self._property_listeners, listener)

    def add_gateway_event_listener(self, listener: GatewayEventListener) -> Callable[[], None]:
        return _add_listener(self._event_listeners, listener)

    async def close(self) -> None:
        if self.closed:
            return
        self._closing = True
        self._cancel_runtime_tasks()
        error = ActorClosed("gateway session is closed")
        self._fail_write_requests(self._pending_writes, error)
        self._pending_writes.clear()
        await super().close()

    async def handle(self, message: Any) -> Any:
        if isinstance(message, _ConfigureAutoSync):
            self._auto_sync = True
            self._sync_options = message.options
            self._clear_ready()
            return None
        if isinstance(message, _DisableAutoSync):
            self._auto_sync = False
            return None
        if isinstance(message, _SetSessionState):
            await self._set_session_state(message.state, message.error)
            return None
        if isinstance(message, _Connect):
            await self._set_session_state(GatewaySessionState.CONNECTING)
            await self.connection_ref.ask(ConnectConnectionCommand())
            await self._set_session_state(GatewaySessionState.WAITING_TOPOLOGY)
            return None
        if isinstance(message, _Sync):
            return await self._begin_sync(message.options)
        if isinstance(message, _FullPropertyTimeout):
            await self._handle_full_property_timeout(message)
            return None
        if isinstance(message, _ReadNode):
            return await self._read_node(message.node_id)
        if isinstance(message, _WriteRequest):
            await self._queue_write(message)
            return None
        if isinstance(message, _FlushWrites):
            await self._flush_writes()
            return None
        if isinstance(message, _DrainWrites):
            self._closing = True
            await self._flush_writes()
            return None
        if isinstance(message, _ReadbackBatch):
            await self._readback_batch(message.batch_id)
            return None
        if isinstance(message, _ExpireWrites):
            await self._expire_writes()
            return None
        if isinstance(message, _ExpireMotors):
            await self._expire_motors()
            return None
        if isinstance(message, ConnectionOnlineEvent):
            await self._handle_connection_online(message)
            return None
        if isinstance(message, ConnectionLostEvent):
            await self._handle_connection_lost(message)
            return None
        if isinstance(message, RpcPushEvent):
            await self._handle_rpc_push(message)
            return None
        raise TypeError(f"unsupported session message: {type(message).__name__}")

    async def _request(
        self,
        method: str,
        payload: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> JSONDict:
        return await self.connection_ref.ask(GatewayRpcRequest(method=method, payload=payload, timeout=timeout))

    async def _begin_sync(self, options: _SyncOptions) -> asyncio.Future[None]:
        if self._sync_waiter is not None and not self._sync_waiter.done():
            return self._sync_waiter
        self._cancel_sync_timeout()
        self._clear_ready()
        self._sync_waiter = asyncio.get_running_loop().create_future()
        self.last_full_sync_source = None
        before = self._visible_snapshot()
        state_result = self.store.clear_pending()
        motor_affected = self.motor.clear_all()
        self._cancel_write_tasks()
        await self._publish_if_changed(
            before,
            StateChangeReason.SESSION_RESET,
            {"method": SyntheticSessionMethod.SESSION_RESET},
            state_result=state_result,
            force_node_ids=motor_affected,
        )
        try:
            await self._set_session_state(GatewaySessionState.WAITING_TOPOLOGY)
            topology_message = await self._request(GatewayMethod.GET_TOPOLOGY)
            before = self._visible_snapshot()
            _topology, result = self.store.apply_topology(topology_message)
            self.motor.clear_missing_nodes(self.store.nodes)
            await self._publish_if_changed(
                before,
                StateChangeReason.TOPOLOGY_SYNC,
                {"method": SyntheticSessionMethod.SYNC_TOPOLOGY},
                state_result=result,
            )
            await self._begin_full_property_wait(options)
        except Exception as exc:
            self._fail_sync(exc, notify_waiter=False)
            self._fail_ready(exc)
            raise
        return self._sync_waiter

    async def _begin_full_property_wait(self, options: _SyncOptions) -> None:
        self._sync_id += 1
        sync_id = self._sync_id
        self._cancel_sync_timeout()
        self._clear_ready()
        self._sync_options_by_id.clear()
        self._sync_options_by_id[sync_id] = options
        self.last_full_sync_source = None
        await self._set_session_state(GatewaySessionState.WAITING_FULL_PROP)
        self._sync_timeout_task = self.defer_later(
            self.full_prop_timeout,
            _FullPropertyTimeout(sync_id, options),
            name="yeelight-pro-full-property-timeout",
        )

    async def _handle_full_property_timeout(self, message: _FullPropertyTimeout) -> None:
        if message.sync_id != self._sync_id or self.session_state != GatewaySessionState.WAITING_FULL_PROP:
            return
        try:
            await self._set_session_state(GatewaySessionState.RECOVERING)
            response = await self._request(GatewayMethod.GET_NODE, _id_payload(0))
            await self._apply_properties(response, StateChangeReason.POLL_FULL_PROPERTIES)
            self.last_full_sync_at = datetime.now(UTC)
            self.last_full_sync_source = FullSyncSource.POLL
            await self._finish_sync(message.options)
        except Exception as exc:
            self._fail_sync(exc)
            self._fail_ready(exc)
            raise

    async def _finish_sync(self, options: _SyncOptions) -> None:
        self._cancel_sync_timeout()
        if options.include_groups:
            await self._apply_groups(
                await self._request(GatewayMethod.GET_GROUP, _id_payload(0)),
                StateChangeReason.STATE_READBACK,
            )
        if options.include_rooms:
            response = await self._request(GatewayMethod.GET_ROOM, _id_payload(0))
            before = self._visible_snapshot()
            result = self.store.apply_rooms(response)
            await self._publish_if_changed(before, StateChangeReason.STATE_READBACK, response, state_result=result)
        if options.include_scenes:
            response = await self._request(GatewayMethod.GET_SCENE, _id_payload(0))
            before = self._visible_snapshot()
            result = self.store.apply_scenes(response)
            await self._publish_if_changed(before, StateChangeReason.STATE_READBACK, response, state_result=result)
        await self._set_session_state(GatewaySessionState.READY)
        self._set_ready()
        waiter = self._sync_waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        self._sync_waiter = None
        self._sync_options_by_id.pop(self._sync_id, None)
        await self._publish_snapshot(
            StateChangeReason.SYNC_COMPLETE,
            {"method": SyntheticSessionMethod.SYNC_COMPLETE},
            (),
        )

    async def _handle_connection_online(self, event: ConnectionOnlineEvent) -> None:
        self._connection_epoch = event.epoch
        if not self._auto_sync:
            return
        try:
            await self._set_session_state(GatewaySessionState.RECOVERING)
            await self._begin_sync(self._sync_options)
        except Exception as exc:
            self._fail_ready(exc)
            await self._set_session_state(GatewaySessionState.DISCONNECTED, exc)

    async def _handle_connection_lost(self, event: ConnectionLostEvent) -> None:
        if event.epoch != self._connection_epoch:
            return
        self._clear_ready()
        self._cancel_sync_timeout()
        if self.session_state == GatewaySessionState.CLOSING:
            return
        error = event.error or YeelightProError("gateway connection closed")
        self._fail_sync(error)
        self._fail_ready(error)
        self._fail_write_requests(self._pending_writes, error)
        self._pending_writes.clear()
        before = self._visible_snapshot()
        state_result = self.store.clear_pending()
        motor_affected = self.motor.clear_all()
        self._cancel_write_tasks()
        await self._publish_if_changed(
            before,
            StateChangeReason.SESSION_RESET,
            {"method": SyntheticSessionMethod.SESSION_RESET},
            state_result=state_result,
            force_node_ids=motor_affected,
        )
        await self._set_session_state(GatewaySessionState.DISCONNECTED, error)

    async def _read_node(self, node_id: NodeId) -> JSONDict:
        node = _mapping_node(self.store.raw.nodes, node_id)
        if node is not None and node.nt == NodeType.MESH_GROUP:
            response = await self._request(GatewayMethod.GET_GROUP, _id_payload(node_id))
            await self._apply_groups(response, StateChangeReason.STATE_READBACK)
            return response
        response = await self._request(GatewayMethod.GET_NODE, _id_payload(node_id))
        await self._apply_properties(response, StateChangeReason.STATE_READBACK)
        return response

    async def _handle_rpc_push(self, event: RpcPushEvent) -> None:
        if event.epoch != self._connection_epoch:
            return
        message = event.message
        method = message.get("method")
        if method == GatewayMethod.POST_PROP:
            await self._apply_properties(message, StateChangeReason.PROPERTY_PUSH)
            if (
                self.store.full_property_coverage(message)
                and self.session_state == GatewaySessionState.WAITING_FULL_PROP
            ):
                self.last_full_sync_at = datetime.now(UTC)
                self.last_full_sync_source = FullSyncSource.PUSH
                try:
                    await self._finish_sync(self._sync_options_by_id.get(self._sync_id, self._sync_options))
                except Exception as exc:
                    self._fail_sync(exc)
                    self._fail_ready(exc)
                    raise
        elif method == GatewayMethod.POST_TOPOLOGY:
            before = self._visible_snapshot()
            _topology, result = self.store.apply_topology(message, replace_existing=False)
            motor_affected = self.motor.clear_missing_nodes(self.store.nodes)
            await self._publish_if_changed(
                before,
                StateChangeReason.TOPOLOGY_PUSH,
                message,
                state_result=result,
                force_node_ids=motor_affected,
            )
            await self._begin_full_property_wait(self._sync_options_by_id.get(self._sync_id, self._sync_options))

        for gateway_event in iter_gateway_events(message):
            self._notify_gateway_event(GatewayEventReceived(gateway_event))

    async def _apply_properties(
        self,
        message: Mapping[str, Any],
        reason: StateChangeReason,
        *,
        match_batch_id: int | None = None,
        publish: bool = True,
    ) -> StateResult:
        before = self._visible_snapshot()
        result = self.store.apply_properties(message, match_batch_id=match_batch_id)
        motor_affected = self.motor.apply_authoritative_message(
            message,
            self.store.raw.nodes,
            now=asyncio.get_running_loop().time(),
        )
        self._after_store_result(result)
        self._schedule_motor_expiry()
        if publish:
            await self._publish_if_changed(
                before,
                reason,
                message,
                state_result=result,
                force_node_ids=motor_affected,
            )
        return result

    async def _apply_groups(
        self,
        message: Mapping[str, Any],
        reason: StateChangeReason,
        *,
        match_batch_id: int | None = None,
        publish: bool = True,
    ) -> StateResult:
        before = self._visible_snapshot()
        result = self.store.apply_groups(message, match_batch_id=match_batch_id)
        now = asyncio.get_running_loop().time()
        motor_affected: set[NodeId] = set()
        for item in list_payload(message, "groups"):
            node_id = _item_id(item)
            if node_id is not None:
                motor_affected.update(
                    self.motor.apply_authoritative_node(node_id, item.get("params"), self.store.raw.nodes, now=now)
                )
        self._after_store_result(result)
        self._schedule_motor_expiry()
        if publish:
            await self._publish_if_changed(
                before,
                reason,
                message,
                state_result=result,
                force_node_ids=motor_affected,
            )
        return result

    async def _queue_write(self, request: _WriteRequest) -> None:
        if self._closing:
            if not request.future.done():
                request.future.set_exception(ActorClosed("gateway session is closed"))
            return
        if self._pending_writes and _batch_conflicts(self._pending_writes, request):
            await self._flush_writes()
        self._pending_writes.append(request)
        if self._write_flush_task is None or self._write_flush_task.done():
            if self._batch_delay == 0:
                await self.defer(_FlushWrites())
            else:
                self._write_flush_task = self.defer_later(
                    self._batch_delay,
                    _FlushWrites(),
                    name="yeelight-pro-set-prop-flush",
                )

    async def _flush_writes(self) -> None:
        self._cancel_write_flush()
        if not self._pending_writes:
            return
        requests = tuple(self._pending_writes)
        self._pending_writes.clear()
        commands = tuple(command for request in requests for command in request.commands)
        payload = _batched_payload_from_commands(commands)
        state_targets = _merge_state_targets(requests)
        batch_id: int | None = None

        if state_targets:
            before = self._visible_snapshot()
            batch_id, result = self.store.prepare_batch(
                state_targets,
                deadline=asyncio.get_running_loop().time() + self._state_deadline,
            )
            self._after_store_result(result)
            await self._publish_if_changed(
                before,
                StateChangeReason.WRITE_SUPERSEDED,
                {"method": SyntheticSessionMethod.WRITE_SUPERSEDED},
                state_result=result,
            )

        request_ids = tuple(request.request_id for request in requests)
        _LOGGER.debug(
            "Yeelight Pro sending set_prop batch: requests=%s state_batch=%s payload=%s targets=%s",
            request_ids,
            batch_id,
            _node_payload_summary(payload),
            _state_targets_summary(state_targets),
        )
        try:
            response = await self._request(GatewayMethod.SET_PROP, {"nodes": payload})
            _raise_for_missing_write_ack(response)
        except Exception as exc:  # noqa: BLE001 - one RPC result belongs to every original caller.
            if batch_id is not None:
                before = self._visible_snapshot()
                result = self.store.fail_batch(batch_id)
                self._after_store_result(result)
                await self._publish_if_changed(
                    before,
                    StateChangeReason.WRITE_FAILED,
                    {"method": SyntheticSessionMethod.WRITE_FAILED},
                    state_result=result,
                )
            self._fail_write_requests(requests, exc)
            return

        before = self._visible_snapshot()
        result = self.store.accept_batch(batch_id) if batch_id is not None else StateResult()
        motor_affected = self._record_motor_commands(payload)
        self._after_store_result(result)
        await self._publish_if_changed(
            before,
            StateChangeReason.WRITE_ACCEPTED,
            {"method": SyntheticSessionMethod.WRITE_ACCEPTED},
            state_result=result,
            force_node_ids=motor_affected,
        )
        if batch_id is not None and self.store.pending_node_ids(batch_id, unresolved_only=False):
            self._readback_tasks[batch_id] = self.defer_later(
                self._state_readback_delay,
                _ReadbackBatch(batch_id),
                name=f"yeelight-pro-state-readback-{batch_id}",
            )
        self._schedule_write_deadline()
        self._schedule_motor_expiry()
        for request in requests:
            if not request.future.done():
                request.future.set_result(response)

    def _record_motor_commands(self, payload: Iterable[Mapping[str, Any]]) -> set[NodeId]:
        targets, stops = _motor_tracking_from_payload(payload)
        now = asyncio.get_running_loop().time()
        affected: set[NodeId] = set()
        for target in targets:
            node = _mapping_node(self.store.raw.nodes, target.node_id)
            current = None if node is None else _int_or_none(node.params.get(target.current_prop))
            affected.update(self.motor.set_target(target, current_value=current, now=now))
        for node_id in stops:
            affected.update(self.motor.clear_node(node_id))
        return affected

    async def _readback_batch(self, batch_id: int) -> None:
        self._readback_tasks.pop(batch_id, None)
        node_ids = self.store.pending_node_ids(batch_id)
        if not node_ids:
            return
        before = self._visible_snapshot()
        combined = StateResult()
        motor_affected: set[NodeId] = set()
        summaries: list[dict[str, Any]] = []
        for node_id in node_ids:
            node = _mapping_node(self.store.raw.nodes, node_id)
            try:
                if node is not None and node.nt == NodeType.MESH_GROUP:
                    response = await self._request(GatewayMethod.GET_GROUP, _id_payload(node_id))
                    result = await self._apply_groups(
                        response,
                        StateChangeReason.STATE_READBACK,
                        match_batch_id=batch_id,
                        publish=False,
                    )
                else:
                    response = await self._request(GatewayMethod.GET_NODE, _id_payload(node_id))
                    result = await self._apply_properties(
                        response,
                        StateChangeReason.STATE_READBACK,
                        match_batch_id=batch_id,
                        publish=False,
                    )
                combined = _merge_state_results(combined, result)
                summaries.append(_message_summary(response))
            except Exception as exc:  # noqa: BLE001 - deadline remains the bounded fallback.
                _LOGGER.debug("Yeelight Pro state readback failed: batch=%s node=%s error=%r", batch_id, node_id, exc)
        after = self._visible_snapshot()
        motor_affected.update(_changed_node_ids(before, after))
        await self._publish_if_changed(
            before,
            StateChangeReason.STATE_READBACK,
            {"method": SyntheticSessionMethod.STATE_READBACK, "responses": summaries},
            state_result=combined,
            force_node_ids=motor_affected,
        )

    async def _expire_writes(self) -> None:
        self._write_deadline_task = None
        before = self._visible_snapshot()
        result = self.store.expire_due(now=asyncio.get_running_loop().time())
        self._after_store_result(result)
        await self._publish_if_changed(
            before,
            StateChangeReason.WRITE_EXPIRED,
            {"method": SyntheticSessionMethod.WRITE_EXPIRED},
            state_result=result,
        )
        self._schedule_write_deadline()

    async def _expire_motors(self) -> None:
        self._motor_expiry_task = None
        before = self._visible_snapshot()
        expired = self.motor.expire_pending(now=asyncio.get_running_loop().time())
        await self._publish_if_changed(
            before,
            StateChangeReason.MOTOR_TRACKING_EXPIRED,
            {"method": SyntheticSessionMethod.MOTOR_TRACKING_EXPIRED},
            force_node_ids={track.node_id for track in expired},
        )
        self._schedule_motor_expiry()

    def _after_store_result(self, result: StateResult) -> None:
        for batch_id in result.ended_batch_ids:
            task = self._readback_tasks.pop(batch_id, None)
            if task is not None:
                task.cancel()
        self._schedule_write_deadline()

    def _schedule_write_deadline(self) -> None:
        if self._write_deadline_task is not None:
            self._write_deadline_task.cancel()
            self._write_deadline_task = None
        deadline = self.store.next_deadline()
        if deadline is None:
            return
        delay = max(0.0, deadline - asyncio.get_running_loop().time())
        self._write_deadline_task = self.defer_later(
            delay,
            _ExpireWrites(),
            name="yeelight-pro-state-deadline",
        )

    def _schedule_motor_expiry(self) -> None:
        if self._motor_expiry_task is not None:
            self._motor_expiry_task.cancel()
            self._motor_expiry_task = None
        now = asyncio.get_running_loop().time()
        expiration = self.motor.next_expiration(now=now)
        if expiration is None:
            return
        self._motor_expiry_task = self.defer_later(
            max(0.0, expiration - now),
            _ExpireMotors(),
            name="yeelight-pro-motor-expiry",
        )

    async def _set_session_state(
        self,
        state: GatewaySessionState,
        error: BaseException | None = None,
    ) -> None:
        previous = self.session_state
        if previous == state and error is None:
            return
        self.session_state = state
        event = SessionStatusChanged(previous=previous, current=state, error=error)
        if state in {GatewaySessionState.CLOSING, GatewaySessionState.DISCONNECTED}:
            self._fail_ready(error or YeelightProError("gateway connection closed"))
        for listener in list(self._status_listeners):
            _schedule_listener(listener, event)

    async def _publish_if_changed(
        self,
        before: Mapping[NodeId, TopologyNode],
        reason: StateChangeReason,
        message: Mapping[str, Any],
        *,
        state_result: StateResult | None = None,
        force_node_ids: Iterable[NodeId] = (),
    ) -> None:
        after = self._visible_snapshot()
        changed_ids = _changed_node_ids(before, after)
        changed_ids.update(force_node_ids)
        metadata_changed = state_result.metadata_changed if state_result is not None else False
        changes = tuple(
            PropertyChange(
                id=node_id,
                before=_mapping_node(before, node_id),
                after=after_node,
                update=_update_for_node(message, node_id),
            )
            for node_id in sorted(changed_ids, key=str)
            if (after_node := _mapping_node(after, node_id)) is not None
            and _mapping_node(before, node_id) != after_node
        )
        if not changes and not metadata_changed:
            self._log_suppressed_snapshot(reason, message)
            return
        for change in changes:
            for listener in list(self._property_listeners):
                _schedule_listener(listener, change)
        await self._publish_snapshot(reason, message, changes)

    async def _publish_snapshot(
        self,
        reason: StateChangeReason,
        message: Mapping[str, Any],
        changes: tuple[PropertyChange, ...],
    ) -> None:
        event = VisibleStateChanged(reason=reason, message=message, changes=changes)
        for listener in list(self._state_listeners):
            _schedule_listener(listener, event)

    def _notify_gateway_event(self, event: GatewayEventReceived) -> None:
        for listener in list(self._event_listeners):
            _schedule_listener(listener, event)

    def _visible_snapshot(self) -> dict[NodeId, TopologyNode]:
        return {node.id: self.motor.visible_node(node) for node in self.store.nodes.values()}

    def _log_suppressed_snapshot(self, reason: StateChangeReason, message: Mapping[str, Any]) -> None:
        key = str(reason)
        count = self._suppressed_snapshot_counts.get(key, 0) + 1
        self._suppressed_snapshot_counts[key] = count
        if count & (count - 1):
            return
        _LOGGER.debug(
            "Yeelight Pro visible state unchanged: reason=%s count=%d summary=%s",
            reason,
            count,
            _message_summary(message),
        )

    def _cancel_write_flush(self) -> None:
        if self._write_flush_task is not None:
            self._write_flush_task.cancel()
            self._write_flush_task = None

    def _cancel_write_tasks(self) -> None:
        self._cancel_write_flush()
        if self._write_deadline_task is not None:
            self._write_deadline_task.cancel()
            self._write_deadline_task = None
        for task in self._readback_tasks.values():
            task.cancel()
        self._readback_tasks.clear()

    def _cancel_sync_timeout(self) -> None:
        if self._sync_timeout_task is not None:
            self._sync_timeout_task.cancel()
            self._sync_timeout_task = None

    def _cancel_runtime_tasks(self) -> None:
        self._cancel_sync_timeout()
        self._cancel_write_tasks()
        if self._motor_expiry_task is not None:
            self._motor_expiry_task.cancel()
            self._motor_expiry_task = None

    def _clear_ready(self) -> None:
        self._ready_error = None
        if self._ready_waiter is not None and self._ready_waiter.done():
            self._ready_waiter = None

    def _set_ready(self) -> None:
        self._ready_error = None
        if self._ready_waiter is not None and not self._ready_waiter.done():
            self._ready_waiter.set_result(None)

    def _fail_ready(self, exc: BaseException) -> None:
        self._ready_error = exc
        if self._ready_waiter is not None and not self._ready_waiter.done():
            self._ready_waiter.set_exception(exc)

    def _fail_sync(self, exc: BaseException, *, notify_waiter: bool = True) -> None:
        self._cancel_sync_timeout()
        waiter = self._sync_waiter
        if notify_waiter and waiter is not None and not waiter.done():
            waiter.set_exception(exc)
        self._sync_waiter = None
        self._sync_options_by_id.pop(self._sync_id, None)

    @staticmethod
    def _fail_write_requests(requests: Iterable[_WriteRequest], exc: BaseException) -> None:
        for request in requests:
            if not request.future.done():
                request.future.set_exception(exc)


def _add_listener(listeners: list[Any], listener: Any) -> Callable[[], None]:
    listeners.append(listener)

    def remove() -> None:
        with suppress(ValueError):
            listeners.remove(listener)

    return remove


def _id_payload(item_id: NodeId | None) -> Mapping[str, Any] | None:
    if item_id is None:
        return None
    return {"params": {"id": item_id}}


def _mapping_node(mapping: Mapping[NodeId, TopologyNode], node_id: NodeId) -> TopologyNode | None:
    direct = mapping.get(node_id)
    if direct is not None:
        return direct
    wanted = str(node_id)
    return next((node for key, node in mapping.items() if str(key) == wanted), None)


def _item_id(item: Mapping[str, Any]) -> NodeId | None:
    value = item.get("id")
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        return value
    return None


def _changed_node_ids(
    before: Mapping[NodeId, TopologyNode],
    after: Mapping[NodeId, TopologyNode],
) -> set[NodeId]:
    ids = {node.id for node in before.values()} | {node.id for node in after.values()}
    return {node_id for node_id in ids if _mapping_node(before, node_id) != _mapping_node(after, node_id)}


def _update_for_node(message: Mapping[str, Any], node_id: NodeId) -> Mapping[str, Any]:
    wanted = str(node_id)
    for key in ("nodes", "groups"):
        for item in list_payload(message, key):
            item_id = _item_id(item)
            if item_id is not None and str(item_id) == wanted:
                return item
    return {"id": node_id}


def _batch_conflicts(existing: Iterable[_WriteRequest], new: _WriteRequest) -> bool:
    targets: dict[tuple[str, str], tuple[Any, tuple[Any, Any, Any]]] = {}
    props_by_node: dict[str, dict[str, Any]] = {}
    for request in existing:
        for key, value in _set_prop_targets(request.commands):
            targets[key] = value
        _merge_command_props(props_by_node, request.commands)
    new_props_by_node: dict[str, dict[str, Any]] = {}
    _merge_command_props(new_props_by_node, new.commands)
    for node_key, props in new_props_by_node.items():
        if node_key in props_by_node and _has_off_attribute_mix({**props_by_node[node_key], **props}):
            return True
    return any(key in targets and targets[key] != value for key, value in _set_prop_targets(new.commands))


def _validate_command_batch(commands: Iterable[NodeCommand | NodeSet]) -> None:
    props_by_node: dict[str, dict[str, Any]] = {}
    _merge_command_props(props_by_node, commands)
    if any(_has_off_attribute_mix(props) for props in props_by_node.values()):
        raise ValueError("power-off cannot be combined with brightness, color temperature, or color properties")


def _merge_command_props(
    result: dict[str, dict[str, Any]],
    commands: Iterable[NodeCommand | NodeSet],
) -> None:
    for command in commands:
        payload = command.to_payload()
        node_key = _node_key(payload.get("id"))
        props = payload.get("set")
        if node_key is not None and isinstance(props, Mapping):
            result.setdefault(node_key, {}).update(props)


def _has_off_attribute_mix(props: Mapping[str, Any]) -> bool:
    return props.get("p") is False and bool(_LIGHT_IMPLICIT_ON_PROPERTIES.intersection(props))


def _set_prop_targets(
    commands: Iterable[NodeCommand | NodeSet],
) -> Iterable[tuple[tuple[str, str], tuple[Any, tuple[Any, Any, Any]]]]:
    for command in commands:
        payload = command.to_payload()
        node_key = _node_key(payload.get("id"))
        props = payload.get("set")
        if node_key is None or not isinstance(props, Mapping):
            continue
        transition = (payload.get("duration"), payload.get("delay"), payload.get("delayOff"))
        for prop, value in props.items():
            if isinstance(prop, str):
                yield (node_key, prop), (value, transition)


def _batched_payload_from_commands(commands: Iterable[NodeCommand | NodeSet]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    merged: dict[tuple[str, Any, Any, Any, Any], dict[str, Any]] = {}
    for command in commands:
        payload = command.to_payload()
        node_key = _node_key(payload.get("id"))
        props = payload.get("set")
        if node_key is None or not isinstance(props, Mapping) or not _is_mergeable_set_payload(payload):
            payloads.append(payload)
            continue
        key = (
            node_key,
            payload.get("nt"),
            payload.get("duration"),
            payload.get("delay"),
            payload.get("delayOff"),
        )
        merged_payload = merged.get(key)
        if merged_payload is None:
            merged_payload = {key: value for key, value in payload.items() if key != "set"}
            merged_payload["set"] = {}
            merged[key] = merged_payload
            payloads.append(merged_payload)
        merged_payload["set"].update(dict(props))
    return payloads


def _is_mergeable_set_payload(payload: Mapping[str, Any]) -> bool:
    return set(payload).issubset({"id", "nt", "duration", "delay", "delayOff", "set"})


def _merge_state_targets(requests: Iterable[_WriteRequest]) -> dict[NodeId, dict[str, Any]]:
    targets: dict[NodeId, dict[str, Any]] = {}
    for request in requests:
        if request.state_targets is None:
            continue
        for node_id, props in request.state_targets.items():
            targets.setdefault(node_id, {}).update(props)
    return targets


def _motor_tracking_from_payload(
    payload: Iterable[Mapping[str, Any]],
) -> tuple[list[MotorTarget], list[NodeId]]:
    targets: list[MotorTarget] = []
    stops: list[NodeId] = []
    for item in payload:
        node_id = _item_id(item)
        if node_id is None:
            continue
        props = item.get("set")
        if isinstance(props, Mapping):
            position = _int_or_none(props.get(MOTOR_TARGET_POSITION_PROP))
            if position is not None:
                targets.append(
                    MotorTarget(
                        node_id=node_id,
                        current_prop=MOTOR_CURRENT_POSITION_PROP,
                        target_prop=MOTOR_TARGET_POSITION_PROP,
                        target_value=position,
                    )
                )
            angle = _int_or_none(props.get(MOTOR_TARGET_ANGLE_PROP))
            if angle is not None:
                targets.append(
                    MotorTarget(
                        node_id=node_id,
                        current_prop=MOTOR_CURRENT_ANGLE_PROP,
                        target_prop=MOTOR_TARGET_ANGLE_PROP,
                        target_value=angle,
                    )
                )
        if _is_motor_pause(item.get("action")):
            stops.append(node_id)
    return targets, stops


def _is_motor_pause(action: object) -> bool:
    if not isinstance(action, Mapping):
        return False
    motor_adjust = action.get("motorAdjust")
    return isinstance(motor_adjust, Mapping) and motor_adjust.get("type") == str(MotorAction.PAUSE)


def _merge_state_results(first: StateResult, second: StateResult) -> StateResult:
    return StateResult(
        changed_node_ids=first.changed_node_ids | second.changed_node_ids,
        metadata_changed=first.metadata_changed or second.metadata_changed,
        ended_batch_ids=tuple(sorted(set(first.ended_batch_ids) | set(second.ended_batch_ids))),
    )


def _raise_for_missing_write_ack(response: Mapping[str, Any]) -> None:
    if response.get("result") != "ok":
        raise ProtocolError(f"{GatewayMethod.SET_PROP} did not return a successful acknowledgement")


def _node_payload_summary(payload: Iterable[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    return tuple(_command_payload_summary(item) for item in payload)


def _command_payload_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {"id": payload.get("id"), "nt": payload.get("nt")}
    for key in ("duration", "delay", "delayOff"):
        if key in payload:
            summary[key] = payload.get(key)
    props = payload.get("set")
    if isinstance(props, Mapping):
        summary["set"] = dict(props)
    action = payload.get("action")
    if isinstance(action, Mapping):
        summary["action_keys"] = sorted(str(key) for key in action)
    return summary


def _state_targets_summary(targets: Mapping[NodeId, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(node_id): dict(props) for node_id, props in targets.items()}


def _message_summary(message: Mapping[str, Any]) -> dict[str, Any]:
    summary = {key: message.get(key) for key in ("id", "method", "result", "data") if key in message}
    for key in ("nodes", "groups"):
        items = list_payload(message, key)
        if items:
            summary[f"{key[:-1]}_count"] = len(items)
            summary[key] = tuple(_command_payload_summary(item) for item in items[:_MAX_LOG_ITEMS])
    return summary


async def _call_listener(listener: Callable[..., Any], *args: Any) -> None:
    try:
        result = listener(*args)
        if inspect.isawaitable(result):
            await result
    except Exception:  # noqa: BLE001 - external listeners must not kill the session actor.
        _LOGGER.exception("Yeelight Pro session listener failed")


def _schedule_listener(listener: Callable[..., Any], *args: Any) -> None:
    create_actor_task(_call_listener(listener, *args), name="yeelight-pro-session-listener")
