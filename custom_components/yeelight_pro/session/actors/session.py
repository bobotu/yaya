from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ...core.events import iter_gateway_events
from ...core.exceptions import YeelightProError
from ...core.protocol import GatewayMethod
from ...core.topology import NodeType
from ..messages import (
    ApplyGenericStateMessageCommand,
    ApplyGroupsCommand,
    ApplyPropertiesCommand,
    ApplyRoomsCommand,
    ApplyScenesCommand,
    ApplyTopologyCommand,
    ConfigureAutoSyncCommand,
    ConnectConnectionCommand,
    ConnectionActorMessage,
    ConnectionLostEvent,
    ConnectionOnlineEvent,
    ConnectSessionCommand,
    DeviceStateActorMessage,
    DisableAutoSyncCommand,
    FullPropertySyncTimedOutEvent,
    FullSyncSource,
    GatewayEventReceived,
    GatewayRpcRequest,
    RefreshNodeCommand,
    RpcPushEvent,
    SessionActorMessage,
    SessionStatusChanged,
    SetSessionStateCommand,
    StateChangeReason,
    SyncCompletedEvent,
    SyncSessionCommand,
    SyncStartedEvent,
    SyntheticSessionMethod,
)
from ..model.status import GatewaySessionState
from .base import Actor, ActorRef, create_actor_task

JSONDict = dict[str, Any]
SessionStatusListener = Callable[[SessionStatusChanged], Awaitable[None] | None]
GatewayEventListener = Callable[[GatewayEventReceived], Awaitable[None] | None]
_LOGGER = logging.getLogger(__name__)
_MAX_LOG_ITEMS = 20


@dataclass(frozen=True)
class _SyncOptions:
    include_groups: bool = False
    include_rooms: bool = False
    include_scenes: bool = False

    @classmethod
    def from_message(
        cls,
        message: SyncSessionCommand | ConfigureAutoSyncCommand | FullPropertySyncTimedOutEvent,
    ) -> _SyncOptions:
        return cls(
            include_groups=message.include_groups,
            include_rooms=message.include_rooms,
            include_scenes=message.include_scenes,
        )

    def to_request(self) -> SyncSessionCommand:
        return SyncSessionCommand(
            include_groups=self.include_groups,
            include_rooms=self.include_rooms,
            include_scenes=self.include_scenes,
        )

    def to_timeout(self, sync_id: int) -> FullPropertySyncTimedOutEvent:
        return FullPropertySyncTimedOutEvent(
            sync_id=sync_id,
            include_groups=self.include_groups,
            include_rooms=self.include_rooms,
            include_scenes=self.include_scenes,
        )


class SessionActor(Actor[SessionActorMessage]):
    """Mailbox-owned authoritative session lifecycle and protocol state machine."""

    def __init__(
        self,
        *,
        connection_ref: ActorRef[ConnectionActorMessage],
        device_state_ref: ActorRef[DeviceStateActorMessage],
    ) -> None:
        super().__init__("yeelight-pro-session")
        self.connection_ref = connection_ref
        self.device_state_ref = device_state_ref
        self.session_state = GatewaySessionState.DISCONNECTED
        self.last_full_sync_at: datetime | None = None
        self.last_full_sync_source: FullSyncSource | None = None
        self.full_prop_timeout = 5.0
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
        self._event_listeners: list[GatewayEventListener] = []

    def add_status_listener(self, listener: SessionStatusListener) -> Callable[[], None]:
        self._status_listeners.append(listener)

        def remove() -> None:
            with suppress(ValueError):
                self._status_listeners.remove(listener)

        return remove

    def add_gateway_event_listener(self, listener: GatewayEventListener) -> Callable[[], None]:
        self._event_listeners.append(listener)

        def remove() -> None:
            with suppress(ValueError):
                self._event_listeners.remove(listener)

        return remove

    async def wait_ready(self) -> None:
        if self.session_state == GatewaySessionState.READY:
            return
        if self._ready_error is not None:
            raise self._ready_error
        if self._ready_waiter is None or self._ready_waiter.done():
            self._ready_waiter = asyncio.get_running_loop().create_future()
        await self._ready_waiter

    async def _request(
        self,
        method: str,
        payload: Mapping[str, Any] | None = None,
        *,
        on_written: Any | None = None,
        timeout: float | None = None,
    ) -> JSONDict:
        return await self.connection_ref.ask(
            GatewayRpcRequest(method=method, payload=payload, on_written=on_written, timeout=timeout)
        )

    async def _get_topology(self) -> JSONDict:
        return await self._request(GatewayMethod.GET_TOPOLOGY)

    async def _get_node(self, node_id: str | int) -> JSONDict:
        return await self._request(GatewayMethod.GET_NODE, _id_payload(node_id))

    async def _get_all_nodes(self) -> JSONDict:
        return await self._request(GatewayMethod.GET_NODE, _id_payload(0))

    async def _get_group(self, group_id: str | int | None = 0) -> JSONDict:
        return await self._request(GatewayMethod.GET_GROUP, _id_payload(group_id))

    async def _get_room(self, room_id: str | int | None = 0) -> JSONDict:
        return await self._request(GatewayMethod.GET_ROOM, _id_payload(room_id))

    async def _get_scene(self, scene_id: str | int | None = 0) -> JSONDict:
        return await self._request(GatewayMethod.GET_SCENE, _id_payload(scene_id))

    async def handle(self, message: SessionActorMessage) -> Any:
        if isinstance(message, ConfigureAutoSyncCommand):
            self._auto_sync = True
            self._sync_options = _SyncOptions.from_message(message)
            self._clear_ready()
            return None
        if isinstance(message, DisableAutoSyncCommand):
            self._auto_sync = False
            return None
        if isinstance(message, SetSessionStateCommand):
            await self._set_session_state(message.state, message.error)
            return None
        if isinstance(message, ConnectSessionCommand):
            await self._set_session_state(GatewaySessionState.CONNECTING)
            await self.connection_ref.ask(ConnectConnectionCommand())
            await self._set_session_state(GatewaySessionState.WAITING_TOPOLOGY)
            return None
        if isinstance(message, SyncSessionCommand):
            return await self._begin_sync(message)
        if isinstance(message, FullPropertySyncTimedOutEvent):
            await self._handle_full_property_timeout(message)
            return None
        if isinstance(message, RefreshNodeCommand):
            return await self._refresh_node(
                message.node_id,
                message.node_type,
                request_generations=message.request_generations,
            )
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

    async def _begin_sync(self, message: SyncSessionCommand) -> asyncio.Future[None]:
        if self._sync_waiter is not None and not self._sync_waiter.done():
            _LOGGER.debug("Yeelight Pro sync joined existing waiter: sync_id=%s", self._sync_id)
            return self._sync_waiter
        self._cancel_sync_timeout()
        self._clear_ready()
        self._sync_waiter = asyncio.get_running_loop().create_future()
        options = _SyncOptions.from_message(message)
        self.last_full_sync_source = None
        _LOGGER.debug("Yeelight Pro sync starting: options=%s", options)
        try:
            await self.device_state_ref.tell(SyncStartedEvent(reason=StateChangeReason.MANUAL_SYNC))
            await self._set_session_state(GatewaySessionState.WAITING_TOPOLOGY)
            topology = await self._get_topology()
            await self.device_state_ref.ask(
                ApplyTopologyCommand(
                    payload=topology,
                    reason=StateChangeReason.TOPOLOGY_SYNC,
                    message={"method": SyntheticSessionMethod.SYNC_TOPOLOGY},
                )
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
        _LOGGER.debug(
            "Yeelight Pro waiting for full property push: sync_id=%s timeout=%s options=%s",
            sync_id,
            self.full_prop_timeout,
            options,
        )
        self._sync_timeout_task = self.defer_later(
            self.full_prop_timeout,
            options.to_timeout(sync_id),
            name="yeelight-pro-full-property-timeout",
        )

    async def _handle_full_property_timeout(self, message: FullPropertySyncTimedOutEvent) -> None:
        if message.sync_id != self._sync_id or self.session_state != GatewaySessionState.WAITING_FULL_PROP:
            _LOGGER.debug(
                "Yeelight Pro ignored full property timeout: message_sync_id=%s active_sync_id=%s state=%s",
                message.sync_id,
                self._sync_id,
                self.session_state,
            )
            return
        try:
            _LOGGER.debug("Yeelight Pro full property push timeout; polling all nodes: sync_id=%s", message.sync_id)
            await self._set_session_state(GatewaySessionState.RECOVERING)
            result = await self._get_all_nodes()
            await self.device_state_ref.ask(
                ApplyPropertiesCommand(payload=result, reason=StateChangeReason.POLL_FULL_PROPERTIES)
            )
            self.last_full_sync_at = datetime.now(UTC)
            self.last_full_sync_source = FullSyncSource.POLL
            await self._finish_sync(_SyncOptions.from_message(message))
        except Exception as exc:
            self._fail_sync(exc)
            self._fail_ready(exc)
            raise

    async def _finish_sync(self, options: _SyncOptions) -> None:
        self._cancel_sync_timeout()
        _LOGGER.debug(
            "Yeelight Pro finishing sync: sync_id=%s source=%s options=%s",
            self._sync_id,
            self.last_full_sync_source,
            options,
        )
        if options.include_groups:
            await self.device_state_ref.ask(ApplyGroupsCommand(await self._get_group()))
        if options.include_rooms:
            await self.device_state_ref.ask(ApplyRoomsCommand(await self._get_room()))
        if options.include_scenes:
            await self.device_state_ref.ask(ApplyScenesCommand(await self._get_scene()))
        await self._set_session_state(GatewaySessionState.READY)
        self._set_ready()
        await self.device_state_ref.tell(SyncCompletedEvent(source=self.last_full_sync_source))
        waiter = self._sync_waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        self._sync_waiter = None
        self._sync_options_by_id.pop(self._sync_id, None)

    async def _handle_connection_online(self, event: ConnectionOnlineEvent) -> None:
        self._connection_epoch = event.epoch
        _LOGGER.debug("Yeelight Pro session connection online: epoch=%s auto_sync=%s", event.epoch, self._auto_sync)
        if not self._auto_sync:
            return
        try:
            await self._set_session_state(GatewaySessionState.RECOVERING)
            await self._begin_sync(self._sync_options.to_request())
        except Exception as exc:
            self._fail_ready(exc)
            await self._set_session_state(GatewaySessionState.DISCONNECTED, exc)

    async def _handle_connection_lost(self, event: ConnectionLostEvent) -> None:
        if event.epoch != self._connection_epoch:
            _LOGGER.debug("Dropping stale connection lost event for epoch %s", event.epoch)
            return
        _LOGGER.debug(
            "Yeelight Pro session connection lost: epoch=%s state=%s error=%s",
            event.epoch,
            self.session_state,
            repr(event.error),
        )
        self._clear_ready()
        self._cancel_sync_timeout()
        if self.session_state == GatewaySessionState.CLOSING:
            return
        error = event.error or YeelightProError("gateway connection closed")
        self._fail_sync(error)
        self._fail_ready(error)
        await self._set_session_state(
            GatewaySessionState.DISCONNECTED,
            error,
        )

    async def _refresh_node(
        self,
        node_id: str | int,
        node_type: int | None,
        *,
        request_generations: Mapping[str, int] | None = None,
    ) -> JSONDict:
        _LOGGER.debug("Yeelight Pro refreshing node: node_id=%s node_type=%s", node_id, node_type)
        if _is_mesh_group_node(node_type):
            result = await self._get_group(node_id)
            _LOGGER.debug(
                "Yeelight Pro refresh group response: node_id=%s summary=%s",
                node_id,
                _message_summary(result),
            )
            await self.device_state_ref.ask(
                ApplyGroupsCommand(
                    payload=result,
                    reason=StateChangeReason.NODE_REFRESH,
                    request_generations=None if request_generations is None else {node_id: request_generations},
                )
            )
            return result
        result = await self._get_node(node_id)
        _LOGGER.debug("Yeelight Pro refresh node response: node_id=%s summary=%s", node_id, _message_summary(result))
        await self.device_state_ref.ask(
            ApplyPropertiesCommand(
                payload=result,
                reason=StateChangeReason.NODE_REFRESH,
                request_generations=None if request_generations is None else {node_id: request_generations},
            )
        )
        return result

    async def _handle_rpc_push(self, event: RpcPushEvent) -> None:
        if event.epoch != self._connection_epoch:
            _LOGGER.debug("Dropping stale RPC push for epoch %s", event.epoch)
            return
        message = event.message
        method = message.get("method")
        _LOGGER.debug(
            "Yeelight Pro RPC push dispatch: epoch=%s state=%s summary=%s",
            event.epoch,
            self.session_state,
            _message_summary(message),
        )
        if method == GatewayMethod.POST_PROP:
            applied = await self.device_state_ref.ask(
                ApplyPropertiesCommand(payload=message, reason=StateChangeReason.PROPERTY_PUSH)
            )
            if applied.full_property_coverage and self.session_state == GatewaySessionState.WAITING_FULL_PROP:
                self.last_full_sync_at = datetime.now(UTC)
                self.last_full_sync_source = FullSyncSource.PUSH
                try:
                    await self._finish_sync(self._sync_options_by_id.get(self._sync_id, self._sync_options))
                except Exception as exc:
                    self._fail_sync(exc)
                    self._fail_ready(exc)
                    raise
        elif method == GatewayMethod.POST_TOPOLOGY:
            await self.device_state_ref.ask(
                ApplyGenericStateMessageCommand(payload=message, reason=StateChangeReason.TOPOLOGY_PUSH)
            )
            await self._begin_full_property_wait(self._sync_options_by_id.get(self._sync_id, self._sync_options))
        else:
            await self.device_state_ref.ask(
                ApplyGenericStateMessageCommand(payload=message, reason=StateChangeReason.GENERIC_PUSH)
            )

        for gateway_event in iter_gateway_events(message):
            await self._notify_gateway_event(GatewayEventReceived(gateway_event))

    async def _set_session_state(
        self,
        state: GatewaySessionState,
        error: BaseException | None = None,
    ) -> None:
        previous = self.session_state
        if previous == state and error is None:
            return
        self.session_state = state
        _LOGGER.debug("Yeelight Pro session state changed: %s -> %s error=%s", previous, state, repr(error))
        event = SessionStatusChanged(previous=previous, current=state, error=error)
        await self.device_state_ref.tell(event)
        if state in {GatewaySessionState.CLOSING, GatewaySessionState.DISCONNECTED}:
            self._fail_ready(error or YeelightProError("gateway connection closed"))
        for listener in list(self._status_listeners):
            _schedule_listener(listener, event)

    async def _notify_gateway_event(self, event: GatewayEventReceived) -> None:
        for listener in list(self._event_listeners):
            _schedule_listener(listener, event)

    def _cancel_sync_timeout(self) -> None:
        if self._sync_timeout_task is not None:
            self._sync_timeout_task.cancel()
            self._sync_timeout_task = None

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


def _id_payload(item_id: str | int | None) -> Mapping[str, Any] | None:
    if item_id is None:
        return None
    return {"params": {"id": item_id}}


def _message_summary(message: Mapping[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key in ("id", "method", "result", "data"):
        if key in message:
            summary[key] = message.get(key)
    nodes = message.get("nodes")
    if isinstance(nodes, list):
        summary["node_count"] = len(nodes)
        summary["nodes"] = tuple(_node_summary(item) for item in nodes[:_MAX_LOG_ITEMS] if isinstance(item, Mapping))
    groups = message.get("groups")
    if isinstance(groups, list):
        summary["group_count"] = len(groups)
        summary["groups"] = tuple(_node_summary(item) for item in groups[:_MAX_LOG_ITEMS] if isinstance(item, Mapping))
    return summary


def _node_summary(item: Mapping[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {"id": item.get("id")}
    for key in ("nt", "type"):
        if key in item:
            summary[key] = item.get(key)
    params = item.get("params")
    if isinstance(params, Mapping):
        summary["params"] = dict(params)
    return summary


def _is_mesh_group_node(node_type: int | None) -> bool:
    return node_type == NodeType.MESH_GROUP


async def _call_listener(listener: Callable[..., Any], *args: Any) -> None:
    try:
        result = listener(*args)
        if inspect.isawaitable(result):
            await result
    except Exception:  # noqa: BLE001 - HA boundary listeners must not kill the actor.
        _LOGGER.exception("Yeelight Pro session listener failed")


def _schedule_listener(listener: Callable[..., Any], *args: Any) -> None:
    create_actor_task(
        _call_listener(listener, *args),
        name="yeelight-pro-session-listener",
    )
