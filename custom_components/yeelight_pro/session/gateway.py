from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import datetime
from typing import Any

from ..gateway.commands import MotorAction, NodeCommand, NodeSet, motor_adjust_action
from ..gateway.const import DEFAULT_MESH_NODE_TYPE, GATEWAY_CONTROL_PORT
from ..gateway.devices.base import Device
from ..gateway.devices.factory import create_device
from ..gateway.events import GatewayEvent, iter_gateway_events
from ..gateway.protocol import GatewayMethod
from ..gateway.topology import NodeId, TopologyNode
from ..gateway.updates import PropertyChange
from .actor import ActorRef
from .connection import (
    CloseConnectionCommand,
    ConnectionActor,
    GatewayRpcRequest,
    StartConnectionCommand,
)
from .events import FullSyncSource, GatewayEventReceived, SessionEvent, VisibleStateChanged
from .motor import MOTOR_TRACKING_TTL
from .rpc import GatewayRPC
from .runtime import (
    DEFAULT_STATE_DEADLINE,
    DEFAULT_STATE_READBACK_DELAY,
    GatewaySession,
)
from .status import GatewaySessionState

JSONDict = dict[str, Any]
WriteCallback = Callable[[], None]
EventListener = Callable[[GatewayEvent], Awaitable[None] | None]
PropertyListener = Callable[[PropertyChange], Awaitable[None] | None]
StateListener = Callable[[VisibleStateChanged], Awaitable[None] | None]
SessionListener = Callable[[SessionEvent], Awaitable[None] | None]


class YeelightProGateway:
    """Public facade over the gateway connection and serialized session runtime."""

    def __init__(
        self,
        host: str,
        *,
        port: int = GATEWAY_CONTROL_PORT,
        request_timeout: float = 5.0,
        reconnect_delay: float = 2.0,
        set_prop_batch_delay: float = 0.01,
        state_readback_delay: float = DEFAULT_STATE_READBACK_DELAY,
        state_deadline: float = DEFAULT_STATE_DEADLINE,
        motor_tracking_ttl: float = MOTOR_TRACKING_TTL,
        rpc: GatewayRPC | None = None,
    ) -> None:
        self._rpc = rpc or GatewayRPC(
            host,
            port=port,
            request_timeout=request_timeout,
            reconnect_delay=reconnect_delay,
        )
        self._connection = ConnectionActor(self._rpc)
        self._connection_ref = ActorRef(self._connection)
        self._connection.bind_push_listener(self._connection_ref)
        self._session = GatewaySession(
            connection_ref=self._connection_ref,
            set_prop_batch_delay=set_prop_batch_delay,
            state_readback_delay=state_readback_delay,
            state_deadline=state_deadline,
            motor_tracking_ttl=motor_tracking_ttl,
        )
        self._connection.set_session_sink(self._session.ref.tell)

    async def __aenter__(self) -> YeelightProGateway:
        await self.connect()
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.close()

    @property
    def is_connected(self) -> bool:
        return self._connection.is_connected

    @property
    def last_disconnect_error(self) -> BaseException | None:
        return self._connection.last_disconnect_error

    @property
    def session_state(self) -> GatewaySessionState:
        return self._session.session_state

    @property
    def last_full_sync_at(self) -> datetime | None:
        return self._session.last_full_sync_at

    @property
    def last_full_sync_source(self) -> FullSyncSource | None:
        return self._session.last_full_sync_source

    @property
    def full_prop_timeout(self) -> float:
        return self._session.full_prop_timeout

    @full_prop_timeout.setter
    def full_prop_timeout(self, value: float) -> None:
        self._session.full_prop_timeout = value

    async def start(
        self,
        *,
        include_groups: bool = False,
        include_rooms: bool = False,
        include_scenes: bool = False,
    ) -> None:
        await self._session.configure_auto_sync(
            include_groups=include_groups,
            include_rooms=include_rooms,
            include_scenes=include_scenes,
        )
        await self._connection_ref.ask(StartConnectionCommand(connection_ref=self._connection_ref))
        await self._session.wait_ready()

    async def stop(self) -> None:
        await self.close()

    async def connect(self) -> None:
        await self._session.disable_auto_sync()
        await self._session.connect()

    async def reconnect(self) -> None:
        await self._session.connect()

    async def close(self) -> None:
        if self._session.closed:
            return
        await self._session.drain_writes()
        await self._session.disable_auto_sync()
        await self._session.set_session_state(GatewaySessionState.CLOSING)
        await self._connection_ref.ask(CloseConnectionCommand())
        await self._connection.shutdown()
        await self._session.set_session_state(
            GatewaySessionState.DISCONNECTED,
            self._connection.last_disconnect_error,
        )
        await self._session.close()

    async def wait_closed(self) -> None:
        await self._connection.wait_closed()

    async def sync(
        self,
        *,
        include_groups: bool = False,
        include_rooms: bool = False,
        include_scenes: bool = False,
    ) -> None:
        await self._session.sync(
            include_groups=include_groups,
            include_rooms=include_rooms,
            include_scenes=include_scenes,
        )

    async def request(
        self,
        method: str,
        payload: Mapping[str, Any] | None = None,
        *,
        on_written: WriteCallback | None = None,
        timeout: float | None = None,
    ) -> JSONDict:
        return await self._connection_ref.ask(
            GatewayRpcRequest(method=method, payload=payload, on_written=on_written, timeout=timeout)
        )

    async def get_topology(self) -> JSONDict:
        return await self.request(GatewayMethod.GET_TOPOLOGY)

    async def get_node(self, node_id: NodeId) -> JSONDict:
        return await self.request(GatewayMethod.GET_NODE, _id_payload(node_id))

    async def get_all_nodes(self) -> JSONDict:
        return await self.request(GatewayMethod.GET_NODE, _id_payload(0))

    async def readback_node(self, node_id: NodeId) -> JSONDict:
        return await self._session.read_node(node_id)

    async def get_group(self, group_id: NodeId | None = 0) -> JSONDict:
        return await self.request(GatewayMethod.GET_GROUP, _id_payload(group_id))

    async def get_room(self, room_id: NodeId | None = 0) -> JSONDict:
        return await self.request(GatewayMethod.GET_ROOM, _id_payload(room_id))

    async def get_scene(self, scene_id: NodeId | None = 0) -> JSONDict:
        return await self.request(GatewayMethod.GET_SCENE, _id_payload(scene_id))

    async def _send_node_commands(
        self,
        commands: Iterable[NodeCommand | NodeSet],
    ) -> JSONDict:
        return await self._session.submit_commands(commands)

    async def set_scenes(self, scenes: Iterable[Mapping[str, Any]]) -> JSONDict:
        payload = [dict(scene) for scene in scenes]
        if not payload:
            raise ValueError("set_scenes requires at least one scene command")
        return await self.request(GatewayMethod.SET_PROP, {"scenes": payload})

    async def set_event(self, events: Iterable[Mapping[str, Any]]) -> JSONDict:
        payload = [dict(event) for event in events]
        if not payload:
            raise ValueError("set_event requires at least one event payload")
        return await self.request(GatewayMethod.SET_EVENT, {"nodes": payload})

    async def send_node_command(
        self,
        command: NodeCommand | NodeSet,
    ) -> JSONDict:
        return await self._send_node_commands([command])

    async def set_node_props(
        self,
        node_id: NodeId,
        props: Mapping[str, Any],
        *,
        nt: int = DEFAULT_MESH_NODE_TYPE,
        duration: int | None = None,
    ) -> JSONDict:
        return await self._send_node_commands([NodeCommand(id=node_id, nt=nt, props=props, duration=duration)])

    async def motor_adjust(
        self,
        node_id: NodeId,
        action_type: MotorAction | str,
        *,
        nt: int = DEFAULT_MESH_NODE_TYPE,
    ) -> JSONDict:
        return await self.send_node_command(NodeCommand(id=node_id, nt=nt, action=motor_adjust_action(action_type)))

    async def set_curtain_position(
        self,
        node_id: NodeId,
        position: int,
        *,
        nt: int = DEFAULT_MESH_NODE_TYPE,
        duration: int | None = None,
    ) -> JSONDict:
        _validate_range("position", position, 0, 100)
        return await self.set_node_props(node_id, {"tp": position}, nt=nt, duration=duration)

    async def stop_curtain(self, node_id: NodeId, *, nt: int = DEFAULT_MESH_NODE_TYPE) -> JSONDict:
        return await self.motor_adjust(node_id, MotorAction.PAUSE, nt=nt)

    def device(self, node_id: NodeId) -> Device | None:
        snapshot = self.visible_node(node_id)
        return None if snapshot is None else create_device(snapshot, self)

    def devices(self) -> list[Device]:
        return [create_device(snapshot, self) for snapshot in self.visible_nodes()]

    def visible_node(self, node_id: NodeId) -> TopologyNode | None:
        return self._session.visible_node(node_id)

    def visible_nodes(self) -> list[TopologyNode]:
        return self._session.visible_nodes()

    def room_records(self) -> tuple[Mapping[str, Any], ...]:
        return self._session.room_records()

    def room_id_for_node(self, node: TopologyNode) -> NodeId | None:
        return self._session.room_id_for_node(node)

    def room_name(self, room_id: NodeId | None) -> str | None:
        return self._session.room_name(room_id)

    def is_full_property_snapshot(self, message: Mapping[str, Any]) -> bool:
        return self._session.is_full_property_snapshot(message)

    def snapshot_diagnostics(self) -> dict[str, Any]:
        return self._session.snapshot_diagnostics()

    def has_pending_write(self, node_id: NodeId, props: Iterable[str] | None = None) -> bool:
        return self._session.has_pending_write(node_id, props)

    def write_diagnostics(self) -> dict[str, Any]:
        return self._session.write_diagnostics()

    def motor_tracking_diagnostics(self) -> dict[str, Any]:
        return self._session.motor_diagnostics()

    def add_event_listener(self, listener: EventListener) -> Callable[[], None]:
        return self._session.add_gateway_event_listener(_wrap_gateway_event_listener(listener))

    def add_property_listener(self, listener: PropertyListener) -> Callable[[], None]:
        return self._session.add_property_listener(listener)

    def add_state_listener(self, listener: StateListener) -> Callable[[], None]:
        return self._session.add_state_listener(listener)

    def add_session_listener(self, listener: SessionListener) -> Callable[[], None]:
        removers = [
            self._session.add_status_listener(_wrap_session_listener(listener)),
            self._session.add_state_listener(_wrap_session_listener(listener)),
            self._session.add_gateway_event_listener(_wrap_session_listener(listener)),
        ]

        def remove() -> None:
            for remover in removers:
                remover()

        return remove

    @staticmethod
    def events_from_message(message: Mapping[str, Any]) -> list[GatewayEvent]:
        return list(iter_gateway_events(message))


def _wrap_gateway_event_listener(listener: EventListener) -> Callable[[GatewayEventReceived], Awaitable[None]]:
    async def _wrapped(event: GatewayEventReceived) -> None:
        await _call_listener(listener, event.event)

    return _wrapped


def _wrap_session_listener(listener: SessionListener) -> Callable[[SessionEvent], Awaitable[None]]:
    async def _wrapped(event: SessionEvent) -> None:
        await _call_listener(listener, event)

    return _wrapped


async def _call_listener(listener: Callable[..., Any], *args: Any) -> None:
    result = listener(*args)
    if inspect.isawaitable(result):
        await result


def _validate_range(name: str, value: int, minimum: int, maximum: int) -> None:
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")


def _id_payload(item_id: NodeId | None) -> Mapping[str, Any] | None:
    if item_id is None:
        return None
    return {"params": {"id": item_id}}
