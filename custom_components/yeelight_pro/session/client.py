from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextlib import suppress
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from ..core.commands import MotorAction, NodeCommand, NodeSet, motor_adjust_action
from ..core.const import DEFAULT_MESH_NODE_TYPE, GATEWAY_CONTROL_PORT
from ..core.devices.base import Device
from ..core.devices.factory import create_device
from ..core.events import GatewayEvent, iter_gateway_events
from ..core.updates import PropertyChange
from .rpc import GatewayRPC
from .state import GatewayState

JSONDict = dict[str, Any]
EventListener = Callable[[GatewayEvent], Awaitable[None] | None]
PropertyListener = Callable[[PropertyChange], Awaitable[None] | None]
StateListener = Callable[[Mapping[str, Any]], Awaitable[None] | None]


class GatewaySessionState(StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    WAITING_TOPOLOGY = "waiting_topology"
    WAITING_FULL_PROP = "waiting_full_prop"
    READY = "ready"
    RECOVERING = "recovering"
    CLOSING = "closing"


class YeelightProGateway:
    """High-level gateway facade over the low-level RPC client."""

    def __init__(
        self,
        host: str,
        *,
        port: int = GATEWAY_CONTROL_PORT,
        request_timeout: float = 5.0,
        reconnect_delay: float = 2.0,
        rpc: GatewayRPC | None = None,
    ) -> None:
        self.rpc = rpc or GatewayRPC(
            host,
            port=port,
            request_timeout=request_timeout,
            reconnect_delay=reconnect_delay,
        )
        self.state = GatewayState()
        self.session_state = GatewaySessionState.DISCONNECTED
        self.last_full_sync_at: datetime | None = None
        self.last_full_sync_source: str | None = None
        self.full_prop_timeout = 5.0
        self._event_listeners: list[EventListener] = []
        self._property_listeners: list[PropertyListener] = []
        self._state_listeners: list[StateListener] = []
        self._full_prop_event = asyncio.Event()
        self.rpc.add_push_listener(self._handle_push)

    async def __aenter__(self) -> YeelightProGateway:
        await self.connect()
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.close()

    @property
    def is_connected(self) -> bool:
        return self.rpc.is_connected

    @property
    def last_disconnect_error(self) -> BaseException | None:
        return self.rpc.last_disconnect_error

    async def connect(self) -> None:
        self.session_state = GatewaySessionState.CONNECTING
        await self.rpc.connect()
        self.session_state = GatewaySessionState.WAITING_TOPOLOGY

    async def close(self) -> None:
        self.session_state = GatewaySessionState.CLOSING
        await self.rpc.close()
        self.session_state = GatewaySessionState.DISCONNECTED

    async def run_forever(self) -> None:
        await self.rpc.run_forever()

    async def wait_closed(self) -> None:
        await self.rpc.wait_closed()

    async def sync(
        self,
        *,
        include_groups: bool = False,
        include_rooms: bool = False,
        include_scenes: bool = False,
    ) -> None:
        """Read topology, then wait for the protocol-defined full property sync."""

        self.session_state = GatewaySessionState.WAITING_TOPOLOGY
        self._full_prop_event.clear()
        topology = await self.get_topology()
        self.state.apply_topology(topology)
        self.session_state = GatewaySessionState.WAITING_FULL_PROP

        try:
            await asyncio.wait_for(self._full_prop_event.wait(), timeout=self.full_prop_timeout)
        except TimeoutError:
            self.session_state = GatewaySessionState.RECOVERING
            self.state.apply_properties(await self.get_all_nodes())
            self.last_full_sync_at = datetime.now(UTC)
            self.last_full_sync_source = "poll"

        if include_groups:
            self.state.apply_groups(await self.get_group())
        if include_rooms:
            self.state.apply_rooms(await self.get_room())
        if include_scenes:
            self.state.apply_scenes(await self.get_scene())
        self.session_state = GatewaySessionState.READY

    async def request(
        self,
        method: str,
        payload: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> JSONDict:
        return await self.rpc.request(method, payload, timeout=timeout)

    async def get_topology(self) -> JSONDict:
        return await self.request("gateway_get.topology")

    async def get_node(self, node_id: str | int) -> JSONDict:
        return await self.request("gateway_get.node", _id_payload(node_id))

    async def get_all_nodes(self) -> JSONDict:
        return await self.request("gateway_get.node", _id_payload(0))

    async def refresh_node(self, node_id: str | int) -> JSONDict:
        result = await self.get_node(node_id)
        self.state.apply_properties(result)
        return result

    async def get_group(self, group_id: str | int | None = 0) -> JSONDict:
        return await self.request("gateway_get.group", _id_payload(group_id))

    async def get_room(self, room_id: str | int | None = 0) -> JSONDict:
        return await self.request("gateway_get.room", _id_payload(room_id))

    async def get_scene(self, scene_id: str | int | None = 0) -> JSONDict:
        return await self.request("gateway_get.scene", _id_payload(scene_id))

    async def set_prop(self, commands: Iterable[NodeCommand | NodeSet | Mapping[str, Any]]) -> JSONDict:
        payload = []
        for command in commands:
            if isinstance(command, (NodeCommand, NodeSet)):
                payload.append(command.to_payload())
            else:
                payload.append(dict(command))

        if not payload:
            raise ValueError("set_prop requires at least one node command")

        return await self.request("gateway_set.prop", {"nodes": payload})

    async def set_scenes(self, scenes: Iterable[Mapping[str, Any]]) -> JSONDict:
        payload = [dict(scene) for scene in scenes]
        if not payload:
            raise ValueError("set_scenes requires at least one scene command")
        return await self.request("gateway_set.prop", {"scenes": payload})

    async def set_event(self, events: Iterable[Mapping[str, Any]]) -> JSONDict:
        payload = [dict(event) for event in events]
        if not payload:
            raise ValueError("set_event requires at least one event payload")
        return await self.request("gateway_set.event", {"nodes": payload})

    async def send_node_command(self, command: NodeCommand | NodeSet) -> JSONDict:
        return await self.set_prop([command])

    async def set_node_props(
        self,
        node_id: str | int,
        props: Mapping[str, Any],
        *,
        nt: int = DEFAULT_MESH_NODE_TYPE,
        duration: int | None = None,
    ) -> JSONDict:
        return await self.send_node_command(NodeCommand(id=node_id, nt=nt, props=props, duration=duration))

    async def motor_adjust(
        self,
        node_id: str | int,
        action_type: MotorAction | str,
        *,
        nt: int = DEFAULT_MESH_NODE_TYPE,
    ) -> JSONDict:
        return await self.send_node_command(NodeCommand(id=node_id, nt=nt, action=motor_adjust_action(action_type)))

    async def set_curtain_position(
        self,
        node_id: str | int,
        position: int,
        *,
        nt: int = DEFAULT_MESH_NODE_TYPE,
        duration: int | None = None,
    ) -> JSONDict:
        _validate_range("position", position, 0, 100)
        return await self.set_node_props(node_id, {"tp": position}, nt=nt, duration=duration)

    async def stop_curtain(self, node_id: str | int, *, nt: int = DEFAULT_MESH_NODE_TYPE) -> JSONDict:
        return await self.motor_adjust(node_id, MotorAction.PAUSE, nt=nt)

    def device(self, node_id: str | int) -> Device | None:
        snapshot = self.state.nodes.get(node_id)
        if snapshot is None:
            return None
        return create_device(snapshot, self)

    def devices(self) -> list[Device]:
        return [create_device(snapshot, self) for snapshot in self.state.nodes.values()]

    def add_event_listener(self, listener: EventListener) -> Callable[[], None]:
        self._event_listeners.append(listener)

        def remove() -> None:
            with suppress(ValueError):
                self._event_listeners.remove(listener)

        return remove

    def add_property_listener(self, listener: PropertyListener) -> Callable[[], None]:
        self._property_listeners.append(listener)

        def remove() -> None:
            with suppress(ValueError):
                self._property_listeners.remove(listener)

        return remove

    def add_state_listener(self, listener: StateListener) -> Callable[[], None]:
        self._state_listeners.append(listener)

        def remove() -> None:
            with suppress(ValueError):
                self._state_listeners.remove(listener)

        return remove

    async def _handle_push(self, message: Mapping[str, Any]) -> None:
        changes = self.state.apply_properties(message) if message.get("method") == "gateway_post.prop" else []
        if message.get("method") == "gateway_post.prop" and self.state.full_property_coverage(message):
            self.last_full_sync_at = datetime.now(UTC)
            self.last_full_sync_source = "push"
            self._full_prop_event.set()
        state_updated = bool(changes)
        if changes:
            for change in changes:
                for listener in list(self._property_listeners):
                    result = listener(change)
                    if inspect.isawaitable(result):
                        await result
        else:
            self.state.apply_message(message)
            state_updated = message.get("method") in {"gateway_post.topology", "gateway_post.prop"}
            if message.get("method") == "gateway_post.topology":
                self.session_state = GatewaySessionState.WAITING_FULL_PROP

        if state_updated:
            for listener in list(self._state_listeners):
                result = listener(message)
                if inspect.isawaitable(result):
                    await result

        for event in iter_gateway_events(message):
            for listener in list(self._event_listeners):
                result = listener(event)
                if inspect.isawaitable(result):
                    await result

    @staticmethod
    def events_from_message(message: Mapping[str, Any]) -> list[GatewayEvent]:
        return list(iter_gateway_events(message))


YeelightProGatewayClient = YeelightProGateway


def _id_payload(item_id: str | int | None) -> Mapping[str, Any] | None:
    if item_id is None:
        return None
    return {"params": {"id": item_id}}


def _validate_range(name: str, value: int, minimum: int, maximum: int) -> None:
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
