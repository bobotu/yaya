from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from ...core.const import GATEWAY_CONTROL_PORT
from ...core.protocol import GatewayMethod
from ..actors import ActorRef, ConnectionActor, DeviceStateActor, SessionActor
from ..messages import (
    ApplyMotorStopCommand,
    ApplyMotorTargetsCommand,
    ApplyOptimisticPropsCommand,
    CloseConnectionCommand,
    ConfigureAutoSyncCommand,
    ConnectSessionCommand,
    DisableAutoSyncCommand,
    FullSyncSource,
    GatewayRpcRequest,
    RefreshNodeCommand,
    RefreshNodeRequestedEvent,
    SetSessionStateCommand,
    StartConnectionCommand,
    SyncSessionCommand,
)
from ..model.motor import MotorTargetIntent
from ..model.optimistic import OPTIMISTIC_STATE_TTL
from ..model.status import GatewaySessionState
from ..transport import GatewayRPC

JSONDict = dict[str, Any]


class YeelightProRuntime:
    """Owns the singleton actors for one Yeelight Pro gateway session."""

    def __init__(
        self,
        host: str,
        *,
        port: int = GATEWAY_CONTROL_PORT,
        request_timeout: float = 5.0,
        reconnect_delay: float = 2.0,
        optimistic_state_ttl: float = OPTIMISTIC_STATE_TTL,
        rpc: GatewayRPC | None = None,
    ) -> None:
        self.rpc = rpc or GatewayRPC(
            host,
            port=port,
            request_timeout=request_timeout,
            reconnect_delay=reconnect_delay,
        )
        self.connection = ConnectionActor(self.rpc)
        self.connection_ref = ActorRef(self.connection)
        self.connection.bind_push_listener(self.connection_ref)
        self.state = DeviceStateActor(ttl=optimistic_state_ttl)
        self.state_ref = ActorRef(self.state)
        self.session = SessionActor(connection_ref=self.connection_ref, device_state_ref=self.state_ref)
        self.session_ref = ActorRef(self.session)
        self.connection.set_session_sink(self.session_ref.tell)
        self.state.set_refresh_requester(self._handle_refresh_requested)

    @property
    def is_connected(self) -> bool:
        return self.connection.is_connected

    @property
    def last_disconnect_error(self) -> BaseException | None:
        return self.connection.last_disconnect_error

    @property
    def session_state(self) -> GatewaySessionState:
        return self.session.session_state

    @property
    def last_full_sync_at(self) -> datetime | None:
        return self.session.last_full_sync_at

    @property
    def last_full_sync_source(self) -> FullSyncSource | None:
        return self.session.last_full_sync_source

    @property
    def full_prop_timeout(self) -> float:
        return self.session.full_prop_timeout

    @full_prop_timeout.setter
    def full_prop_timeout(self, value: float) -> None:
        self.session.full_prop_timeout = value

    async def start(
        self,
        *,
        include_groups: bool = False,
        include_rooms: bool = False,
        include_scenes: bool = False,
    ) -> None:
        await self.session_ref.ask(
            ConfigureAutoSyncCommand(
                include_groups=include_groups,
                include_rooms=include_rooms,
                include_scenes=include_scenes,
            )
        )
        await self.connection_ref.ask(StartConnectionCommand(connection_ref=self.connection_ref))
        await self.session.wait_ready()

    async def connect(self) -> None:
        await self.session_ref.ask(DisableAutoSyncCommand())
        await self.session_ref.ask(ConnectSessionCommand())

    async def close(self) -> None:
        await self.session_ref.ask(DisableAutoSyncCommand())
        await self.session_ref.ask(SetSessionStateCommand(GatewaySessionState.CLOSING))
        await self.connection_ref.ask(CloseConnectionCommand())
        await self.connection.shutdown()
        await self.session_ref.ask(
            SetSessionStateCommand(GatewaySessionState.DISCONNECTED, self.connection.last_disconnect_error)
        )
        await self.session.close()
        await self.state.close()

    async def wait_closed(self) -> None:
        await self.connection.wait_closed()

    async def sync(
        self,
        *,
        include_groups: bool = False,
        include_rooms: bool = False,
        include_scenes: bool = False,
    ) -> None:
        waiter = await self.session_ref.ask(
            SyncSessionCommand(
                include_groups=include_groups,
                include_rooms=include_rooms,
                include_scenes=include_scenes,
            )
        )
        await waiter

    async def request(
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

    async def apply_optimistic_props(self, props_by_node: Mapping[str | int, Mapping[str, Any]]) -> None:
        await self.state_ref.ask(ApplyOptimisticPropsCommand(props_by_node))

    async def apply_motor_targets(self, targets: tuple[MotorTargetIntent, ...]) -> None:
        await self.state_ref.ask(ApplyMotorTargetsCommand(targets))

    async def apply_motor_stop(self, node_ids: tuple[str | int, ...]) -> None:
        await self.state_ref.ask(ApplyMotorStopCommand(node_ids))

    async def get_topology(self) -> JSONDict:
        return await self.request(GatewayMethod.GET_TOPOLOGY)

    async def get_node(self, node_id: str | int) -> JSONDict:
        return await self.request(GatewayMethod.GET_NODE, _id_payload(node_id))

    async def get_all_nodes(self) -> JSONDict:
        return await self.request(GatewayMethod.GET_NODE, _id_payload(0))

    async def refresh_node(self, node_id: str | int) -> JSONDict:
        return await self.session_ref.ask(RefreshNodeCommand(node_id=node_id))

    async def get_group(self, group_id: str | int | None = 0) -> JSONDict:
        return await self.request(GatewayMethod.GET_GROUP, _id_payload(group_id))

    async def get_room(self, room_id: str | int | None = 0) -> JSONDict:
        return await self.request(GatewayMethod.GET_ROOM, _id_payload(room_id))

    async def get_scene(self, scene_id: str | int | None = 0) -> JSONDict:
        return await self.request(GatewayMethod.GET_SCENE, _id_payload(scene_id))

    async def _handle_refresh_requested(self, event: RefreshNodeRequestedEvent) -> None:
        await self.session_ref.ask(RefreshNodeCommand(node_id=event.node_id))


def _id_payload(item_id: str | int | None) -> Mapping[str, Any] | None:
    if item_id is None:
        return None
    return {"params": {"id": item_id}}
