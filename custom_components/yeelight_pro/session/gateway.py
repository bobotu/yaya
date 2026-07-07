from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeAlias

from ..core.commands import MotorAction, NodeCommand, NodeSet, motor_adjust_action
from ..core.const import DEFAULT_MESH_NODE_TYPE, GATEWAY_CONTROL_PORT
from ..core.devices.base import Device
from ..core.devices.factory import create_device
from ..core.events import GatewayEvent, iter_gateway_events
from ..core.exceptions import ProtocolError
from ..core.protocol import GatewayMethod
from ..core.topology import TopologyNode
from ..core.updates import PropertyChange
from .actors import Actor, ActorRef, create_actor_task
from .messages import (
    FullSyncSource,
    GatewayEventReceived,
    SessionEvent,
    StateSnapshotChanged,
)
from .model import (
    COMMAND_INTENT_TTL,
    MOTOR_CURRENT_ANGLE_PROP,
    MOTOR_CURRENT_POSITION_PROP,
    MOTOR_TARGET_ANGLE_PROP,
    MOTOR_TARGET_POSITION_PROP,
    GatewaySessionState,
    MotorTargetIntent,
)
from .runtime.gateway import YeelightProRuntime
from .transport import GatewayRPC

JSONDict = dict[str, Any]
WriteCallback = Callable[[], None]
EventListener = Callable[[GatewayEvent], Awaitable[None] | None]
PropertyListener = Callable[[PropertyChange], Awaitable[None] | None]
StateListener = Callable[[StateSnapshotChanged], Awaitable[None] | None]
SessionListener = Callable[[SessionEvent], Awaitable[None] | None]


class YeelightProGateway:
    """Public gateway facade over transport, session, and device-state actors."""

    def __init__(
        self,
        host: str,
        *,
        port: int = GATEWAY_CONTROL_PORT,
        request_timeout: float = 5.0,
        reconnect_delay: float = 2.0,
        command_intent_ttl: float = COMMAND_INTENT_TTL,
        set_prop_batch_delay: float = 0.01,
        rpc: GatewayRPC | None = None,
    ) -> None:
        self._runtime = YeelightProRuntime(
            host,
            port=port,
            request_timeout=request_timeout,
            reconnect_delay=reconnect_delay,
            command_intent_ttl=command_intent_ttl,
            rpc=rpc,
        )
        self._state_actor = self._runtime.state
        self._session = self._runtime.session
        self.state = self._state_actor.state
        self.command_intent_ttl = command_intent_ttl
        self._set_prop_batcher = _SetPropBatcher(self, delay=set_prop_batch_delay)

    async def __aenter__(self) -> YeelightProGateway:
        await self.connect()
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.close()

    @property
    def is_connected(self) -> bool:
        return self._runtime.is_connected

    @property
    def last_disconnect_error(self) -> BaseException | None:
        return self._runtime.last_disconnect_error

    @property
    def session_state(self) -> GatewaySessionState:
        return self._runtime.session_state

    @property
    def last_full_sync_at(self) -> datetime | None:
        return self._runtime.last_full_sync_at

    @property
    def last_full_sync_source(self) -> FullSyncSource | None:
        return self._runtime.last_full_sync_source

    @property
    def full_prop_timeout(self) -> float:
        return self._runtime.full_prop_timeout

    @full_prop_timeout.setter
    def full_prop_timeout(self, value: float) -> None:
        self._runtime.full_prop_timeout = value

    async def start(
        self,
        *,
        include_groups: bool = False,
        include_rooms: bool = False,
        include_scenes: bool = False,
    ) -> None:
        await self._runtime.start(
            include_groups=include_groups,
            include_rooms=include_rooms,
            include_scenes=include_scenes,
        )

    async def stop(self) -> None:
        await self.close()

    async def connect(self) -> None:
        await self._runtime.connect()

    async def reconnect(self) -> None:
        await self._runtime.reconnect()

    async def close(self) -> None:
        await self._set_prop_batcher.close()
        await self._runtime.close()

    async def wait_closed(self) -> None:
        await self._runtime.wait_closed()

    async def sync(
        self,
        *,
        include_groups: bool = False,
        include_rooms: bool = False,
        include_scenes: bool = False,
    ) -> None:
        await self._runtime.sync(
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
        return await self._runtime.request(method, payload, on_written=on_written, timeout=timeout)

    async def get_topology(self) -> JSONDict:
        return await self._runtime.get_topology()

    async def get_node(self, node_id: str | int) -> JSONDict:
        return await self._runtime.get_node(node_id)

    async def get_all_nodes(self) -> JSONDict:
        return await self._runtime.get_all_nodes()

    async def refresh_node(self, node_id: str | int) -> JSONDict:
        return await self._runtime.refresh_node(node_id)

    async def get_group(self, group_id: str | int | None = 0) -> JSONDict:
        return await self._runtime.get_group(group_id)

    async def get_room(self, room_id: str | int | None = 0) -> JSONDict:
        return await self._runtime.get_room(room_id)

    async def get_scene(self, scene_id: str | int | None = 0) -> JSONDict:
        return await self._runtime.get_scene(scene_id)

    async def _send_node_commands(
        self,
        commands: Iterable[NodeCommand | NodeSet],
        *,
        intent_props_by_node: Mapping[str | int, Mapping[str, Any]] | None = None,
    ) -> JSONDict:
        commands = tuple(commands)
        if not commands:
            raise ValueError("_send_node_commands requires at least one node command")

        return await self._set_prop_batcher.submit(commands, intent_props_by_node)

    async def _send_node_command_batch(self, requests: tuple[_PendingSetPropRequest, ...]) -> JSONDict:
        commands: list[NodeCommand | NodeSet] = []
        intent_props_by_node: dict[str | int, dict[str, Any]] = {}
        for request in requests:
            commands.extend(request.commands)
            if request.intent_props_by_node is None:
                continue
            for node_id, props in request.intent_props_by_node.items():
                intent_props_by_node.setdefault(node_id, {}).update(dict(props))

        payload = _batched_payload_from_commands(commands)
        response = await self.request(GatewayMethod.SET_PROP, {"nodes": payload})
        _raise_for_missing_write_ack(GatewayMethod.SET_PROP, response)
        targets, stops = _motor_tracking_from_payload(payload)
        if intent_props_by_node:
            await self._runtime.record_command_intents(
                intent_props_by_node,
                ttl_by_node=_intent_ttl_by_node(
                    commands,
                    intent_props_by_node,
                    base_ttl=self.command_intent_ttl,
                ),
                motor_targets=tuple(targets),
                motor_stops=tuple(stops),
            )
        elif targets or stops:
            await self._runtime.record_command_intents(
                {},
                motor_targets=tuple(targets),
                motor_stops=tuple(stops),
            )
        return response

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
        *,
        intent_props: Mapping[str, Any] | None = None,
    ) -> JSONDict:
        intent_props_by_node = {command.id: intent_props} if intent_props else None
        return await self._send_node_commands([command], intent_props_by_node=intent_props_by_node)

    async def set_node_props(
        self,
        node_id: str | int,
        props: Mapping[str, Any],
        *,
        nt: int = DEFAULT_MESH_NODE_TYPE,
        duration: int | None = None,
        intent_props: Mapping[str, Any] | None = None,
    ) -> JSONDict:
        intent_props_by_node = {node_id: intent_props} if intent_props else None
        return await self._send_node_commands(
            [NodeCommand(id=node_id, nt=nt, props=props, duration=duration)],
            intent_props_by_node=intent_props_by_node,
        )

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

    def visible_node(self, node_id: str | int) -> TopologyNode | None:
        return self._state_actor.visible_node(node_id)

    def visible_nodes(self) -> list[TopologyNode]:
        return self._state_actor.visible_nodes()

    def has_pending_intent(self, node_id: str | int, props: Iterable[str] | None = None) -> bool:
        return self._state_actor.has_pending(node_id, props)

    def intent_diagnostics(self) -> dict[str, Any]:
        return self._state_actor.diagnostics()

    def motor_tracking_diagnostics(self) -> dict[str, Any]:
        return self._state_actor.motor_diagnostics()

    def add_event_listener(self, listener: EventListener) -> Callable[[], None]:
        return self._session.add_gateway_event_listener(_wrap_gateway_event_listener(listener))

    def add_property_listener(self, listener: PropertyListener) -> Callable[[], None]:
        return self._state_actor.add_property_listener(_wrap_property_listener(listener))

    def add_state_listener(self, listener: StateListener) -> Callable[[], None]:
        return self._state_actor.add_state_listener(listener)

    def add_session_listener(self, listener: SessionListener) -> Callable[[], None]:
        removers = [
            self._session.add_status_listener(_wrap_session_listener(listener)),
            self._state_actor.add_state_listener(_wrap_session_listener(listener)),
            self._session.add_gateway_event_listener(_wrap_session_listener(listener)),
        ]

        def remove() -> None:
            for remover in removers:
                remover()

        return remove

    @staticmethod
    def events_from_message(message: Mapping[str, Any]) -> list[GatewayEvent]:
        return list(iter_gateway_events(message))


@dataclass(frozen=True)
class _PendingSetPropRequest:
    commands: tuple[NodeCommand | NodeSet, ...]
    intent_props_by_node: Mapping[str | int, Mapping[str, Any]] | None
    future: asyncio.Future[JSONDict]


@dataclass(frozen=True)
class _FlushSetPropBatch:
    pass


@dataclass(frozen=True)
class _DrainSetPropBatcher:
    pass


_SetPropBatcherMessage: TypeAlias = _PendingSetPropRequest | _FlushSetPropBatch | _DrainSetPropBatcher


class _SetPropBatcher(Actor[_SetPropBatcherMessage]):
    def __init__(self, gateway: YeelightProGateway, *, delay: float) -> None:
        super().__init__("yeelight-pro-set-prop-batcher")
        self._gateway = gateway
        self._ref: ActorRef[_SetPropBatcherMessage] = ActorRef(self)
        self._delay = max(0.0, delay)
        self._pending: list[_PendingSetPropRequest] = []
        self._flush_task: asyncio.Task[None] | None = None
        self._send_tail: asyncio.Task[None] | None = None

    async def submit(
        self,
        commands: tuple[NodeCommand | NodeSet, ...],
        intent_props_by_node: Mapping[str | int, Mapping[str, Any]] | None,
    ) -> JSONDict:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[JSONDict] = loop.create_future()
        await self._ref.ask(
            _PendingSetPropRequest(
                commands=commands,
                intent_props_by_node=intent_props_by_node,
                future=future,
            )
        )
        return await future

    async def close(self) -> None:
        if self.closed:
            return
        try:
            await self._ref.ask(_DrainSetPropBatcher())
        finally:
            await super().close()

    async def handle(self, message: _SetPropBatcherMessage) -> None:
        if isinstance(message, _PendingSetPropRequest):
            await self._submit(message)
            return
        if isinstance(message, _FlushSetPropBatch):
            self._flush_pending()
            return
        if isinstance(message, _DrainSetPropBatcher):
            await self._drain()
            return
        raise TypeError(f"unsupported set_prop batcher message: {type(message).__name__}")

    async def _submit(self, request: _PendingSetPropRequest) -> None:
        if self._pending and _batch_conflicts(self._pending, request):
            self._flush_pending()
        self._pending.append(request)
        await self._schedule_flush()

    async def _schedule_flush(self) -> None:
        if self._flush_task is not None and not self._flush_task.done():
            return
        if self._delay == 0:
            await self.defer(_FlushSetPropBatch())
            return
        self._flush_task = self.defer_later(
            self._delay,
            _FlushSetPropBatch(),
            name="yeelight-pro-set-prop-batcher-flush",
        )

    async def _drain(self) -> None:
        self._flush_pending()
        if self._send_tail is not None:
            await self._send_tail

    def _flush_pending(self) -> None:
        if self._flush_task is not None and not self._flush_task.done():
            self._flush_task.cancel()
        self._flush_task = None
        if not self._pending:
            return
        requests = tuple(self._pending)
        self._pending.clear()
        previous = self._send_tail if self._send_tail is not None and not self._send_tail.done() else None
        task = create_actor_task(
            self._send_batch_after(previous, requests),
            name="yeelight-pro-set-prop-batch-send",
        )
        self._send_tail = task

    async def _send_batch_after(
        self,
        previous: asyncio.Task[None] | None,
        requests: tuple[_PendingSetPropRequest, ...],
    ) -> None:
        if previous is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await previous
        await self._send_batch(requests)

    async def _send_batch(self, requests: tuple[_PendingSetPropRequest, ...]) -> None:
        try:
            response = await self._gateway._send_node_command_batch(requests)
        except Exception as exc:  # noqa: BLE001 - propagate RPC failure to every original caller.
            for request in requests:
                if not request.future.done():
                    request.future.set_exception(exc)
            return
        for request in requests:
            if not request.future.done():
                request.future.set_result(response)


def _wrap_gateway_event_listener(listener: EventListener) -> Callable[[GatewayEventReceived], Awaitable[None]]:
    async def _wrapped(event: GatewayEventReceived) -> None:
        await _call_listener(listener, event.event)

    return _wrapped


def _wrap_property_listener(listener: PropertyListener) -> Callable[[PropertyChange], Awaitable[None]]:
    async def _wrapped(change: PropertyChange) -> None:
        await _call_listener(listener, change)

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


def _motor_tracking_from_payload(
    payload: Iterable[Mapping[str, Any]],
) -> tuple[list[MotorTargetIntent], list[str | int]]:
    targets: list[MotorTargetIntent] = []
    stops: list[str | int] = []
    for item in payload:
        node_id = item.get("id")
        if isinstance(node_id, bool) or not isinstance(node_id, (str, int)):
            continue
        props = item.get("set")
        if isinstance(props, Mapping):
            position = _int_or_none(props.get(MOTOR_TARGET_POSITION_PROP))
            if position is not None:
                targets.append(
                    MotorTargetIntent(
                        node_id=node_id,
                        current_prop=MOTOR_CURRENT_POSITION_PROP,
                        target_prop=MOTOR_TARGET_POSITION_PROP,
                        target_value=position,
                    )
                )
            angle = _int_or_none(props.get(MOTOR_TARGET_ANGLE_PROP))
            if angle is not None:
                targets.append(
                    MotorTargetIntent(
                        node_id=node_id,
                        current_prop=MOTOR_CURRENT_ANGLE_PROP,
                        target_prop=MOTOR_TARGET_ANGLE_PROP,
                        target_value=angle,
                    )
                )
        action = item.get("action")
        if _is_motor_pause(action):
            stops.append(node_id)
    return targets, stops


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


def _batch_conflicts(existing: Iterable[_PendingSetPropRequest], new: _PendingSetPropRequest) -> bool:
    targets: dict[tuple[str, str], tuple[Any, tuple[Any, Any, Any]]] = {}
    for request in existing:
        for key, value in _set_prop_targets(request.commands):
            targets[key] = value
    for key, value in _set_prop_targets(new.commands):
        if key in targets and targets[key] != value:
            return True
    return False


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


def _is_motor_pause(action: object) -> bool:
    if not isinstance(action, Mapping):
        return False
    motor_adjust = action.get("motorAdjust")
    if not isinstance(motor_adjust, Mapping):
        return False
    return motor_adjust.get("type") == str(MotorAction.PAUSE)


def _intent_ttl_by_node(
    commands: Iterable[NodeCommand | NodeSet],
    intent_props_by_node: Mapping[str | int, Mapping[str, Any]],
    *,
    base_ttl: float,
) -> dict[str | int, float]:
    ttls: dict[str | int, float] = {}
    for command in commands:
        if command.id not in intent_props_by_node or command.duration is None:
            continue
        ttl = max(base_ttl, command.duration / 1000 + base_ttl)
        ttls[command.id] = max(ttls.get(command.id, 0.0), ttl)
    return ttls


def _raise_for_missing_write_ack(method: str, response: Mapping[str, Any]) -> None:
    if response.get("result") == "ok":
        return
    raise ProtocolError(f"{method} did not return a successful acknowledgement")


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _node_key(node_id: object) -> str | None:
    if isinstance(node_id, bool) or not isinstance(node_id, (str, int)):
        return None
    return str(node_id)
