from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextlib import suppress
from typing import Any

from ...core.protocol import list_payload
from ...core.topology import TopologyNode
from ...core.updates import PropertyChange
from ..messages import (
    AppliedPropertiesResult,
    ApplyGenericStateMessageCommand,
    ApplyGroupsCommand,
    ApplyMotorStopCommand,
    ApplyMotorTargetsCommand,
    ApplyOptimisticPropsCommand,
    ApplyPropertiesCommand,
    ApplyRoomsCommand,
    ApplyScenesCommand,
    ApplyTopologyCommand,
    AuthoritativeStateChangedEvent,
    DeviceStateActorMessage,
    ExpireMotorTrackingCommand,
    ExpireOptimisticStateCommand,
    RefreshNodeRequestedEvent,
    SessionStatusChanged,
    StateChangeReason,
    StateSnapshotChanged,
    SyncCompletedEvent,
    SyncStartedEvent,
    SyntheticSessionMethod,
)
from ..model.motor import MOTOR_TRACKING_TTL, MotorStateTracker
from ..model.optimistic import OPTIMISTIC_STATE_TTL, OptimisticStateOverlay
from ..model.state import GatewayState
from ..model.status import GatewaySessionState
from .base import Actor, create_actor_task

_LOGGER = logging.getLogger(__name__)
StateListener = Callable[[StateSnapshotChanged], Awaitable[None] | None]
PropertyListener = Callable[[PropertyChange], Awaitable[None] | None]
RefreshRequester = Callable[[RefreshNodeRequestedEvent], Awaitable[None] | None]


class DeviceStateActor(Actor[DeviceStateActorMessage]):
    """Owns authoritative gateway state plus short-lived visible-state overlay."""

    def __init__(self, *, ttl: float = OPTIMISTIC_STATE_TTL, motor_tracking_ttl: float = MOTOR_TRACKING_TTL) -> None:
        super().__init__("yeelight-pro-device-state")
        self.state = GatewayState()
        self.overlay = OptimisticStateOverlay(ttl=ttl)
        self.motor = MotorStateTracker(ttl=motor_tracking_ttl)
        self._visible_nodes: dict[str | int, TopologyNode] = {}
        self._watchdog: asyncio.Task[None] | None = None
        self._motor_watchdog: asyncio.Task[None] | None = None
        self._state_listeners: list[StateListener] = []
        self._property_listeners: list[PropertyListener] = []
        self._refresh_requester: RefreshRequester | None = None

    def add_state_listener(self, listener: StateListener) -> Callable[[], None]:
        self._state_listeners.append(listener)

        def remove() -> None:
            with suppress(ValueError):
                self._state_listeners.remove(listener)

        return remove

    def add_property_listener(self, listener: PropertyListener) -> Callable[[], None]:
        self._property_listeners.append(listener)

        def remove() -> None:
            with suppress(ValueError):
                self._property_listeners.remove(listener)

        return remove

    def set_refresh_requester(self, requester: RefreshRequester) -> None:
        self._refresh_requester = requester

    def visible_node(self, node_id: str | int) -> TopologyNode | None:
        return self._visible_nodes.get(node_id)

    def visible_nodes(self) -> list[TopologyNode]:
        return list(self._visible_nodes.values())

    def has_pending(self, node_id: str | int, props: Iterable[str] | None = None) -> bool:
        return self.overlay.has_pending(node_id, props)

    def diagnostics(self) -> dict[str, Any]:
        return self.overlay.diagnostics(now=asyncio.get_running_loop().time())

    async def close(self) -> None:
        self._cancel_watchdog()
        self._cancel_motor_watchdog()
        await super().close()

    async def handle(self, message: DeviceStateActorMessage) -> Any:
        if isinstance(message, ApplyTopologyCommand):
            return await self._apply_topology(message)
        if isinstance(message, ApplyPropertiesCommand):
            return await self._apply_properties(message)
        if isinstance(message, ApplyGenericStateMessageCommand):
            return await self._apply_generic_message(message)
        if isinstance(message, ApplyGroupsCommand):
            self.state.apply_groups(message.payload)
            return None
        if isinstance(message, ApplyRoomsCommand):
            self.state.apply_rooms(message.payload)
            return None
        if isinstance(message, ApplyScenesCommand):
            self.state.apply_scenes(message.payload)
            return None
        if isinstance(message, ApplyOptimisticPropsCommand):
            return await self._apply_optimistic_props(message.props_by_node)
        if isinstance(message, ApplyMotorTargetsCommand):
            return await self._apply_motor_targets(message)
        if isinstance(message, ApplyMotorStopCommand):
            return await self._apply_motor_stop(message)
        if isinstance(message, SyncStartedEvent):
            return await self._handle_sync_started()
        if isinstance(message, SyncCompletedEvent):
            return await self._publish_snapshot(
                StateChangeReason.SYNC_COMPLETE,
                {"method": SyntheticSessionMethod.SYNC_COMPLETE},
            )
        if isinstance(message, SessionStatusChanged):
            return await self._handle_session_status(message)
        if isinstance(message, ExpireOptimisticStateCommand):
            return await self._expire_optimistic()
        if isinstance(message, ExpireMotorTrackingCommand):
            return await self._expire_motor_tracking()
        raise TypeError(f"unsupported device state message: {type(message).__name__}")

    async def _apply_topology(self, message: ApplyTopologyCommand) -> None:
        topology = self.state.apply_topology(message.payload, replace=message.replace)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=message.reason, message=message.message),
            topology_changed=True,
            active_topology_node_ids={node.id for node in topology.nodes} if message.replace else None,
        )

    async def _apply_properties(self, message: ApplyPropertiesCommand) -> AppliedPropertiesResult:
        changes = tuple(self.state.apply_properties(message.payload))
        full_coverage = self.state.full_property_coverage(message.payload)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=message.reason, message=message.payload, changes=changes),
            topology_changed=False,
            active_topology_node_ids=None,
        )
        return AppliedPropertiesResult(changes=changes, full_property_coverage=full_coverage)

    async def _apply_generic_message(self, message: ApplyGenericStateMessageCommand) -> None:
        self.state.apply_message(message.payload)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=message.reason, message=message.payload),
            topology_changed=message.reason in {StateChangeReason.TOPOLOGY_PUSH, StateChangeReason.TOPOLOGY_SYNC},
            active_topology_node_ids=None,
        )

    async def _after_authoritative_changed(
        self,
        event: AuthoritativeStateChangedEvent,
        *,
        topology_changed: bool,
        active_topology_node_ids: Iterable[str | int] | None,
    ) -> None:
        affected = self._reconcile_overlay_from_message(event.message)
        motor_affected = self.motor.apply_authoritative_changes(
            event.changes,
            self.state.nodes,
            now=asyncio.get_running_loop().time(),
        )
        if not event.changes:
            motor_affected.update(
                self.motor.apply_authoritative_message(
                    event.message,
                    self.state.nodes,
                    now=asyncio.get_running_loop().time(),
                )
            )
        if topology_changed and active_topology_node_ids is not None:
            affected.update(self.overlay.clear_missing_nodes(active_topology_node_ids))
            motor_affected.update(self.motor.clear_missing_nodes(active_topology_node_ids))
        if affected:
            self._schedule_watchdog()
        if motor_affected:
            self._schedule_motor_watchdog()
        self._rebuild_visible_cache()
        snapshot_reasons = {
            StateChangeReason.PROPERTY_PUSH,
            StateChangeReason.TOPOLOGY_PUSH,
            StateChangeReason.TOPOLOGY_SYNC,
            StateChangeReason.NODE_REFRESH,
            StateChangeReason.POLL_FULL_PROPERTIES,
        }
        if event.changes:
            for change in event.changes:
                await self._notify_property(change)
        if event.changes or affected or motor_affected or event.reason in snapshot_reasons:
            await self._publish_snapshot(event.reason, event.message, event.changes)

    async def _apply_motor_targets(self, message: ApplyMotorTargetsCommand) -> None:
        now = asyncio.get_running_loop().time()
        affected: set[str | int] = set()
        for target in message.targets:
            node = self.state.nodes.get(target.node_id)
            current_value = _int_or_none(node.params.get(target.current_prop)) if node is not None else None
            affected.update(self.motor.set_target(target, current_value=current_value, now=now))
        if not affected:
            return
        self._schedule_motor_watchdog()
        self._rebuild_visible_cache()
        await self._publish_snapshot(
            StateChangeReason.MOTOR_TARGET,
            {"method": SyntheticSessionMethod.MOTOR_TARGET, "nodes": [{"id": node_id} for node_id in affected]},
        )

    async def _apply_motor_stop(self, message: ApplyMotorStopCommand) -> None:
        affected: set[str | int] = set()
        for node_id in message.node_ids:
            affected.update(self.motor.clear_node(node_id))
        if not affected:
            return
        self._schedule_motor_watchdog()
        self._rebuild_visible_cache()
        await self._publish_snapshot(
            StateChangeReason.MOTOR_STOPPED,
            {"method": SyntheticSessionMethod.MOTOR_STOP, "nodes": [{"id": node_id} for node_id in affected]},
        )

    async def _apply_optimistic_props(self, optimistic_props: Mapping[str | int, Mapping[str, Any]]) -> None:
        now = asyncio.get_running_loop().time()
        affected: set[str | int] = set()
        for node_id, props in optimistic_props.items():
            current = self.state.nodes.get(node_id)
            if current is not None:
                already_authoritative = [prop for prop, value in props.items() if current.params.get(prop) == value]
                affected.update(self.overlay.clear_props(node_id, already_authoritative))
                props = {prop: value for prop, value in props.items() if current.params.get(prop) != value}
            affected.update(self.overlay.set_props(node_id, props, now=now))
        if not affected:
            return
        self._schedule_watchdog()
        self._rebuild_visible_cache()
        await self._publish_snapshot(
            StateChangeReason.OPTIMISTIC_UPDATE,
            {"method": SyntheticSessionMethod.OVERLAY_OPTIMISTIC, "nodes": [{"id": node_id} for node_id in affected]},
        )

    async def _handle_sync_started(self) -> None:
        affected = self.overlay.clear_all()
        motor_affected = self.motor.clear_all()
        self._cancel_watchdog()
        self._cancel_motor_watchdog()
        if affected or motor_affected:
            self._rebuild_visible_cache()
            await self._publish_snapshot(
                StateChangeReason.OPTIMISTIC_CLEARED if affected else StateChangeReason.MOTOR_TRACKING_CLEARED,
                {"method": SyntheticSessionMethod.OVERLAY_CLEAR if affected else SyntheticSessionMethod.MOTOR_CLEAR},
            )

    async def _handle_session_status(self, event: SessionStatusChanged) -> None:
        if event.current not in {GatewaySessionState.DISCONNECTED, GatewaySessionState.CLOSING}:
            return
        affected = self.overlay.clear_all()
        motor_affected = self.motor.clear_all()
        self._cancel_watchdog()
        self._cancel_motor_watchdog()
        if affected or motor_affected:
            self._rebuild_visible_cache()
            await self._publish_snapshot(
                StateChangeReason.OPTIMISTIC_CLEARED if affected else StateChangeReason.MOTOR_TRACKING_CLEARED,
                {"method": SyntheticSessionMethod.OVERLAY_CLEAR if affected else SyntheticSessionMethod.MOTOR_CLEAR},
            )

    async def _expire_optimistic(self) -> None:
        affected = self.overlay.expire(now=asyncio.get_running_loop().time())
        self._schedule_watchdog()
        if not affected:
            return
        self._rebuild_visible_cache()
        await self._publish_snapshot(
            StateChangeReason.OPTIMISTIC_EXPIRED,
            {"method": SyntheticSessionMethod.OVERLAY_EXPIRED, "nodes": [{"id": node_id} for node_id in affected]},
        )
        if self._refresh_requester is None:
            return
        for node_id in affected:
            create_actor_task(
                _call_listener(self._refresh_requester, RefreshNodeRequestedEvent(node_id=node_id)),
                name=f"yeelight-pro-refresh-node-{node_id}",
            )

    async def _expire_motor_tracking(self) -> None:
        affected = self.motor.expire(now=asyncio.get_running_loop().time())
        self._schedule_motor_watchdog()
        if not affected:
            return
        self._rebuild_visible_cache()
        await self._publish_snapshot(
            StateChangeReason.MOTOR_TRACKING_EXPIRED,
            {"method": SyntheticSessionMethod.MOTOR_EXPIRED, "nodes": [{"id": node_id} for node_id in affected]},
        )
        if self._refresh_requester is None:
            return
        for node_id in affected:
            create_actor_task(
                _call_listener(self._refresh_requester, RefreshNodeRequestedEvent(node_id=node_id)),
                name=f"yeelight-pro-refresh-node-{node_id}",
            )

    def _reconcile_overlay_from_message(self, message: Mapping[str, Any]) -> set[str | int]:
        affected: set[str | int] = set()
        for item in list_payload(message, "nodes"):
            node_id = _payload_node_id(item)
            params = item.get("params")
            if node_id is None or not isinstance(params, Mapping):
                continue
            affected.update(self.overlay.reconcile_node_props(node_id, params))
        return affected

    def _schedule_watchdog(self) -> None:
        self._cancel_watchdog()
        now = asyncio.get_running_loop().time()
        next_expiration = self.overlay.next_expiration(now=now)
        if next_expiration is None:
            return
        self._watchdog = self.defer_later(
            max(0.0, next_expiration - now),
            ExpireOptimisticStateCommand(),
            name="yeelight-pro-device-state-overlay-watchdog",
        )

    def _schedule_motor_watchdog(self) -> None:
        self._cancel_motor_watchdog()
        now = asyncio.get_running_loop().time()
        next_expiration = self.motor.next_expiration(now=now)
        if next_expiration is None:
            return
        self._motor_watchdog = self.defer_later(
            max(0.0, next_expiration - now),
            ExpireMotorTrackingCommand(),
            name="yeelight-pro-device-state-motor-watchdog",
        )

    def _cancel_watchdog(self) -> None:
        if self._watchdog is not None:
            self._watchdog.cancel()
            self._watchdog = None

    def _cancel_motor_watchdog(self) -> None:
        if self._motor_watchdog is not None:
            self._motor_watchdog.cancel()
            self._motor_watchdog = None

    def _rebuild_visible_cache(self) -> None:
        now = asyncio.get_running_loop().time()
        self._visible_nodes = {
            node_id: self.motor.visible_node(self.overlay.visible_node(node, now=now))
            for node_id, node in self.state.nodes.items()
        }

    async def _publish_snapshot(
        self,
        reason: StateChangeReason,
        message: Mapping[str, Any],
        changes: tuple[Any, ...] = (),
    ) -> None:
        event = StateSnapshotChanged(reason=reason, message=message, changes=changes)
        for listener in list(self._state_listeners):
            _schedule_listener(listener, event)

    async def _notify_property(self, change: PropertyChange) -> None:
        for listener in list(self._property_listeners):
            _schedule_listener(listener, change)


def _payload_node_id(item: Mapping[str, Any]) -> str | int | None:
    item_id = item.get("id")
    if isinstance(item_id, bool) or not isinstance(item_id, (str, int)):
        return None
    return item_id


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


async def _call_listener(listener: Callable[..., Any], *args: Any) -> None:
    try:
        result = listener(*args)
        if inspect.isawaitable(result):
            await result
    except Exception:  # noqa: BLE001 - HA boundary listeners must not kill actors.
        _LOGGER.exception("Yeelight Pro device state listener failed")


def _schedule_listener(listener: Callable[..., Any], *args: Any) -> None:
    create_actor_task(
        _call_listener(listener, *args),
        name="yeelight-pro-device-state-listener",
    )
