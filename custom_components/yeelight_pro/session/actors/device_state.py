from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextlib import suppress
from typing import Any

from ...core.coercion import int_or_none as _int_or_none
from ...core.coercion import node_id_or_none
from ...core.coercion import node_key as _node_key
from ...core.protocol import list_payload
from ...core.topology import TopologyNode
from ...core.updates import PropertyChange
from ..messages import (
    AcceptPendingWritesCommand,
    AppliedPropertiesResult,
    ApplyGenericStateMessageCommand,
    ApplyGroupsCommand,
    ApplyPropertiesCommand,
    ApplyRoomsCommand,
    ApplyScenesCommand,
    ApplyTopologyCommand,
    AuthoritativeStateChangedEvent,
    CaptureWriteWatermarkCommand,
    DeviceStateActorMessage,
    FailPendingWritesCommand,
    PendingWritesTickCommand,
    PreparePendingWritesCommand,
    RefreshNodeRequestedEvent,
    ResolvePendingRefreshCommand,
    SessionStatusChanged,
    StateChangeReason,
    StateSnapshotChanged,
    SyncCompletedEvent,
    SyntheticSessionMethod,
)
from ..model.motor import MOTOR_TRACKING_TTL, MotorStateTracker
from ..model.pending import PendingRefresh, PendingWriteTracker
from ..model.state import GatewayState
from ..model.status import GatewaySessionState
from .base import Actor, ActorClosed, ActorRef, create_actor_task

_LOGGER = logging.getLogger(__name__)
_MAX_LOG_ITEMS = 20
StateListener = Callable[[StateSnapshotChanged], Awaitable[None] | None]
PropertyListener = Callable[[PropertyChange], Awaitable[None] | None]
RefreshRequester = Callable[[RefreshNodeRequestedEvent], Awaitable[Mapping[str, Any]] | Mapping[str, Any]]
VisibleProjectionSignature = tuple[object, ...]


class DeviceStateActor(Actor[DeviceStateActorMessage]):
    """Own raw gateway state and the settled HA-visible projection."""

    def __init__(
        self,
        *,
        report_grace: float | None = None,
        quiet_window: float | None = None,
        motor_tracking_ttl: float | None = None,
        refresh_timeout: float = 5.0,
    ) -> None:
        super().__init__("yeelight-pro-device-state")
        self._ref: ActorRef[DeviceStateActorMessage] = ActorRef(self)
        self.state = GatewayState()
        tracker_kwargs: dict[str, float] = {}
        if report_grace is not None:
            tracker_kwargs["report_grace"] = report_grace
        if quiet_window is not None:
            tracker_kwargs["quiet_window"] = quiet_window
        self.pending_writes = PendingWriteTracker(**tracker_kwargs)
        self.motor = MotorStateTracker(ttl=MOTOR_TRACKING_TTL if motor_tracking_ttl is None else motor_tracking_ttl)
        self._visible_nodes: dict[str | int, TopologyNode] = {}
        self._watchdog: asyncio.Task[None] | None = None
        self._state_listeners: list[StateListener] = []
        self._property_listeners: list[PropertyListener] = []
        self._refresh_requester: RefreshRequester | None = None
        self._refresh_timeout = max(0.0, refresh_timeout)
        self._suppressed_snapshot_counts: dict[str, int] = {}

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
        return self.pending_writes.has_pending(node_id, props) or (props is None and self.motor.has_tracking(node_id))

    def diagnostics(self) -> dict[str, Any]:
        return self.pending_writes.diagnostics(now=asyncio.get_running_loop().time())

    def motor_diagnostics(self) -> dict[str, Any]:
        return self.motor.diagnostics(now=asyncio.get_running_loop().time())

    async def close(self) -> None:
        self._cancel_watchdog()
        await super().close()

    async def handle(self, message: DeviceStateActorMessage) -> Any:
        if isinstance(message, ApplyTopologyCommand):
            return await self._apply_topology(message)
        if isinstance(message, ApplyPropertiesCommand):
            return await self._apply_properties(message)
        if isinstance(message, ApplyGenericStateMessageCommand):
            return await self._apply_generic_message(message)
        if isinstance(message, ApplyGroupsCommand):
            return await self._apply_groups(message)
        if isinstance(message, ApplyRoomsCommand):
            self.state.apply_rooms(message.payload)
            return None
        if isinstance(message, ApplyScenesCommand):
            self.state.apply_scenes(message.payload)
            return None
        if isinstance(message, PreparePendingWritesCommand):
            return await self._prepare_writes(message)
        if isinstance(message, AcceptPendingWritesCommand):
            return await self._accept_writes(message)
        if isinstance(message, FailPendingWritesCommand):
            return await self._fail_writes(message)
        if isinstance(message, CaptureWriteWatermarkCommand):
            return self.pending_writes.capture_write_ids()
        if isinstance(message, PendingWritesTickCommand):
            return await self._tick_pending()
        if isinstance(message, ResolvePendingRefreshCommand):
            return await self._resolve_refresh(message)
        if isinstance(message, SyncCompletedEvent):
            return await self._publish_snapshot(
                StateChangeReason.SYNC_COMPLETE,
                {"method": SyntheticSessionMethod.SYNC_COMPLETE},
            )
        if isinstance(message, SessionStatusChanged):
            return await self._handle_session_status(message)
        raise TypeError(f"unsupported device state message: {type(message).__name__}")

    async def _apply_topology(self, message: ApplyTopologyCommand) -> None:
        before = self._visible_projection_signature()
        payload = self.pending_writes.filter_stale_pull(message.payload, message.captured_write_ids)
        self.state.apply_topology(payload, replace=message.replace)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=message.reason, message=payload),
            before_visible=before,
        )

    async def _apply_properties(self, message: ApplyPropertiesCommand) -> AppliedPropertiesResult:
        before = self._visible_projection_signature()
        payload = self.pending_writes.filter_stale_pull(message.payload, message.captured_write_ids)
        changes = tuple(self.state.apply_properties(payload))
        full_coverage = self.state.full_property_coverage(message.payload)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=message.reason, message=payload, changes=changes),
            before_visible=before,
        )
        return AppliedPropertiesResult(changes=changes, full_property_coverage=full_coverage)

    async def _apply_groups(self, message: ApplyGroupsCommand) -> None:
        before = self._visible_projection_signature()
        payload = self.pending_writes.filter_stale_pull(message.payload, message.captured_write_ids)
        changes = tuple(self.state.apply_groups(payload))
        reason = message.reason or StateChangeReason.POLL_FULL_PROPERTIES
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=reason, message=payload, changes=changes),
            before_visible=before,
        )

    async def _apply_generic_message(self, message: ApplyGenericStateMessageCommand) -> None:
        before = self._visible_projection_signature()
        self.state.apply_message(message.payload)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=message.reason, message=message.payload),
            before_visible=before,
        )

    async def _after_authoritative_changed(
        self,
        event: AuthoritativeStateChangedEvent,
        *,
        before_visible: VisibleProjectionSignature,
    ) -> None:
        now = asyncio.get_running_loop().time()
        pending_affected = self.pending_writes.apply_observation(event.message, now=now)
        motor_affected = self.motor.apply_authoritative_message(event.message, self.state.nodes, now=now)
        _LOGGER.debug(
            "Yeelight Pro observation applied: reason=%s summary=%s pending=%s motor=%s",
            event.reason,
            _message_summary(event.message),
            sorted(str(node_id) for node_id in pending_affected),
            sorted(str(node_id) for node_id in motor_affected),
        )
        self._schedule_watchdog()
        self._rebuild_visible_cache()
        for change in event.changes:
            await self._notify_property(change)
        await self._publish_if_visible_changed(
            event.reason,
            event.message,
            before_visible=before_visible,
            changes=event.changes,
        )

    async def _prepare_writes(self, message: PreparePendingWritesCommand) -> None:
        before = self._visible_projection_signature()
        affected = self.pending_writes.prepare_writes(
            message.write_id,
            message.props_by_node,
            nodes=self.state.nodes,
            now=asyncio.get_running_loop().time(),
            transition_delays=message.transition_delays,
        )
        _LOGGER.debug(
            "Yeelight Pro pending write barriers prepared: write=%s props=%s affected=%s",
            message.write_id,
            _props_by_node_summary(message.props_by_node),
            sorted(str(node_id) for node_id in affected),
        )
        self._rebuild_visible_cache()
        await self._publish_if_visible_changed(
            StateChangeReason.PENDING_WRITE_PREPARED,
            {"method": SyntheticSessionMethod.PENDING_WRITE_PREPARED},
            before_visible=before,
        )

    async def _accept_writes(self, message: AcceptPendingWritesCommand) -> None:
        before = self._visible_projection_signature()
        now = asyncio.get_running_loop().time()
        self.pending_writes.accept_writes(message.write_ids, now=now)
        motor_affected: set[str | int] = set()
        for target in message.motor_targets:
            node = self.state.nodes.get(target.node_id)
            current = _int_or_none(node.params.get(target.current_prop)) if node is not None else None
            motor_affected.update(self.motor.set_target(target, current_value=current, now=now))
        for node_id in message.motor_stops:
            motor_affected.update(self.motor.clear_node(node_id))
        self._schedule_watchdog()
        self._rebuild_visible_cache()
        await self._publish_if_visible_changed(
            StateChangeReason.PENDING_WRITE_PREPARED,
            {"method": SyntheticSessionMethod.PENDING_WRITE_PREPARED},
            before_visible=before,
        )

    async def _fail_writes(self, message: FailPendingWritesCommand) -> None:
        before = self._visible_projection_signature()
        affected = self.pending_writes.fail_writes(message.write_ids)
        self._schedule_watchdog()
        self._rebuild_visible_cache()
        if affected:
            await self._publish_if_visible_changed(
                StateChangeReason.PENDING_WRITE_RELEASED,
                {"method": SyntheticSessionMethod.PENDING_WRITE_RELEASED},
                before_visible=before,
            )

    async def _handle_session_status(self, event: SessionStatusChanged) -> None:
        if event.current not in {GatewaySessionState.DISCONNECTED, GatewaySessionState.CLOSING}:
            return
        before = self._visible_projection_signature()
        affected = self.pending_writes.clear_all()
        affected.update(self.motor.clear_all())
        self._cancel_watchdog()
        self._rebuild_visible_cache()
        if affected:
            await self._publish_if_visible_changed(
                StateChangeReason.PENDING_WRITE_RELEASED,
                {"method": SyntheticSessionMethod.PENDING_WRITE_RELEASED},
                before_visible=before,
            )

    async def _tick_pending(self) -> None:
        before = self._visible_projection_signature()
        now = asyncio.get_running_loop().time()
        result = self.pending_writes.tick(now=now)
        expired_motor = self.motor.expire_pending(now=now)
        visible_affected = set(result.visible_affected)
        visible_affected.update(track.node_id for track in expired_motor)
        self._schedule_watchdog()
        self._rebuild_visible_cache()
        if visible_affected:
            await self._publish_if_visible_changed(
                StateChangeReason.PROPERTY_PUSH,
                {"method": "gateway_settled.confirmed"},
                before_visible=before,
            )
        for refresh in result.refreshes:
            node = self.state.nodes.get(refresh.node_id)
            create_actor_task(
                self._refresh_pending(refresh, node_type=node.nt if node is not None else None),
                name=f"yeelight-pro-refresh-node-{refresh.node_id}",
            )
        for track in expired_motor:
            node = self.state.nodes.get(track.node_id)
            create_actor_task(
                self._refresh_pending(
                    PendingRefresh(node_id=track.node_id, write_ids={}),
                    node_type=node.nt if node is not None else None,
                ),
                name=f"yeelight-pro-refresh-node-{track.node_id}",
            )

    async def _refresh_pending(self, refresh: PendingRefresh, *, node_type: int | None) -> None:
        response: Mapping[str, Any] | None = None
        failed = False
        try:
            response = await asyncio.wait_for(
                _call_listener_strict(
                    self._refresh_requester,
                    RefreshNodeRequestedEvent(
                        node_id=refresh.node_id,
                        node_type=node_type,
                        write_ids=refresh.write_ids,
                    ),
                ),
                timeout=self._refresh_timeout,
            )
        except Exception:  # noqa: BLE001 - failed refresh releases held state to latest raw evidence.
            failed = True
        try:
            await self._ref.tell(ResolvePendingRefreshCommand(refresh=refresh, response=response, failed=failed))
        except ActorClosed:
            return

    async def _resolve_refresh(self, message: ResolvePendingRefreshCommand) -> None:
        before = self._visible_projection_signature()
        affected = self.pending_writes.complete_refresh(
            message.refresh,
            message.response,
            failed=message.failed,
            now=asyncio.get_running_loop().time(),
        )
        self._schedule_watchdog()
        self._rebuild_visible_cache()
        if affected:
            await self._publish_if_visible_changed(
                StateChangeReason.PENDING_WRITE_RELEASED,
                {"method": SyntheticSessionMethod.PENDING_WRITE_RELEASED},
                before_visible=before,
            )

    def _schedule_watchdog(self) -> None:
        self._cancel_watchdog()
        now = asyncio.get_running_loop().time()
        deadlines = [
            deadline
            for deadline in (
                self.pending_writes.next_deadline(now=now),
                self.motor.next_expiration(now=now),
            )
            if deadline is not None
        ]
        if not deadlines:
            return
        deadline = min(deadlines)
        self._watchdog = self.defer_later(
            max(0.0, deadline - now),
            PendingWritesTickCommand(),
            name="yeelight-pro-device-state-pending-watchdog",
        )

    def _cancel_watchdog(self) -> None:
        if self._watchdog is not None:
            self._watchdog.cancel()
            self._watchdog = None

    def _rebuild_visible_cache(self) -> None:
        self._visible_nodes = {node_id: self._project_visible(node) for node_id, node in self.state.nodes.items()}

    def _project_visible(self, node: TopologyNode) -> TopologyNode:
        return self.motor.visible_node(self.pending_writes.project_visible(node))

    async def _publish_if_visible_changed(
        self,
        reason: StateChangeReason,
        message: Mapping[str, Any],
        *,
        before_visible: VisibleProjectionSignature,
        changes: tuple[Any, ...] = (),
    ) -> None:
        if self._visible_projection_signature() != before_visible:
            await self._publish_snapshot(reason, message, changes)
            return
        self._log_suppressed_snapshot(reason, message, changes)

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

    def _visible_projection_signature(self) -> VisibleProjectionSignature:
        nodes = []
        for node_id, node in self.state.nodes.items():
            visible = self._project_visible(node)
            node_key = _node_key(node_id)
            nodes.append((str(node_key if node_key is not None else node_id), _node_signature(visible)))
        return (
            tuple(sorted(nodes, key=lambda item: item[0])),
            _state_mapping_signature(self.state.groups, excluded_keys=("params",)),
            _state_mapping_signature(self.state.rooms),
        )

    def _log_suppressed_snapshot(
        self,
        reason: StateChangeReason,
        message: Mapping[str, Any],
        changes: tuple[Any, ...],
    ) -> None:
        key = str(reason)
        count = self._suppressed_snapshot_counts.get(key, 0) + 1
        self._suppressed_snapshot_counts[key] = count
        if count & (count - 1):
            return
        _LOGGER.debug(
            "Yeelight Pro state snapshot suppressed: reason=%s count=%d raw_changes=%d summary=%s",
            reason,
            count,
            len(changes),
            _message_summary(message),
        )


def _payload_node_id(item: Mapping[str, Any]) -> str | int | None:
    return node_id_or_none(item.get("id"))


def _node_signature(node: TopologyNode) -> tuple[object, ...]:
    return (
        str(node.id),
        node.nt,
        node.type,
        node.product_id,
        node.property_type,
        node.name,
        None if node.room_id is None else str(node.room_id),
        node.channel_count,
        node.component_type_ids,
        _freeze_value(node.params),
        node.online,
    )


def _state_mapping_signature(
    items: Mapping[str | int, Mapping[str, Any]],
    *,
    excluded_keys: Iterable[str] = (),
) -> tuple[tuple[str, object], ...]:
    excluded = frozenset(excluded_keys)
    return tuple(
        sorted(
            (str(item_id), _freeze_value({key: value for key, value in item.items() if key not in excluded}))
            for item_id, item in items.items()
        )
    )


def _freeze_value(value: Any) -> object:
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _freeze_value(item)) for key, item in value.items()))
    if isinstance(value, list | tuple):
        return tuple(_freeze_value(item) for item in value)
    return value


def _message_summary(message: Mapping[str, Any]) -> dict[str, Any]:
    raw_nodes = list_payload(message, "nodes")
    nodes = []
    for item in raw_nodes[:_MAX_LOG_ITEMS]:
        params = item.get("params")
        nodes.append(
            {
                "id": _payload_node_id(item),
                "params": dict(params) if isinstance(params, Mapping) else None,
            }
        )
    summary: dict[str, Any] = {
        "method": message.get("method"),
        "node_count": len(raw_nodes),
        "nodes": nodes,
    }
    raw_groups = list_payload(message, "groups")
    if raw_groups:
        summary["group_count"] = len(raw_groups)
    return summary


def _props_by_node_summary(props_by_node: Mapping[str | int, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(node_id): dict(props) for node_id, props in props_by_node.items()}


async def _call_listener(listener: Callable[..., Any], *args: Any) -> None:
    try:
        result = listener(*args)
        if inspect.isawaitable(result):
            await result
    except Exception:  # noqa: BLE001 - HA boundary listeners must not kill actors.
        _LOGGER.exception("Yeelight Pro device state listener failed")


async def _call_listener_strict(listener: Callable[..., Any] | None, *args: Any) -> Mapping[str, Any]:
    if listener is None:
        raise RuntimeError("refresh requester is not configured")
    result = listener(*args)
    if inspect.isawaitable(result):
        result = await result
    if not isinstance(result, Mapping):
        raise TypeError("refresh requester must return a gateway response")
    return result


def _schedule_listener(listener: Callable[..., Any], *args: Any) -> None:
    create_actor_task(
        _call_listener(listener, *args),
        name="yeelight-pro-device-state-listener",
    )
