from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import replace
from typing import Any

from ...core.coercion import node_id_or_none
from ...core.coercion import node_key as _node_key
from ...core.protocol import list_payload
from ...core.topology import TopologyNode
from ...core.updates import PropertyChange
from ..messages import (
    AppliedPropertiesResult,
    ApplyGenericStateMessageCommand,
    ApplyGroupsCommand,
    ApplyPropertiesCommand,
    ApplyRoomsCommand,
    ApplyScenesCommand,
    ApplyTopologyCommand,
    AuthoritativeStateChangedEvent,
    DeviceStateActorMessage,
    ExpireCommandIntentsCommand,
    PrepareCommandIntentCommand,
    RecordCommandIntentCommand,
    RefreshNodeRequestedEvent,
    ResolveExpiredIntentRefreshCommand,
    SessionStatusChanged,
    StateChangeReason,
    StateSnapshotChanged,
    SyncCompletedEvent,
    SyncStartedEvent,
    SyntheticSessionMethod,
)
from ..model.intent import CommandIntentRegistry
from ..model.state import GatewayState
from ..model.status import GatewaySessionState
from .base import Actor, ActorClosed, ActorRef, create_actor_task

_LOGGER = logging.getLogger(__name__)
_MAX_LOG_ITEMS = 20
StateListener = Callable[[StateSnapshotChanged], Awaitable[None] | None]
PropertyListener = Callable[[PropertyChange], Awaitable[None] | None]
RefreshRequester = Callable[[RefreshNodeRequestedEvent], Awaitable[None] | None]
VisibleProjectionSignature = tuple[object, ...]


class DeviceStateActor(Actor[DeviceStateActorMessage]):
    """Owns authoritative gateway state and registry-owned command intents."""

    def __init__(
        self,
        *,
        ttl: float | None = None,
        motor_tracking_ttl: float | None = None,
        refresh_timeout: float = 5.0,
    ) -> None:
        super().__init__("yeelight-pro-device-state")
        self._ref: ActorRef[DeviceStateActorMessage] = ActorRef(self)
        self.state = GatewayState()
        intent_kwargs: dict[str, float] = {}
        if ttl is not None:
            intent_kwargs["ttl"] = ttl
        if motor_tracking_ttl is not None:
            intent_kwargs["motor_tracking_ttl"] = motor_tracking_ttl
        self.intents = CommandIntentRegistry(**intent_kwargs)
        self._stale_node_props: dict[str, tuple[str | int, set[str]]] = {}
        self._refreshing_expired_props: dict[str, tuple[str | int, dict[str, int]]] = {}
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
        return self.intents.has_pending(node_id, props)

    def diagnostics(self) -> dict[str, Any]:
        diagnostics = self.intents.diagnostics(now=asyncio.get_running_loop().time())
        diagnostics["stale"] = [
            {"node_id": str(node_id), "properties": sorted(props)} for node_id, props in self._stale_node_props.values()
        ]
        diagnostics["refreshing"] = [
            {"node_id": str(node_id), "properties": sorted(props)}
            for node_id, props in self._refreshing_expired_props.values()
        ]
        return diagnostics

    def motor_diagnostics(self) -> dict[str, Any]:
        return self.intents.motor.diagnostics(now=asyncio.get_running_loop().time())

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
        if isinstance(message, RecordCommandIntentCommand):
            return await self._record_command_intent(message)
        if isinstance(message, PrepareCommandIntentCommand):
            return self.intents.prepare_property_intents(message.props_by_node)
        if isinstance(message, SyncStartedEvent):
            return await self._handle_sync_started()
        if isinstance(message, SyncCompletedEvent):
            return await self._publish_snapshot(
                StateChangeReason.SYNC_COMPLETE,
                {"method": SyntheticSessionMethod.SYNC_COMPLETE},
            )
        if isinstance(message, SessionStatusChanged):
            return await self._handle_session_status(message)
        if isinstance(message, ExpireCommandIntentsCommand):
            return await self._expire_command_intents()
        if isinstance(message, ResolveExpiredIntentRefreshCommand):
            return await self._resolve_expired_intent_refresh(message)
        raise TypeError(f"unsupported device state message: {type(message).__name__}")

    async def _apply_topology(self, message: ApplyTopologyCommand) -> None:
        before = self._visible_projection_signature()
        topology = self.state.apply_topology(message.payload, replace=message.replace)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=message.reason, message=message.message),
            topology_changed=True,
            active_topology_node_ids={node.id for node in topology.nodes} if message.replace else None,
            before_visible=before,
        )

    async def _apply_properties(self, message: ApplyPropertiesCommand) -> AppliedPropertiesResult:
        before = self._visible_projection_signature()
        changes = tuple(self.state.apply_properties(message.payload))
        full_coverage = self.state.full_property_coverage(message.payload)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(
                reason=message.reason,
                message=message.payload,
                changes=changes,
                request_generations=message.request_generations,
            ),
            topology_changed=False,
            active_topology_node_ids=None,
            before_visible=before,
        )
        return AppliedPropertiesResult(changes=changes, full_property_coverage=full_coverage)

    async def _apply_groups(self, message: ApplyGroupsCommand) -> None:
        before = self._visible_projection_signature()
        changes = tuple(self.state.apply_groups(message.payload))
        if message.reason is None:
            if changes:
                self._rebuild_visible_cache()
                for change in changes:
                    await self._notify_property(change)
            return
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(
                reason=message.reason,
                message=message.payload,
                changes=changes,
                request_generations=message.request_generations,
            ),
            topology_changed=False,
            active_topology_node_ids=None,
            before_visible=before,
        )

    async def _apply_generic_message(self, message: ApplyGenericStateMessageCommand) -> None:
        before = self._visible_projection_signature()
        self.state.apply_message(message.payload)
        await self._after_authoritative_changed(
            AuthoritativeStateChangedEvent(reason=message.reason, message=message.payload),
            topology_changed=message.reason in {StateChangeReason.TOPOLOGY_PUSH, StateChangeReason.TOPOLOGY_SYNC},
            active_topology_node_ids=None,
            before_visible=before,
        )

    async def _after_authoritative_changed(
        self,
        event: AuthoritativeStateChangedEvent,
        *,
        topology_changed: bool,
        active_topology_node_ids: Iterable[str | int] | None,
        before_visible: VisibleProjectionSignature,
    ) -> None:
        now = asyncio.get_running_loop().time()
        affected = self.intents.apply_authoritative_message(
            event.message,
            nodes=self.state.nodes,
            now=now,
            request_generations=event.request_generations,
        )
        stale_affected = self._clear_stale_from_message(event.message)
        refreshing_affected = self._clear_refreshing_from_message(event.message, event.request_generations)
        _LOGGER.debug(
            "Yeelight Pro authoritative state applied: "
            "reason=%s summary=%s intent_cleared_nodes=%s stale_cleared_nodes=%s refreshing_cleared_nodes=%s",
            event.reason,
            _message_summary(event.message),
            sorted(str(node_id) for node_id in affected),
            sorted(str(node_id) for node_id in stale_affected),
            sorted(str(node_id) for node_id in refreshing_affected),
        )
        if topology_changed and active_topology_node_ids is not None:
            affected.update(self.intents.clear_missing_nodes(active_topology_node_ids))
            stale_affected.update(self._clear_missing_stale_nodes(active_topology_node_ids))
            refreshing_affected.update(self._clear_missing_refreshing_nodes(active_topology_node_ids))
        if affected:
            self._schedule_watchdog()
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
        if event.changes or affected or stale_affected or refreshing_affected or event.reason in snapshot_reasons:
            await self._publish_if_visible_changed(
                event.reason,
                event.message,
                before_visible=before_visible,
                changes=event.changes,
            )

    async def _record_command_intent(self, message: RecordCommandIntentCommand) -> None:
        now = asyncio.get_running_loop().time()
        before = self._visible_projection_signature()
        affected: set[str | int] = set()
        for node_id, props in message.props_by_node.items():
            affected.update(self._clear_stale_props(node_id, props))
            affected.update(self._clear_refreshing_props(node_id, props))
        affected.update(
            self.intents.record_property_intents(
                message.props_by_node,
                nodes=self.state.nodes,
                now=now,
                token=message.token,
            )
        )
        affected.update(self.intents.record_motor_targets(message.motor_targets, nodes=self.state.nodes, now=now))
        for node_id in message.motor_stops:
            affected.update(self.intents.clear_node(node_id))
        _LOGGER.debug(
            "Yeelight Pro command intent recorded: props=%s motor_targets=%s motor_stops=%s affected=%s diagnostics=%s",
            _props_by_node_summary(message.props_by_node),
            _motor_targets_summary(message.motor_targets),
            tuple(str(node_id) for node_id in message.motor_stops),
            sorted(str(node_id) for node_id in affected),
            self.intents.diagnostics(now=now),
        )
        if not affected:
            return
        self._schedule_watchdog()
        self._rebuild_visible_cache()
        await self._publish_if_visible_changed(
            StateChangeReason.COMMAND_INTENT_RECORDED,
            {
                "method": SyntheticSessionMethod.COMMAND_INTENT_RECORDED,
                "nodes": [{"id": node_id} for node_id in affected],
            },
            before_visible=before,
        )

    async def _handle_sync_started(self) -> None:
        before = self._visible_projection_signature()
        affected = self.intents.clear_all()
        self._cancel_watchdog()
        _LOGGER.debug(
            "Yeelight Pro command intents cleared on sync start: affected=%s",
            sorted(str(node_id) for node_id in affected),
        )
        if affected:
            self._rebuild_visible_cache()
            await self._publish_if_visible_changed(
                StateChangeReason.COMMAND_INTENT_CLEARED,
                {"method": SyntheticSessionMethod.COMMAND_INTENT_CLEAR},
                before_visible=before,
            )

    async def _handle_session_status(self, event: SessionStatusChanged) -> None:
        if event.current not in {GatewaySessionState.DISCONNECTED, GatewaySessionState.CLOSING}:
            return
        before = self._visible_projection_signature()
        affected = self.intents.clear_all()
        self._cancel_watchdog()
        _LOGGER.debug(
            "Yeelight Pro command intents cleared on session state: state=%s affected=%s error=%s",
            event.current,
            sorted(str(node_id) for node_id in affected),
            repr(event.error) if event.error is not None else None,
        )
        if affected:
            self._rebuild_visible_cache()
            await self._publish_if_visible_changed(
                StateChangeReason.COMMAND_INTENT_CLEARED,
                {"method": SyntheticSessionMethod.COMMAND_INTENT_CLEAR},
                before_visible=before,
            )

    async def _expire_command_intents(self) -> None:
        expired = self.intents.expire_pending(now=asyncio.get_running_loop().time())
        affected = self._mark_refreshing(expired)
        _LOGGER.debug(
            "Yeelight Pro command intents expired, requesting refresh: expired=%s affected=%s refreshing=%s",
            _expired_summary(expired),
            sorted(str(node_id) for node_id in affected),
            _stale_summary(self._refreshing_expired_props),
        )
        self._schedule_watchdog()
        if not affected:
            return
        if self._refresh_requester is None:
            await self._resolve_expired_intent_refresh(ResolveExpiredIntentRefreshCommand(expired=expired, failed=True))
            return
        for item in expired:
            node = self.state.nodes.get(item.node_id)
            create_actor_task(
                self._refresh_expired_intent(item, node_type=node.nt if node is not None else None),
                name=f"yeelight-pro-refresh-node-{item.node_id}",
            )

    async def _refresh_expired_intent(self, item: Any, *, node_type: int | None) -> None:
        failed = False
        try:
            await asyncio.wait_for(
                _call_listener_strict(
                    self._refresh_requester,
                    RefreshNodeRequestedEvent(
                        node_id=item.node_id,
                        node_type=node_type,
                        request_generations=item.generation_by_prop(),
                    ),
                ),
                timeout=self._refresh_timeout,
            )
        except Exception:  # noqa: BLE001 - stale fallback is intentionally conservative on refresh failure.
            failed = True
        try:
            await self._ref.tell(ResolveExpiredIntentRefreshCommand(expired=(item,), failed=failed))
        except ActorClosed:
            return

    async def _resolve_expired_intent_refresh(self, message: ResolveExpiredIntentRefreshCommand) -> None:
        before = self._visible_projection_signature()
        affected = self._mark_stale_from_refreshing(message.expired)
        if not affected:
            return
        self._rebuild_visible_cache()
        await self._publish_if_visible_changed(
            StateChangeReason.COMMAND_INTENT_EXPIRED,
            {
                "method": SyntheticSessionMethod.COMMAND_INTENT_EXPIRED,
                "nodes": [{"id": node_id} for node_id in affected],
            },
            before_visible=before,
        )

    def _mark_refreshing(self, expired: Iterable[Any]) -> set[str | int]:
        affected: set[str | int] = set()
        for item in expired:
            node_key = _node_key(item.node_id)
            if node_key is None:
                continue
            current = self._refreshing_expired_props.get(node_key)
            if current is None:
                current = (item.node_id, {})
                self._refreshing_expired_props[node_key] = current
            generations = item.generation_by_prop()
            current[1].update({prop: generations.get(prop, 0) for prop in item.props})
            affected.add(item.node_id)
        return affected

    def _mark_stale_from_refreshing(self, expired: Iterable[Any]) -> set[str | int]:
        affected: set[str | int] = set()
        for item in expired:
            node_key = _node_key(item.node_id)
            if node_key is None:
                continue
            refreshing = self._refreshing_expired_props.get(node_key)
            if refreshing is None:
                continue
            _refreshing_node_id, refreshing_props = refreshing
            item_generations = item.generation_by_prop()
            failed_props: set[str] = set()
            for prop in item.props:
                generation = item_generations.get(prop, 0)
                if refreshing_props.get(prop) != generation:
                    continue
                refreshing_props.pop(prop, None)
                if generation == 0 or self.intents.is_latest_requested(item.node_id, prop, generation):
                    failed_props.add(prop)
            if not refreshing_props:
                self._refreshing_expired_props.pop(node_key, None)
            if not failed_props:
                continue
            stale = self._stale_node_props.get(node_key)
            if stale is None:
                stale = (item.node_id, set())
                self._stale_node_props[node_key] = stale
            stale[1].update(failed_props)
            affected.add(item.node_id)
        return affected

    def _clear_stale_from_message(self, message: Mapping[str, Any]) -> set[str | int]:
        affected: set[str | int] = set()
        for item in _authoritative_property_items(message):
            node_id = _payload_node_id(item)
            params = item.get("params")
            if node_id is None or not isinstance(params, Mapping):
                continue
            affected.update(self._clear_stale_props(node_id, params))
        return affected

    def _clear_refreshing_from_message(
        self,
        message: Mapping[str, Any],
        request_generations: Mapping[str | int, Mapping[str, int]] | None,
    ) -> set[str | int]:
        affected: set[str | int] = set()
        for item in _authoritative_property_items(message):
            node_id = _payload_node_id(item)
            params = item.get("params")
            if node_id is None or not isinstance(params, Mapping):
                continue
            affected.update(
                self._clear_refreshing_props(
                    node_id,
                    params,
                    request_generations=None
                    if request_generations is None
                    else _request_generations_for_node(request_generations, node_id),
                )
            )
        return affected

    def _clear_stale_props(self, node_id: str | int, props: Iterable[str] | Mapping[str, Any]) -> set[str | int]:
        node_key = _node_key(node_id)
        if node_key is None:
            return set()
        current = self._stale_node_props.get(node_key)
        if current is None:
            return set()
        stale_node_id, stale_props = current
        before = len(stale_props)
        for prop in props:
            if isinstance(prop, str):
                stale_props.discard(prop)
        if stale_props:
            affected = {stale_node_id} if len(stale_props) != before else set()
            if affected:
                _LOGGER.debug(
                    "Yeelight Pro stale properties partially cleared: node_id=%s remaining=%s",
                    stale_node_id,
                    sorted(stale_props),
                )
            return affected
        self._stale_node_props.pop(node_key, None)
        _LOGGER.debug("Yeelight Pro stale node cleared: node_id=%s", stale_node_id)
        return {stale_node_id}

    def _clear_refreshing_props(
        self,
        node_id: str | int,
        props: Iterable[str] | Mapping[str, Any],
        request_generations: Mapping[str, int] | None = None,
    ) -> set[str | int]:
        node_key = _node_key(node_id)
        if node_key is None:
            return set()
        current = self._refreshing_expired_props.get(node_key)
        if current is None:
            return set()
        refreshing_node_id, refreshing_props = current
        before = len(refreshing_props)
        for prop in props:
            if not isinstance(prop, str):
                continue
            if request_generations is not None and request_generations.get(prop) != refreshing_props.get(prop):
                continue
            refreshing_props.pop(prop, None)
        if refreshing_props:
            return {refreshing_node_id} if len(refreshing_props) != before else set()
        self._refreshing_expired_props.pop(node_key, None)
        return {refreshing_node_id}

    def _clear_missing_stale_nodes(self, node_ids: Iterable[str | int]) -> set[str | int]:
        known = {_node_key(node_id) for node_id in node_ids}
        known.discard(None)
        affected: set[str | int] = set()
        for node_key, (node_id, _props) in list(self._stale_node_props.items()):
            if node_key in known:
                continue
            self._stale_node_props.pop(node_key, None)
            affected.add(node_id)
        return affected

    def _clear_missing_refreshing_nodes(self, node_ids: Iterable[str | int]) -> set[str | int]:
        known = {_node_key(node_id) for node_id in node_ids}
        known.discard(None)
        affected: set[str | int] = set()
        for node_key, (node_id, _props) in list(self._refreshing_expired_props.items()):
            if node_key in known:
                continue
            self._refreshing_expired_props.pop(node_key, None)
            affected.add(node_id)
        return affected

    def _schedule_watchdog(self) -> None:
        self._cancel_watchdog()
        now = asyncio.get_running_loop().time()
        next_expiration = self.intents.next_expiration(now=now)
        if next_expiration is None:
            return
        _LOGGER.debug(
            "Yeelight Pro command intent watchdog scheduled: expires_in=%.3f",
            max(0.0, next_expiration - now),
        )
        self._watchdog = self.defer_later(
            max(0.0, next_expiration - now),
            ExpireCommandIntentsCommand(),
            name="yeelight-pro-device-state-intent-watchdog",
        )

    def _cancel_watchdog(self) -> None:
        if self._watchdog is not None:
            self._watchdog.cancel()
            self._watchdog = None

    def _rebuild_visible_cache(self) -> None:
        self._visible_nodes = {}
        for node_id, node in self.state.nodes.items():
            visible = self.intents.project_visible(node)
            if _node_key(node_id) in self._stale_node_props:
                visible = replace(visible, online=False)
            self._visible_nodes[node_id] = visible
        _LOGGER.debug(
            "Yeelight Pro visible cache rebuilt: nodes=%s stale=%s",
            len(self._visible_nodes),
            _stale_summary(self._stale_node_props),
        )

    async def _publish_if_visible_changed(
        self,
        reason: StateChangeReason,
        message: Mapping[str, Any],
        *,
        before_visible: VisibleProjectionSignature,
        changes: tuple[Any, ...] = (),
    ) -> None:
        after_visible = self._visible_projection_signature()
        if after_visible != before_visible:
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
            visible = self.intents.project_visible(node)
            node_key = _node_key(node_id)
            if node_key in self._stale_node_props:
                visible = replace(visible, online=False)
            nodes.append((str(node_key if node_key is not None else node_id), _node_signature(visible)))
        return (
            tuple(sorted(nodes, key=lambda item: item[0])),
            _state_mapping_signature(self.state.groups, excluded_keys=("params",)),
            _state_mapping_signature(self.state.rooms),
            self.intents.signature(),
            _stale_signature(self._stale_node_props),
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


def _stale_signature(stale: Mapping[str, tuple[str | int, set[str]]]) -> tuple[tuple[str, tuple[str, ...]], ...]:
    return tuple(sorted((node_key, tuple(sorted(props))) for node_key, (_node_id, props) in stale.items()))


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
    nodes = []
    raw_nodes = list_payload(message, "nodes")
    for item in raw_nodes[:_MAX_LOG_ITEMS]:
        node_id = _payload_node_id(item)
        params = item.get("params")
        nodes.append(
            {
                "id": node_id,
                "params": dict(params) if isinstance(params, Mapping) else None,
            }
        )
    summary: dict[str, Any] = {
        "method": message.get("method"),
        "node_count": len(raw_nodes),
        "nodes": nodes,
    }
    for key in ("id", "result"):
        if key in message:
            summary[key] = message.get(key)
    raw_groups = list_payload(message, "groups")
    if raw_groups:
        summary["group_count"] = len(raw_groups)
        summary["groups"] = tuple(_summary_state_item(item) for item in raw_groups[:_MAX_LOG_ITEMS])
    return summary


def _summary_state_item(item: Mapping[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {"id": _payload_node_id(item)}
    for key in ("nt", "type"):
        if key in item:
            summary[key] = item.get(key)
    params = item.get("params")
    if isinstance(params, Mapping):
        summary["params"] = dict(params)
    return summary


def _props_by_node_summary(props_by_node: Mapping[str | int, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(node_id): dict(props) for node_id, props in props_by_node.items()}


def _motor_targets_summary(targets: Iterable[Any]) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "node_id": str(target.node_id),
            "current_prop": target.current_prop,
            "target_prop": target.target_prop,
            "target_value": target.target_value,
        }
        for target in targets
    )


def _expired_summary(expired: Iterable[Any]) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "node_id": str(item.node_id),
            "props": tuple(item.props),
        }
        for item in expired
    )


def _stale_summary(stale: Mapping[str, tuple[str | int, Iterable[str]]]) -> dict[str, tuple[str, ...]]:
    return {str(node_id): tuple(sorted(props)) for node_id, props in stale.values()}


def _authoritative_property_items(message: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    yield from list_payload(message, "nodes")
    yield from list_payload(message, "groups")


def _request_generations_for_node(
    generations: Mapping[str | int, Mapping[str, int]],
    node_id: str | int,
) -> Mapping[str, int]:
    direct = generations.get(node_id)
    if direct is not None:
        return direct
    node_key = _node_key(node_id)
    if node_key is None:
        return {}
    for candidate_id, candidate in generations.items():
        if _node_key(candidate_id) == node_key:
            return candidate
    return {}


async def _call_listener(listener: Callable[..., Any], *args: Any) -> None:
    try:
        result = listener(*args)
        if inspect.isawaitable(result):
            await result
    except Exception:  # noqa: BLE001 - HA boundary listeners must not kill actors.
        _LOGGER.exception("Yeelight Pro device state listener failed")


async def _call_listener_strict(listener: Callable[..., Any] | None, *args: Any) -> None:
    if listener is None:
        raise RuntimeError("refresh requester is not configured")
    result = listener(*args)
    if inspect.isawaitable(result):
        await result


def _schedule_listener(listener: Callable[..., Any], *args: Any) -> None:
    create_actor_task(
        _call_listener(listener, *args),
        name="yeelight-pro-device-state-listener",
    )
