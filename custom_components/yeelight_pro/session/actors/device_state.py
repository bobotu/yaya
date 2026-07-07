from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import replace
from typing import Any

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
    RecordCommandIntentCommand,
    RefreshNodeRequestedEvent,
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
from .base import Actor, create_actor_task

_LOGGER = logging.getLogger(__name__)
StateListener = Callable[[StateSnapshotChanged], Awaitable[None] | None]
PropertyListener = Callable[[PropertyChange], Awaitable[None] | None]
RefreshRequester = Callable[[RefreshNodeRequestedEvent], Awaitable[None] | None]


class DeviceStateActor(Actor[DeviceStateActorMessage]):
    """Owns authoritative gateway state and registry-owned command intents."""

    def __init__(self, *, ttl: float | None = None, motor_tracking_ttl: float | None = None) -> None:
        super().__init__("yeelight-pro-device-state")
        self.state = GatewayState()
        intent_kwargs: dict[str, float] = {}
        if ttl is not None:
            intent_kwargs["ttl"] = ttl
        if motor_tracking_ttl is not None:
            intent_kwargs["motor_tracking_ttl"] = motor_tracking_ttl
        self.intents = CommandIntentRegistry(**intent_kwargs)
        self._stale_node_props: dict[str, tuple[str | int, set[str]]] = {}
        self._visible_nodes: dict[str | int, TopologyNode] = {}
        self._watchdog: asyncio.Task[None] | None = None
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
        return self.intents.has_pending(node_id, props)

    def diagnostics(self) -> dict[str, Any]:
        diagnostics = self.intents.diagnostics(now=asyncio.get_running_loop().time())
        diagnostics["stale"] = [
            {"node_id": str(node_id), "properties": sorted(props)} for node_id, props in self._stale_node_props.values()
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
            self.state.apply_groups(message.payload)
            return None
        if isinstance(message, ApplyRoomsCommand):
            self.state.apply_rooms(message.payload)
            return None
        if isinstance(message, ApplyScenesCommand):
            self.state.apply_scenes(message.payload)
            return None
        if isinstance(message, RecordCommandIntentCommand):
            return await self._record_command_intent(message)
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
        now = asyncio.get_running_loop().time()
        affected = self.intents.apply_authoritative_message(event.message, nodes=self.state.nodes, now=now)
        stale_affected = self._clear_stale_from_message(event.message)
        if topology_changed and active_topology_node_ids is not None:
            affected.update(self.intents.clear_missing_nodes(active_topology_node_ids))
            stale_affected.update(self._clear_missing_stale_nodes(active_topology_node_ids))
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
        if event.changes or affected or stale_affected or event.reason in snapshot_reasons:
            await self._publish_snapshot(event.reason, event.message, event.changes)

    async def _record_command_intent(self, message: RecordCommandIntentCommand) -> None:
        now = asyncio.get_running_loop().time()
        affected: set[str | int] = set()
        for node_id, props in message.props_by_node.items():
            affected.update(self._clear_stale_props(node_id, props))
        affected.update(
            self.intents.record_property_intents(
                message.props_by_node,
                nodes=self.state.nodes,
                now=now,
                ttl_by_node=message.ttl_by_node,
            )
        )
        affected.update(self.intents.record_motor_targets(message.motor_targets, nodes=self.state.nodes, now=now))
        for node_id in message.motor_stops:
            affected.update(self.intents.clear_node(node_id))
        if not affected:
            return
        self._schedule_watchdog()
        self._rebuild_visible_cache()
        await self._publish_snapshot(
            StateChangeReason.COMMAND_INTENT_RECORDED,
            {
                "method": SyntheticSessionMethod.COMMAND_INTENT_RECORDED,
                "nodes": [{"id": node_id} for node_id in affected],
            },
        )

    async def _handle_sync_started(self) -> None:
        affected = self.intents.clear_all()
        self._cancel_watchdog()
        if affected:
            self._rebuild_visible_cache()
            await self._publish_snapshot(
                StateChangeReason.COMMAND_INTENT_CLEARED,
                {"method": SyntheticSessionMethod.COMMAND_INTENT_CLEAR},
            )

    async def _handle_session_status(self, event: SessionStatusChanged) -> None:
        if event.current not in {GatewaySessionState.DISCONNECTED, GatewaySessionState.CLOSING}:
            return
        affected = self.intents.clear_all()
        self._cancel_watchdog()
        if affected:
            self._rebuild_visible_cache()
            await self._publish_snapshot(
                StateChangeReason.COMMAND_INTENT_CLEARED,
                {"method": SyntheticSessionMethod.COMMAND_INTENT_CLEAR},
            )

    async def _expire_command_intents(self) -> None:
        expired = self.intents.expire_pending(now=asyncio.get_running_loop().time())
        affected = self._mark_stale(expired)
        self._schedule_watchdog()
        if not affected:
            return
        self._rebuild_visible_cache()
        await self._publish_snapshot(
            StateChangeReason.COMMAND_INTENT_EXPIRED,
            {
                "method": SyntheticSessionMethod.COMMAND_INTENT_EXPIRED,
                "nodes": [{"id": node_id} for node_id in affected],
            },
        )
        if self._refresh_requester is None:
            return
        for node_id in affected:
            create_actor_task(
                _call_listener(self._refresh_requester, RefreshNodeRequestedEvent(node_id=node_id)),
                name=f"yeelight-pro-refresh-node-{node_id}",
            )

    def _mark_stale(self, expired: Iterable[Any]) -> set[str | int]:
        affected: set[str | int] = set()
        for item in expired:
            node_key = _node_key(item.node_id)
            if node_key is None:
                continue
            current = self._stale_node_props.get(node_key)
            if current is None:
                current = (item.node_id, set())
                self._stale_node_props[node_key] = current
            current[1].update(item.props)
            affected.add(item.node_id)
        return affected

    def _clear_stale_from_message(self, message: Mapping[str, Any]) -> set[str | int]:
        affected: set[str | int] = set()
        for item in list_payload(message, "nodes"):
            node_id = _payload_node_id(item)
            params = item.get("params")
            if node_id is None or not isinstance(params, Mapping):
                continue
            affected.update(self._clear_stale_props(node_id, params))
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
            return {stale_node_id} if len(stale_props) != before else set()
        self._stale_node_props.pop(node_key, None)
        return {stale_node_id}

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

    def _schedule_watchdog(self) -> None:
        self._cancel_watchdog()
        now = asyncio.get_running_loop().time()
        next_expiration = self.intents.next_expiration(now=now)
        if next_expiration is None:
            return
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


def _node_key(node_id: object) -> str | None:
    if isinstance(node_id, bool) or not isinstance(node_id, (str, int)):
        return None
    return str(node_id)


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
