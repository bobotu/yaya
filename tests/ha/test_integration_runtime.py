from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

pytest.importorskip("homeassistant")
pytest.importorskip("pytest_homeassistant_custom_component")

from homeassistant.components.climate.const import ATTR_FAN_MODE, FAN_AUTO, FAN_HIGH, FAN_LOW, FAN_MEDIUM, HVACMode
from homeassistant.components.cover import ATTR_POSITION, ATTR_TILT_POSITION
from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_COLOR_TEMP_KELVIN,
    ATTR_FLASH,
    ATTR_TRANSITION,
    FLASH_SHORT,
)
from homeassistant.const import ATTR_ENTITY_ID, CONF_HOST, CONF_PORT, STATE_OFF, STATE_ON, STATE_UNAVAILABLE
from homeassistant.core import Event, HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity import EntityCategory
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.yeelight_pro.const import (
    CONF_IMPORT_ROOM_IDS,
    CONF_SWITCH_MODES,
    DOMAIN,
    EVENT_YEELIGHT_PRO,
    SWITCH_MODE_WIRELESS,
)
from custom_components.yeelight_pro.core import GatewayEvent, NodeCommand
from custom_components.yeelight_pro.helpers import node_unique_id
from custom_components.yeelight_pro.session import (
    GatewaySessionState,
    SessionStatusChanged,
    StateChangeReason,
    StateSnapshotChanged,
)
from custom_components.yeelight_pro.session.model import GatewayState, OptimisticStateOverlay

pytestmark = pytest.mark.usefixtures("enable_custom_integrations")


class FakeGateway:
    def __init__(self, fixture: dict[str, Any]) -> None:
        self.fixture = fixture
        self.state = GatewayState()
        self.session_state = "ready"
        self.last_full_sync_at = None
        self.last_full_sync_source = None
        self.last_disconnect_error = None
        self.connected = False
        self.commands: list[NodeCommand] = []
        self.refreshed_node_ids: list[str | int] = []
        self.sync_kwargs: list[dict[str, Any]] = []
        self._overlay = OptimisticStateOverlay()
        self._event_listeners = []
        self._property_listeners = []
        self._session_listeners = []
        self._state_listeners = []
        self._closed = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        self.connected = True
        self._closed.clear()
        previous = self.session_state
        self.session_state = GatewaySessionState.CONNECTING.value
        self._notify_session(GatewaySessionState.CONNECTING, previous=previous)

    async def close(self) -> None:
        previous = self.session_state
        self.connected = False
        self.session_state = GatewaySessionState.DISCONNECTED.value
        self._notify_session(GatewaySessionState.DISCONNECTED, previous=previous)
        self._closed.set()

    async def wait_closed(self) -> None:
        await self._closed.wait()

    async def start(self, **kwargs: Any) -> None:
        await self.connect()
        await self.sync(**kwargs)

    async def stop(self) -> None:
        await self.close()

    async def sync(self, **kwargs: Any) -> None:
        self.sync_kwargs.append(kwargs)
        self._overlay.clear_all()
        self.state.apply_topology(self.fixture)
        self.last_full_sync_source = "poll"
        previous = self.session_state
        self.session_state = GatewaySessionState.READY.value
        self._notify_session(GatewaySessionState.READY, previous=previous)

    async def refresh_node(self, node_id: str | int) -> dict[str, Any]:
        self.refreshed_node_ids.append(node_id)
        return {"nodes": []}

    async def send_node_command(self, command: NodeCommand) -> dict[str, Any]:
        self.commands.append(command)
        return {"result": "ok"}

    async def set_node_props(
        self,
        node_id: str | int,
        props: dict[str, Any],
        *,
        nt: int = 2,
        duration: int | None = None,
        optimistic_props: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.commands.append(NodeCommand(id=node_id, nt=nt, props=props, duration=duration))
        if optimistic_props:
            self._overlay.set_props(node_id, optimistic_props, now=asyncio.get_running_loop().time())
            self._notify_state({"method": "gateway_overlay.optimistic"})
        return {"result": "ok"}

    def visible_node(self, node_id: str | int) -> Any:
        node = self.state.nodes.get(node_id)
        return None if node is None else self._overlay.visible_node(node, now=asyncio.get_running_loop().time())

    def visible_nodes(self) -> list[Any]:
        now = asyncio.get_running_loop().time()
        return [self._overlay.visible_node(node, now=now) for node in self.state.nodes.values()]

    def has_pending_overlay(self, node_id: str | int, props: Any = None) -> bool:
        return self._overlay.has_pending(node_id, props)

    def optimistic_diagnostics(self) -> dict[str, Any]:
        return self._overlay.diagnostics(now=asyncio.get_running_loop().time())

    def motor_tracking_diagnostics(self) -> dict[str, Any]:
        return {"count": 0, "entries": []}

    def add_event_listener(self, listener: Any) -> Any:
        self._event_listeners.append(listener)

        def remove() -> None:
            self._event_listeners.remove(listener)

        return remove

    def add_property_listener(self, listener: Any) -> Any:
        self._property_listeners.append(listener)

        def remove() -> None:
            self._property_listeners.remove(listener)

        return remove

    def add_session_listener(self, listener: Any) -> Any:
        self._session_listeners.append(listener)

        def remove() -> None:
            self._session_listeners.remove(listener)

        return remove

    def add_state_listener(self, listener: Any) -> Any:
        self._state_listeners.append(listener)

        def remove() -> None:
            self._state_listeners.remove(listener)

        return remove

    def emit_event(self, event: GatewayEvent) -> None:
        for listener in list(self._event_listeners):
            listener(event)

    def update_node_params(self, node_id: str | int, params: dict[str, Any]) -> None:
        self.state.apply_properties(
            {
                "method": "gateway_post.prop",
                "nodes": [
                    {
                        "id": node_id,
                        "nt": 2,
                        "params": params,
                    }
                ],
            }
        )
        self._overlay.reconcile_node_props(node_id, params)
        self._notify_state({"method": "gateway_post.prop"})

    def _notify_state(self, message: dict[str, Any]) -> None:
        event = StateSnapshotChanged(reason=_state_reason(message), message=message)
        for listener in list(self._state_listeners):
            listener(event)

    def _notify_session(
        self,
        current: GatewaySessionState,
        *,
        previous: str | GatewaySessionState | None = None,
        error: BaseException | None = None,
    ) -> None:
        if previous is None:
            previous_state = current
        elif isinstance(previous, GatewaySessionState):
            previous_state = previous
        else:
            previous_state = GatewaySessionState(previous)
        event = SessionStatusChanged(previous=previous_state, current=current, error=error)
        for listener in list(self._session_listeners):
            listener(event)

    def replace_topology(self, fixture: dict[str, Any]) -> None:
        self.fixture = fixture
        self.state.apply_topology(fixture)
        self._overlay.clear_missing_nodes(self.state.topology_node_ids)
        for listener in list(self._state_listeners):
            listener(
                StateSnapshotChanged(
                    reason=StateChangeReason.TOPOLOGY_SYNC,
                    message={"method": "gateway_sync.topology"},
                )
            )

    def push_topology(self, fixture: dict[str, Any]) -> None:
        self.fixture = fixture
        self.state.apply_topology(fixture, replace=False)
        for listener in list(self._state_listeners):
            listener(
                StateSnapshotChanged(
                    reason=StateChangeReason.TOPOLOGY_PUSH,
                    message={"method": "gateway_post.topology"},
                )
            )


@pytest.fixture
def topology_fixture() -> dict[str, Any]:
    fixture = json.loads((Path(__file__).parents[1] / "unit" / "fixtures" / "topology-direct.json").read_text())
    fixture = deepcopy(fixture)
    fixture["nodes"].append(
        {
            "id": "knob-panel-1",
            "nt": 2,
            "type": 128,
            "pt": 137,
            "name": "Knob panel",
        }
    )
    return fixture


async def test_relay_switch_service_uses_optimistic_state_until_gateway_push(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        switch_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_switch-1_relay_2")
        switch_state = hass.states.get(switch_entity_id)
        assert switch_state.state == STATE_ON
        assert switch_state.attributes["friendly_name"] == "Wireless switch Relay 2"

        await hass.services.async_call(
            "switch",
            "turn_off",
            {ATTR_ENTITY_ID: switch_entity_id},
            blocking=True,
        )
        await hass.async_block_till_done()

        assert gateway.commands[-1].to_payload()["set"] == {"2-sp": False}
        assert hass.states.get(switch_entity_id).state == STATE_OFF
        assert gateway.has_pending_overlay("switch-1", ["2-sp"])

        gateway.update_node_params("switch-1", {"2-sp": False})
        await hass.async_block_till_done()
        assert hass.states.get(switch_entity_id).state == "off"
        assert not gateway.has_pending_overlay("switch-1", ["2-sp"])
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_relay_switch_conflicting_gateway_push_overrides_optimistic_state(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        switch_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_switch-1_relay_2")

        await hass.services.async_call(
            "switch",
            "turn_off",
            {ATTR_ENTITY_ID: switch_entity_id},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert hass.states.get(switch_entity_id).state == STATE_OFF
        assert gateway.has_pending_overlay("switch-1", ["2-sp"])

        gateway.update_node_params("switch-1", {"2-sp": True})
        await hass.async_block_till_done()

        assert hass.states.get(switch_entity_id).state == STATE_ON
        assert not gateway.has_pending_overlay("switch-1", ["2-sp"])
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_relay_switch_mode_hides_programmable_events(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        registry = er.async_get(hass)
        entity_unique_ids = {item.unique_id for item in er.async_entries_for_config_entry(registry, entry.entry_id)}
        assert any(unique_id.endswith("_switch-1_relay_1") for unique_id in entity_unique_ids)
        assert not any(unique_id.endswith("_switch-1_control_1_events") for unique_id in entity_unique_ids)
        assert not any(unique_id.endswith("_panel-1_0-blp") for unique_id in entity_unique_ids)

        device = dr.async_get(hass).async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:switch-1")})
        assert device is not None

        from custom_components.yeelight_pro import device_trigger

        assert await device_trigger.async_get_triggers(hass, device.id) == []

        events: list[Event] = []
        hass.bus.async_listen(EVENT_YEELIGHT_PRO, events.append)
        gateway.emit_event(
            GatewayEvent(
                id="switch-1",
                nt=2,
                value="panel.click",
                params={"key": 1, "count": 1},
            )
        )
        await hass.async_block_till_done()
        assert events == []
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_relay_switch_mode_removes_stale_wireless_entities(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    registry = er.async_get(hass)

    stale_entity_ids: list[str] = []

    def create_stale_entries(entry: MockConfigEntry) -> None:
        stale_event = registry.async_get_or_create(
            "event",
            DOMAIN,
            node_unique_id("127.0.0.1:65443", "switch-1", "control_1_events"),
            config_entry=entry,
        )
        stale_diagnostic = registry.async_get_or_create(
            "binary_sensor",
            DOMAIN,
            node_unique_id("127.0.0.1:65443", "switch-1", "relay_1_state"),
            config_entry=entry,
        )
        stale_entity_ids.extend([stale_event.entity_id, stale_diagnostic.entity_id])

    entry = await _setup_entry(hass, gateway, before_setup=create_stale_entries)

    try:
        for entity_id in stale_entity_ids:
            assert registry.async_get(entity_id) is None
            assert hass.states.get(entity_id) is None
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_wireless_switch_mode_exposes_events_and_relay_diagnostics(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway, switch_modes={"switch-1": SWITCH_MODE_WIRELESS})

    try:
        registry = er.async_get(hass)
        entity_unique_ids = {item.unique_id for item in er.async_entries_for_config_entry(registry, entry.entry_id)}
        assert not any(unique_id.endswith("_switch-1_relay_1") for unique_id in entity_unique_ids)

        relay_1_state_id = _entity_id_for_unique_id(hass, entry.entry_id, "_switch-1_relay_1_state")
        relay_2_state_id = _entity_id_for_unique_id(hass, entry.entry_id, "_switch-1_relay_2_state")
        event_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_switch-1_control_1_events")

        assert registry.async_get(relay_1_state_id).entity_category is EntityCategory.DIAGNOSTIC
        assert registry.async_get(relay_2_state_id).entity_category is EntityCategory.DIAGNOSTIC
        assert hass.states.get(relay_1_state_id).state == STATE_OFF
        assert hass.states.get(relay_2_state_id).state == STATE_ON

        device = dr.async_get(hass).async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:switch-1")})
        assert device is not None

        from custom_components.yeelight_pro import device_trigger

        triggers = await device_trigger.async_get_triggers(hass, device.id)
        assert {
            "platform": "device",
            "domain": DOMAIN,
            "device_id": device.id,
            "type": "panel_click",
            "subtype": "key_1",
        } in triggers

        events: list[Event] = []
        hass.bus.async_listen(EVENT_YEELIGHT_PRO, events.append)
        gateway.emit_event(
            GatewayEvent(
                id="switch-1",
                nt=2,
                value="panel.click",
                params={"key": 1, "count": 1},
            )
        )
        await hass.async_block_till_done()

        assert events[-1].data["type"] == "panel_click"
        assert events[-1].data["subtype"] == "key_1"
        assert hass.states.get(event_entity_id).attributes["event_type"] == "panel_click"

        gateway.update_node_params("switch-1", {"1-sp": True, "2-sp": False})
        await hass.async_block_till_done()
        assert hass.states.get(relay_1_state_id).state == STATE_ON
        assert hass.states.get(relay_2_state_id).state == STATE_OFF
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_wireless_switch_mode_removes_stale_relay_switch_entities(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    registry = er.async_get(hass)
    stale_entity_ids: list[str] = []

    def create_stale_entries(entry: MockConfigEntry) -> None:
        stale_switch = registry.async_get_or_create(
            "switch",
            DOMAIN,
            node_unique_id("127.0.0.1:65443", "switch-1", "relay_1"),
            config_entry=entry,
        )
        stale_entity_ids.append(stale_switch.entity_id)

    entry = await _setup_entry(
        hass,
        gateway,
        switch_modes={"switch-1": SWITCH_MODE_WIRELESS},
        before_setup=create_stale_entries,
    )

    try:
        for entity_id in stale_entity_ids:
            assert registry.async_get(entity_id) is None
            assert hass.states.get(entity_id) is None
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_knob_event_bus_payload_and_device_triggers(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        events: list[Event] = []
        hass.bus.async_listen(EVENT_YEELIGHT_PRO, events.append)

        gateway.emit_event(
            GatewayEvent(
                id="knob-panel-1",
                nt=2,
                value="knob.spin",
                params={"idx": 3, "3-free_spin": 4},
            )
        )
        await hass.async_block_till_done()

        assert events[-1].data["type"] == "knob_spin"
        assert events[-1].data["subtype"] == "idx_3"
        assert events[-1].data["idx"] == 3
        assert events[-1].data["delta"] == 4
        assert events[-1].data["direction"] == "clockwise"

        event_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_knob-panel-1_control_3_events")
        event_state = hass.states.get(event_entity_id)
        assert event_state.attributes["event_type"] == "knob_spin"
        assert event_state.attributes["type"] == "knob_spin"
        assert event_state.attributes["idx"] == 3
        assert event_state.attributes["subtype"] == "idx_3"
        assert event_state.attributes["delta"] == 4
        assert event_state.attributes["direction"] == "clockwise"
        assert event_state.attributes["friendly_name"] == "Knob panel Control 3 events"

        other_event_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_knob-panel-1_control_2_events")
        assert hass.states.get(other_event_entity_id).state == "unknown"

        device = dr.async_get(hass).async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:knob-panel-1")})
        assert device is not None

        from custom_components.yeelight_pro import device_trigger

        triggers = await device_trigger.async_get_triggers(hass, device.id)
        trigger = {
            "platform": "device",
            "domain": DOMAIN,
            "device_id": device.id,
            "type": "knob_spin",
            "subtype": "idx_3",
        }
        assert trigger in triggers
        assert await device_trigger.async_validate_trigger_config(hass, trigger) == trigger

        trigger_calls: list[dict[str, Any]] = []

        async def _trigger_action(run_variables: dict[str, Any], _context: Any = None) -> None:
            trigger_calls.append(run_variables)

        remove_trigger = await device_trigger.async_attach_trigger(
            hass,
            trigger,
            _trigger_action,
            _trigger_info(),
        )
        try:
            gateway.emit_event(
                GatewayEvent(
                    id="knob-panel-1",
                    nt=2,
                    value="knob.spin",
                    params={"idx": 3, "3-free_spin": -2},
                )
            )
            await hass.async_block_till_done()

            assert trigger_calls
            trigger_event = trigger_calls[-1]["trigger"]["event"]
            assert trigger_event.data["type"] == "knob_spin"
            assert trigger_event.data["subtype"] == "idx_3"
            assert trigger_event.data["delta"] == -2
            assert hass.states.get(event_entity_id).attributes["event_type"] == "knob_spin"
            assert hass.states.get(event_entity_id).attributes["direction"] == "counterclockwise"
        finally:
            remove_trigger()
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_light_groups_are_imported_by_default(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        group_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_group-node-1_light")
        state = hass.states.get(group_entity_id)
        assert state is not None
        assert state.attributes["node_id"] == "group-node-1"
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_import_room_filter_limits_created_entities(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway, import_room_ids=["room-2"])

    try:
        registry = er.async_get(hass)
        entity_unique_ids = {entry.unique_id for entry in er.async_entries_for_config_entry(registry, entry.entry_id)}

        assert any(unique_id.endswith("_curtain-1_cover") for unique_id in entity_unique_ids)
        assert not any(unique_id.endswith("_light-1_light") for unique_id in entity_unique_ids)
        assert not any(unique_id.endswith("_switch-1_relay_1") for unique_id in entity_unique_ids)
        assert gateway.sync_kwargs[-1]["include_rooms"] is True
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_import_room_filter_keeps_stale_filtered_registry_entries(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    registry = er.async_get(hass)
    device_registry = dr.async_get(hass)
    stale_entity_ids: list[str] = []
    stale_device_ids: list[str] = []

    def create_stale_entries(entry: MockConfigEntry) -> None:
        device = device_registry.async_get_or_create(
            config_entry_id=entry.entry_id,
            identifiers={(DOMAIN, "127.0.0.1:65443:light-1")},
            manufacturer="Yeelight",
        )
        stale_entity = registry.async_get_or_create(
            "light",
            DOMAIN,
            node_unique_id("127.0.0.1:65443", "light-1", "light"),
            config_entry=entry,
            device_id=device.id,
        )
        stale_device_ids.append(device.id)
        stale_entity_ids.append(stale_entity.entity_id)

    entry = await _setup_entry(hass, gateway, import_room_ids=["room-2"], before_setup=create_stale_entries)

    try:
        for entity_id in stale_entity_ids:
            assert registry.async_get(entity_id) is not None
            assert hass.states.get(entity_id) is None
        for device_id in stale_device_ids:
            assert device_registry.async_get(device_id) is not None
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_topology_push_missing_node_keeps_entity_and_device(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        registry = er.async_get(hass)
        device_registry = dr.async_get(hass)
        light_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_light-1_light")
        light_device = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:light-1")})
        assert light_device is not None

        updated_fixture = deepcopy(topology_fixture)
        updated_fixture["nodes"] = [node for node in updated_fixture["nodes"] if node["id"] != "light-1"]
        gateway.push_topology(updated_fixture)
        await hass.async_block_till_done()

        assert registry.async_get(light_entity_id) is not None
        assert hass.states.get(light_entity_id) is not None
        assert device_registry.async_get(light_device.id) is not None
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_full_topology_sync_missing_node_keeps_entity_unavailable(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        registry = er.async_get(hass)
        device_registry = dr.async_get(hass)
        light_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_light-1_light")
        light_device = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:light-1")})
        assert light_device is not None

        updated_fixture = deepcopy(topology_fixture)
        updated_fixture["nodes"] = [node for node in updated_fixture["nodes"] if node["id"] != "light-1"]
        gateway.replace_topology(updated_fixture)
        await hass.async_block_till_done()

        assert registry.async_get(light_entity_id) is not None
        light_state = hass.states.get(light_entity_id)
        assert light_state is not None
        assert light_state.state == STATE_UNAVAILABLE
        assert device_registry.async_get(light_device.id) is not None
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_light_service_maps_transition_and_flash(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        light_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_light-1_light")
        state = hass.states.get(light_entity_id)
        assert state is not None
        assert state.attributes["min_color_temp_kelvin"] == 2700
        assert state.attributes["max_color_temp_kelvin"] == 6500

        await hass.services.async_call(
            "light",
            "turn_on",
            {
                ATTR_ENTITY_ID: light_entity_id,
                ATTR_BRIGHTNESS: 128,
                ATTR_COLOR_TEMP_KELVIN: 3000,
                ATTR_TRANSITION: 1.5,
            },
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"p": True, "l": 50, "ct": 3000}
        assert gateway.commands[-1].to_payload()["duration"] == 1500
        state = hass.states.get(light_entity_id)
        assert state.state == STATE_ON
        assert state.attributes["brightness"] == 128
        assert state.attributes["color_temp_kelvin"] == 3000

        await hass.services.async_call(
            "light",
            "turn_off",
            {ATTR_ENTITY_ID: light_entity_id, ATTR_TRANSITION: 2},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"p": False}
        assert gateway.commands[-1].to_payload()["duration"] == 2000
        assert hass.states.get(light_entity_id).state == STATE_OFF

        await hass.services.async_call(
            "light",
            "turn_on",
            {ATTR_ENTITY_ID: light_entity_id, ATTR_FLASH: FLASH_SHORT},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["action"] == {"blink": {"repeat": 4, "type": "urgent"}}
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_light_service_optimistic_state_reconciles_with_conflicting_push(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        light_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_light-1_light")

        await hass.services.async_call(
            "light",
            "turn_on",
            {
                ATTR_ENTITY_ID: light_entity_id,
                ATTR_BRIGHTNESS: 128,
                ATTR_COLOR_TEMP_KELVIN: 3000,
            },
            blocking=True,
        )
        await hass.async_block_till_done()

        state = hass.states.get(light_entity_id)
        assert state.state == STATE_ON
        assert state.attributes["brightness"] == 128
        assert state.attributes["color_temp_kelvin"] == 3000
        assert gateway.has_pending_overlay("light-1", ["p", "l", "ct"])

        gateway.update_node_params("light-1", {"p": True, "l": 80, "ct": 4000})
        await hass.async_block_till_done()

        state = hass.states.get(light_entity_id)
        assert state.state == STATE_ON
        assert state.attributes["brightness"] == 204
        assert state.attributes["color_temp_kelvin"] == 4000
        assert not gateway.has_pending_overlay("light-1", ["p", "l", "ct"])
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_light_flash_command_does_not_create_optimistic_overlay(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        light_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_light-1_light")

        await hass.services.async_call(
            "light",
            "turn_on",
            {ATTR_ENTITY_ID: light_entity_id, ATTR_FLASH: FLASH_SHORT},
            blocking=True,
        )
        await hass.async_block_till_done()

        assert gateway.commands[-1].to_payload()["action"] == {"blink": {"repeat": 4, "type": "urgent"}}
        assert not gateway.has_pending_overlay("light-1")
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_knob_without_battery_props_still_gets_unknown_battery_entities(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        battery_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_knob-panel-1_battery")
        charging_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_knob-panel-1_battery_charging")
        registry = er.async_get(hass)

        assert registry.async_get(battery_entity_id).entity_category is EntityCategory.DIAGNOSTIC
        assert registry.async_get(charging_entity_id).entity_category is EntityCategory.DIAGNOSTIC

        assert hass.states.get(battery_entity_id).state == "unknown"
        assert hass.states.get(charging_entity_id).state == "unknown"

        gateway.update_node_params("knob-panel-1", {"bp": 88, "bc": False})
        await hass.async_block_till_done()

        assert hass.states.get(battery_entity_id).state == "88"
        assert hass.states.get(charging_entity_id).state == "off"
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_sensor_measurement_entities(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        level_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_sensor-1_light_level")
        luminance_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_sensor-1_luminance")

        assert hass.states.get(level_entity_id).state == "3"
        luminance_state = hass.states.get(luminance_entity_id)
        assert luminance_state.state == "128"
        assert luminance_state.attributes["device_class"] == "illuminance"
        assert luminance_state.attributes["unit_of_measurement"] == "lx"
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_person_sensor_exposes_occupancy_binary_sensor(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        occupancy_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_sensor-1_occupancy")
        assert er.async_get(hass).async_get(occupancy_entity_id).entity_category is None
        occupancy_state = hass.states.get(occupancy_entity_id)
        assert occupancy_state.state == "on"
        assert occupancy_state.attributes["device_class"] == "occupancy"
        assert occupancy_state.attributes["friendly_name"] == "Motion Occupancy"

        gateway.update_node_params("sensor-1", {"mv": False})
        await hass.async_block_till_done()

        assert hass.states.get(occupancy_entity_id).state == "off"
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_air_conditioner_climate_and_config_entities(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        climate_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_air-1_air_conditioner_1")
        remote_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_air-1_1-acrc")
        deflector_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_air-1_1-acdfltr")
        registry = er.async_get(hass)

        assert hass.states.get(climate_entity_id).state == "off"
        climate_state = hass.states.get(climate_entity_id)
        assert climate_state is not None
        assert climate_state.attributes["friendly_name"] == "Bedroom AC"
        assert climate_state.attributes["temperature"] == 24.0
        assert climate_state.attributes["hvac_modes"] == ["off", "cool", "heat", "dry", "fan_only"]
        assert climate_state.attributes["fan_mode"] == FAN_MEDIUM
        assert climate_state.attributes["fan_modes"] == [FAN_AUTO, FAN_LOW, FAN_MEDIUM, FAN_HIGH]
        assert registry.async_get(remote_entity_id).entity_category is EntityCategory.CONFIG
        with pytest.raises(AssertionError):
            _entity_id_for_unique_id(hass, entry.entry_id, "_air-1_1-acd")
        assert registry.async_get(deflector_entity_id).entity_category is EntityCategory.CONFIG

        await hass.services.async_call(
            "climate",
            "set_hvac_mode",
            {ATTR_ENTITY_ID: climate_entity_id, "hvac_mode": HVACMode.HEAT},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"1-acp": True, "1-acm": 8}
        assert hass.states.get(climate_entity_id).state == HVACMode.HEAT

        await hass.services.async_call(
            "climate",
            "set_hvac_mode",
            {ATTR_ENTITY_ID: climate_entity_id, "hvac_mode": HVACMode.DRY},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"1-acp": True, "1-acm": 2}
        assert hass.states.get(climate_entity_id).state == HVACMode.DRY

        await hass.services.async_call(
            "climate",
            "set_fan_mode",
            {ATTR_ENTITY_ID: climate_entity_id, ATTR_FAN_MODE: FAN_AUTO},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"1-acf": 0}
        assert hass.states.get(climate_entity_id).attributes["fan_mode"] == FAN_AUTO

        await hass.services.async_call(
            "climate",
            "set_fan_mode",
            {ATTR_ENTITY_ID: climate_entity_id, ATTR_FAN_MODE: FAN_HIGH},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"1-acf": 1}
        assert hass.states.get(climate_entity_id).attributes["fan_mode"] == FAN_HIGH

        await hass.services.async_call(
            "switch",
            "turn_off",
            {ATTR_ENTITY_ID: remote_entity_id},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"1-acrc": False}
        assert hass.states.get(remote_entity_id).state == STATE_OFF

        await hass.services.async_call(
            "number",
            "set_value",
            {ATTR_ENTITY_ID: deflector_entity_id, "value": 64},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"1-acdfltr": 64}
        assert float(hass.states.get(deflector_entity_id).state) == 64.0
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_multi_channel_air_conditioner_climate_entities_keep_channel_names(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    fixture = deepcopy(topology_fixture)
    air_node = next(node for node in fixture["nodes"] if node["id"] == "air-1")
    air_node["params"].update(
        {
            "2-aco": True,
            "2-acp": True,
            "2-acm": 8,
            "2-acct": 25,
            "2-actt": 23,
            "2-acf": 1,
        }
    )
    gateway = FakeGateway(fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        climate_1_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_air-1_air_conditioner_1")
        climate_2_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_air-1_air_conditioner_2")

        climate_1_state = hass.states.get(climate_1_entity_id)
        climate_2_state = hass.states.get(climate_2_entity_id)
        assert climate_1_state is not None
        assert climate_2_state is not None
        assert climate_1_state.attributes["friendly_name"] == "Bedroom AC Air conditioner 1"
        assert climate_2_state.attributes["friendly_name"] == "Bedroom AC Air conditioner 2"
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_air_conditioner_delay_number_is_removed_as_obsolete_entity(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    registry = er.async_get(hass)
    stale_entity_ids: list[str] = []

    def create_stale_entries(entry: MockConfigEntry) -> None:
        stale_delay = registry.async_get_or_create(
            "number",
            DOMAIN,
            node_unique_id("127.0.0.1:65443", "air-1", "1-acd"),
            config_entry=entry,
        )
        stale_entity_ids.append(stale_delay.entity_id)

    entry = await _setup_entry(hass, gateway, before_setup=create_stale_entries)

    try:
        for entity_id in stale_entity_ids:
            assert registry.async_get(entity_id) is None
            assert hass.states.get(entity_id) is None
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_bath_heater_standard_platform_entities(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        climate_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_bath-1_bath_heater_climate")
        fan_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_bath-1_ve")
        heat_number_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_bath-1_he")
        mode_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_bath-1_bath_mode")
        registry = er.async_get(hass)

        assert hass.states.get(climate_entity_id).state == "heat"
        assert hass.states.get(climate_entity_id).attributes["temperature"] == 38.0
        assert registry.async_get(fan_entity_id).entity_category is EntityCategory.CONFIG
        assert registry.async_get(heat_number_entity_id).entity_category is EntityCategory.CONFIG
        assert registry.async_get(mode_entity_id).entity_category is EntityCategory.CONFIG

        await hass.services.async_call(
            "climate",
            "set_temperature",
            {ATTR_ENTITY_ID: climate_entity_id, "temperature": 40},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"tgt": 40}
        assert hass.states.get(climate_entity_id).attributes["temperature"] == 40.0

        await hass.services.async_call(
            "fan",
            "set_percentage",
            {ATTR_ENTITY_ID: fan_entity_id, "percentage": 100},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"ve": 3}
        assert hass.states.get(fan_entity_id).attributes["percentage"] == 100

        await hass.services.async_call(
            "number",
            "set_value",
            {ATTR_ENTITY_ID: heat_number_entity_id, "value": 1},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"he": 1}
        assert float(hass.states.get(heat_number_entity_id).state) == 1.0

        await hass.services.async_call(
            "select",
            "select_option",
            {ATTR_ENTITY_ID: mode_entity_id, "option": "mode_4"},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"bhm": 4}
        assert hass.states.get(mode_entity_id).state == "mode_4"
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_bath_heater_config_entities_use_optimistic_state_until_push(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        fan_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_bath-1_ve")
        heat_number_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_bath-1_he")
        mode_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_bath-1_bath_mode")

        await hass.services.async_call(
            "fan",
            "set_percentage",
            {ATTR_ENTITY_ID: fan_entity_id, "percentage": 100},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert hass.states.get(fan_entity_id).attributes["percentage"] == 100
        assert gateway.has_pending_overlay("bath-1", ["ve"])
        gateway.update_node_params("bath-1", {"ve": 3})
        await hass.async_block_till_done()
        assert not gateway.has_pending_overlay("bath-1", ["ve"])

        await hass.services.async_call(
            "number",
            "set_value",
            {ATTR_ENTITY_ID: heat_number_entity_id, "value": 1},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert float(hass.states.get(heat_number_entity_id).state) == 1.0
        assert gateway.has_pending_overlay("bath-1", ["he"])
        gateway.update_node_params("bath-1", {"he": 1})
        await hass.async_block_till_done()
        assert not gateway.has_pending_overlay("bath-1", ["he"])

        await hass.services.async_call(
            "select",
            "select_option",
            {ATTR_ENTITY_ID: mode_entity_id, "option": "mode_4"},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert hass.states.get(mode_entity_id).state == "mode_4"
        assert gateway.has_pending_overlay("bath-1", ["bhm"])
        gateway.update_node_params("bath-1", {"bhm": 4})
        await hass.async_block_till_done()
        assert not gateway.has_pending_overlay("bath-1", ["bhm"])
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_device_registry_uses_model_without_protocol_diagnostics(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        device_registry = dr.async_get(hass)

        light = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:light-1")})
        assert light is not None
        assert light.name == "Kitchen light"
        assert light.model == "Light"
        assert light.model_id is None
        assert light.serial_number == "light-1"
        assert light.hw_version is None

        cover = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:curtain-1")})
        assert cover is not None
        assert cover.name == "Dream curtain"
        assert cover.model == "Dream curtain"
        assert cover.model_id is None
        assert cover.serial_number == "curtain-1"
        assert cover.hw_version is None

        knob_panel = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:knob-panel-1")})
        assert knob_panel is not None
        assert knob_panel.name == "Knob panel"
        assert knob_panel.model == "Knob panel"
        assert knob_panel.model_id is None
        assert knob_panel.serial_number == "knob-panel-1"
        assert knob_panel.hw_version is None

        gateway = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443")})
        assert gateway is not None
        assert gateway.name == "Yeelight Pro Gateway 127.0.0.1"
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_device_registry_model_is_not_translated(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    hass.config.language = "zh-Hans"
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        device_registry = dr.async_get(hass)

        light = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:light-1")})
        assert light is not None
        assert light.model == "Light"

        cover = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:curtain-1")})
        assert cover is not None
        assert cover.model == "Dream curtain"

        knob_panel = device_registry.async_get_device(identifiers={(DOMAIN, "127.0.0.1:65443:knob-panel-1")})
        assert knob_panel is not None
        assert knob_panel.model == "Knob panel"
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_unknown_property_nodes_are_diagnostics_only(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        gateway.update_node_params("unknown-prop-only", {"p": True, "l": 42})
        await hass.async_block_till_done()

        registry = er.async_get(hass)
        entity_unique_ids = {item.unique_id for item in er.async_entries_for_config_entry(registry, entry.entry_id)}
        assert not any("unknown-prop-only" in unique_id for unique_id in entity_unique_ids)

        from custom_components.yeelight_pro.diagnostics import async_get_config_entry_diagnostics

        diagnostics = await async_get_config_entry_diagnostics(hass, entry)
        assert diagnostics["unknown_property_nodes"]["count"] == 1
        assert "nt=2;pt=None;params=l,p" in diagnostics["unknown_property_nodes"]["by_shape"]
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_cover_services_send_standard_commands(
    hass: HomeAssistant,
    topology_fixture: dict[str, Any],
) -> None:
    gateway = FakeGateway(topology_fixture)
    entry = await _setup_entry(hass, gateway)

    try:
        cover_entity_id = _entity_id_for_unique_id(hass, entry.entry_id, "_curtain-1_cover")

        await hass.services.async_call(
            "cover",
            "set_cover_position",
            {ATTR_ENTITY_ID: cover_entity_id, ATTR_POSITION: 66},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"tp": 66}
        assert not gateway.has_pending_overlay("curtain-1")
        assert hass.states.get(cover_entity_id).attributes["current_position"] == 20

        await hass.services.async_call(
            "cover",
            "stop_cover",
            {ATTR_ENTITY_ID: cover_entity_id},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["action"] == {"motorAdjust": {"type": "pause"}}
        assert not gateway.has_pending_overlay("curtain-1")

        await hass.services.async_call(
            "cover",
            "set_cover_tilt_position",
            {ATTR_ENTITY_ID: cover_entity_id, ATTR_TILT_POSITION: 50},
            blocking=True,
        )
        await hass.async_block_till_done()
        assert gateway.commands[-1].to_payload()["set"] == {"tra": 90}
        assert not gateway.has_pending_overlay("curtain-1")
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def _setup_entry(
    hass: HomeAssistant,
    gateway: FakeGateway,
    *,
    import_room_ids: list[str] | None = None,
    switch_modes: dict[str, str] | None = None,
    before_setup: Callable[[MockConfigEntry], None] | None = None,
) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "127.0.0.1", CONF_PORT: 65443},
        options={
            CONF_IMPORT_ROOM_IDS: import_room_ids or [],
            CONF_SWITCH_MODES: switch_modes or {},
        },
    )
    entry.add_to_hass(hass)
    if before_setup is not None:
        before_setup(entry)
    with patch("custom_components.yeelight_pro.coordinator.YeelightProGateway", return_value=gateway):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    return entry


def _entity_id_for_unique_id(hass: HomeAssistant, entry_id: str, unique_id_suffix: str) -> str:
    registry = er.async_get(hass)
    for entry in er.async_entries_for_config_entry(registry, entry_id):
        if entry.unique_id.endswith(unique_id_suffix):
            return entry.entity_id
    raise AssertionError(f"entity with unique id suffix {unique_id_suffix} not found")


def _state_reason(message: dict[str, Any]) -> StateChangeReason:
    method = message.get("method")
    if method == "gateway_post.topology":
        return StateChangeReason.TOPOLOGY_PUSH
    if method == "gateway_post.prop":
        return StateChangeReason.PROPERTY_PUSH
    if method == "gateway_overlay.optimistic":
        return StateChangeReason.OPTIMISTIC_UPDATE
    return StateChangeReason.GENERIC_PUSH


def _trigger_info() -> dict[str, Any]:
    return {
        "domain": DOMAIN,
        "name": "test",
        "home_assistant_start": False,
        "variables": {},
        "trigger_data": {
            "id": "0",
            "idx": "0",
            "alias": None,
        },
    }
