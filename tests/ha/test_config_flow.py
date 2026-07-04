from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("homeassistant")
pytest.importorskip("pytest_homeassistant_custom_component")

from homeassistant import data_entry_flow
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.yeelight_pro.config_flow import CannotConnect, GatewayOptions
from custom_components.yeelight_pro.const import (
    CONF_IMPORT_ROOM_IDS,
    CONF_SWITCH_MODES,
    CONF_WIRELESS_SWITCH_NODE_IDS,
    DOMAIN,
    SWITCH_MODE_RELAY,
    SWITCH_MODE_WIRELESS,
)

pytestmark = pytest.mark.usefixtures("enable_custom_integrations")


async def test_config_flow_creates_entry(hass: HomeAssistant) -> None:
    with patch(
        "custom_components.yeelight_pro.config_flow._async_validate_gateway",
        AsyncMock(
            return_value=GatewayOptions(
                room_options=[{"value": "room-1", "label": "Kitchen"}],
                switch_options=[
                    {"value": "switch-1", "label": "Wireless switch"},
                    {"value": "double-switch-1", "label": "Double relay"},
                ],
            )
        ),
    ) as validate:
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": "user"},
            data={
                CONF_HOST: "192.0.2.10",
                CONF_PORT: 65443,
            },
        )

        assert result["type"] is data_entry_flow.FlowResultType.FORM
        assert result["step_id"] == "import_filter"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_IMPORT_ROOM_IDS: ["room-1"],
                CONF_WIRELESS_SWITCH_NODE_IDS: ["switch-1"],
            },
        )

    assert result["type"] is data_entry_flow.FlowResultType.CREATE_ENTRY
    assert result["title"] == "192.0.2.10"
    assert result["data"] == {CONF_HOST: "192.0.2.10", CONF_PORT: 65443}
    assert result["options"] == {
        CONF_IMPORT_ROOM_IDS: ["room-1"],
        CONF_SWITCH_MODES: {
            "switch-1": SWITCH_MODE_WIRELESS,
            "double-switch-1": SWITCH_MODE_RELAY,
        },
    }
    validate.assert_awaited_once_with("192.0.2.10", 65443)


async def test_config_flow_import_filter_defaults_to_all_rooms(hass: HomeAssistant) -> None:
    with patch(
        "custom_components.yeelight_pro.config_flow._async_validate_gateway",
        AsyncMock(
            return_value=GatewayOptions(
                room_options=[
                    {"value": "room-1", "label": "Kitchen"},
                    {"value": "room-2", "label": "Bedroom"},
                ],
                switch_options=[{"value": "switch-1", "label": "Wireless switch"}],
            )
        ),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": "user"},
            data={
                CONF_HOST: "192.0.2.10",
                CONF_PORT: 65443,
            },
        )

    schema = result["data_schema"].schema
    import_room_marker = next(marker for marker in schema if marker.schema == CONF_IMPORT_ROOM_IDS)
    wireless_marker = next(marker for marker in schema if marker.schema == CONF_WIRELESS_SWITCH_NODE_IDS)
    assert import_room_marker.default() == ["room-1", "room-2"]
    assert wireless_marker.default() == []


async def test_config_flow_reports_connection_failure(hass: HomeAssistant) -> None:
    with patch(
        "custom_components.yeelight_pro.config_flow._async_validate_gateway",
        AsyncMock(side_effect=CannotConnect),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": "user"},
            data={
                CONF_HOST: "192.0.2.11",
                CONF_PORT: 65443,
            },
        )

    assert result["type"] is data_entry_flow.FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}


async def test_options_flow_updates_switch_modes(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "192.0.2.10", CONF_PORT: 65443},
        options={
            CONF_IMPORT_ROOM_IDS: ["room-1"],
            CONF_SWITCH_MODES: {
                "switch-1": SWITCH_MODE_WIRELESS,
                "double-switch-1": SWITCH_MODE_RELAY,
            },
        },
    )
    entry.add_to_hass(hass)

    with patch(
        "custom_components.yeelight_pro.config_flow._async_gateway_options",
        AsyncMock(
            return_value=GatewayOptions(
                room_options=[
                    {"value": "room-1", "label": "Kitchen"},
                    {"value": "room-2", "label": "Bedroom"},
                ],
                switch_options=[
                    {"value": "switch-1", "label": "Wireless switch"},
                    {"value": "double-switch-1", "label": "Double relay"},
                ],
            )
        ),
    ):
        result = await hass.config_entries.options.async_init(entry.entry_id)

        schema = result["data_schema"].schema
        import_room_marker = next(marker for marker in schema if marker.schema == CONF_IMPORT_ROOM_IDS)
        wireless_marker = next(marker for marker in schema if marker.schema == CONF_WIRELESS_SWITCH_NODE_IDS)
        assert import_room_marker.default() == ["room-1"]
        assert wireless_marker.default() == ["switch-1"]

        result = await hass.config_entries.options.async_configure(
            result["flow_id"],
            {
                CONF_IMPORT_ROOM_IDS: ["room-2"],
                CONF_WIRELESS_SWITCH_NODE_IDS: ["double-switch-1"],
            },
        )

    assert result["type"] is data_entry_flow.FlowResultType.CREATE_ENTRY
    assert result["data"] == {
        CONF_IMPORT_ROOM_IDS: ["room-2"],
        CONF_SWITCH_MODES: {
            "switch-1": SWITCH_MODE_RELAY,
            "double-switch-1": SWITCH_MODE_WIRELESS,
        },
    }
