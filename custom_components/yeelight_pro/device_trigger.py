from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.components.device_automation import DEVICE_TRIGGER_BASE_SCHEMA
from homeassistant.components.homeassistant.triggers import event as event_trigger
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_DEVICE_ID, CONF_DOMAIN, CONF_EVENT_DATA, CONF_PLATFORM, CONF_TYPE
from homeassistant.core import HomeAssistant
from homeassistant.helpers.trigger import TriggerActionType, TriggerInfo

from .const import ATTR_EVENT_TYPE, ATTR_SUBTYPE, DOMAIN, EVENT_YEELIGHT_PRO
from .coordinator import YeelightProCoordinator
from .helpers import event_subtypes_for_node, event_types_for_node

CONF_SUBTYPE = "subtype"

TRIGGER_SCHEMA = DEVICE_TRIGGER_BASE_SCHEMA.extend(
    {
        vol.Required(CONF_TYPE): str,
        vol.Optional(CONF_SUBTYPE): str,
    }
)


async def async_get_triggers(hass: HomeAssistant, device_id: str) -> list[dict[str, str]]:
    coordinator, entry, node_id = _coordinator_entry_node_for_device(hass, device_id)
    if coordinator is None or entry is None or node_id is None:
        return []

    node = coordinator.node(node_id)
    if node is None:
        return []
    if not coordinator.exposes_events_for_node(node):
        return []

    triggers: list[dict[str, str]] = []
    for event_type in event_types_for_node(node):
        subtypes = event_subtypes_for_node(node, event_type)
        if not subtypes:
            triggers.append(_trigger(device_id, event_type))
            continue
        for subtype in subtypes:
            triggers.append(_trigger(device_id, event_type, subtype))
    return triggers


async def async_attach_trigger(
    hass: HomeAssistant,
    config: dict[str, Any],
    action: TriggerActionType,
    trigger_info: TriggerInfo,
) -> Any:
    event_data = {
        CONF_DEVICE_ID: config[CONF_DEVICE_ID],
        ATTR_EVENT_TYPE: config[CONF_TYPE],
    }
    if CONF_SUBTYPE in config:
        event_data[ATTR_SUBTYPE] = config[CONF_SUBTYPE]

    event_config = event_trigger.TRIGGER_SCHEMA(
        {
            CONF_PLATFORM: "event",
            event_trigger.CONF_EVENT_TYPE: EVENT_YEELIGHT_PRO,
            CONF_EVENT_DATA: event_data,
        }
    )

    return await event_trigger.async_attach_trigger(
        hass,
        event_config,
        action,
        trigger_info,
        platform_type="device",
    )


async def async_validate_trigger_config(hass: HomeAssistant, config: dict[str, Any]) -> dict[str, Any]:
    config = TRIGGER_SCHEMA(config)
    coordinator, _entry, node_id = _coordinator_entry_node_for_device(hass, config[CONF_DEVICE_ID])
    if coordinator is None or node_id is None:
        raise vol.Invalid("Unknown Yeelight Pro device trigger device")

    node = coordinator.node(node_id)
    if node is None:
        raise vol.Invalid("Unknown Yeelight Pro device trigger node")
    if not coordinator.exposes_events_for_node(node):
        raise vol.Invalid("Unsupported Yeelight Pro device trigger mode")

    event_type = config[CONF_TYPE]
    if event_type not in event_types_for_node(node):
        raise vol.Invalid("Unsupported Yeelight Pro device trigger type")
    if CONF_SUBTYPE in config and config[CONF_SUBTYPE] not in event_subtypes_for_node(node, event_type):
        raise vol.Invalid("Unsupported Yeelight Pro device trigger subtype")
    return config


def _trigger(device_id: str, event_type: str, subtype: str | None = None) -> dict[str, str]:
    trigger = {
        CONF_PLATFORM: "device",
        CONF_DOMAIN: DOMAIN,
        CONF_DEVICE_ID: device_id,
        CONF_TYPE: event_type,
    }
    if subtype is not None:
        trigger[CONF_SUBTYPE] = subtype
    return trigger


def _coordinator_entry_node_for_device(
    hass: HomeAssistant,
    device_id: str,
) -> tuple[YeelightProCoordinator | None, ConfigEntry | None, str | int | None]:
    for entry in hass.config_entries.async_entries(DOMAIN):
        coordinator: YeelightProCoordinator | None = getattr(entry, "runtime_data", None)
        if coordinator is None:
            continue
        node_id = coordinator.node_id_for_device_id(device_id)
        if node_id is None:
            continue
        return coordinator, entry, node_id
    return None, None, None
