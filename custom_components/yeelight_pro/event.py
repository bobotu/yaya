from __future__ import annotations

from typing import Any

from homeassistant.components.event import EventDeviceClass, EventEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .entity import YeelightProEntity
from .gateway import GatewayEvent, is_knob_capable
from .helpers import button_count, event_data, event_types_for_node, is_programmable_node, node_unique_id
from .platform import async_add_dynamic_entities

PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: YeelightProCoordinator = entry.runtime_data
    async_add_dynamic_entities(
        entry,
        coordinator,
        async_add_entities,
        lambda node: (
            [YeelightProButtonEvent(coordinator, node, index) for index in range(1, button_count(node) + 1)]
            if is_programmable_node(node) and coordinator.exposes_events_for_node(node)
            else []
        ),
        "event",
        lambda node: _stale_event_unique_ids_for_node(coordinator, node),
    )


def _stale_event_unique_ids_for_node(coordinator: YeelightProCoordinator, node: Any) -> tuple[str, ...]:
    if not is_programmable_node(node):
        return ()
    return tuple(
        node_unique_id(coordinator.gateway_id, node.id, f"control_{index}_events")
        for index in range(1, button_count(node) + 1)
    )


class YeelightProButtonEvent(YeelightProEntity, EventEntity):
    _attr_device_class = EventDeviceClass.BUTTON

    def __init__(self, coordinator: YeelightProCoordinator, node: Any, index: int) -> None:
        super().__init__(coordinator, node, f"control_{index}_events")
        self._index = index
        self._attr_translation_key = "control_events" if is_knob_capable(node) else "key_events"
        self._attr_translation_placeholders = {"index": str(index)}
        self._attr_icon = "mdi:tune-variant" if is_knob_capable(node) else "mdi:gesture-tap-button"

    @property
    def event_types(self) -> list[str]:
        node = self.node
        return [] if node is None else list(event_types_for_node(node))

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(self.coordinator.add_event_listener(self._node_id, self._async_handle_event))

    @callback
    def _async_handle_event(self, event: GatewayEvent) -> None:
        if event.event_type not in self.event_types:
            return
        if event.key != self._index and event.index != self._index:
            return
        self._trigger_event(event.event_type, event_data(event))
        self.async_write_ha_state()
