from __future__ import annotations

from typing import Any

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .core.topology import DeviceType
from .entity import YeelightProEntity, async_call_gateway
from .helpers import device_type
from .platform import async_add_dynamic_entities

BATH_MODE_OPTIONS = {
    "mode_1": 1,
    "mode_2": 2,
    "mode_3": 3,
    "mode_4": 4,
}
BATH_MODE_VALUES = {value: key for key, value in BATH_MODE_OPTIONS.items()}


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
            [YeelightProBathModeSelect(coordinator, node)]
            if device_type(node) == DeviceType.BATH_HEATER and "bhm" in node.params
            else []
        ),
        "select",
    )


class YeelightProBathModeSelect(YeelightProEntity, SelectEntity):
    _attr_entity_category = EntityCategory.CONFIG
    _attr_options = list(BATH_MODE_OPTIONS)
    _attr_translation_key = "bath_mode"

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "bath_mode")

    @property
    def current_option(self) -> str | None:
        node = self.node
        if node is None:
            return None
        value = node.params.get("bhm")
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return BATH_MODE_VALUES.get(value)

    async def async_select_option(self, option: str) -> None:
        node = self.node
        if node is None:
            return
        await async_call_gateway(
            self.coordinator.gateway.set_node_props(node.id, {"bhm": BATH_MODE_OPTIONS[option]}, nt=node.nt)
        )
        await async_call_gateway(self.coordinator.async_refresh_node(node.id))
