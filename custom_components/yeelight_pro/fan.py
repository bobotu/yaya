from __future__ import annotations

from typing import Any

from homeassistant.components.fan import FanEntity, FanEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .entity import YeelightProEntity, async_set_node_props
from .gateway.topology import DeviceType
from .helpers import device_type
from .platform import async_add_dynamic_entities

PARALLEL_UPDATES = 1

BATH_FANS = {
    "ve": "ventilation",
    "fa": "fan",
}


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
        lambda node: _fan_entities_for_node(coordinator, node),
        "fan",
    )


def _fan_entities_for_node(coordinator: YeelightProCoordinator, node: Any) -> list[YeelightProEntity]:
    if device_type(node) != DeviceType.BATH_HEATER:
        return []
    return [
        YeelightProBathHeaterFan(coordinator, node, prop, translation_key)
        for prop, translation_key in BATH_FANS.items()
        if prop in node.params
    ]


class YeelightProBathHeaterFan(YeelightProEntity, FanEntity):
    _attr_entity_category = EntityCategory.CONFIG
    _attr_supported_features = FanEntityFeature.SET_SPEED

    def __init__(self, coordinator: YeelightProCoordinator, node: Any, prop: str, translation_key: str) -> None:
        super().__init__(coordinator, node, prop)
        self._prop = prop
        self._attr_translation_key = translation_key

    @property
    def is_on(self) -> bool | None:
        level = _level(self.node, self._prop)
        return None if level is None else level > 0

    @property
    def percentage(self) -> int | None:
        level = _level(self.node, self._prop)
        return None if level is None else round(level * 100 / 3)

    async def async_turn_on(
        self,
        percentage: int | None = None,
        preset_mode: str | None = None,
        **kwargs: Any,
    ) -> None:
        await self.async_set_percentage(percentage if percentage is not None else 100)

    async def async_turn_off(self, **kwargs: Any) -> None:
        await self.async_set_percentage(0)

    async def async_set_percentage(self, percentage: int) -> None:
        node = self.require_current_node()
        level = max(0, min(3, round(percentage * 3 / 100)))
        await async_set_node_props(self.coordinator, node, {self._prop: level})


def _level(node: Any, prop: str) -> int | None:
    if node is None:
        return None
    value = node.params.get(prop)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None
