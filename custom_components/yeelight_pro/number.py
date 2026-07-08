from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.number import NumberEntity, NumberEntityDescription, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .core.topology import DeviceType
from .entity import YeelightProEntity, async_set_node_props
from .helpers import device_type, indexed_props, int_param, node_unique_id
from .platform import async_add_dynamic_entities

PARALLEL_UPDATES = 1


@dataclass(frozen=True, kw_only=True)
class YeelightProNumberDescription(NumberEntityDescription):
    value_fn: Callable[[Any], float | None]
    native_min_value: float = 0
    native_max_value: float = 100
    native_step: float = 1
    mode: NumberMode = NumberMode.BOX


BATH_NUMBER_DESCRIPTIONS = (
    YeelightProNumberDescription(
        key="do",
        translation_key="delay_off",
        native_min_value=0,
        native_max_value=120,
        native_unit_of_measurement="min",
        value_fn=lambda node: _number_param(node, "do"),
    ),
    YeelightProNumberDescription(
        key="he",
        translation_key="heat_level",
        native_min_value=0,
        native_max_value=3,
        value_fn=lambda node: _number_param(node, "he"),
    ),
)


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
        lambda node: _number_entities_for_node(coordinator, node),
        "number",
        lambda node: _stale_number_unique_ids_for_node(coordinator, node),
    )


def _number_entities_for_node(coordinator: YeelightProCoordinator, node: Any) -> list[YeelightProEntity]:
    entities: list[YeelightProEntity] = []
    item = device_type(node)
    if item in {DeviceType.AIR_CONDITION, DeviceType.AIR_CONDITION_VRF}:
        for key in indexed_props(node, "acdfltr"):
            entities.append(
                YeelightProPropertyNumber(
                    coordinator,
                    node,
                    YeelightProNumberDescription(
                        key=key,
                        translation_key="air_conditioner_deflector",
                        native_min_value=0,
                        native_max_value=255,
                        value_fn=lambda target, prop=key: _number_param(target, prop),
                    ),
                )
            )
    elif item == DeviceType.BATH_HEATER:
        entities.extend(
            YeelightProPropertyNumber(coordinator, node, description)
            for description in BATH_NUMBER_DESCRIPTIONS
            if description.key in node.params
        )
    return entities


def _stale_number_unique_ids_for_node(coordinator: YeelightProCoordinator, node: Any) -> tuple[str, ...]:
    if device_type(node) not in {DeviceType.AIR_CONDITION, DeviceType.AIR_CONDITION_VRF}:
        return ()
    return tuple(node_unique_id(coordinator.gateway_id, node.id, key) for key in indexed_props(node, "acd"))


class YeelightProPropertyNumber(YeelightProEntity, NumberEntity):
    _attr_entity_category = EntityCategory.CONFIG
    entity_description: YeelightProNumberDescription

    def __init__(
        self,
        coordinator: YeelightProCoordinator,
        node: Any,
        description: YeelightProNumberDescription,
    ) -> None:
        super().__init__(coordinator, node, description.key)
        self.entity_description = description

    @property
    def intent_properties(self) -> tuple[str, ...]:
        return (self.entity_description.key,)

    @property
    def native_value(self) -> float | None:
        node = self.node
        if node is None:
            return None
        return self.entity_description.value_fn(node)

    async def async_set_native_value(self, value: float) -> None:
        node = self.require_current_node()
        await async_set_node_props(self.coordinator, node, {self.entity_description.key: round(value)})


def _number_param(node: Any, key: str) -> float | None:
    value = int_param(node, key)
    return None if value is None else float(value)
