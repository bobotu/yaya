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
from .entity import YeelightProEntity, async_call_gateway
from .helpers import device_type
from .platform import async_add_dynamic_entities


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
    )


def _number_entities_for_node(coordinator: YeelightProCoordinator, node: Any) -> list[YeelightProEntity]:
    entities: list[YeelightProEntity] = []
    item = device_type(node)
    if item in {DeviceType.AIR_CONDITION, DeviceType.AIR_CONDITION_VRF}:
        for key in _indexed_props(node, "acdfltr"):
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
    def native_value(self) -> float | None:
        node = self.node
        if node is None:
            return None
        return self.entity_description.value_fn(node)

    async def async_set_native_value(self, value: float) -> None:
        node = self.node
        if node is None:
            return
        await async_call_gateway(
            self.coordinator.gateway.set_node_props(node.id, {self.entity_description.key: round(value)}, nt=node.nt)
        )
        await async_call_gateway(self.coordinator.async_refresh_node(node.id))


def _indexed_props(node: Any, suffix: str) -> tuple[str, ...]:
    props = []
    for key in node.params:
        if isinstance(key, str) and key.endswith(f"-{suffix}") and key.split("-", 1)[0].isdigit():
            props.append(key)
    return tuple(sorted(props, key=lambda item: int(item.split("-", 1)[0])))


def _number_param(node: Any, key: str) -> float | None:
    value = node.params.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return float(value)
    return None
