from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorEntityDescription, SensorStateClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import PERCENTAGE, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .core import is_knob_capable
from .entity import YeelightProEntity
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
        lambda node: _sensor_entities_for_node(coordinator, node),
        "sensor",
    )


@dataclass(frozen=True, kw_only=True)
class YeelightProSensorDescription(SensorEntityDescription):
    value_fn: Callable[[Any], int | float | None]
    exists_fn: Callable[[Any], bool] = lambda node: True


SENSOR_DESCRIPTIONS: tuple[YeelightProSensorDescription, ...] = (
    YeelightProSensorDescription(
        key="battery",
        translation_key="battery",
        device_class=SensorDeviceClass.BATTERY,
        entity_category=EntityCategory.DIAGNOSTIC,
        native_unit_of_measurement=PERCENTAGE,
        exists_fn=lambda node: "bp" in node.params or is_knob_capable(node),
        value_fn=lambda node: _int_param(node, "bp"),
    ),
    YeelightProSensorDescription(
        key="light_level",
        translation_key="light_level",
        entity_category=EntityCategory.DIAGNOSTIC,
        exists_fn=lambda node: "level" in node.params,
        value_fn=lambda node: _int_param(node, "level"),
    ),
    YeelightProSensorDescription(
        key="luminance",
        translation_key="luminance",
        device_class=SensorDeviceClass.ILLUMINANCE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement="lx",
        exists_fn=lambda node: "luminance" in node.params,
        value_fn=lambda node: _int_param(node, "luminance"),
    ),
    YeelightProSensorDescription(
        key="humidity",
        translation_key="humidity",
        device_class=SensorDeviceClass.HUMIDITY,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=PERCENTAGE,
        exists_fn=lambda node: "h" in node.params,
        value_fn=lambda node: _int_param(node, "h"),
    ),
    YeelightProSensorDescription(
        key="temperature",
        translation_key="temperature",
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        exists_fn=lambda node: "t" in node.params,
        value_fn=lambda node: _scaled_int_param(node, "t", 100),
    ),
)


def _sensor_entities_for_node(coordinator: YeelightProCoordinator, node: Any) -> list[YeelightProEntity]:
    return [
        YeelightProSensor(coordinator, node, description)
        for description in SENSOR_DESCRIPTIONS
        if description.exists_fn(node)
    ]


class YeelightProSensor(YeelightProEntity, SensorEntity):
    entity_description: YeelightProSensorDescription

    def __init__(
        self,
        coordinator: YeelightProCoordinator,
        node: Any,
        description: YeelightProSensorDescription,
    ) -> None:
        super().__init__(coordinator, node, description.key)
        self.entity_description = description

    @property
    def native_value(self) -> int | float | None:
        node = self.node
        if node is None:
            return None
        return self.entity_description.value_fn(node)


def _int_param(node: Any, key: str) -> int | None:
    value = node.params.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _scaled_int_param(node: Any, key: str, scale: int) -> float | None:
    value = _int_param(node, key)
    return None if value is None else value / scale
