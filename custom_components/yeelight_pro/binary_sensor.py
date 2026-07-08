from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import BinarySensorDeviceClass, BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .core import is_knob_capable
from .core.topology import DeviceType
from .entity import YeelightProEntity
from .helpers import bool_param, device_type, node_unique_id, relay_channel_numbers, relay_prop_name
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
        lambda node: _binary_sensors_for_node(coordinator, node),
        "binary_sensor",
        lambda node: _stale_binary_sensor_unique_ids_for_node(coordinator, node),
    )


def _binary_sensors_for_node(coordinator: YeelightProCoordinator, node: Any) -> list[YeelightProEntity]:
    entities: list[YeelightProEntity] = []
    item = device_type(node)
    if item == DeviceType.SENSOR_PERSON:
        entities.append(YeelightProOccupancyBinarySensor(coordinator, node))
    elif (
        item in {DeviceType.SENSOR_HUMAN_LIGHT, DeviceType.SENSOR_MERRYTEK, DeviceType.SENSOR_TOF}
        or "mv" in node.params
    ):
        entities.append(YeelightProMotionBinarySensor(coordinator, node))
    if item == DeviceType.SENSOR_DOOR or "dc" in node.params:
        entities.append(YeelightProDoorBinarySensor(coordinator, node))
    if "alm" in node.params:
        entities.append(YeelightProAlarmBinarySensor(coordinator, node))
    if "bc" in node.params or is_knob_capable(node):
        entities.append(YeelightProBatteryChargingBinarySensor(coordinator, node))
    if coordinator.exposes_wireless_relay_diagnostics_for_node(node):
        entities.extend(
            YeelightProRelayStateBinarySensor(coordinator, node, channel) for channel in relay_channel_numbers(node)
        )
    return entities


def _stale_binary_sensor_unique_ids_for_node(coordinator: YeelightProCoordinator, node: Any) -> tuple[str, ...]:
    return tuple(
        node_unique_id(coordinator.gateway_id, node.id, f"relay_{channel}_state")
        for channel in relay_channel_numbers(node)
    )


class YeelightProMotionBinarySensor(YeelightProEntity, BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.MOTION

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "motion")
        self._attr_translation_key = "motion"

    @property
    def is_on(self) -> bool | None:
        return bool_param(self.node, "mv")


class YeelightProOccupancyBinarySensor(YeelightProEntity, BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.OCCUPANCY

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "occupancy")
        self._attr_translation_key = "occupancy"

    @property
    def is_on(self) -> bool | None:
        return bool_param(self.node, "mv")


class YeelightProDoorBinarySensor(YeelightProEntity, BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.DOOR

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "door")
        self._attr_translation_key = "door"

    @property
    def is_on(self) -> bool | None:
        value = bool_param(self.node, "dc")
        return None if value is None else not value


class YeelightProAlarmBinarySensor(YeelightProEntity, BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.PROBLEM

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "alarm")
        self._attr_translation_key = "alarm"

    @property
    def is_on(self) -> bool | None:
        return bool_param(self.node, "alm")


class YeelightProBatteryChargingBinarySensor(YeelightProEntity, BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.BATTERY_CHARGING
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "battery_charging")
        self._attr_translation_key = "battery_charging"

    @property
    def is_on(self) -> bool | None:
        return bool_param(self.node, "bc")


class YeelightProRelayStateBinarySensor(YeelightProEntity, BinarySensorEntity):
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:electric-switch"

    def __init__(self, coordinator: YeelightProCoordinator, node: Any, channel: int) -> None:
        super().__init__(coordinator, node, f"relay_{channel}_state")
        self._channel = channel
        self._attr_translation_key = "relay_state"
        self._attr_translation_placeholders = {"channel": str(channel)}

    @property
    def is_on(self) -> bool | None:
        node = self.node
        if node is None:
            return None
        return bool_param(node, relay_prop_name(node, self._channel))
