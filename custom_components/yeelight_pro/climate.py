from __future__ import annotations

from typing import Any

from homeassistant.components.climate import ClimateEntity, ClimateEntityFeature
from homeassistant.components.climate.const import FAN_AUTO, FAN_HIGH, FAN_LOW, FAN_MEDIUM, HVACMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .core.topology import DeviceType
from .entity import YeelightProEntity, async_set_node_props
from .helpers import device_type
from .platform import async_add_dynamic_entities

PARALLEL_UPDATES = 1

AC_MODE_TO_HVAC = {
    1: HVACMode.COOL,
    2: HVACMode.DRY,
    4: HVACMode.FAN_ONLY,
    8: HVACMode.HEAT,
}
HVAC_TO_AC_MODE = {value: key for key, value in AC_MODE_TO_HVAC.items()}
AC_FAN_TO_HA = {
    0: FAN_AUTO,
    4: FAN_LOW,
    5: FAN_LOW,
    2: FAN_MEDIUM,
    3: FAN_HIGH,
    1: FAN_HIGH,
}
HA_FAN_TO_AC = {
    FAN_AUTO: 0,
    FAN_LOW: 4,
    FAN_MEDIUM: 2,
    FAN_HIGH: 1,
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
        lambda node: _climate_entities_for_node(coordinator, node),
        "climate",
    )


def _climate_entities_for_node(coordinator: YeelightProCoordinator, node: Any) -> list[YeelightProEntity]:
    item = device_type(node)
    if item in {DeviceType.AIR_CONDITION, DeviceType.AIR_CONDITION_VRF}:
        indexes = _air_condition_indexes(node)
        return [
            YeelightProAirConditionClimate(coordinator, node, index, primary_entity=len(indexes) == 1)
            for index in indexes
        ]
    if item == DeviceType.BATH_HEATER:
        return [YeelightProBathHeaterClimate(coordinator, node)]
    return []


class YeelightProAirConditionClimate(YeelightProEntity, ClimateEntity):
    _attr_hvac_modes = [HVACMode.OFF, HVACMode.COOL, HVACMode.HEAT, HVACMode.DRY, HVACMode.FAN_ONLY]
    _attr_supported_features = (
        ClimateEntityFeature.TARGET_TEMPERATURE
        | ClimateEntityFeature.FAN_MODE
        | ClimateEntityFeature.TURN_ON
        | ClimateEntityFeature.TURN_OFF
    )
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_min_temp = 16
    _attr_max_temp = 32
    _attr_target_temperature_step = 1
    _attr_fan_modes = list(HA_FAN_TO_AC)

    def __init__(
        self,
        coordinator: YeelightProCoordinator,
        node: Any,
        index: int,
        *,
        primary_entity: bool = False,
    ) -> None:
        super().__init__(coordinator, node, f"air_conditioner_{index}")
        self._index = index
        if primary_entity:
            self._attr_name = None
        else:
            self._attr_translation_key = "air_conditioner"
            self._attr_translation_placeholders = {"index": str(index)}

    @property
    def optimistic_properties(self) -> tuple[str, ...]:
        return (self._key("acp"), self._key("acm"), self._key("actt"), self._key("acf"))

    @property
    def current_temperature(self) -> float | None:
        value = _int_param(self.node, self._key("acct"))
        return None if value is None else float(value)

    @property
    def target_temperature(self) -> float | None:
        value = _int_param(self.node, self._key("actt"))
        return None if value is None else float(value)

    @property
    def hvac_mode(self) -> HVACMode | None:
        if _bool_param(self.node, self._key("acp")) is False:
            return HVACMode.OFF
        return AC_MODE_TO_HVAC.get(_int_param(self.node, self._key("acm")))

    @property
    def fan_mode(self) -> str | None:
        value = _int_param(self.node, self._key("acf"))
        return None if value is None else AC_FAN_TO_HA.get(value)

    async def async_turn_on(self) -> None:
        node = self.require_current_node()
        await async_set_node_props(self.coordinator, node, {self._key("acp"): True})

    async def async_turn_off(self) -> None:
        node = self.require_current_node()
        await async_set_node_props(self.coordinator, node, {self._key("acp"): False})

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        node = self.require_current_node()
        if hvac_mode == HVACMode.OFF:
            await async_set_node_props(self.coordinator, node, {self._key("acp"): False})
        else:
            props = {self._key("acp"): True}
            mode = HVAC_TO_AC_MODE.get(hvac_mode)
            if mode is not None:
                props[self._key("acm")] = mode
            await async_set_node_props(self.coordinator, node, props)

    async def async_set_temperature(self, **kwargs: Any) -> None:
        temperature = kwargs.get(ATTR_TEMPERATURE)
        if temperature is None:
            return
        node = self.require_current_node()
        await async_set_node_props(self.coordinator, node, {self._key("actt"): int(temperature)})

    async def async_set_fan_mode(self, fan_mode: str) -> None:
        node = self.require_current_node()
        speed = HA_FAN_TO_AC.get(fan_mode)
        if speed is None:
            raise ValueError(f"unsupported fan mode: {fan_mode}")
        await async_set_node_props(self.coordinator, node, {self._key("acf"): speed})

    def _key(self, suffix: str) -> str:
        return f"{self._index}-{suffix}"


class YeelightProBathHeaterClimate(YeelightProEntity, ClimateEntity):
    _attr_hvac_modes = [HVACMode.OFF, HVACMode.HEAT]
    _attr_supported_features = (
        ClimateEntityFeature.TARGET_TEMPERATURE | ClimateEntityFeature.TURN_ON | ClimateEntityFeature.TURN_OFF
    )
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_min_temp = 0
    _attr_max_temp = 50
    _attr_target_temperature_step = 1

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "bath_heater_climate")
        self._attr_translation_key = "bath_heater_climate"

    @property
    def optimistic_properties(self) -> tuple[str, ...]:
        return ("p", "tgt")

    @property
    def current_temperature(self) -> float | None:
        value = _int_param(self.node, "t")
        return None if value is None else float(value)

    @property
    def target_temperature(self) -> float | None:
        value = _int_param(self.node, "tgt")
        return None if value is None else float(value)

    @property
    def hvac_mode(self) -> HVACMode | None:
        return HVACMode.HEAT if _bool_param(self.node, "p") else HVACMode.OFF

    async def async_turn_on(self) -> None:
        node = self.require_current_node()
        await async_set_node_props(self.coordinator, node, {"p": True})

    async def async_turn_off(self) -> None:
        node = self.require_current_node()
        await async_set_node_props(self.coordinator, node, {"p": False})

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        if hvac_mode == HVACMode.OFF:
            await self.async_turn_off()
        else:
            await self.async_turn_on()

    async def async_set_temperature(self, **kwargs: Any) -> None:
        temperature = kwargs.get(ATTR_TEMPERATURE)
        if temperature is None:
            return
        node = self.require_current_node()
        await async_set_node_props(self.coordinator, node, {"tgt": int(temperature)})


def _air_condition_indexes(node: Any) -> tuple[int, ...]:
    indexes = set()
    for key in node.params:
        if isinstance(key, str) and "-" in key:
            prefix, suffix = key.split("-", 1)
            if prefix.isdigit() and suffix.startswith("ac"):
                indexes.add(int(prefix))
    return tuple(sorted(indexes)) or (1,)


def _int_param(node: Any, key: str) -> int | None:
    if node is None:
        return None
    value = node.params.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _bool_param(node: Any, key: str) -> bool | None:
    if node is None:
        return None
    value = node.params.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    return None
