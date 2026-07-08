from __future__ import annotations

from typing import Any

from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_COLOR_TEMP_KELVIN,
    ATTR_FLASH,
    ATTR_RGB_COLOR,
    ATTR_TRANSITION,
    FLASH_LONG,
    FLASH_SHORT,
    ColorMode,
    LightEntity,
    LightEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .core.commands import BlinkType
from .core.devices import LightDevice
from .core.devices.light import color_temp_kelvin_range
from .core.topology import DeviceType
from .entity import YeelightProEntity, async_call_gateway, async_set_node_props
from .helpers import int_param, light_device_type
from .platform import async_add_dynamic_entities

PARALLEL_UPDATES = 1


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
        lambda node: [YeelightProLight(coordinator, node)] if light_device_type(node) is not None else [],
        "light",
    )


class YeelightProLight(YeelightProEntity, LightEntity):
    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "light")
        self._attr_name = None
        min_color_temp_kelvin, max_color_temp_kelvin = color_temp_kelvin_range(node.product_id)
        self._attr_min_color_temp_kelvin = min_color_temp_kelvin
        self._attr_max_color_temp_kelvin = max_color_temp_kelvin
        self._attr_supported_features = LightEntityFeature.TRANSITION | LightEntityFeature.FLASH

    @property
    def intent_properties(self) -> tuple[str, ...]:
        return ("p", "l", "ct", "c")

    @property
    def supported_color_modes(self) -> set[ColorMode]:
        node = self.node
        if node is None:
            return {ColorMode.ONOFF}
        item = light_device_type(node)
        if item == DeviceType.LIGHT_COLOR:
            return {ColorMode.RGB, ColorMode.COLOR_TEMP}
        if item in {DeviceType.LIGHT_TEMPERATURE, DeviceType.LAMP_DFT}:
            return {ColorMode.COLOR_TEMP}
        if item == DeviceType.LIGHT_BRIGHTNESS:
            return {ColorMode.BRIGHTNESS}
        return {ColorMode.ONOFF}

    @property
    def color_mode(self) -> ColorMode | None:
        modes = self.supported_color_modes
        if ColorMode.RGB in modes:
            return ColorMode.RGB
        if ColorMode.COLOR_TEMP in modes:
            return ColorMode.COLOR_TEMP
        if ColorMode.BRIGHTNESS in modes:
            return ColorMode.BRIGHTNESS
        return ColorMode.ONOFF

    @property
    def is_on(self) -> bool | None:
        node = self.node
        if node is None:
            return None
        value = node.params.get("p")
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return bool(value)
        return None

    @property
    def brightness(self) -> int | None:
        level = int_param(self.node, "l")
        return None if level is None else round(level * 255 / 100)

    @property
    def color_temp_kelvin(self) -> int | None:
        return int_param(self.node, "ct")

    @property
    def rgb_color(self) -> tuple[int, int, int] | None:
        value = int_param(self.node, "c")
        if value is None:
            return None
        return ((value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF)

    async def async_turn_on(self, **kwargs: Any) -> None:
        node = self.require_current_node()
        brightness = kwargs.get(ATTR_BRIGHTNESS)
        brightness_percent = None if brightness is None else max(1, min(100, round(brightness * 100 / 255)))
        color_temperature = kwargs.get(ATTR_COLOR_TEMP_KELVIN)
        rgb_color = kwargs.get(ATTR_RGB_COLOR)
        color = None if rgb_color is None else _rgb_to_int(rgb_color)
        flash = kwargs.get(ATTR_FLASH)
        device = LightDevice(node, self.coordinator.gateway)
        if flash is not None:
            await async_call_gateway(device.blink(blink_type=_flash_to_blink_type(flash)))
            return
        props: dict[str, Any] = {"p": True}
        if brightness_percent is not None:
            props["l"] = brightness_percent
        if color_temperature is not None:
            props["ct"] = color_temperature
        if color is not None:
            props["c"] = color
        await async_set_node_props(
            self.coordinator,
            node,
            props,
            duration=_transition_to_duration(kwargs.get(ATTR_TRANSITION)),
        )

    async def async_turn_off(self, **kwargs: Any) -> None:
        node = self.require_current_node()
        await async_set_node_props(
            self.coordinator,
            node,
            {"p": False},
            duration=_transition_to_duration(kwargs.get(ATTR_TRANSITION)),
        )


def _rgb_to_int(rgb_color: tuple[int, int, int]) -> int:
    red, green, blue = rgb_color
    return (red << 16) + (green << 8) + blue


def _transition_to_duration(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return max(0, min(3_600_000, round(value * 1000)))


def _flash_to_blink_type(value: object) -> BlinkType:
    if value == FLASH_LONG:
        return BlinkType.SMOOTH
    if value == FLASH_SHORT:
        return BlinkType.URGENT
    return BlinkType.NOTIFY
