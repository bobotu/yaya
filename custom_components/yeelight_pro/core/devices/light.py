from __future__ import annotations

from typing import Any

from ..commands import BlinkType, NodeCommand, blink_action
from .base import Device

MIN_COLOR_TEMP_KELVIN = 2700
MAX_COLOR_TEMP_KELVIN = 6500

COLOR_TEMP_KELVIN_BY_PRODUCT_ID = {
    198672: (1600, 8000),
}


def color_temp_kelvin_range(product_id: int | None) -> tuple[int, int]:
    return COLOR_TEMP_KELVIN_BY_PRODUCT_ID.get(product_id, (MIN_COLOR_TEMP_KELVIN, MAX_COLOR_TEMP_KELVIN))


class LightDevice(Device):
    async def turn_on(
        self,
        *,
        brightness: int | None = None,
        color_temperature: int | None = None,
        color: int | None = None,
        angle: int | None = None,
        duration: int | None = None,
    ) -> dict[str, Any]:
        props: dict[str, Any] = {"p": True}
        if brightness is not None:
            self._validate_range("brightness", brightness, 1, 100)
            props["l"] = brightness
        if color_temperature is not None:
            self._validate_color_temperature(color_temperature)
            props["ct"] = color_temperature
        if color is not None:
            self._validate_range("color", color, 0, 16_777_215)
            props["c"] = color
        if angle is not None:
            self._validate_range("angle", angle, 0, 255)
            props["angle"] = angle
        return await self.set_props(props, duration=duration)

    async def turn_off(self, *, duration: int | None = None) -> dict[str, Any]:
        return await self.set_props({"p": False}, duration=duration)

    async def set_brightness(self, brightness: int, *, duration: int | None = None) -> dict[str, Any]:
        self._validate_range("brightness", brightness, 1, 100)
        return await self.set_props({"l": brightness}, duration=duration)

    async def set_color_temperature(self, color_temperature: int, *, duration: int | None = None) -> dict[str, Any]:
        self._validate_color_temperature(color_temperature)
        return await self.set_props({"ct": color_temperature}, duration=duration)

    async def set_color(self, color: int, *, duration: int | None = None) -> dict[str, Any]:
        self._validate_range("color", color, 0, 16_777_215)
        return await self.set_props({"c": color}, duration=duration)

    async def blink(
        self,
        *,
        blink_type: BlinkType | str = BlinkType.NOTIFY,
        repeat: int = 4,
    ) -> dict[str, Any]:
        return await self._executor.send_node_command(
            NodeCommand(id=self.id, nt=self.nt, action=blink_action(blink_type, repeat=repeat))
        )

    def _validate_color_temperature(self, color_temperature: int) -> None:
        min_color_temp_kelvin, max_color_temp_kelvin = color_temp_kelvin_range(self.node.product_id)
        self._validate_range(
            "color temperature",
            color_temperature,
            min_color_temp_kelvin,
            max_color_temp_kelvin,
        )
