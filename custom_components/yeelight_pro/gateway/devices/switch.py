from __future__ import annotations

from typing import Any

from ..const import PANEL_EVENT_VALUES, SWITCH_DOUBLE_KEYS, SWITCH_MORE_KEYS
from .base import Device


class DoubleSwitchDevice(Device):
    @property
    def channels(self) -> tuple[str, ...]:
        return SWITCH_DOUBLE_KEYS

    async def set_all(self, is_on: bool) -> dict[str, Any]:
        return await self.set_props({"p": is_on})

    async def set_channel(self, channel: int, is_on: bool) -> dict[str, Any]:
        if channel not in (1, 2):
            raise ValueError("double switch channel must be 1 or 2")
        return await self.set_props({f"{channel}-p": is_on})


class MultiSwitchDevice(Device):
    @property
    def event_values(self) -> tuple[str, ...]:
        return PANEL_EVENT_VALUES

    @property
    def channels(self) -> tuple[str, ...]:
        params = self.node.params
        return tuple(key for key in SWITCH_MORE_KEYS if key in params)

    @property
    def backlight(self) -> bool | None:
        value = self.node.params.get("0-blp")
        return value if isinstance(value, bool) else None

    async def set_channel(self, channel: int, is_on: bool) -> dict[str, Any]:
        if not 1 <= channel <= 6:
            raise ValueError("multi switch channel must be between 1 and 6")
        return await self.set_props({f"{channel}-sp": is_on})
