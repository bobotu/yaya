from __future__ import annotations

from typing import Any

from .base import Device


class AirConditionDevice(Device):
    def channel_key(self, index: int, prop: str) -> str:
        if index < 1:
            raise ValueError("air conditioner index must be >= 1")
        return f"{index}-{prop}"

    async def set_power(self, is_on: bool, *, index: int = 1) -> dict[str, Any]:
        return await self.set_props({self.channel_key(index, "acp"): is_on})

    async def set_mode(self, mode: int, *, index: int = 1) -> dict[str, Any]:
        return await self.set_props({self.channel_key(index, "acm"): mode})

    async def set_target_temperature(self, temperature: int, *, index: int = 1) -> dict[str, Any]:
        self._validate_range("temperature", temperature, 16, 32)
        return await self.set_props({self.channel_key(index, "actt"): temperature})

    async def set_fan_speed(self, speed: int, *, index: int = 1) -> dict[str, Any]:
        self._validate_range("fan speed", speed, 0, 5)
        return await self.set_props({self.channel_key(index, "acf"): speed})

    async def set_delay(self, milliseconds: int, *, index: int = 1) -> dict[str, Any]:
        self._validate_range("delay", milliseconds, 1, 43_200_000)
        return await self.set_props({self.channel_key(index, "acd"): milliseconds})

    async def set_deflector(self, value: int, *, index: int = 1) -> dict[str, Any]:
        self._validate_range("deflector", value, 0, 255)
        return await self.set_props({self.channel_key(index, "acdfltr"): value})

    async def set_remote_controller(self, enabled: bool, *, index: int = 1) -> dict[str, Any]:
        return await self.set_props({self.channel_key(index, "acrc"): enabled})
