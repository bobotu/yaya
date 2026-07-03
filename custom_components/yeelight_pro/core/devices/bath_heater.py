from __future__ import annotations

from typing import Any

from .base import Device


class BathHeaterDevice(Device):
    async def set_power(self, is_on: bool) -> dict[str, Any]:
        return await self.set_props({"p": is_on})

    async def set_mode(self, mode: int) -> dict[str, Any]:
        self._validate_range("mode", mode, 1, 4)
        return await self.set_props({"bhm": mode})

    async def set_delay_off(self, minutes: int) -> dict[str, Any]:
        self._validate_range("delay off", minutes, 1, 120)
        return await self.set_props({"do": minutes})

    async def set_ventilation(self, level: int) -> dict[str, Any]:
        self._validate_range("ventilation", level, 0, 3)
        return await self.set_props({"ve": level})

    async def set_fan(self, level: int) -> dict[str, Any]:
        self._validate_range("fan", level, 0, 3)
        return await self.set_props({"fa": level})

    async def set_heat(self, level: int) -> dict[str, Any]:
        self._validate_range("heat", level, 0, 3)
        return await self.set_props({"he": level})

    async def set_target_temperature(self, temperature: int) -> dict[str, Any]:
        self._validate_range("temperature", temperature, 0, 50)
        return await self.set_props({"tgt": temperature})
