from __future__ import annotations

from typing import Any

from ..commands import MotorAction, NodeCommand, motor_adjust_action
from .base import Device


class CurtainDevice(Device):
    @property
    def current_position(self) -> int | None:
        return _int_or_none(self.node.params.get("cp"))

    @property
    def target_position(self) -> int | None:
        return _int_or_none(self.node.params.get("tp"))

    @property
    def is_route_calibrated(self) -> bool | None:
        value = _int_or_none(self.node.params.get("rs"))
        return None if value is None else value == 1

    async def set_position(self, position: int, *, duration: int | None = None) -> dict[str, Any]:
        self._validate_range("position", position, 0, 100)
        return await self.set_props({"tp": position}, duration=duration)

    async def open(self, *, duration: int | None = None) -> dict[str, Any]:
        return await self.set_position(100, duration=duration)

    async def close(self, *, duration: int | None = None) -> dict[str, Any]:
        return await self.set_position(0, duration=duration)

    async def stop(self) -> dict[str, Any]:
        return await self.motor_adjust(MotorAction.PAUSE)

    async def motor_adjust(self, action_type: MotorAction | str) -> dict[str, Any]:
        return await self._executor.send_node_command(
            NodeCommand(id=self.id, nt=self.nt, action=motor_adjust_action(action_type))
        )


class DreamCurtainDevice(CurtainDevice):
    @property
    def current_angle(self) -> int | None:
        return _int_or_none(self.node.params.get("cra"))

    @property
    def target_angle(self) -> int | None:
        return _int_or_none(self.node.params.get("tra"))

    @property
    def is_tilt_route_calibrated(self) -> bool | None:
        value = _int_or_none(self.node.params.get("trs"))
        return None if value is None else value == 1

    async def set_angle(self, angle: int, *, duration: int | None = None) -> dict[str, Any]:
        self._validate_range("angle", angle, 0, 180)
        return await self.set_props({"tra": angle}, duration=duration)

    async def open_tilt(self, *, duration: int | None = None) -> dict[str, Any]:
        return await self.set_angle(180, duration=duration)

    async def close_tilt(self, *, duration: int | None = None) -> dict[str, Any]:
        return await self.set_angle(0, duration=duration)

    async def stop_tilt(self) -> dict[str, Any]:
        return await self.stop()


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None
