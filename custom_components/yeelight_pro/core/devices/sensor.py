from __future__ import annotations

from typing import Any

from .base import Device


class ReadOnlySensorDevice(Device):
    pass


class MotionSensorDevice(ReadOnlySensorDevice):
    @property
    def is_motion_detected(self) -> bool | None:
        return _flag(self.node.params.get("mv"))

    @property
    def battery_percent(self) -> int | None:
        return _int_or_none(self.node.params.get("bp"))

    @property
    def battery_charging(self) -> bool | None:
        return _flag(self.node.params.get("bc"))


class HumanLightSensorDevice(MotionSensorDevice):
    @property
    def light_level(self) -> int | None:
        return _int_or_none(self.node.params.get("level"))


class MerrytekSensorDevice(MotionSensorDevice):
    @property
    def luminance(self) -> int | None:
        return _int_or_none(self.node.params.get("luminance"))


class HumitureSensorDevice(ReadOnlySensorDevice):
    @property
    def humidity(self) -> int | None:
        return _int_or_none(self.node.params.get("h"))

    @property
    def temperature_celsius(self) -> float | None:
        raw = _int_or_none(self.node.params.get("t"))
        return None if raw is None else raw / 100


class DoorSensorDevice(ReadOnlySensorDevice):
    @property
    def is_closed(self) -> bool | None:
        value = _int_or_none(self.node.params.get("dc"))
        return None if value is None else value == 1

    @property
    def is_alarm(self) -> bool | None:
        value = _int_or_none(self.node.params.get("alm"))
        return None if value is None else value == 1


def _flag(value: Any) -> bool | None:
    parsed = _int_or_none(value)
    return None if parsed is None else parsed == 1


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None
