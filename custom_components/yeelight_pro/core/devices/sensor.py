from __future__ import annotations

from ..coercion import int_or_none
from .base import Device


class ReadOnlySensorDevice(Device):
    pass


class MotionSensorDevice(ReadOnlySensorDevice):
    @property
    def is_motion_detected(self) -> bool | None:
        return _flag(self.node.params.get("mv"))

    @property
    def battery_percent(self) -> int | None:
        return int_or_none(self.node.params.get("bp"), bool_as_int=True)

    @property
    def battery_charging(self) -> bool | None:
        return _flag(self.node.params.get("bc"))


class HumanLightSensorDevice(MotionSensorDevice):
    @property
    def light_level(self) -> int | None:
        return int_or_none(self.node.params.get("level"), bool_as_int=True)


class MerrytekSensorDevice(MotionSensorDevice):
    @property
    def luminance(self) -> int | None:
        return int_or_none(self.node.params.get("luminance"), bool_as_int=True)


class HumitureSensorDevice(ReadOnlySensorDevice):
    @property
    def humidity(self) -> int | None:
        return int_or_none(self.node.params.get("h"), bool_as_int=True)

    @property
    def temperature_celsius(self) -> float | None:
        raw = int_or_none(self.node.params.get("t"), bool_as_int=True)
        return None if raw is None else raw / 100


class DoorSensorDevice(ReadOnlySensorDevice):
    @property
    def is_closed(self) -> bool | None:
        value = int_or_none(self.node.params.get("dc"), bool_as_int=True)
        return None if value is None else value == 1

    @property
    def is_alarm(self) -> bool | None:
        value = int_or_none(self.node.params.get("alm"), bool_as_int=True)
        return None if value is None else value == 1


def _flag(value: object) -> bool | None:
    parsed = int_or_none(value, bool_as_int=True)
    return None if parsed is None else parsed == 1
