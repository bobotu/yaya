from __future__ import annotations

from ..capabilities import is_dream_curtain, is_knob_capable
from ..topology import DeviceType, TopologyNode
from .air_condition import AirConditionDevice
from .base import CommandExecutor, Device
from .bath_heater import BathHeaterDevice
from .curtain import CurtainDevice, DreamCurtainDevice
from .light import LightDevice
from .sensor import (
    DoorSensorDevice,
    HumanLightSensorDevice,
    HumitureSensorDevice,
    MerrytekSensorDevice,
    MotionSensorDevice,
    ReadOnlySensorDevice,
)
from .switch import DoubleSwitchDevice, MultiSwitchDevice
from .trigger import KnobDevice, ProgrammableSwitchDevice


def create_device(node: TopologyNode, executor: CommandExecutor) -> Device:
    device_type = _device_type(node.type)
    if device_type in {
        DeviceType.LIGHT_SWITCHABLE,
        DeviceType.LIGHT_BRIGHTNESS,
        DeviceType.LIGHT_TEMPERATURE,
        DeviceType.LIGHT_COLOR,
        DeviceType.LAMP_DFT,
    }:
        return LightDevice(node, executor)
    if is_dream_curtain(node):
        return DreamCurtainDevice(node, executor)
    if device_type == DeviceType.MOTOR_CURTAIN:
        return CurtainDevice(node, executor)
    if device_type == DeviceType.SWITCH_DOUBLE:
        return DoubleSwitchDevice(node, executor)
    if device_type == DeviceType.SWITCH_MORE:
        return MultiSwitchDevice(node, executor)
    if device_type in {DeviceType.AIR_CONDITION, DeviceType.AIR_CONDITION_VRF}:
        return AirConditionDevice(node, executor)
    if is_knob_capable(node):
        return KnobDevice(node, executor)
    if device_type in {DeviceType.CONTROL_PANEL}:
        return ProgrammableSwitchDevice(node, executor)
    if device_type == DeviceType.SENSOR_PERSON:
        return MotionSensorDevice(node, executor)
    if device_type == DeviceType.SENSOR_DOOR:
        return DoorSensorDevice(node, executor)
    if device_type == DeviceType.SENSOR_HUMAN_LIGHT:
        return HumanLightSensorDevice(node, executor)
    if device_type == DeviceType.SENSOR_HUMITURE:
        return HumitureSensorDevice(node, executor)
    if device_type == DeviceType.SENSOR_MERRYTEK:
        return MerrytekSensorDevice(node, executor)
    if device_type == DeviceType.BATH_HEATER:
        return BathHeaterDevice(node, executor)
    return ReadOnlySensorDevice(node, executor)


def _device_type(value: int) -> DeviceType | None:
    try:
        return DeviceType(value)
    except ValueError:
        return None
