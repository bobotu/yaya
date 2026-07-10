from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .const import KNOB_EVENT_VALUES, PANEL_EVENT_VALUES, SWITCH_DOUBLE_KEYS, SWITCH_MORE_KEYS
from .topology import DeviceType, TopologyNode


@dataclass(frozen=True)
class CommandCapability:
    name: str
    description: str
    arguments: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeviceCapabilities:
    category: str
    readable_properties: tuple[str, ...] = ()
    writable_properties: tuple[str, ...] = ()
    commands: tuple[CommandCapability, ...] = ()
    events: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "readable_properties": list(self.readable_properties),
            "writable_properties": list(self.writable_properties),
            "commands": [
                {
                    "name": command.name,
                    "description": command.description,
                    "arguments": list(command.arguments),
                }
                for command in self.commands
            ],
            "events": list(self.events),
        }


def capabilities_for_node(node: TopologyNode) -> DeviceCapabilities:
    device_type = _device_type(node.type)
    if device_type in {
        DeviceType.LIGHT_SWITCHABLE,
        DeviceType.LIGHT_BRIGHTNESS,
        DeviceType.LIGHT_TEMPERATURE,
        DeviceType.LIGHT_COLOR,
        DeviceType.LAMP_DFT,
    }:
        return _light_capabilities(device_type)
    if is_dream_curtain(node):
        return _curtain_capabilities(has_tilt=True)
    if device_type == DeviceType.MOTOR_CURTAIN:
        return _curtain_capabilities(has_tilt=False)
    if device_type == DeviceType.SWITCH_DOUBLE:
        return _double_switch_capabilities()
    if device_type == DeviceType.SWITCH_MORE:
        return _multi_switch_capabilities(node.params)
    if device_type in {DeviceType.AIR_CONDITION, DeviceType.AIR_CONDITION_VRF}:
        return _air_condition_capabilities()
    if is_knob_capable(node):
        return DeviceCapabilities(category="knob", events=PANEL_EVENT_VALUES + KNOB_EVENT_VALUES)
    if device_type == DeviceType.CONTROL_PANEL:
        return DeviceCapabilities(category="programmable_switch", events=PANEL_EVENT_VALUES)
    if device_type == DeviceType.SENSOR_PERSON:
        return DeviceCapabilities(category="motion_sensor", readable_properties=("mv", "bp", "bc"))
    if device_type == DeviceType.SENSOR_DOOR:
        return DeviceCapabilities(category="door_sensor", readable_properties=("dc", "alm"))
    if device_type == DeviceType.SENSOR_HUMAN_LIGHT:
        return DeviceCapabilities(category="human_light_sensor", readable_properties=("mv", "level"))
    if device_type == DeviceType.SENSOR_HUMITURE:
        return DeviceCapabilities(category="temperature_humidity_sensor", readable_properties=("t", "h"))
    if device_type == DeviceType.SENSOR_MERRYTEK:
        return DeviceCapabilities(category="merrytek_sensor", readable_properties=("mv", "luminance"))
    if device_type == DeviceType.BATH_HEATER:
        return _bath_heater_capabilities()
    return DeviceCapabilities(category="unknown", readable_properties=tuple(sorted(node.params.keys())))


def _light_capabilities(device_type: DeviceType | None) -> DeviceCapabilities:
    readable = ["p"]
    writable = ["p"]
    commands = [
        CommandCapability("turn-on", "Turn the light on", ("brightness", "color-temperature", "color", "duration")),
        CommandCapability("turn-off", "Turn the light off", ("duration",)),
        CommandCapability("blink", "Blink the light as a visual notification", ("type", "repeat")),
    ]
    if device_type in {
        DeviceType.LIGHT_BRIGHTNESS,
        DeviceType.LIGHT_TEMPERATURE,
        DeviceType.LIGHT_COLOR,
        DeviceType.LAMP_DFT,
    }:
        readable.append("l")
        writable.append("l")
        commands.append(CommandCapability("set-brightness", "Set brightness percentage", ("value", "duration")))
    if device_type in {DeviceType.LIGHT_TEMPERATURE, DeviceType.LIGHT_COLOR, DeviceType.LAMP_DFT}:
        readable.append("ct")
        writable.append("ct")
        commands.append(CommandCapability("set-color-temperature", "Set color temperature", ("value", "duration")))
    if device_type == DeviceType.LIGHT_COLOR:
        readable.append("c")
        writable.append("c")
        commands.append(CommandCapability("set-color", "Set RGB color as integer", ("value", "duration")))
    if device_type == DeviceType.LAMP_DFT:
        readable.append("angle")
        writable.append("angle")
    return DeviceCapabilities("light", tuple(readable), tuple(writable), tuple(commands))


def _curtain_capabilities(*, has_tilt: bool) -> DeviceCapabilities:
    readable = ["cp", "tp", "rs"]
    writable = ["tp"]
    commands = [
        CommandCapability("open", "Open curtain", ("duration",)),
        CommandCapability("close", "Close curtain", ("duration",)),
        CommandCapability("stop", "Pause current motor movement"),
        CommandCapability("set-position", "Set curtain target position", ("position", "duration")),
        CommandCapability("motor-adjust", "Send native motorAdjust action", ("action",)),
    ]
    if has_tilt:
        readable.extend(["cra", "tra", "trs"])
        writable.append("tra")
        commands.extend(
            [
                CommandCapability("set-angle", "Set dream curtain target angle", ("angle", "duration")),
                CommandCapability("open-tilt", "Open dream curtain tilt", ("duration",)),
                CommandCapability("close-tilt", "Close dream curtain tilt", ("duration",)),
                CommandCapability("stop-tilt", "Pause dream curtain tilt movement"),
            ]
        )
    return DeviceCapabilities("cover", tuple(readable), tuple(writable), tuple(commands))


def _double_switch_capabilities() -> DeviceCapabilities:
    return DeviceCapabilities(
        "relay_switch",
        readable_properties=SWITCH_DOUBLE_KEYS,
        writable_properties=("p",) + SWITCH_DOUBLE_KEYS,
        commands=(
            CommandCapability("set-all", "Set both relay channels", ("value",)),
            CommandCapability("set-channel", "Set a relay channel", ("channel", "value")),
        ),
        events=PANEL_EVENT_VALUES,
    )


def _multi_switch_capabilities(params: Mapping[str, Any]) -> DeviceCapabilities:
    channels = tuple(key for key in SWITCH_MORE_KEYS if key in params)
    readable = channels + tuple(key for key in ("0-blp",) if key in params)
    return DeviceCapabilities(
        "multi_switch",
        readable_properties=readable,
        writable_properties=channels + tuple(key for key in ("0-blp",) if key in params),
        commands=(CommandCapability("set-channel", "Set a switch channel", ("channel", "value")),),
        events=PANEL_EVENT_VALUES,
    )


def _air_condition_capabilities() -> DeviceCapabilities:
    readable = (
        "{index}-aco",
        "{index}-acp",
        "{index}-acm",
        "{index}-acct",
        "{index}-actt",
        "{index}-acf",
        "{index}-acd",
    )
    writable = (
        "{index}-acp",
        "{index}-acm",
        "{index}-actt",
        "{index}-acf",
        "{index}-acd",
        "{index}-acdfltr",
        "{index}-acrc",
    )
    return DeviceCapabilities(
        "air_conditioner",
        readable_properties=readable,
        writable_properties=writable,
        commands=(
            CommandCapability("ac-power", "Set air conditioner power", ("index", "value")),
            CommandCapability("ac-mode", "Set air conditioner mode", ("index", "value")),
            CommandCapability("ac-temp", "Set air conditioner target temperature", ("index", "value")),
            CommandCapability("ac-fan", "Set air conditioner fan speed", ("index", "value")),
            CommandCapability("ac-delay", "Set air conditioner delay milliseconds", ("index", "value")),
            CommandCapability("ac-deflector", "Set air conditioner deflector value", ("index", "value")),
            CommandCapability("ac-remote", "Set remote controller enable", ("index", "value")),
        ),
    )


def _bath_heater_capabilities() -> DeviceCapabilities:
    return DeviceCapabilities(
        "bath_heater",
        readable_properties=("p", "bhm", "do", "ve", "fa", "he", "t", "tgt"),
        writable_properties=("p", "bhm", "do", "ve", "fa", "he", "tgt"),
        commands=(
            CommandCapability("bath-power", "Set bath heater power", ("value",)),
            CommandCapability("bath-mode", "Set bath heater mode", ("value",)),
            CommandCapability("bath-delay-off", "Set bath heater delay off minutes", ("value",)),
            CommandCapability("bath-ventilation", "Set bath heater ventilation level", ("value",)),
            CommandCapability("bath-fan", "Set bath heater fan level", ("value",)),
            CommandCapability("bath-heat", "Set bath heater heat level", ("value",)),
            CommandCapability("bath-temp", "Set bath heater target temperature", ("value",)),
        ),
    )


def _device_type(value: int) -> DeviceType | None:
    try:
        return DeviceType(value)
    except ValueError:
        return None


def is_knob_capable(node: TopologyNode) -> bool:
    device_type = _device_type(node.type)
    property_type = _device_type(node.property_type or 0)
    return device_type in {DeviceType.KNOB, DeviceType.KNOB_EXT} or (
        device_type == DeviceType.CONTROL_PANEL and property_type in {DeviceType.KNOB, DeviceType.KNOB_EXT}
    )


def is_dream_curtain(node: TopologyNode) -> bool:
    device_type = _device_type(node.type)
    property_type = _device_type(node.property_type or 0)
    return device_type == DeviceType.DREAM_CURTAIN or (
        device_type == DeviceType.MOTOR_CURTAIN and property_type == DeviceType.DREAM_CURTAIN
    )
