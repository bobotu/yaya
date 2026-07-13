from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from homeassistant.const import ATTR_DEVICE_ID

from .const import (
    ATTR_COUNT,
    ATTR_DELTA,
    ATTR_DIRECTION,
    ATTR_EVENT_TYPE,
    ATTR_INDEX,
    ATTR_KEY,
    ATTR_NODE_ID,
    ATTR_SPIN_MODE,
    ATTR_SUBTYPE,
    SWITCH_MODE_RELAY,
    SWITCH_MODE_WIRELESS,
)
from .gateway import GatewayEvent, TopologyNode, is_dream_curtain, is_knob_capable
from .gateway.const import KNOB_EVENT_VALUES, PANEL_EVENT_VALUES, SWITCH_MORE_KEYS
from .gateway.topology import DeviceType, NodeType


def node_key(node_id: str | int) -> str:
    return str(node_id)


def node_unique_id(gateway_id: str, node_id: str | int, suffix: str) -> str:
    return f"{gateway_id}_{node_key(node_id)}_{suffix}"


def node_identifier(gateway_id: str, node_id: str | int) -> str:
    return f"{gateway_id}:{node_key(node_id)}"


def orient_dream_curtain_slat_position(position: int, *, reversed_: bool) -> int:
    return 100 - position if reversed_ else position


def device_type(node: TopologyNode) -> DeviceType | None:
    try:
        return DeviceType(node.type)
    except ValueError:
        return None


def light_device_type(node: TopologyNode) -> DeviceType | None:
    item = device_type(node)
    if item in {
        DeviceType.LIGHT_SWITCHABLE,
        DeviceType.LIGHT_BRIGHTNESS,
        DeviceType.LIGHT_TEMPERATURE,
        DeviceType.LIGHT_COLOR,
        DeviceType.LAMP_DFT,
    }:
        return item
    return None


def device_model_key(node: TopologyNode) -> str:
    if node.nt == NodeType.MESH_GROUP and light_device_type(node) is not None:
        return "light_group"
    if light_device_type(node) is not None:
        return "light"
    if is_dream_curtain(node):
        return "dream_curtain"

    item = device_type(node)
    if item == DeviceType.MOTOR_CURTAIN:
        return "curtain"
    if item == DeviceType.SWITCH_DOUBLE:
        return "double_relay_switch"
    if item == DeviceType.SWITCH_MORE:
        return "multi_key_relay_switch"
    if is_knob_capable(node):
        return "knob_panel"
    if item == DeviceType.CONTROL_PANEL:
        return "scene_panel"
    if item in {DeviceType.AIR_CONDITION, DeviceType.AIR_CONDITION_VRF}:
        return "air_conditioner_controller"
    if item == DeviceType.BATH_HEATER:
        return "bath_heater"
    if item in {
        DeviceType.SENSOR_PERSON,
        DeviceType.SENSOR_DOOR,
        DeviceType.SENSOR_HUMAN_LIGHT,
        DeviceType.SENSOR_BRIGHTNESS,
        DeviceType.SENSOR_HUMITURE,
        DeviceType.SENSOR_MERRYTEK,
        DeviceType.SENSOR_TOF,
    }:
        return "sensor"
    return "yeelight_pro_device"


def should_import_node(
    node: TopologyNode,
    *,
    import_room_ids: Iterable[str | int] = (),
    room_id: str | int | None = None,
) -> bool:
    if node.nt == NodeType.MESH_SUBDEVICE:
        importable = True
    else:
        importable = node.nt == NodeType.MESH_GROUP and light_device_type(node) is not None
    if not importable:
        return False

    selected_room_ids = {str(room) for room in import_room_ids}
    if not selected_room_ids:
        return True
    explicit_room_id = node.room_id if room_id is None else room_id
    return explicit_room_id is not None and str(explicit_room_id) in selected_room_ids


def is_cover_node(node: TopologyNode) -> bool:
    item = device_type(node)
    return item in {DeviceType.MOTOR_CURTAIN, DeviceType.DREAM_CURTAIN} or is_dream_curtain(node)


def is_multi_switch_node(node: TopologyNode) -> bool:
    return device_type(node) == DeviceType.SWITCH_MORE


def is_double_switch_node(node: TopologyNode) -> bool:
    return device_type(node) == DeviceType.SWITCH_DOUBLE


def is_switch_mode_configurable_node(node: TopologyNode) -> bool:
    return is_double_switch_node(node) or is_multi_switch_node(node)


def switch_mode_for_node(
    node: TopologyNode,
    switch_modes: Mapping[str, str] | None = None,
) -> str:
    if not is_switch_mode_configurable_node(node):
        return SWITCH_MODE_WIRELESS
    if switch_modes is None:
        return SWITCH_MODE_RELAY
    mode = switch_modes.get(node_key(node.id), SWITCH_MODE_RELAY)
    return SWITCH_MODE_WIRELESS if mode == SWITCH_MODE_WIRELESS else SWITCH_MODE_RELAY


def switch_node_is_relay_mode(
    node: TopologyNode,
    switch_modes: Mapping[str, str] | None = None,
) -> bool:
    return is_switch_mode_configurable_node(node) and switch_mode_for_node(node, switch_modes) == SWITCH_MODE_RELAY


def switch_node_is_wireless_mode(
    node: TopologyNode,
    switch_modes: Mapping[str, str] | None = None,
) -> bool:
    return is_switch_mode_configurable_node(node) and switch_mode_for_node(node, switch_modes) == SWITCH_MODE_WIRELESS


def is_programmable_node(node: TopologyNode) -> bool:
    return device_type(node) in {
        DeviceType.SWITCH_DOUBLE,
        DeviceType.SWITCH_MORE,
        DeviceType.CONTROL_PANEL,
        DeviceType.KNOB,
        DeviceType.KNOB_EXT,
    }


def button_count(node: TopologyNode) -> int:
    if node.channel_count is not None and node.channel_count > 0:
        return node.channel_count
    if node.component_type_ids:
        return len(node.component_type_ids)
    keys = [event_key for event_key in relay_channel_numbers(node)]
    if keys:
        return max(keys)
    if is_knob_capable(node):
        return 6
    return 1 if is_programmable_node(node) else 0


def relay_channel_numbers(node: TopologyNode) -> tuple[int, ...]:
    if is_double_switch_node(node):
        return (1, 2)
    if not is_multi_switch_node(node):
        return ()
    if node.channel_count is not None and node.channel_count > 0:
        return tuple(range(1, node.channel_count + 1))
    return tuple(
        int(key.split("-", 1)[0]) for key in SWITCH_MORE_KEYS if key in node.params and key.split("-", 1)[0].isdigit()
    )


def relay_prop_name(node: TopologyNode, channel: int) -> str:
    suffix = "p" if is_double_switch_node(node) else "sp"
    return f"{channel}-{suffix}"


def event_types_for_node(node: TopologyNode) -> tuple[str, ...]:
    if not is_programmable_node(node):
        return ()
    values: list[str] = list(PANEL_EVENT_VALUES)
    if is_knob_capable(node):
        values.extend(KNOB_EVENT_VALUES)
    return tuple(value.replace(".", "_") for value in values)


def event_subtypes_for_node(node: TopologyNode, event_type: str) -> tuple[str, ...]:
    count = button_count(node)
    if count <= 0:
        return ()
    prefix = "idx" if event_type == "knob_spin" else "key"
    return tuple(f"{prefix}_{index}" for index in range(1, count + 1))


def event_data(event: GatewayEvent, *, device_id: str | None = None) -> dict[str, Any]:
    data: dict[str, Any] = {
        ATTR_NODE_ID: node_key(event.id),
        ATTR_EVENT_TYPE: event.event_type,
        "value": event.value,
        "nt": event.nt,
    }
    if device_id is not None:
        data[ATTR_DEVICE_ID] = device_id
    if event.key is not None:
        data[ATTR_KEY] = event.key
        data[ATTR_SUBTYPE] = f"key_{event.key}"
    if event.count is not None:
        data[ATTR_COUNT] = event.count
    if event.index is not None:
        data[ATTR_INDEX] = event.index
        data.setdefault(ATTR_SUBTYPE, f"idx_{event.index}")
    if event.spin_delta is not None:
        data[ATTR_DELTA] = event.spin_delta
    if event.spin_direction is not None:
        data[ATTR_DIRECTION] = event.spin_direction
    if event.spin_mode is not None:
        data[ATTR_SPIN_MODE] = event.spin_mode
    data["params"] = dict(event.params)
    return data


def first_present_bool(params: dict[str, Any], keys: Iterable[str]) -> bool | None:
    for key in keys:
        value = params.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
    return None


def int_param(node: Any, key: str) -> int | None:
    if node is None:
        return None
    value = node.params.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def bool_param(node: Any, key: str) -> bool | None:
    if node is None:
        return None
    value = node.params.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    return None


def true_bool_param(node: Any, key: str) -> bool:
    if node is None:
        return False
    return node.params.get(key) is True


def indexed_props(node: Any, suffix: str) -> tuple[str, ...]:
    props = []
    for key in node.params:
        if isinstance(key, str) and key.endswith(f"-{suffix}") and key.split("-", 1)[0].isdigit():
            props.append(key)
    return tuple(sorted(props, key=lambda item: int(item.split("-", 1)[0])))
