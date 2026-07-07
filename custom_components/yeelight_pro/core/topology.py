from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import IntEnum
from typing import Any

from .protocol import list_payload

NodeId = str | int


class NodeType(IntEnum):
    ROOM = 1
    MESH_SUBDEVICE = 2
    CUSTOM_GROUP = 3
    MESH_GROUP = 4
    HOUSE = 5
    SCENE = 6


class DeviceType(IntEnum):
    LIGHT_SWITCHABLE = 1
    LIGHT_BRIGHTNESS = 2
    LIGHT_TEMPERATURE = 3
    LIGHT_COLOR = 4
    MOTOR_CURTAIN = 6
    SWITCH_DOUBLE = 7
    AIR_CONDITION_VRF = 10
    SWITCH_MORE = 13
    LAMP_DFT = 14
    AIR_CONDITION = 15
    DREAM_CURTAIN = 22
    CONTROL_PANEL = 128
    SENSOR_PERSON = 129
    SENSOR_DOOR = 130
    KNOB = 132
    SENSOR_HUMAN_LIGHT = 134
    SENSOR_BRIGHTNESS = 135
    SENSOR_HUMITURE = 136
    KNOB_EXT = 137
    SENSOR_MERRYTEK = 138
    BATH_HEATER = 2049
    SENSOR_TOF = 2052


@dataclass(frozen=True)
class TopologyNode:
    id: NodeId
    nt: int
    type: int
    product_id: int | None = None
    property_type: int | None = None
    name: str | None = None
    room_id: NodeId | None = None
    channel_count: int | None = None
    component_type_ids: tuple[int, ...] = ()
    params: Mapping[str, Any] = field(default_factory=dict)
    online: bool | None = None

    @classmethod
    def from_mapping(cls, item: Mapping[str, Any]) -> TopologyNode:
        return cls(
            id=_node_id(item.get("id")),
            nt=_int_or_default(item.get("nt"), NodeType.MESH_SUBDEVICE),
            type=_int_or_default(item.get("type"), 0),
            product_id=_int_or_none(item.get("pid")),
            property_type=_int_or_none(item.get("pt")),
            name=_str_or_none(item.get("name", item.get("n"))),
            room_id=_optional_node_id(_first_present(item, "roomId", "room_id", "roomid", "rid")),
            channel_count=_int_or_none(item.get("ch_num")),
            component_type_ids=tuple(_int_items(item.get("cids"))),
            params=_mapping_or_empty(item.get("params")),
            online=_bool_or_none(item.get("o")),
        )

    def merge_update(self, item: Mapping[str, Any]) -> TopologyNode:
        params = dict(self.params)
        params.update(_mapping_or_empty(item.get("params")))
        return replace(
            self,
            nt=_int_or_default(item.get("nt"), self.nt),
            type=_int_or_default(item.get("type"), self.type),
            product_id=_int_or_none(item.get("pid")) if "pid" in item else self.product_id,
            property_type=_int_or_none(item.get("pt")) if "pt" in item else self.property_type,
            name=_str_or_none(item.get("name", item.get("n"))) or self.name,
            room_id=(
                _optional_node_id(_first_present(item, "roomId", "room_id", "roomid", "rid"))
                if _has_any(item, "roomId", "room_id", "roomid", "rid")
                else self.room_id
            ),
            channel_count=_int_or_none(item.get("ch_num")) if "ch_num" in item else self.channel_count,
            component_type_ids=tuple(_int_items(item.get("cids"))) if "cids" in item else self.component_type_ids,
            params=params,
            online=_bool_or_none(item.get("o")) if "o" in item else self.online,
        )


@dataclass(frozen=True)
class Topology:
    nodes: tuple[TopologyNode, ...]
    groups: tuple[Mapping[str, Any], ...]
    rooms: tuple[Mapping[str, Any], ...]
    scenes: tuple[Mapping[str, Any], ...]

    @classmethod
    def from_message(cls, message: Mapping[str, Any]) -> Topology:
        return cls(
            nodes=tuple(TopologyNode.from_mapping(item) for item in list_payload(message, "nodes")),
            groups=tuple(list_payload(message, "groups")),
            rooms=tuple(list_payload(message, "rooms")),
            scenes=tuple(list_payload(message, "scenes")),
        )


def _node_id(value: object) -> NodeId:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("topology node is missing a valid id")
    return value


def _optional_node_id(value: object) -> NodeId | None:
    if isinstance(value, bool):
        return None
    return value if isinstance(value, (str, int)) else None


def _mapping_or_empty(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _int_or_default(value: object, default: int) -> int:
    result = _int_or_none(value)
    return default if result is None else result


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, IntEnum):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _int_items(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    return [item for item in (_int_or_none(item) for item in value) if item is not None]


def _str_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _bool_or_none(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _first_present(item: Mapping[str, Any], *keys: str) -> object:
    for key in keys:
        if key in item:
            return item[key]
    return None


def _has_any(item: Mapping[str, Any], *keys: str) -> bool:
    return any(key in item for key in keys)
