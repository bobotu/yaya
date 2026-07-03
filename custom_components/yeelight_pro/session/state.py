from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from ..core.protocol import list_payload
from ..core.topology import Topology, TopologyNode
from ..core.updates import PropertyChange


@dataclass
class UnknownPropertyNode:
    """Property snapshot for a node not currently present in topology."""

    id: str | int
    nt: int | None
    property_type: int | None
    params: Mapping[str, Any]
    first_seen: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_seen: datetime = field(default_factory=lambda: datetime.now(UTC))
    count: int = 1

    def update(self, item: Mapping[str, Any]) -> None:
        self.last_seen = datetime.now(UTC)
        self.count += 1
        self.params = _mapping_or_empty(item.get("params"))
        self.nt = _int_or_none(item.get("nt")) if "nt" in item else self.nt
        self.property_type = _int_or_none(item.get("pt")) if "pt" in item else self.property_type


class GatewayState:
    """In-memory topology and property state assembled from reads and pushes."""

    def __init__(self) -> None:
        self.nodes: dict[str | int, TopologyNode] = {}
        self.groups: dict[str | int, Mapping[str, Any]] = {}
        self.rooms: dict[str | int, Mapping[str, Any]] = {}
        self.scenes: dict[str | int, Mapping[str, Any]] = {}
        self.unknown_property_nodes: dict[str | int, UnknownPropertyNode] = {}

    def apply_topology(self, message: Mapping[str, Any]) -> Topology:
        topology = Topology.from_message(message)
        nodes = {}
        for node in topology.nodes:
            current = self.nodes.get(node.id)
            if current is not None:
                params = dict(current.params)
                params.update(node.params)
                online = node.online if node.online is not None else current.online
                node = replace(node, params=params, online=online)
            unknown = self.unknown_property_nodes.pop(node.id, None)
            if unknown is not None:
                node = node.merge_update(
                    {
                        "id": unknown.id,
                        "nt": unknown.nt,
                        "pt": unknown.property_type,
                        "params": unknown.params,
                    }
                )
            nodes[node.id] = node
        self.nodes = nodes
        self.groups = {_item_id(group): group for group in topology.groups if _item_id(group) is not None}
        self.rooms = {_item_id(room): room for room in topology.rooms if _item_id(room) is not None}
        self.scenes = {_item_id(scene): scene for scene in topology.scenes if _item_id(scene) is not None}
        return topology

    def apply_groups(self, message: Mapping[str, Any]) -> None:
        self.groups.update(
            {_item_id(group): group for group in list_payload(message, "groups") if _item_id(group) is not None}
        )

    def apply_rooms(self, message: Mapping[str, Any]) -> None:
        self.rooms.update(
            {_item_id(room): room for room in list_payload(message, "rooms") if _item_id(room) is not None}
        )

    def apply_scenes(self, message: Mapping[str, Any]) -> None:
        self.scenes.update(
            {_item_id(scene): scene for scene in list_payload(message, "scenes") if _item_id(scene) is not None}
        )

    def apply_message(self, message: Mapping[str, Any]) -> None:
        method = message.get("method")
        if method == "gateway_post.topology":
            self.apply_topology(message)
        elif method == "gateway_post.prop":
            self.apply_properties(message)

    def apply_properties(self, message: Mapping[str, Any]) -> list[PropertyChange]:
        changes: list[PropertyChange] = []
        for item in list_payload(message, "nodes"):
            node_id = _item_id(item)
            if node_id is None:
                continue
            current = self.nodes.get(node_id)
            if current is None:
                self._remember_unknown_property_node(node_id, item)
                continue
            updated = current.merge_update(item)
            self.nodes[node_id] = updated
            changes.append(PropertyChange(id=node_id, before=current, after=updated, update=item))
        return changes

    def full_property_coverage(self, message: Mapping[str, Any]) -> bool:
        node_ids = {_item_id(item) for item in list_payload(message, "nodes")}
        return bool(self.nodes) and set(self.nodes).issubset(node_ids)

    def unknown_summary(self) -> dict[str, Any]:
        by_shape: dict[str, int] = {}
        for item in self.unknown_property_nodes.values():
            param_keys = ",".join(sorted(item.params))
            key = f"nt={item.nt};pt={item.property_type};params={param_keys}"
            by_shape[key] = by_shape.get(key, 0) + 1
        return {
            "count": len(self.unknown_property_nodes),
            "by_shape": by_shape,
        }

    def _remember_unknown_property_node(self, node_id: str | int, item: Mapping[str, Any]) -> None:
        current = self.unknown_property_nodes.get(node_id)
        if current is not None:
            current.update(item)
            return
        self.unknown_property_nodes[node_id] = UnknownPropertyNode(
            id=node_id,
            nt=_int_or_none(item.get("nt")),
            property_type=_int_or_none(item.get("pt")),
            params=_mapping_or_empty(item.get("params")),
        )

    def room_name(self, room_id: str | int | None) -> str | None:
        if room_id is None:
            return None
        room = self.rooms.get(room_id)
        if room is None and isinstance(room_id, str):
            try:
                room = self.rooms.get(int(room_id))
            except ValueError:
                room = None
        elif room is None and isinstance(room_id, int):
            room = self.rooms.get(str(room_id))
        if room is None:
            return None
        name = room.get("name", room.get("n"))
        return name if isinstance(name, str) else None

    def room_id_for_node(self, node: TopologyNode) -> str | int | None:
        if node.room_id is not None:
            return node.room_id
        group_room_id = self.inherited_group_room_id(node.id)
        if group_room_id is not None:
            return group_room_id
        return self.inherited_room_id(node.id)

    def room_name_for_node(self, node: TopologyNode) -> str | None:
        return self.room_name(self.room_id_for_node(node))

    def inherited_group_room_id(self, node_id: str | int) -> str | int | None:
        for group in self.groups.values():
            group_room_id = _room_id(group)
            if group_room_id is None:
                continue
            if _contains_node_id(group, node_id):
                return group_room_id
        return None

    def inherited_room_id(self, node_id: str | int) -> str | int | None:
        for room_key, room in self.rooms.items():
            room_id = _item_id(room) or room_key
            if _contains_node_id(room, node_id):
                return room_id
            for group_id in _contained_group_ids(room):
                group = _mapping_get(self.groups, group_id)
                if group is not None and _contains_node_id(group, node_id):
                    return room_id
        return None


def _item_id(item: Mapping[str, Any]) -> str | int | None:
    item_id = item.get("id")
    return item_id if isinstance(item_id, (str, int)) and not isinstance(item_id, bool) else None


def _mapping_or_empty(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _int_or_none(value: object) -> int | None:
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


def _room_id(item: Mapping[str, Any]) -> str | int | None:
    for key in ("roomId", "room_id", "roomid", "rid"):
        value = item.get(key)
        if isinstance(value, (str, int)) and not isinstance(value, bool):
            return value
    return None


def _contains_node_id(group: Mapping[str, Any], node_id: str | int) -> bool:
    wanted = {node_id, str(node_id)}
    for key in ("nodes", "children", "devices", "members", "items", "subdevices"):
        for member_id in _member_ids(group.get(key)):
            if member_id in wanted:
                return True
    for key in ("node_ids", "nodeIds", "ids", "dids", "children_ids", "member_ids"):
        for member_id in _scalar_ids(group.get(key)):
            if member_id in wanted:
                return True
    return False


def _contained_group_ids(room: Mapping[str, Any]) -> list[str | int]:
    ids: list[str | int] = []
    for key in ("groups", "group_ids", "groupIds", "gids"):
        ids.extend(_member_ids(room.get(key)))
    return ids


def _mapping_get(mapping: Mapping[str | int, Mapping[str, Any]], item_id: str | int) -> Mapping[str, Any] | None:
    item = mapping.get(item_id)
    if item is not None:
        return item
    if isinstance(item_id, str):
        try:
            return mapping.get(int(item_id))
        except ValueError:
            return None
    return mapping.get(str(item_id))


def _member_ids(value: object) -> list[str | int]:
    if not isinstance(value, list):
        return []
    ids: list[str | int] = []
    for item in value:
        if isinstance(item, (str, int)) and not isinstance(item, bool):
            ids.append(item)
        elif isinstance(item, Mapping):
            item_id = _item_id(item)
            if item_id is not None:
                ids.append(item_id)
    return ids


def _scalar_ids(value: object) -> list[str | int]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, (str, int)) and not isinstance(item, bool)]
