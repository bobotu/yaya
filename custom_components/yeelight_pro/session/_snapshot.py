from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from dataclasses import replace as dataclass_replace
from datetime import UTC, datetime
from typing import Any

from ..gateway.coercion import int_or_none as _int_or_none
from ..gateway.coercion import node_id_or_none
from ..gateway.protocol import GatewayMethod, list_payload
from ..gateway.topology import NodeType, Topology, TopologyNode
from ..gateway.updates import PropertyChange


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


class GatewaySnapshot:
    """In-memory topology and property state assembled from reads and pushes."""

    def __init__(self) -> None:
        self.nodes: dict[str | int, TopologyNode] = {}
        self.topology_node_ids: set[str | int] = set()
        self.groups: dict[str | int, Mapping[str, Any]] = {}
        self.rooms: dict[str | int, Mapping[str, Any]] = {}
        self.scenes: dict[str | int, Mapping[str, Any]] = {}
        self.unknown_property_nodes: dict[str | int, UnknownPropertyNode] = {}

    def apply_topology(self, message: Mapping[str, Any], *, replace: bool = True) -> Topology:
        topology = Topology.from_message(message)
        nodes = {} if replace else dict(self.nodes)
        present_node_ids: set[str | int] = set()
        for node in topology.nodes:
            present_node_ids.add(node.id)
            current = self.nodes.get(node.id)
            if current is not None:
                params = dict(current.params)
                params.update(node.params)
                current_online = current.online if node.id in self.topology_node_ids else None
                online = node.online if node.online is not None else current_online
                node = dataclass_replace(node, params=params, online=online)
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
        if replace:
            for node_id, current in self.nodes.items():
                if node_id not in nodes:
                    nodes[node_id] = dataclass_replace(current, online=False)
            self.topology_node_ids = present_node_ids
        else:
            self.topology_node_ids.update(present_node_ids)
        self.nodes = nodes
        if replace:
            self.groups = _items_by_id(topology.groups)
            self.rooms = _items_by_id(topology.rooms)
            self.scenes = _items_by_id(topology.scenes)
        else:
            self.groups.update(_items_by_id(topology.groups))
            self.rooms.update(_items_by_id(topology.rooms))
            self.scenes.update(_items_by_id(topology.scenes))
        return topology

    def apply_groups(self, message: Mapping[str, Any]) -> list[PropertyChange]:
        changes: list[PropertyChange] = []
        groups = list_payload(message, "groups")
        self.groups.update(_items_by_id(groups))
        for item in groups:
            node_id = _item_id(item)
            if node_id is None:
                continue
            current = self.nodes.get(node_id)
            if current is None or current.nt != NodeType.MESH_GROUP:
                continue
            updated = current.merge_update(item)
            self.nodes[node_id] = updated
            changes.append(PropertyChange(id=node_id, before=current, after=updated, update=item))
        return changes

    def apply_rooms(self, message: Mapping[str, Any]) -> None:
        self.rooms.update(_items_by_id(list_payload(message, "rooms")))

    def apply_scenes(self, message: Mapping[str, Any]) -> None:
        self.scenes.update(_items_by_id(list_payload(message, "scenes")))

    def apply_message(self, message: Mapping[str, Any]) -> None:
        method = message.get("method")
        if method == GatewayMethod.POST_TOPOLOGY:
            self.apply_topology(message, replace=False)
        elif method == GatewayMethod.POST_PROP:
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
        items_by_id = {
            node_id: item for item in list_payload(message, "nodes") if (node_id := _item_id(item)) is not None
        }
        return bool(self.topology_node_ids) and all(
            node_id in items_by_id and items_by_id[node_id].get("o") is True for node_id in self.topology_node_ids
        )

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
    return node_id_or_none(item.get("id"))


def _items_by_id(items: Iterable[Mapping[str, Any]]) -> dict[str | int, Mapping[str, Any]]:
    result: dict[str | int, Mapping[str, Any]] = {}
    for item in items:
        item_id = _item_id(item)
        if item_id is not None:
            result[item_id] = item
    return result


def _mapping_or_empty(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _room_id(item: Mapping[str, Any]) -> str | int | None:
    for key in ("roomId", "room_id", "roomid", "rid"):
        value = node_id_or_none(item.get(key))
        if value is not None:
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
    return [item for raw in value if (item := node_id_or_none(raw)) is not None]
