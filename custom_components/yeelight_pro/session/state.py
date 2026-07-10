from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, TypeAlias

from ..core.protocol import GatewayMethod, list_payload
from ..core.topology import NodeId, Topology, TopologyNode
from ._snapshot import GatewaySnapshot, UnknownPropertyNode

PropertyKey: TypeAlias = tuple[str, str]


@dataclass
class PendingBatch:
    """Observed-state hold for one accepted gateway write batch."""

    id: int
    deadline: float
    targets: dict[PropertyKey, Any]
    node_ids: dict[str, NodeId]
    accepted: bool = False
    observed: set[PropertyKey] = field(default_factory=set)


@dataclass(frozen=True)
class StateResult:
    """Visible effects produced by one deterministic state-store operation."""

    changed_node_ids: frozenset[NodeId] = frozenset()
    metadata_changed: bool = False
    ended_batch_ids: tuple[int, ...] = ()

    @property
    def visible_changed(self) -> bool:
        return self.metadata_changed or bool(self.changed_node_ids)


class StateStore:
    """Own raw gateway observations, HA-visible nodes, and pending write holds."""

    def __init__(self) -> None:
        self.raw = GatewaySnapshot()
        self.visible_nodes: dict[NodeId, TopologyNode] = {}
        self._batches: dict[int, PendingBatch] = {}
        self._owner: dict[PropertyKey, int] = {}
        self._next_batch_id = 1

    @property
    def nodes(self) -> dict[NodeId, TopologyNode]:
        """Return the HA-visible node map."""

        return self.visible_nodes

    @property
    def groups(self) -> dict[NodeId, Mapping[str, Any]]:
        return self.raw.groups

    @property
    def rooms(self) -> dict[NodeId, Mapping[str, Any]]:
        return self.raw.rooms

    @property
    def scenes(self) -> dict[NodeId, Mapping[str, Any]]:
        return self.raw.scenes

    @property
    def unknown_property_nodes(self) -> dict[NodeId, UnknownPropertyNode]:
        return self.raw.unknown_property_nodes

    def prepare_batch(
        self,
        targets_by_node: Mapping[NodeId, Mapping[str, Any]],
        *,
        deadline: float,
    ) -> tuple[int, StateResult]:
        """Hold target properties before their gateway RPC is sent."""

        targets: dict[PropertyKey, Any] = {}
        node_ids: dict[str, NodeId] = {}
        for node_id, props in targets_by_node.items():
            node_key = _node_key(node_id)
            node_ids[node_key] = node_id
            for prop, value in props.items():
                targets[(node_key, prop)] = value
        if not targets:
            raise ValueError("a pending batch requires at least one target property")

        batch_id = self._next_batch_id
        self._next_batch_id += 1
        batch = PendingBatch(
            id=batch_id,
            deadline=deadline,
            targets=targets,
            node_ids=node_ids,
        )
        self._batches[batch_id] = batch

        superseded: set[int] = set()
        ended: list[int] = []
        for key in targets:
            previous_id = self._owner.get(key)
            if previous_id is not None and previous_id != batch_id:
                previous = self._batches.get(previous_id)
                if previous is not None:
                    previous.targets.pop(key, None)
                    previous.observed.discard(key)
                    superseded.add(previous_id)
                    if not previous.targets:
                        self._batches.pop(previous_id, None)
                        ended.append(previous_id)
            self._owner[key] = batch_id

        result = self._release_ready_batches(superseded)
        if ended:
            result = _merge_results(result, StateResult(ended_batch_ids=tuple(sorted(ended))))
        return batch_id, result

    def accept_batch(self, batch_id: int) -> StateResult:
        """Record aggregate gateway acceptance without confirming device state."""

        batch = self._batches.get(batch_id)
        if batch is None:
            return StateResult()
        batch.accepted = True
        return self._release_ready_batches({batch_id})

    def fail_batch(self, batch_id: int) -> StateResult:
        """Remove a failed pre-send hold and expose the latest observed raw state."""

        return self._end_batches((batch_id,))

    def expire_batch(self, batch_id: int) -> StateResult:
        """Bound reconciliation by releasing one batch to current raw state."""

        return self._end_batches((batch_id,))

    def expire_due(self, *, now: float) -> StateResult:
        batch_ids = tuple(batch.id for batch in self._batches.values() if batch.deadline <= now)
        return self._end_batches(batch_ids)

    def clear_pending(self) -> StateResult:
        """Drop all command holds, for example when a connection is lost."""

        return self._end_batches(tuple(self._batches))

    def apply_topology(
        self,
        message: Mapping[str, Any],
        *,
        replace_existing: bool = True,
        match_batch_id: int | None = None,
    ) -> tuple[Topology, StateResult]:
        """Apply a topology observation and project it through pending holds."""

        metadata_before = self._metadata_signature()
        topology = self.raw.apply_topology(message, replace=replace_existing)
        explicit_by_node = _explicit_params_by_node(list_payload(message, "nodes"))
        self._mark_observed_targets(explicit_by_node, match_batch_id=match_batch_id)
        changed = self._reproject_nodes(self.raw.nodes)
        result = self._release_ready_batches(self._batch_ids_for(explicit_by_node))
        return topology, _merge_results(
            StateResult(
                changed_node_ids=frozenset(changed),
                metadata_changed=metadata_before != self._metadata_signature(),
            ),
            result,
        )

    def apply_properties(
        self,
        message: Mapping[str, Any],
        *,
        match_batch_id: int | None = None,
    ) -> StateResult:
        """Apply ordinary node property observations."""

        items = list_payload(message, "nodes")
        self.raw.apply_properties(message)
        explicit_by_node = _explicit_params_by_node(items)
        self._mark_observed_targets(explicit_by_node, match_batch_id=match_batch_id)
        changed = self._reproject_nodes(
            item_id for item_id in (_item_id(item) for item in items) if item_id is not None
        )
        return _merge_results(
            StateResult(changed_node_ids=frozenset(changed)),
            self._release_ready_batches(self._batch_ids_for(explicit_by_node)),
        )

    def apply_groups(
        self,
        message: Mapping[str, Any],
        *,
        match_batch_id: int | None = None,
    ) -> StateResult:
        """Apply native-group observations."""

        metadata_before = self._metadata_signature()
        items = list_payload(message, "groups")
        self.raw.apply_groups(message)
        explicit_by_node = _explicit_params_by_node(items)
        self._mark_observed_targets(explicit_by_node, match_batch_id=match_batch_id)
        changed = self._reproject_nodes(
            item_id for item_id in (_item_id(item) for item in items) if item_id is not None
        )
        return _merge_results(
            StateResult(
                changed_node_ids=frozenset(changed),
                metadata_changed=metadata_before != self._metadata_signature(),
            ),
            self._release_ready_batches(self._batch_ids_for(explicit_by_node)),
        )

    def apply_rooms(self, message: Mapping[str, Any]) -> StateResult:
        metadata_before = self._metadata_signature()
        self.raw.apply_rooms(message)
        return StateResult(metadata_changed=metadata_before != self._metadata_signature())

    def apply_scenes(self, message: Mapping[str, Any]) -> StateResult:
        metadata_before = self._metadata_signature()
        self.raw.apply_scenes(message)
        return StateResult(metadata_changed=metadata_before != self._metadata_signature())

    def apply_message(self, message: Mapping[str, Any]) -> StateResult:
        method = message.get("method")
        if method == GatewayMethod.POST_TOPOLOGY:
            _topology, result = self.apply_topology(message, replace_existing=False)
            return result
        if method == GatewayMethod.POST_PROP:
            return self.apply_properties(message)
        return StateResult()

    def pending_node_ids(self, batch_id: int, *, unresolved_only: bool = True) -> tuple[NodeId, ...]:
        batch = self._batches.get(batch_id)
        if batch is None:
            return ()
        keys = batch.targets.keys() - batch.observed if unresolved_only else batch.targets.keys()
        node_keys = {node_key for node_key, _prop in keys if self._owner.get((node_key, _prop)) == batch_id}
        return tuple(batch.node_ids[node_key] for node_key in sorted(node_keys))

    def has_pending(self, node_id: NodeId, props: Iterable[str] | None = None) -> bool:
        node_key = _node_key(node_id)
        if props is None:
            return any(owner_node == node_key for owner_node, _prop in self._owner)
        return any((node_key, prop) in self._owner for prop in props)

    def next_deadline(self) -> float | None:
        return min((batch.deadline for batch in self._batches.values()), default=None)

    def diagnostics(self, *, now: float) -> dict[str, Any]:
        return {
            "active_batches": len(self._batches),
            "accepted_batches": sum(batch.accepted for batch in self._batches.values()),
            "pending_nodes": len({node_key for node_key, _prop in self._owner}),
            "pending_properties": len(self._owner),
            "observed_properties": sum(len(batch.observed) for batch in self._batches.values()),
            "next_deadline_in": min(
                (round(batch.deadline - now, 3) for batch in self._batches.values()),
                default=None,
            ),
        }

    def full_property_coverage(self, message: Mapping[str, Any]) -> bool:
        return self.raw.full_property_coverage(message)

    def unknown_summary(self) -> dict[str, Any]:
        return self.raw.unknown_summary()

    def room_name(self, room_id: NodeId | None) -> str | None:
        return self.raw.room_name(room_id)

    def room_id_for_node(self, node: TopologyNode) -> NodeId | None:
        return self.raw.room_id_for_node(node)

    def room_name_for_node(self, node: TopologyNode) -> str | None:
        return self.raw.room_name_for_node(node)

    def _mark_observed_targets(
        self,
        explicit_by_node: Mapping[str, Mapping[str, Any]],
        *,
        match_batch_id: int | None,
    ) -> None:
        for node_key, props in explicit_by_node.items():
            for prop, value in props.items():
                key = (node_key, prop)
                batch_id = self._owner.get(key)
                if batch_id is None or (match_batch_id is not None and match_batch_id != batch_id):
                    continue
                batch = self._batches.get(batch_id)
                if batch is not None and batch.targets.get(key) == value:
                    batch.observed.add(key)

    def _batch_ids_for(self, explicit_by_node: Mapping[str, Mapping[str, Any]]) -> set[int]:
        return {
            batch_id
            for node_key, props in explicit_by_node.items()
            for prop in props
            if (batch_id := self._owner.get((node_key, prop))) is not None
        }

    def _release_ready_batches(self, batch_ids: Iterable[int]) -> StateResult:
        ready: list[int] = []
        for batch_id in set(batch_ids):
            batch = self._batches.get(batch_id)
            if batch is not None and batch.accepted and batch.targets.keys() <= batch.observed:
                ready.append(batch_id)
        return self._end_batches(tuple(sorted(ready)))

    def _end_batches(self, batch_ids: Iterable[int]) -> StateResult:
        touched_node_keys: set[str] = set()
        ended: list[int] = []
        for batch_id in batch_ids:
            batch = self._batches.pop(batch_id, None)
            if batch is None:
                continue
            ended.append(batch_id)
            for key in batch.targets:
                if self._owner.get(key) == batch_id:
                    self._owner.pop(key, None)
                    touched_node_keys.add(key[0])
        changed = self._reproject_nodes(touched_node_keys)
        return StateResult(
            changed_node_ids=frozenset(changed),
            ended_batch_ids=tuple(sorted(ended)),
        )

    def _reproject_nodes(self, node_ids: Iterable[NodeId]) -> set[NodeId]:
        changed: set[NodeId] = set()
        for requested_id in node_ids:
            raw_node = _mapping_node(self.raw.nodes, requested_id)
            if raw_node is None:
                continue
            visible = _mapping_node(self.visible_nodes, raw_node.id)
            projected = self._project_node(raw_node, visible)
            if visible == projected:
                continue
            _set_mapping_node(self.visible_nodes, projected)
            changed.add(projected.id)
        return changed

    def _project_node(self, raw_node: TopologyNode, visible: TopologyNode | None) -> TopologyNode:
        params = dict(raw_node.params)
        node_key = _node_key(raw_node.id)
        held_props = (prop for (owner_node, prop), _batch_id in self._owner.items() if owner_node == node_key)
        for prop in held_props:
            if visible is not None and prop in visible.params:
                params[prop] = visible.params[prop]
            else:
                params.pop(prop, None)
        return replace(raw_node, params=params)

    def _metadata_signature(self) -> tuple[Any, ...]:
        return (
            _mapping_signature(self.raw.groups),
            _mapping_signature(self.raw.rooms),
            _mapping_signature(self.raw.scenes),
        )


def _merge_results(*results: StateResult) -> StateResult:
    changed: set[NodeId] = set()
    ended: set[int] = set()
    metadata_changed = False
    for result in results:
        changed.update(result.changed_node_ids)
        ended.update(result.ended_batch_ids)
        metadata_changed = metadata_changed or result.metadata_changed
    return StateResult(
        changed_node_ids=frozenset(changed),
        metadata_changed=metadata_changed,
        ended_batch_ids=tuple(sorted(ended)),
    )


def _explicit_params_by_node(items: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        node_id = _item_id(item)
        params = item.get("params")
        if node_id is not None and isinstance(params, Mapping):
            result.setdefault(_node_key(node_id), {}).update(params)
    return result


def _item_id(item: Mapping[str, Any]) -> NodeId | None:
    value = item.get("id")
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        return value
    return None


def _node_key(node_id: NodeId) -> str:
    return str(node_id)


def _mapping_node(mapping: Mapping[NodeId, TopologyNode], node_id: NodeId) -> TopologyNode | None:
    direct = mapping.get(node_id)
    if direct is not None:
        return direct
    wanted = _node_key(node_id)
    return next((node for key, node in mapping.items() if _node_key(key) == wanted), None)


def _set_mapping_node(mapping: dict[NodeId, TopologyNode], node: TopologyNode) -> None:
    wanted = _node_key(node.id)
    for key in tuple(mapping):
        if key != node.id and _node_key(key) == wanted:
            mapping.pop(key)
    mapping[node.id] = node


def _mapping_signature(mapping: Mapping[NodeId, Mapping[str, Any]]) -> tuple[Any, ...]:
    return tuple(sorted((str(key), _freeze(_metadata_mapping(value))) for key, value in mapping.items()))


def _metadata_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key not in {"o", "params"}}


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _freeze(item)) for key, item in value.items()))
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value
