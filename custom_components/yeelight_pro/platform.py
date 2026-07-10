from __future__ import annotations

from collections.abc import Callable, Iterable

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import YeelightProCoordinator
from .entity import YeelightProEntity
from .gateway import TopologyNode
from .helpers import node_key

EntityFactory = Callable[[TopologyNode], Iterable[YeelightProEntity]]
StaleUniqueIdFactory = Callable[[TopologyNode], Iterable[str]]


def async_add_dynamic_entities(
    entry: ConfigEntry,
    coordinator: YeelightProCoordinator,
    async_add_entities: AddEntitiesCallback,
    entity_factory: EntityFactory,
    platform: str,
    stale_unique_id_factory: StaleUniqueIdFactory | None = None,
) -> None:
    known_unique_ids: set[str] = set()
    known_unique_id_node_keys: dict[str, str] = {}
    last_signature: tuple[object, ...] | None = None

    @callback
    def _async_add_new_entities() -> None:
        nonlocal last_signature
        nodes = coordinator.nodes()
        signature = _dynamic_entity_signature(nodes)
        if signature == last_signature:
            return
        last_signature = signature

        entities: list[YeelightProEntity] = []
        desired_unique_ids: set[str] = set()
        unique_id_node_keys: dict[str, str] = {}
        for node in nodes:
            current_node_key = node_key(node.id)
            for entity in entity_factory(node):
                desired_unique_ids.add(entity.unique_id)
                unique_id_node_keys[entity.unique_id] = current_node_key
                if entity.unique_id in known_unique_ids:
                    continue
                known_unique_ids.add(entity.unique_id)
                entities.append(entity)
            if stale_unique_id_factory is not None:
                for unique_id in stale_unique_id_factory(node):
                    unique_id_node_keys[unique_id] = current_node_key
        known_unique_id_node_keys.update(unique_id_node_keys)
        removed_unique_ids = _async_remove_stale_platform_entities(
            entry,
            coordinator,
            platform,
            desired_unique_ids,
            known_unique_id_node_keys,
        )
        known_unique_ids.difference_update(removed_unique_ids)
        for unique_id in removed_unique_ids:
            known_unique_id_node_keys.pop(unique_id, None)
        if entities:
            async_add_entities(entities)

    _async_add_new_entities()
    entry.async_on_unload(coordinator.async_add_listener(_async_add_new_entities))


@callback
def _async_remove_stale_platform_entities(
    entry: ConfigEntry,
    coordinator: YeelightProCoordinator,
    platform: str,
    desired_unique_ids: set[str],
    unique_id_node_keys: dict[str, str],
) -> set[str]:
    registry = er.async_get(coordinator.hass)
    gateway_unique_id_prefix = f"{coordinator.gateway_id}_"
    current_node_keys = {node_key(node.id) for node in coordinator.nodes()}
    removed_unique_ids: set[str] = set()
    for registry_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if registry_entry.platform != DOMAIN:
            continue
        if registry_entry.entity_id.split(".", 1)[0] != platform:
            continue
        if not registry_entry.unique_id.startswith(gateway_unique_id_prefix):
            continue
        if registry_entry.unique_id in desired_unique_ids:
            continue
        entry_node_key = unique_id_node_keys.get(registry_entry.unique_id)
        if entry_node_key not in current_node_keys:
            continue
        registry.async_remove(registry_entry.entity_id)
        removed_unique_ids.add(registry_entry.unique_id)
    return removed_unique_ids


def _dynamic_entity_signature(nodes: Iterable[TopologyNode]) -> tuple[object, ...]:
    return tuple(
        (
            node_key(node.id),
            node.nt,
            node.type,
            node.property_type,
            node.channel_count,
            node.component_type_ids,
            tuple(sorted(str(key) for key in node.params)),
        )
        for node in nodes
    )
