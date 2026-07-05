from __future__ import annotations

import logging
from collections.abc import Callable, Iterable

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import YeelightProCoordinator
from .core import TopologyNode
from .entity import YeelightProEntity
from .helpers import node_key

EntityFactory = Callable[[TopologyNode], Iterable[YeelightProEntity]]
_LOGGER = logging.getLogger(__name__)


def async_remove_obsolete_entities(entry: ConfigEntry, coordinator: YeelightProCoordinator) -> None:
    """Remove registry entries for entities no longer exposed by this integration."""

    registry = er.async_get(coordinator.hass)
    gateway_unique_id_prefix = f"{coordinator.gateway_id}_"
    removed = 0
    for registry_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if registry_entry.platform != DOMAIN:
            continue
        if not registry_entry.unique_id.startswith(gateway_unique_id_prefix):
            continue
        if _obsolete_unique_id_suffix(registry_entry.unique_id):
            registry.async_remove(registry_entry.entity_id)
            removed += 1
    if removed:
        _LOGGER.debug("Removed %d obsolete Yeelight Pro entity registry entries", removed)


def async_add_dynamic_entities(
    entry: ConfigEntry,
    coordinator: YeelightProCoordinator,
    async_add_entities: AddEntitiesCallback,
    entity_factory: EntityFactory,
    platform: str,
) -> None:
    known_unique_ids: set[str] = set()

    @callback
    def _async_add_new_entities() -> None:
        entities: list[YeelightProEntity] = []
        desired_unique_ids: set[str] = set()
        for node in coordinator.nodes():
            for entity in entity_factory(node):
                desired_unique_ids.add(entity.unique_id)
                if entity.unique_id in known_unique_ids:
                    continue
                known_unique_ids.add(entity.unique_id)
                entities.append(entity)
        removed_unique_ids = _async_remove_stale_platform_entities(entry, coordinator, platform, desired_unique_ids)
        known_unique_ids.difference_update(removed_unique_ids)
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
        if _unique_id_node_key(registry_entry.unique_id, gateway_unique_id_prefix, current_node_keys) is None:
            continue
        registry.async_remove(registry_entry.entity_id)
        removed_unique_ids.add(registry_entry.unique_id)
    return removed_unique_ids


def _obsolete_unique_id_suffix(unique_id: str) -> bool:
    suffix = unique_id.rsplit("_", 1)[-1]
    return suffix.endswith("-acd")


def _unique_id_node_key(
    unique_id: str,
    gateway_unique_id_prefix: str,
    known_node_keys: set[str],
) -> str | None:
    suffix = unique_id.removeprefix(gateway_unique_id_prefix)
    for known_node_key in sorted(known_node_keys, key=len, reverse=True):
        if suffix == known_node_key or suffix.startswith(f"{known_node_key}_"):
            return known_node_key
    return None
