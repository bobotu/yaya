from __future__ import annotations

import logging
from collections.abc import Callable, Iterable

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import YeelightProCoordinator
from .core import TopologyNode
from .entity import YeelightProEntity

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
        _async_remove_stale_node_devices(entry, coordinator)
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
        registry.async_remove(registry_entry.entity_id)
        removed_unique_ids.add(registry_entry.unique_id)
    return removed_unique_ids


@callback
def _async_remove_stale_node_devices(entry: ConfigEntry, coordinator: YeelightProCoordinator) -> None:
    device_registry = dr.async_get(coordinator.hass)
    entity_registry = er.async_get(coordinator.hass)
    expected_identifiers = {coordinator.node_identifier(node.id) for node in coordinator.nodes()}
    node_identifier_prefix = f"{coordinator.gateway_id}:"

    for device in dr.async_entries_for_config_entry(device_registry, entry.entry_id):
        if coordinator.gateway_identifier() in device.identifiers:
            continue
        yeelight_identifiers = {
            value
            for domain, value in device.identifiers
            if domain == DOMAIN and value.startswith(node_identifier_prefix)
        }
        if not yeelight_identifiers or any((DOMAIN, value) in expected_identifiers for value in yeelight_identifiers):
            continue
        if any(
            registry_entry.config_entry_id == entry.entry_id and registry_entry.platform == DOMAIN
            for registry_entry in er.async_entries_for_device(entity_registry, device.id)
        ):
            continue
        device_registry.async_remove_device(device.id)


def _obsolete_unique_id_suffix(unique_id: str) -> bool:
    suffix = unique_id.rsplit("_", 1)[-1]
    return suffix.endswith("-acd")
