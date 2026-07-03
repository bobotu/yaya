from __future__ import annotations

from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .core.devices import DoubleSwitchDevice, MultiSwitchDevice
from .core.topology import DeviceType
from .entity import YeelightProEntity, async_call_gateway
from .helpers import (
    device_type,
    is_double_switch_node,
    is_multi_switch_node,
    relay_channel_numbers,
    relay_prop_name,
)
from .platform import async_add_dynamic_entities


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: YeelightProCoordinator = entry.runtime_data
    async_add_dynamic_entities(
        entry,
        coordinator,
        async_add_entities,
        lambda node: _switch_entities_for_node(coordinator, node),
        "switch",
    )


def _switch_entities_for_node(coordinator: YeelightProCoordinator, node: Any) -> list[YeelightProEntity]:
    entities: list[YeelightProEntity] = [
        YeelightProRelaySwitch(coordinator, node, channel)
        for channel in relay_channel_numbers(node)
        if (is_multi_switch_node(node) or is_double_switch_node(node))
        and coordinator.exposes_relay_switches_for_node(node)
    ]
    if "0-blp" in node.params:
        entities.append(YeelightProPropertySwitch(coordinator, node, "0-blp", "backlight"))
    if device_type(node) in {DeviceType.AIR_CONDITION, DeviceType.AIR_CONDITION_VRF}:
        for key in _indexed_props(node, "acrc"):
            entities.append(YeelightProPropertySwitch(coordinator, node, key, "remote_controller"))
    return entities


class YeelightProRelaySwitch(YeelightProEntity, SwitchEntity):
    _attr_icon = "mdi:light-switch"

    def __init__(self, coordinator: YeelightProCoordinator, node: Any, channel: int) -> None:
        super().__init__(coordinator, node, f"relay_{channel}")
        self._channel = channel
        self._attr_translation_key = "relay"
        self._attr_translation_placeholders = {"channel": str(channel)}

    @property
    def is_on(self) -> bool | None:
        node = self.node
        if node is None:
            return None
        value = node.params.get(relay_prop_name(node, self._channel))
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs = super().extra_state_attributes
        node = self.node
        if node is None:
            return attrs
        prop = relay_prop_name(node, self._channel)
        return {
            **attrs,
            "channel": self._channel,
            "relay_property": prop,
            "relay_property_present": prop in node.params,
        }

    async def async_turn_on(self, **kwargs: Any) -> None:
        await self._async_set_channel(True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        await self._async_set_channel(False)

    async def _async_set_channel(self, is_on: bool) -> None:
        node = self.node
        if node is None:
            return
        if is_double_switch_node(node):
            await async_call_gateway(
                DoubleSwitchDevice(node, self.coordinator.gateway).set_channel(self._channel, is_on)
            )
        else:
            await async_call_gateway(
                MultiSwitchDevice(node, self.coordinator.gateway).set_channel(self._channel, is_on)
            )
        await async_call_gateway(self.coordinator.async_refresh_node(node.id))


class YeelightProPropertySwitch(YeelightProEntity, SwitchEntity):
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator: YeelightProCoordinator,
        node: Any,
        prop: str,
        translation_key: str,
    ) -> None:
        super().__init__(coordinator, node, prop)
        self._prop = prop
        self._attr_translation_key = translation_key

    @property
    def is_on(self) -> bool | None:
        node = self.node
        if node is None:
            return None
        value = node.params.get(self._prop)
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        return None

    async def async_turn_on(self, **kwargs: Any) -> None:
        await self._async_set_value(True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        await self._async_set_value(False)

    async def _async_set_value(self, value: bool) -> None:
        node = self.node
        if node is None:
            return
        await async_call_gateway(self.coordinator.gateway.set_node_props(node.id, {self._prop: value}, nt=node.nt))
        await async_call_gateway(self.coordinator.async_refresh_node(node.id))


def _indexed_props(node: Any, suffix: str) -> tuple[str, ...]:
    props = []
    for key in node.params:
        if isinstance(key, str) and key.endswith(f"-{suffix}") and key.split("-", 1)[0].isdigit():
            props.append(key)
    return tuple(sorted(props, key=lambda item: int(item.split("-", 1)[0])))
