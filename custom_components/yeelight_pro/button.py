from __future__ import annotations

from typing import Any

from homeassistant.components.button import ButtonDeviceClass, ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import YeelightProCoordinator
from .core.devices import LightDevice
from .entity import YeelightProEntity, YeelightProGatewayUnavailableError, async_call_gateway
from .helpers import light_device_type
from .platform import async_add_dynamic_entities

PARALLEL_UPDATES = 1


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
        lambda node: [YeelightProIdentifyButton(coordinator, node)] if light_device_type(node) is not None else [],
        "button",
    )


class YeelightProIdentifyButton(YeelightProEntity, ButtonEntity):
    _attr_device_class = ButtonDeviceClass.IDENTIFY
    _attr_translation_key = "identify"

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "identify")

    async def async_press(self) -> None:
        node = self.node
        if node is None:
            raise YeelightProGatewayUnavailableError(
                translation_domain=DOMAIN,
                translation_key="gateway_unavailable",
            )
        device = LightDevice(node, self.coordinator.gateway)
        await async_call_gateway(device.blink())
