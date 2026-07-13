from __future__ import annotations

from typing import Any

from homeassistant.components.cover import (
    ATTR_POSITION,
    CoverDeviceClass,
    CoverEntity,
    CoverEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .entity import YeelightProEntity, async_call_gateway
from .gateway.devices import CurtainDevice, curtain_position_known
from .helpers import int_param, is_cover_node, true_bool_param
from .platform import async_add_dynamic_entities
from .session.motor import (
    MOTOR_MOTION_CLOSING,
    MOTOR_MOTION_OPENING,
    MOTOR_TRACKING_ASSUMED,
    MOTOR_TRACKING_POSITION_MOTION,
    MOTOR_TRACKING_TARGET_POSITION,
)

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
        lambda node: [YeelightProCover(coordinator, node)] if is_cover_node(node) else [],
        "cover",
    )


class YeelightProCover(YeelightProEntity, CoverEntity):
    _attr_device_class = CoverDeviceClass.CURTAIN

    def __init__(self, coordinator: YeelightProCoordinator, node: Any) -> None:
        super().__init__(coordinator, node, "cover")
        self._attr_name = None

    @property
    def supported_features(self) -> CoverEntityFeature:
        features = CoverEntityFeature.OPEN | CoverEntityFeature.CLOSE | CoverEntityFeature.STOP
        node = self.node
        if node is None or curtain_position_known(node):
            features |= CoverEntityFeature.SET_POSITION
        return features

    @property
    def current_cover_position(self) -> int | None:
        node = self.node
        if node is None or not curtain_position_known(node):
            return None
        return int_param(node, "cp")

    @property
    def target_cover_position(self) -> int | None:
        node = self.node
        if node is None or not curtain_position_known(node):
            return None
        return int_param(node, MOTOR_TRACKING_TARGET_POSITION)

    @property
    def is_opening(self) -> bool | None:
        motion = _str_param(self.node, MOTOR_TRACKING_POSITION_MOTION)
        return None if motion is None else motion == MOTOR_MOTION_OPENING

    @property
    def is_closing(self) -> bool | None:
        motion = _str_param(self.node, MOTOR_TRACKING_POSITION_MOTION)
        return None if motion is None else motion == MOTOR_MOTION_CLOSING

    @property
    def assumed_state(self) -> bool:
        node = self.node
        if node is not None and not curtain_position_known(node):
            return True
        return true_bool_param(node, MOTOR_TRACKING_ASSUMED) or super().assumed_state

    @property
    def is_closed(self) -> bool | None:
        position = self.current_cover_position
        return None if position is None else position == 0

    async def async_open_cover(self, **kwargs: Any) -> None:
        node = self.require_current_node()
        await async_call_gateway(CurtainDevice(node, self.coordinator.gateway).open())

    async def async_close_cover(self, **kwargs: Any) -> None:
        node = self.require_current_node()
        await async_call_gateway(CurtainDevice(node, self.coordinator.gateway).close())

    async def async_stop_cover(self, **kwargs: Any) -> None:
        node = self.require_current_node()
        await async_call_gateway(CurtainDevice(node, self.coordinator.gateway).stop())

    async def async_set_cover_position(self, **kwargs: Any) -> None:
        node = self.require_current_node()
        await async_call_gateway(CurtainDevice(node, self.coordinator.gateway).set_position(kwargs[ATTR_POSITION]))


def _str_param(node: Any, key: str) -> str | None:
    if node is None:
        return None
    value = node.params.get(key)
    return value if isinstance(value, str) else None
