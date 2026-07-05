from __future__ import annotations

from typing import Any

from homeassistant.components.cover import (
    ATTR_POSITION,
    ATTR_TILT_POSITION,
    CoverDeviceClass,
    CoverEntity,
    CoverEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import YeelightProCoordinator
from .core import is_dream_curtain
from .core.devices import CurtainDevice, DreamCurtainDevice, curtain_position_known, curtain_tilt_position_known
from .entity import YeelightProEntity, async_call_gateway
from .helpers import is_cover_node
from .platform import async_add_dynamic_entities
from .session.model import (
    MOTOR_MOTION_CLOSING,
    MOTOR_MOTION_OPENING,
    MOTOR_TRACKING_ANGLE_MOTION,
    MOTOR_TRACKING_ASSUMED,
    MOTOR_TRACKING_POSITION_MOTION,
    MOTOR_TRACKING_TARGET_ANGLE,
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
        if node is not None and _has_tilt(node):
            features |= CoverEntityFeature.OPEN_TILT | CoverEntityFeature.CLOSE_TILT | CoverEntityFeature.STOP_TILT
            if curtain_tilt_position_known(node):
                features |= CoverEntityFeature.SET_TILT_POSITION
        return features

    @property
    def current_cover_position(self) -> int | None:
        node = self.node
        if node is None or not curtain_position_known(node):
            return None
        return _int_param(node, "cp")

    @property
    def target_cover_position(self) -> int | None:
        node = self.node
        if node is None or not curtain_position_known(node):
            return None
        return _int_param(node, MOTOR_TRACKING_TARGET_POSITION)

    @property
    def is_opening(self) -> bool | None:
        motion = _str_param(self.node, MOTOR_TRACKING_POSITION_MOTION) or _str_param(
            self.node, MOTOR_TRACKING_ANGLE_MOTION
        )
        return None if motion is None else motion == MOTOR_MOTION_OPENING

    @property
    def is_closing(self) -> bool | None:
        motion = _str_param(self.node, MOTOR_TRACKING_POSITION_MOTION) or _str_param(
            self.node, MOTOR_TRACKING_ANGLE_MOTION
        )
        return None if motion is None else motion == MOTOR_MOTION_CLOSING

    @property
    def assumed_state(self) -> bool:
        node = self.node
        if node is not None and _has_unknown_route_position(node):
            return True
        return _bool_param(node, MOTOR_TRACKING_ASSUMED) or super().assumed_state

    @property
    def is_closed(self) -> bool | None:
        position = self.current_cover_position
        return None if position is None else position == 0

    @property
    def current_cover_tilt_position(self) -> int | None:
        node = self.node
        if node is None or not curtain_tilt_position_known(node):
            return None
        angle = _int_param(node, "cra")
        return None if angle is None else _angle_to_tilt(angle)

    @property
    def target_cover_tilt_position(self) -> int | None:
        node = self.node
        if node is None or not curtain_tilt_position_known(node):
            return None
        angle = _int_param(node, MOTOR_TRACKING_TARGET_ANGLE)
        return None if angle is None else _angle_to_tilt(angle)

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

    async def async_open_cover_tilt(self, **kwargs: Any) -> None:
        await self._async_set_tilt_angle(180)

    async def async_close_cover_tilt(self, **kwargs: Any) -> None:
        await self._async_set_tilt_angle(0)

    async def async_stop_cover_tilt(self, **kwargs: Any) -> None:
        node = self.require_current_node()
        await async_call_gateway(DreamCurtainDevice(node, self.coordinator.gateway).stop_tilt())

    async def async_set_cover_tilt_position(self, **kwargs: Any) -> None:
        await self._async_set_tilt_angle(_tilt_to_angle(kwargs[ATTR_TILT_POSITION]))

    async def _async_set_tilt_angle(self, angle: int) -> None:
        node = self.require_current_node()
        await async_call_gateway(DreamCurtainDevice(node, self.coordinator.gateway).set_angle(angle))


def _has_tilt(node: Any) -> bool:
    return is_dream_curtain(node) or any(key in node.params for key in ("cra", "tra", "trs"))


def _has_unknown_route_position(node: Any) -> bool:
    return not curtain_position_known(node) or (_has_tilt(node) and not curtain_tilt_position_known(node))


def _int_param(node: Any, key: str) -> int | None:
    if node is None:
        return None
    value = node.params.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _str_param(node: Any, key: str) -> str | None:
    if node is None:
        return None
    value = node.params.get(key)
    return value if isinstance(value, str) else None


def _bool_param(node: Any, key: str) -> bool:
    if node is None:
        return False
    return node.params.get(key) is True


def _angle_to_tilt(angle: int) -> int:
    return max(0, min(100, round(angle * 100 / 180)))


def _tilt_to_angle(tilt: int) -> int:
    return max(0, min(180, round(tilt * 180 / 100)))
