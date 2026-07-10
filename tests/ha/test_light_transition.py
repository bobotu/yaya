from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("homeassistant")
pytest.importorskip("pytest_homeassistant_custom_component")

from homeassistant.components.light import ATTR_TRANSITION

from custom_components.yeelight_pro.const import DEFAULT_LIGHT_TRANSITION
from custom_components.yeelight_pro.core import DeviceType, NodeType, TopologyNode
from custom_components.yeelight_pro.light import YeelightProLight


def _light(default_transition: float = DEFAULT_LIGHT_TRANSITION) -> tuple[YeelightProLight, TopologyNode]:
    node = TopologyNode(
        id="light-1",
        nt=NodeType.MESH_SUBDEVICE,
        type=DeviceType.LIGHT_BRIGHTNESS,
        params={"p": False},
    )
    coordinator = SimpleNamespace(
        default_light_transition=default_transition,
        gateway=object(),
        gateway_id="192.0.2.10:65443",
        node=lambda node_id: node if node_id == node.id else None,
    )
    return YeelightProLight(coordinator, node), node  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("default_transition", "method_name", "expected_props", "expected_duration"),
    [
        (DEFAULT_LIGHT_TRANSITION, "async_turn_on", {"p": True}, 500),
        (2.0, "async_turn_off", {"p": False}, 2000),
    ],
)
async def test_light_uses_default_transition(
    default_transition: float,
    method_name: str,
    expected_props: dict[str, bool],
    expected_duration: int,
) -> None:
    light, node = _light(default_transition)

    with patch("custom_components.yeelight_pro.light.async_set_node_props", new=AsyncMock()) as set_props:
        await getattr(light, method_name)()

    set_props.assert_awaited_once_with(
        light.coordinator,
        node,
        expected_props,
        duration=expected_duration,
    )


async def test_explicit_transition_overrides_default_including_zero() -> None:
    light, node = _light(2.0)

    with patch("custom_components.yeelight_pro.light.async_set_node_props", new=AsyncMock()) as set_props:
        await light.async_turn_on(**{ATTR_TRANSITION: 0})

    set_props.assert_awaited_once_with(
        light.coordinator,
        node,
        {"p": True},
        duration=0,
    )
