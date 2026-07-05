from __future__ import annotations

import pytest

pytest.importorskip("homeassistant")
pytest.importorskip("pytest_homeassistant_custom_component")

from custom_components.yeelight_pro.core import (
    ConnectionClosed,
    ProtocolError,
    RequestTimeout,
    TopologyNode,
    YeelightProError,
)
from custom_components.yeelight_pro.entity import (
    YeelightProGatewayTimeoutError,
    YeelightProGatewayUnavailableError,
    YeelightProInvalidCommandError,
    YeelightProNodeUnavailableError,
    YeelightProProtocolActionError,
    YeelightProUnknownActionError,
    async_call_gateway,
    require_node_for_action,
)


async def _raise(exc: Exception) -> None:
    raise exc


@pytest.mark.parametrize(
    ("source_error", "ha_error", "translation_key"),
    [
        (ValueError("bad range"), YeelightProInvalidCommandError, "invalid_gateway_command"),
        (RequestTimeout("request timed out"), YeelightProGatewayTimeoutError, "gateway_request_timeout"),
        (ConnectionClosed("connection closed"), YeelightProGatewayUnavailableError, "gateway_unavailable"),
        (ProtocolError("invalid response"), YeelightProProtocolActionError, "gateway_protocol_error"),
        (YeelightProError("unknown"), YeelightProUnknownActionError, "gateway_action_failed"),
    ],
)
async def test_gateway_action_errors_are_mapped_by_type(
    source_error: Exception,
    ha_error: type[Exception],
    translation_key: str,
) -> None:
    with pytest.raises(ha_error) as err:
        await async_call_gateway(_raise(source_error))

    assert err.value.translation_key == translation_key


def test_require_node_for_action_returns_available_node() -> None:
    node = TopologyNode.from_mapping({"id": "light-1", "nt": 2, "type": 3})

    assert require_node_for_action(node, "light-1") is node


@pytest.mark.parametrize(
    "node",
    [
        None,
        TopologyNode.from_mapping({"id": "light-1", "nt": 2, "type": 3, "o": False}),
    ],
)
def test_require_node_for_action_rejects_unavailable_node(node: TopologyNode | None) -> None:
    with pytest.raises(YeelightProNodeUnavailableError) as err:
        require_node_for_action(node, "light-1")

    assert err.value.translation_key == "node_unavailable"
    assert err.value.translation_placeholders == {"node_id": "light-1"}
