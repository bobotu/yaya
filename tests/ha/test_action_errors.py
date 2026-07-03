from __future__ import annotations

import pytest

pytest.importorskip("homeassistant")
pytest.importorskip("pytest_homeassistant_custom_component")

from custom_components.yeelight_pro.core import ConnectionClosed, ProtocolError, RequestTimeout, YeelightProError
from custom_components.yeelight_pro.entity import (
    YeelightProGatewayTimeoutError,
    YeelightProGatewayUnavailableError,
    YeelightProInvalidCommandError,
    YeelightProProtocolActionError,
    YeelightProUnknownActionError,
    async_call_gateway,
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
