from __future__ import annotations

from collections.abc import Awaitable, Iterable, Mapping
from typing import Any, TypeVar

from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import YeelightProCoordinator
from .core import ConnectionClosed, ProtocolError, RequestTimeout, TopologyNode, YeelightProError
from .helpers import node_unique_id


class YeelightProEntity(CoordinatorEntity[YeelightProCoordinator]):
    _attr_has_entity_name = True

    def __init__(self, coordinator: YeelightProCoordinator, node: TopologyNode, suffix: str) -> None:
        super().__init__(coordinator)
        self._node_id = node.id
        self._suffix = suffix
        self._attr_unique_id = node_unique_id(coordinator.gateway_id, node.id, suffix)

    @property
    def node(self) -> TopologyNode | None:
        return self.coordinator.node(self._node_id)

    @property
    def available(self) -> bool:
        node = self.node
        return super().available and node is not None and node.online is not False

    @property
    def device_info(self) -> DeviceInfo:
        node = self.node
        name = node.name if node is not None and node.name else f"Yeelight Pro {self._node_id}"
        info: dict[str, Any] = {
            "identifiers": {self.coordinator.node_identifier(self._node_id)},
            "manufacturer": "Yeelight",
            "hw_version": None,
            "model": self.coordinator.device_model_name(node) if node is not None else None,
            "model_id": None,
            "name": name,
            "serial_number": str(self._node_id),
            "via_device": self.coordinator.gateway_identifier(),
        }
        return info

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        node = self.node
        if node is None:
            return {}
        return {
            "node_id": str(node.id),
            "node_type": node.nt,
            "device_type": node.type,
            "property_type": node.property_type,
        }

    @property
    def assumed_state(self) -> bool:
        return self.coordinator.gateway.has_pending_overlay(self._node_id, self.optimistic_properties)

    @property
    def optimistic_properties(self) -> Iterable[str] | None:
        return ()


def base_entity_name(node: TopologyNode) -> str:
    return node.name or f"Yeelight Pro {node.id}"


_T = TypeVar("_T")


class YeelightProActionError(HomeAssistantError):
    """Base Home Assistant action error for Yeelight Pro service calls."""


class YeelightProGatewayUnavailableError(YeelightProActionError):
    """The gateway connection is not currently available."""


class YeelightProGatewayTimeoutError(YeelightProActionError):
    """The gateway did not answer an RPC request before timeout."""


class YeelightProProtocolActionError(YeelightProActionError):
    """The gateway returned data that did not match the expected protocol."""


class YeelightProUnknownActionError(YeelightProActionError):
    """A gateway action failed for an unclassified client error."""


class YeelightProInvalidCommandError(ServiceValidationError):
    """The requested command is invalid for the target device."""


async def async_call_gateway(action: Awaitable[_T]) -> _T:
    try:
        return await action
    except ValueError as exc:
        raise YeelightProInvalidCommandError(
            translation_domain=DOMAIN,
            translation_key="invalid_gateway_command",
            translation_placeholders={"error": str(exc)},
        ) from exc
    except RequestTimeout as exc:
        raise YeelightProGatewayTimeoutError(
            translation_domain=DOMAIN,
            translation_key="gateway_request_timeout",
        ) from exc
    except ConnectionClosed as exc:
        raise YeelightProGatewayUnavailableError(
            translation_domain=DOMAIN,
            translation_key="gateway_unavailable",
        ) from exc
    except ProtocolError as exc:
        raise YeelightProProtocolActionError(
            translation_domain=DOMAIN,
            translation_key="gateway_protocol_error",
        ) from exc
    except YeelightProError as exc:
        raise YeelightProUnknownActionError(
            translation_domain=DOMAIN,
            translation_key="gateway_action_failed",
        ) from exc


async def async_set_node_props(
    coordinator: YeelightProCoordinator,
    node: TopologyNode,
    props: Mapping[str, Any],
    *,
    duration: int | None = None,
    optimistic: bool = True,
) -> dict[str, Any]:
    optimistic_props = props if optimistic else None
    return await async_call_gateway(
        coordinator.gateway.set_node_props(
            node.id,
            props,
            nt=node.nt,
            duration=duration,
            optimistic_props=optimistic_props,
        )
    )
