from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import selector

from .const import (
    CONF_IMPORT_ROOM_IDS,
    CONF_INCLUDE_LIGHT_GROUPS,
    CONF_SWITCH_MODES,
    CONF_WIRELESS_SWITCH_NODE_IDS,
    DEFAULT_PORT,
    DEFAULT_REQUEST_TIMEOUT,
    DOMAIN,
    SWITCH_MODE_RELAY,
    SWITCH_MODE_WIRELESS,
)
from .core import TopologyNode, YeelightProError
from .helpers import is_switch_mode_configurable_node, node_key
from .session import YeelightProGateway


class CannotConnect(Exception):
    pass


@dataclass(frozen=True)
class GatewayOptions:
    room_options: list[selector.SelectOptionDict]
    switch_options: list[selector.SelectOptionDict]


class YeelightProConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    _host: str
    _port: int
    _gateway_options: GatewayOptions

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry) -> config_entries.OptionsFlow:
        return YeelightProOptionsFlow()

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            host = user_input[CONF_HOST]
            port = user_input.get(CONF_PORT, DEFAULT_PORT)
            try:
                gateway_options = await _async_validate_gateway(host, port)
            except CannotConnect:
                errors["base"] = "cannot_connect"
            else:
                await self.async_set_unique_id(f"{host}:{port}")
                self._abort_if_unique_id_configured()
                self._host = host
                self._port = port
                self._gateway_options = gateway_options
                return await self.async_step_import_filter()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_HOST): str,
                    vol.Optional(CONF_PORT, default=DEFAULT_PORT): int,
                }
            ),
            errors=errors,
        )

    async def async_step_import_filter(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if user_input is not None:
            return self.async_create_entry(
                title=self._host,
                data={
                    CONF_HOST: self._host,
                    CONF_PORT: self._port,
                },
                options=_options_data(user_input, switch_options=self._gateway_options.switch_options),
            )

        return self.async_show_form(
            step_id="import_filter",
            data_schema=_import_filter_schema(
                include_light_groups=False,
                import_room_ids=_all_room_option_values(self._gateway_options.room_options),
                room_options=self._gateway_options.room_options,
                wireless_switch_node_ids=[],
                switch_options=self._gateway_options.switch_options,
            ),
        )


class YeelightProOptionsFlow(config_entries.OptionsFlow):
    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if user_input is not None:
            switch_options = getattr(
                self, "_switch_options", _selected_switch_options(self.config_entry.options.get(CONF_SWITCH_MODES, {}))
            )
            return self.async_create_entry(title="", data=_options_data(user_input, switch_options=switch_options))

        errors: dict[str, str] = {}
        try:
            gateway_options = await _async_gateway_options(
                self.config_entry.data[CONF_HOST],
                self.config_entry.data.get(CONF_PORT, DEFAULT_PORT),
            )
        except CannotConnect:
            errors["base"] = "cannot_connect"
            room_options = _selected_room_options(self.config_entry.options.get(CONF_IMPORT_ROOM_IDS, []))
            switch_options = _selected_switch_options(self.config_entry.options.get(CONF_SWITCH_MODES, {}))
        else:
            room_options = gateway_options.room_options
            switch_options = gateway_options.switch_options
        self._switch_options = switch_options

        return self.async_show_form(
            step_id="init",
            data_schema=_import_filter_schema(
                include_light_groups=self.config_entry.options.get(CONF_INCLUDE_LIGHT_GROUPS, False),
                import_room_ids=_selected_or_all_room_ids(
                    self.config_entry.options.get(CONF_IMPORT_ROOM_IDS, []),
                    room_options,
                ),
                room_options=room_options,
                wireless_switch_node_ids=_wireless_switch_node_ids(
                    self.config_entry.options.get(CONF_SWITCH_MODES, {}),
                    switch_options,
                ),
                switch_options=switch_options,
            ),
            errors=errors,
        )


async def _async_validate_gateway(host: str, port: int) -> GatewayOptions:
    return await _async_gateway_options(host, port)


async def _async_gateway_options(host: str, port: int) -> GatewayOptions:
    gateway = YeelightProGateway(host, port=port, request_timeout=DEFAULT_REQUEST_TIMEOUT)
    try:
        async with asyncio.timeout(DEFAULT_REQUEST_TIMEOUT + 2):
            await gateway.connect()
            await gateway.sync(include_rooms=True)
            return GatewayOptions(
                room_options=_room_options(gateway.state.rooms.values()),
                switch_options=_switch_options(gateway.state.nodes.values()),
            )
    except (OSError, TimeoutError, YeelightProError) as exc:
        raise CannotConnect from exc
    finally:
        await gateway.close()


def _import_filter_schema(
    *,
    include_light_groups: bool,
    import_room_ids: list[str],
    room_options: list[selector.SelectOptionDict],
    wireless_switch_node_ids: list[str],
    switch_options: list[selector.SelectOptionDict],
) -> vol.Schema:
    return vol.Schema(
        {
            vol.Optional(CONF_INCLUDE_LIGHT_GROUPS, default=include_light_groups): bool,
            vol.Optional(CONF_IMPORT_ROOM_IDS, default=list(import_room_ids)): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=room_options,
                    multiple=True,
                    mode=selector.SelectSelectorMode.LIST,
                )
            ),
            vol.Optional(
                CONF_WIRELESS_SWITCH_NODE_IDS, default=list(wireless_switch_node_ids)
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=switch_options,
                    multiple=True,
                    mode=selector.SelectSelectorMode.LIST,
                )
            ),
        }
    )


def _options_data(
    user_input: dict[str, Any],
    *,
    switch_options: list[selector.SelectOptionDict],
) -> dict[str, Any]:
    wireless_switch_node_ids = {str(node_id) for node_id in user_input.get(CONF_WIRELESS_SWITCH_NODE_IDS, [])}
    return {
        CONF_INCLUDE_LIGHT_GROUPS: user_input.get(CONF_INCLUDE_LIGHT_GROUPS, False),
        CONF_IMPORT_ROOM_IDS: [str(room_id) for room_id in user_input.get(CONF_IMPORT_ROOM_IDS, [])],
        CONF_SWITCH_MODES: {
            str(option["value"]): SWITCH_MODE_WIRELESS
            if str(option["value"]) in wireless_switch_node_ids
            else SWITCH_MODE_RELAY
            for option in switch_options
        },
    }


def _room_options(rooms: list[Mapping[str, Any]] | Any) -> list[selector.SelectOptionDict]:
    options: list[selector.SelectOptionDict] = []
    for room in rooms:
        room_id = room.get("id")
        if isinstance(room_id, bool) or not isinstance(room_id, (str, int)):
            continue
        name = room.get("name", room.get("n"))
        label = name if isinstance(name, str) and name else str(room_id)
        options.append({"value": str(room_id), "label": label})
    return sorted(options, key=lambda item: item["label"])


def _selected_room_options(room_ids: list[str] | Any) -> list[selector.SelectOptionDict]:
    return [{"value": str(room_id), "label": str(room_id)} for room_id in room_ids]


def _switch_options(nodes: list[TopologyNode] | Any) -> list[selector.SelectOptionDict]:
    options: list[selector.SelectOptionDict] = []
    for node in nodes:
        if not isinstance(node, TopologyNode) or not is_switch_mode_configurable_node(node):
            continue
        node_id = node_key(node.id)
        label = node.name if isinstance(node.name, str) and node.name else node_id
        options.append({"value": node_id, "label": label})
    return sorted(options, key=lambda item: item["label"])


def _selected_switch_options(switch_modes: Mapping[str, str] | Any) -> list[selector.SelectOptionDict]:
    if not isinstance(switch_modes, Mapping):
        return []
    return sorted(
        [{"value": str(node_id), "label": str(node_id)} for node_id in switch_modes],
        key=lambda item: item["label"],
    )


def _wireless_switch_node_ids(
    switch_modes: Mapping[str, str] | Any,
    switch_options: list[selector.SelectOptionDict],
) -> list[str]:
    if not isinstance(switch_modes, Mapping):
        return []
    allowed = {str(option["value"]) for option in switch_options}
    return sorted(
        str(node_id)
        for node_id, mode in switch_modes.items()
        if str(mode) == SWITCH_MODE_WIRELESS and str(node_id) in allowed
    )


def _all_room_option_values(room_options: list[selector.SelectOptionDict]) -> list[str]:
    return [str(option["value"]) for option in room_options]


def _selected_or_all_room_ids(room_ids: list[str] | Any, room_options: list[selector.SelectOptionDict]) -> list[str]:
    selected = [str(room_id) for room_id in room_ids]
    return selected if selected else _all_room_option_values(room_options)
