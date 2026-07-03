from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import translation
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    CONF_IMPORT_ROOM_IDS,
    CONF_INCLUDE_LIGHT_GROUPS,
    CONF_SWITCH_MODES,
    DEFAULT_HEARTBEAT_WATCHDOG_INTERVAL,
    DEFAULT_RECONNECT_DELAY,
    DEFAULT_REQUEST_TIMEOUT,
    DOMAIN,
    EVENT_YEELIGHT_PRO,
)
from .core import GatewayEvent, TopologyNode, YeelightProError
from .helpers import (
    device_model_key,
    event_data,
    node_identifier,
    node_key,
    should_import_node,
    switch_node_is_relay_mode,
    switch_node_is_wireless_mode,
)
from .session import YeelightProGateway

_LOGGER = logging.getLogger(__name__)


EventCallback = Callable[[GatewayEvent], None]

MODEL_TRANSLATION_PREFIX = f"component.{DOMAIN}.device_model."
DEFAULT_DEVICE_MODELS = {
    "air_conditioner_controller": "Air conditioner controller",
    "bath_heater": "Bath heater",
    "curtain": "Curtain",
    "double_relay_switch": "Double relay switch",
    "dream_curtain": "Dream curtain",
    "knob_panel": "Knob panel",
    "light": "Light",
    "light_group": "Light group",
    "multi_key_relay_switch": "Multi-key relay switch",
    "scene_panel": "Scene panel",
    "sensor": "Sensor",
    "yeelight_pro_device": "Yeelight Pro device",
}


class YeelightProCoordinator(DataUpdateCoordinator[dict[str, TopologyNode]]):
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.entry = entry
        self.host: str = entry.data[CONF_HOST]
        self.port: int = entry.data.get(CONF_PORT, 65443)
        self.include_light_groups: bool = entry.options.get(
            CONF_INCLUDE_LIGHT_GROUPS,
            entry.data.get(CONF_INCLUDE_LIGHT_GROUPS, False),
        )
        self.import_room_ids: frozenset[str] = frozenset(
            str(room_id)
            for room_id in entry.options.get(CONF_IMPORT_ROOM_IDS, entry.data.get(CONF_IMPORT_ROOM_IDS, []))
        )
        self.switch_modes: Mapping[str, str] = {
            str(node_id): str(mode)
            for node_id, mode in entry.options.get(CONF_SWITCH_MODES, entry.data.get(CONF_SWITCH_MODES, {})).items()
        }
        self.gateway_id = f"{self.host}:{self.port}"
        self.gateway = YeelightProGateway(
            self.host,
            port=self.port,
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            reconnect_delay=DEFAULT_RECONNECT_DELAY,
        )
        self._stopped = False
        self._runner: asyncio.Task[None] | None = None
        self._remove_event_listener: Callable[[], None] | None = None
        self._remove_state_listener: Callable[[], None] | None = None
        self._event_listeners: dict[str, list[EventCallback]] = {}
        self._device_model_translations: dict[str, str] = {}
        self._unavailable_logged = False
        self._unavailable_since: datetime | None = None
        self._last_snapshot: tuple[object, ...] | None = None

        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=f"{DOMAIN}-{self.gateway_id}",
            update_interval=DEFAULT_HEARTBEAT_WATCHDOG_INTERVAL,
        )

    async def async_setup(self) -> None:
        await self._async_load_device_model_translations()
        self._remove_state_listener = self.gateway.add_state_listener(self._async_handle_state_update)
        self._remove_event_listener = self.gateway.add_event_listener(self._async_handle_event)
        await self._async_connect_and_sync()
        self._register_gateway_device()
        self._runner = self.entry.async_create_background_task(
            self.hass,
            self._async_reconnect_loop(),
            f"{DOMAIN} reconnect loop",
        )

    async def async_shutdown(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        await super().async_shutdown()
        if self._remove_event_listener is not None:
            self._remove_event_listener()
            self._remove_event_listener = None
        if self._remove_state_listener is not None:
            self._remove_state_listener()
            self._remove_state_listener = None
        if self._runner is not None:
            self._runner.cancel()
            try:
                await self._runner
            except asyncio.CancelledError:
                pass
            self._runner = None
        await self.gateway.close()

    async def _async_connect_and_sync(self) -> None:
        await self.gateway.connect()
        await self.gateway.sync(
            include_groups=self.include_light_groups or bool(self.import_room_ids),
            include_rooms=bool(self.import_room_ids),
        )
        self._mark_gateway_available("initial sync")
        self._log_gateway_snapshot("initial sync")
        self.async_set_updated_data(self._current_data())

    async def _async_update_data(self) -> dict[str, TopologyNode]:
        try:
            if not self.gateway.is_connected:
                await self.gateway.connect()
            await self.gateway.sync(
                include_groups=self.include_light_groups or bool(self.import_room_ids),
                include_rooms=bool(self.import_room_ids),
            )
        except (OSError, TimeoutError, YeelightProError) as exc:
            self._mark_gateway_unavailable("refresh failed", exc)
            raise UpdateFailed(_format_gateway_error(self.host, self.port, "refresh failed", exc)) from exc
        self._mark_gateway_available("refresh")
        self._log_gateway_snapshot("refresh")
        return self._current_data()

    async def _async_reconnect_loop(self) -> None:
        while not self._stopped:
            try:
                if not self.gateway.is_connected:
                    await self._async_connect_and_sync()
                await self.gateway.wait_closed()
                if not self._stopped:
                    error = self.gateway.last_disconnect_error or YeelightProError("gateway connection closed")
                    self._mark_gateway_unavailable("connection closed", error)
                    self.async_set_update_error(
                        UpdateFailed(_format_gateway_error(self.host, self.port, "connection closed", error))
                    )
            except asyncio.CancelledError:
                raise
            except (OSError, YeelightProError) as exc:
                self._mark_gateway_unavailable("reconnect failed", exc)
                _LOGGER.debug(
                    "Yeelight Pro gateway %s:%s reconnect attempt failed; error=%s: %s; session_state=%s",
                    self.host,
                    self.port,
                    type(exc).__name__,
                    exc,
                    self.gateway.session_state,
                )
            except Exception:
                self._mark_gateway_unavailable("unexpected reconnect error")
                _LOGGER.exception("Unexpected Yeelight Pro gateway %s:%s reconnect error", self.host, self.port)

            if not self._stopped:
                await asyncio.sleep(DEFAULT_RECONNECT_DELAY)

    def _current_data(self) -> dict[str, TopologyNode]:
        return {
            node_key(node.id): node
            for node in self.gateway.state.nodes.values()
            if should_import_node(
                node,
                include_light_groups=self.include_light_groups,
                import_room_ids=self.import_room_ids,
                room_id=self.gateway.state.room_id_for_node(node),
            )
        }

    @callback
    def _async_handle_state_update(self, message: Mapping[str, Any]) -> None:
        self.async_set_updated_data(self._current_data())
        method = message.get("method")
        if method == "gateway_post.topology":
            self._log_gateway_snapshot("topology push", force=True)
        elif method == "gateway_post.prop":
            full_property_sync = self.gateway.state.full_property_coverage(message)
            self._log_gateway_snapshot(
                "full property push" if full_property_sync else "property push",
                force=full_property_sync,
            )

    @callback
    def _async_handle_event(self, event: GatewayEvent) -> None:
        node = self.node(event.id)
        if node is not None and not self.exposes_events_for_node(node):
            return
        device_id = self.device_id_for_node(event.id)
        data = event_data(event, device_id=device_id)
        self.hass.bus.async_fire(EVENT_YEELIGHT_PRO, data)
        for listener in list(self._event_listeners.get(node_key(event.id), [])):
            listener(event)

    def add_event_listener(self, node_id: str | int, listener: EventCallback) -> Callable[[], None]:
        key = node_key(node_id)
        self._event_listeners.setdefault(key, []).append(listener)

        def remove() -> None:
            listeners = self._event_listeners.get(key)
            if listeners is None:
                return
            try:
                listeners.remove(listener)
            except ValueError:
                return
            if not listeners:
                self._event_listeners.pop(key, None)

        return remove

    def node(self, node_id: str | int) -> TopologyNode | None:
        node = self.gateway.state.nodes.get(node_id)
        if node is not None:
            return node
        return (self.data or {}).get(node_key(node_id))

    def nodes(self) -> list[TopologyNode]:
        return list((self.data or {}).values())

    def exposes_relay_switches_for_node(self, node: TopologyNode) -> bool:
        return switch_node_is_relay_mode(node, self.switch_modes)

    def exposes_wireless_relay_diagnostics_for_node(self, node: TopologyNode) -> bool:
        return switch_node_is_wireless_mode(node, self.switch_modes)

    def exposes_events_for_node(self, node: TopologyNode) -> bool:
        if switch_node_is_relay_mode(node, self.switch_modes):
            return False
        return True

    def diagnostics(self) -> dict[str, Any]:
        return {
            "session_state": str(self.gateway.session_state),
            "last_full_sync_at": self.gateway.last_full_sync_at.isoformat()
            if self.gateway.last_full_sync_at is not None
            else None,
            "last_full_sync_source": self.gateway.last_full_sync_source,
            "last_disconnect_error": _error_diagnostics(self.gateway.last_disconnect_error),
            "unknown_property_nodes": self.gateway.state.unknown_summary(),
        }

    def _mark_gateway_unavailable(self, reason: str, exc: BaseException | None = None) -> None:
        if self._unavailable_logged:
            return
        self._unavailable_logged = True
        self._unavailable_since = datetime.now(UTC)
        if exc is None:
            _LOGGER.info(
                "Yeelight Pro gateway %s:%s unavailable: %s; session_state=%s",
                self.host,
                self.port,
                reason,
                self.gateway.session_state,
            )
            return
        _LOGGER.info(
            "Yeelight Pro gateway %s:%s unavailable: %s; error=%s: %s; session_state=%s",
            self.host,
            self.port,
            reason,
            type(exc).__name__,
            exc,
            self.gateway.session_state,
        )

    def _mark_gateway_available(self, reason: str) -> None:
        if not self._unavailable_logged:
            _LOGGER.debug("Yeelight Pro gateway %s:%s connected after %s", self.host, self.port, reason)
            return
        unavailable_since = self._unavailable_since
        self._unavailable_logged = False
        self._unavailable_since = None
        if unavailable_since is None:
            _LOGGER.info("Yeelight Pro gateway %s:%s back online after %s", self.host, self.port, reason)
            return
        downtime = datetime.now(UTC) - unavailable_since
        _LOGGER.info(
            "Yeelight Pro gateway %s:%s back online after %s; unavailable_for=%.1fs",
            self.host,
            self.port,
            reason,
            downtime.total_seconds(),
        )

    def _log_gateway_snapshot(self, reason: str = "sync", *, force: bool = False) -> None:
        imported_nodes = len(self._current_data())
        unknown_summary = self.gateway.state.unknown_summary()
        snapshot = (
            len(self.gateway.state.nodes),
            imported_nodes,
            len(self.gateway.state.rooms),
            len(self.gateway.state.groups),
            unknown_summary["count"],
            tuple(sorted(unknown_summary["by_shape"].items())),
            self.gateway.last_full_sync_source,
        )
        if not force and snapshot == self._last_snapshot:
            return
        self._last_snapshot = snapshot
        _LOGGER.debug(
            "Yeelight Pro gateway %s:%s state snapshot after %s; nodes=%d imported=%d rooms=%d groups=%d "
            "unknown_property_nodes=%d full_sync_source=%s room_filter=%s include_light_groups=%s",
            self.host,
            self.port,
            reason,
            snapshot[0],
            snapshot[1],
            snapshot[2],
            snapshot[3],
            snapshot[4],
            snapshot[6],
            sorted(self.import_room_ids),
            self.include_light_groups,
        )
        if unknown_summary["count"]:
            _LOGGER.debug(
                "Yeelight Pro gateway %s:%s unknown property-only node shapes: %s",
                self.host,
                self.port,
                unknown_summary["by_shape"],
            )

    async def async_refresh_node(self, node_id: str | int) -> None:
        await self.gateway.refresh_node(node_id)
        self.async_set_updated_data(self._current_data())

    def gateway_identifier(self) -> tuple[str, str]:
        return (DOMAIN, self.gateway_id)

    def node_identifier(self, node_id: str | int) -> tuple[str, str]:
        return (DOMAIN, node_identifier(self.gateway_id, node_id))

    def device_id_for_node(self, node_id: str | int) -> str | None:
        registry = dr.async_get(self.hass)
        device = registry.async_get_device(identifiers={self.node_identifier(node_id)})
        return device.id if device is not None else None

    def node_id_for_device_id(self, device_id: str) -> str | int | None:
        registry = dr.async_get(self.hass)
        for node in self.nodes():
            device = registry.async_get_device(identifiers={self.node_identifier(node.id)})
            if device is not None and device.id == device_id:
                return node.id
        return None

    def device_model_name(self, node: TopologyNode) -> str:
        key = device_model_key(node)
        return self._device_model_translations.get(key, DEFAULT_DEVICE_MODELS[key])

    def _register_gateway_device(self) -> None:
        registry = dr.async_get(self.hass)
        registry.async_get_or_create(
            config_entry_id=self.entry.entry_id,
            identifiers={self.gateway_identifier()},
            manufacturer="Yeelight",
            translation_key="gateway",
            translation_placeholders={"host": self.host},
            configuration_url=f"http://{self.host}",
        )

    async def _async_load_device_model_translations(self) -> None:
        translations = await translation.async_get_translations(
            self.hass,
            self.hass.config.language,
            "device_model",
            integrations=[DOMAIN],
        )
        self._device_model_translations = {
            key.removeprefix(MODEL_TRANSLATION_PREFIX): value
            for key, value in translations.items()
            if key.startswith(MODEL_TRANSLATION_PREFIX)
        }


def _error_diagnostics(exc: BaseException | None) -> dict[str, str] | None:
    if exc is None:
        return None
    return {
        "type": type(exc).__name__,
        "message": str(exc),
    }


def _format_gateway_error(host: str, port: int, reason: str, exc: BaseException) -> str:
    return f"Yeelight Pro gateway {host}:{port} {reason}: {type(exc).__name__}: {exc}"
