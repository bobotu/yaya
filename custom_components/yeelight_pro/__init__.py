from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    from homeassistant.exceptions import ConfigEntryNotReady

    from .const import PLATFORMS
    from .coordinator import YeelightProCoordinator
    from .core import YeelightProError

    coordinator = YeelightProCoordinator(hass, entry)
    try:
        await coordinator.async_setup()
    except (OSError, TimeoutError, YeelightProError) as exc:
        await coordinator.async_shutdown()
        raise ConfigEntryNotReady(
            f"Unable to connect to Yeelight Pro gateway {coordinator.host}:{coordinator.port}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    entry.runtime_data = coordinator
    try:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    except Exception:
        await coordinator.async_shutdown()
        raise
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    from .const import PLATFORMS
    from .coordinator import YeelightProCoordinator

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    coordinator: YeelightProCoordinator | None = getattr(entry, "runtime_data", None)
    if coordinator is not None:
        await coordinator.async_shutdown()
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)
