from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .coordinator import YeelightProCoordinator


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    coordinator: YeelightProCoordinator | None = getattr(entry, "runtime_data", None)
    if coordinator is None:
        return {}
    return coordinator.diagnostics()
