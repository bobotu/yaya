from __future__ import annotations

import importlib.util

pytest_plugins = (
    ["pytest_homeassistant_custom_component"]
    if importlib.util.find_spec("pytest_homeassistant_custom_component") is not None
    else []
)
