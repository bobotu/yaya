from __future__ import annotations

from datetime import timedelta

from homeassistant.const import Platform

DOMAIN = "yeelight_pro"

DEFAULT_PORT = 65443
DEFAULT_REQUEST_TIMEOUT = 5.0
DEFAULT_RECONNECT_DELAY = 5.0
DEFAULT_HEARTBEAT_WATCHDOG_INTERVAL = timedelta(minutes=11, seconds=30)

CONF_IMPORT_ROOM_IDS = "import_room_ids"
CONF_SWITCH_MODES = "switch_modes"
CONF_WIRELESS_SWITCH_NODE_IDS = "wireless_switch_node_ids"

SWITCH_MODE_RELAY = "relay"
SWITCH_MODE_WIRELESS = "wireless"

EVENT_YEELIGHT_PRO = f"{DOMAIN}_event"

ATTR_NODE_ID = "node_id"
ATTR_EVENT_TYPE = "type"
ATTR_SUBTYPE = "subtype"
ATTR_KEY = "key"
ATTR_COUNT = "count"
ATTR_INDEX = "idx"
ATTR_DELTA = "delta"
ATTR_DIRECTION = "direction"
ATTR_SPIN_MODE = "spin_mode"

PLATFORMS = [
    Platform.LIGHT,
    Platform.BUTTON,
    Platform.COVER,
    Platform.SWITCH,
    Platform.CLIMATE,
    Platform.FAN,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.EVENT,
]
