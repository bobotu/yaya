from __future__ import annotations

from enum import StrEnum


class GatewaySessionState(StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    WAITING_TOPOLOGY = "waiting_topology"
    WAITING_FULL_PROP = "waiting_full_prop"
    READY = "ready"
    RECOVERING = "recovering"
    CLOSING = "closing"
