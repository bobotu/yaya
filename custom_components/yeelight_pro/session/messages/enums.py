from __future__ import annotations

from enum import StrEnum


class StateChangeReason(StrEnum):
    MANUAL_SYNC = "manual sync"
    SYNC_COMPLETE = "sync complete"
    TOPOLOGY_SYNC = "topology sync"
    TOPOLOGY_PUSH = "topology push"
    PROPERTY_PUSH = "property push"
    GENERIC_PUSH = "generic push"
    NODE_REFRESH = "node refresh"
    POLL_FULL_PROPERTIES = "poll full properties"
    COMMAND_INTENT_RECORDED = "command intent recorded"
    COMMAND_INTENT_CLEARED = "command intent cleared"
    COMMAND_INTENT_EXPIRED = "command intent expired"


class FullSyncSource(StrEnum):
    POLL = "poll"
    PUSH = "push"


class SyntheticSessionMethod(StrEnum):
    SYNC_TOPOLOGY = "gateway_sync.topology"
    SYNC_COMPLETE = "gateway_sync.complete"
    COMMAND_INTENT_RECORDED = "gateway_intent.recorded"
    COMMAND_INTENT_CLEAR = "gateway_intent.clear"
    COMMAND_INTENT_EXPIRED = "gateway_intent.expired"
