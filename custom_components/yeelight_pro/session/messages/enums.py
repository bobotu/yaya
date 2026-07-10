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
    PENDING_WRITE_PREPARED = "pending write prepared"
    PENDING_WRITE_RELEASED = "pending write released"


class FullSyncSource(StrEnum):
    POLL = "poll"
    PUSH = "push"


class SyntheticSessionMethod(StrEnum):
    SYNC_TOPOLOGY = "gateway_sync.topology"
    SYNC_COMPLETE = "gateway_sync.complete"
    PENDING_WRITE_PREPARED = "gateway_pending.prepared"
    PENDING_WRITE_RELEASED = "gateway_pending.released"
