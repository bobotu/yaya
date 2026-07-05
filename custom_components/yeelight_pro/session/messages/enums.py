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
    OPTIMISTIC_UPDATE = "optimistic update"
    OPTIMISTIC_CLEARED = "optimistic_cleared"
    OPTIMISTIC_EXPIRED = "optimistic expired"
    MOTOR_TARGET = "motor target"
    MOTOR_STOPPED = "motor stopped"
    MOTOR_TRACKING_CLEARED = "motor tracking cleared"
    MOTOR_TRACKING_EXPIRED = "motor tracking expired"


class FullSyncSource(StrEnum):
    POLL = "poll"
    PUSH = "push"


class SyntheticSessionMethod(StrEnum):
    SYNC_TOPOLOGY = "gateway_sync.topology"
    SYNC_COMPLETE = "gateway_sync.complete"
    OVERLAY_OPTIMISTIC = "gateway_overlay.optimistic"
    OVERLAY_CLEAR = "gateway_overlay.clear"
    OVERLAY_EXPIRED = "gateway_overlay.expired"
    MOTOR_TARGET = "gateway_motor.target"
    MOTOR_STOP = "gateway_motor.stop"
    MOTOR_CLEAR = "gateway_motor.clear"
    MOTOR_EXPIRED = "gateway_motor.expired"
