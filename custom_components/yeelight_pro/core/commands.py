from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

NodeId = str | int


class MotorAction(StrEnum):
    PAUSE = "pause"
    TOGGLE = "toggle"
    CONTINUE = "continue"
    AUTO = "auto"
    OPEN_OR_PAUSE = "openOrPause"
    CLOSE_OR_PAUSE = "closeOrPause"


class BlinkType(StrEnum):
    SMOOTH = "smooth"
    NOTIFY = "notify"
    URGENT = "urgent"


@dataclass(frozen=True)
class NodeCommand:
    id: NodeId
    nt: int
    props: Mapping[str, Any] | None = None
    toggle: Sequence[str] | None = None
    adjust: Mapping[str, str] | None = None
    action: Mapping[str, Any] | None = None
    duration: int | None = None
    delay: int | None = None
    delay_off: int | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"id": self.id, "nt": self.nt}
        if self.duration is not None:
            payload["duration"] = self.duration
        if self.delay is not None:
            payload["delay"] = self.delay
        if self.delay_off is not None:
            payload["delayOff"] = self.delay_off
        if self.props:
            payload["set"] = dict(self.props)
        if self.toggle:
            payload["toggle"] = list(self.toggle)
        if self.adjust:
            payload["adjust"] = dict(self.adjust)
        if self.action:
            payload["action"] = dict(self.action)
        if len(payload) == 2:
            raise ValueError("node command requires props, toggle, adjust, or action")
        return payload


@dataclass(frozen=True)
class NodeSet:
    id: NodeId
    nt: int
    props: Mapping[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {"id": self.id, "nt": self.nt, "set": dict(self.props)}


def motor_adjust_action(action_type: MotorAction | str) -> dict[str, Any]:
    if not action_type:
        raise ValueError("action_type is required")
    return {"motorAdjust": {"type": str(action_type)}}


def blink_action(blink_type: BlinkType | str = BlinkType.NOTIFY, *, repeat: int = 4) -> dict[str, Any]:
    if not blink_type:
        raise ValueError("blink_type is required")
    if repeat < 1:
        raise ValueError("repeat must be greater than zero")
    return {"blink": {"repeat": repeat, "type": str(blink_type)}}


def command_payload(commands: Sequence[NodeCommand | NodeSet | Mapping[str, Any]]) -> dict[str, Any]:
    nodes = [
        command.to_payload() if isinstance(command, (NodeCommand, NodeSet)) else dict(command) for command in commands
    ]
    if not nodes:
        raise ValueError("at least one command is required")
    return {"nodes": nodes}
