from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from .protocol import GatewayMethod


@dataclass(frozen=True)
class GatewayEvent:
    id: str | int
    nt: int | None
    value: str
    params: Mapping[str, Any]

    @property
    def event_type(self) -> str:
        return self.value.replace(".", "_")

    @property
    def key(self) -> int | None:
        return _int_or_none(self.params.get("key"))

    @property
    def count(self) -> int | None:
        return _int_or_none(self.params.get("count"))

    @property
    def index(self) -> int | None:
        return _int_or_none(self.params.get("idx"))

    @property
    def spin_delta(self) -> int | None:
        free_spin = self._spin_value("free_spin")
        if free_spin is not None:
            return free_spin
        return self._spin_value("hold_spin")

    @property
    def spin_mode(self) -> str | None:
        if self._spin_value("free_spin") is not None:
            return "free"
        if self._spin_value("hold_spin") is not None:
            return "hold"
        return None

    @property
    def spin_direction(self) -> str | None:
        delta = self.spin_delta
        if delta is None or delta == 0:
            return None
        return "clockwise" if delta > 0 else "counterclockwise"

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "nt": self.nt,
            "value": self.value,
            "event_type": self.event_type,
            **dict(self.params),
        }

    def _spin_value(self, key: str) -> int | None:
        value = _int_or_none(self.params.get(key))
        if value is not None:
            return value
        if self.index is None:
            return None
        return _int_or_none(self.params.get(f"{self.index}-{key}"))


def iter_gateway_events(message: Mapping[str, Any]) -> Iterator[GatewayEvent]:
    if message.get("method") != GatewayMethod.POST_EVENT:
        return

    nodes = message.get("nodes")
    if not isinstance(nodes, list):
        return

    for node in nodes:
        if not isinstance(node, Mapping):
            continue
        value = node.get("value")
        if not isinstance(value, str):
            continue
        params = node.get("params")
        yield GatewayEvent(
            id=node.get("id", ""),
            nt=_int_or_none(node.get("nt")),
            value=value,
            params=params if isinstance(params, Mapping) else {},
        )


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None
