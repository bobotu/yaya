from __future__ import annotations

from enum import IntEnum

NodeId = str | int


def int_or_none(value: object, *, bool_as_int: bool = False) -> int | None:
    if isinstance(value, bool):
        return int(value) if bool_as_int else None
    if isinstance(value, IntEnum):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def node_id_or_none(value: object) -> NodeId | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    return value


def node_key(value: object) -> str | None:
    node_id = node_id_or_none(value)
    return None if node_id is None else str(node_id)
