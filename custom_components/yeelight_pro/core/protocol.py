from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .const import DEFAULT_VERSION
from .exceptions import ProtocolError

JSONDict = dict[str, Any]


def build_request(
    method: str,
    *,
    request_id: int,
    payload: Mapping[str, Any] | None = None,
    version: str = DEFAULT_VERSION,
    include_version: bool = True,
) -> bytes:
    if not method:
        raise ValueError("method is required")

    message: JSONDict = {}
    if include_version:
        message["version"] = version
    message["id"] = request_id
    message["method"] = method

    if payload:
        reserved = {"version", "id", "method"}.intersection(payload)
        if reserved:
            raise ValueError(f"payload cannot override reserved keys: {sorted(reserved)}")
        message.update(payload)

    return (json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\r\n").encode("utf-8")


def parse_line(line: bytes | str) -> JSONDict:
    if isinstance(line, bytes):
        try:
            text = line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("gateway line is not valid UTF-8") from exc
    else:
        text = line

    text = text.rstrip("\r\n")
    if not text:
        raise ProtocolError("gateway sent an empty line")

    try:
        message = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProtocolError("gateway line is not valid JSON") from exc

    if not isinstance(message, dict):
        raise ProtocolError("gateway line must decode to a JSON object")
    return message


def normalize_payload(message: Mapping[str, Any]) -> Mapping[str, Any]:
    data = message.get("data")
    return data if isinstance(data, Mapping) else message


def list_payload(message: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    value = normalize_payload(message).get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]
