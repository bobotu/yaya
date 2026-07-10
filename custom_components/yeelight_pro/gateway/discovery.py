from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from .const import GATEWAY_DISCOVERY_PAYLOAD, GATEWAY_DISCOVERY_PORT
from .exceptions import ProtocolError


@dataclass(frozen=True)
class DiscoveredGateway:
    fields: dict[str, str]
    host: str | None = None
    port: int = GATEWAY_DISCOVERY_PORT

    @property
    def pid(self) -> str | None:
        return self.fields.get("pid")

    @property
    def mac(self) -> str | None:
        return self.fields.get("mac")

    @property
    def did(self) -> str | None:
        return self.fields.get("did")

    @property
    def ip(self) -> str | None:
        return self.fields.get("ip") or self.host


class _DiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue[tuple[bytes, tuple[Any, ...]]]) -> None:
        self.queue = queue

    def datagram_received(self, data: bytes, addr: tuple[Any, ...]) -> None:
        self.queue.put_nowait((data, addr))


def parse_discovery_response(payload: bytes | str, *, host: str | None = None) -> DiscoveredGateway:
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    fields: dict[str, str] = {}
    for raw_line in text.splitlines():
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        key = key.strip().lower()
        if key:
            fields[key] = value.strip()

    if not fields:
        raise ProtocolError("discovery response did not contain key/value fields")

    return DiscoveredGateway(fields=fields, host=host)


async def discover_gateways(
    *,
    timeout: float = 3.0,
    target_host: str = "255.255.255.255",
    port: int = GATEWAY_DISCOVERY_PORT,
) -> list[DiscoveredGateway]:
    """Broadcast a read-only discovery probe and collect gateway responses."""

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[tuple[bytes, tuple[Any, ...]]] = asyncio.Queue()
    transport, _protocol = await loop.create_datagram_endpoint(
        lambda: _DiscoveryProtocol(queue),
        local_addr=("0.0.0.0", 0),
        allow_broadcast=True,
    )

    try:
        transport.sendto(GATEWAY_DISCOVERY_PAYLOAD, (target_host, port))
        deadline = loop.time() + timeout
        results: dict[str, DiscoveredGateway] = {}
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                data, addr = await asyncio.wait_for(queue.get(), timeout=remaining)
            except TimeoutError:
                break

            host = str(addr[0]) if addr else None
            gateway = parse_discovery_response(data, host=host)
            key = gateway.mac or gateway.did or gateway.ip or host or repr(addr)
            results[key] = gateway
        return list(results.values())
    finally:
        transport.close()
