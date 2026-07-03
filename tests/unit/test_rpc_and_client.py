from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "custom_components"))

from yeelight_pro.core import NodeSet, RequestTimeout, parse_line  # noqa: E402
from yeelight_pro.session import (
    GatewayRPC,  # noqa: E402
    YeelightProGateway,  # noqa: E402
)


class RpcClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        server = getattr(self, "server", None)
        if server is not None:
            server.close()
            await server.wait_closed()

    async def start_gateway(self, handler: Any) -> tuple[str, int]:
        self.server = await asyncio.start_server(handler, "127.0.0.1", 0)
        assert self.server.sockets is not None
        host, port = self.server.sockets[0].getsockname()[:2]
        return str(host), int(port)

    async def test_request_id_matching_push_dispatch_and_state_update(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readline())
                writer.write(b'{"method":"gateway_post.prop","nodes":[{"id":"light-1","params":{"p":true}}]}\r\n')
                writer.write(
                    (
                        json.dumps(
                            {
                                "id": request["id"],
                                "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False}}],
                                "groups": [],
                                "rooms": [],
                                "scenes": [],
                            },
                            separators=(",", ":"),
                        )
                        + "\r\n"
                    ).encode("utf-8")
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)

        try:
            await gateway.connect()
            response = await gateway.get_topology()
            gateway.state.apply_topology(response)
            await asyncio.sleep(0)
        finally:
            await gateway.close()

        self.assertEqual(response["id"], 1)
        self.assertEqual(gateway.state.nodes["light-1"].params["p"], True)

    async def test_property_listener_receives_before_and_after_snapshots(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                writer.write(
                    b'{"method":"gateway_post.prop","nodes":[{"id":"light-1","nt":2,"params":{"p":true,"l":70}}]}\r\n'
                )
                await writer.drain()
                await reader.readline()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)
        changes: list[Any] = []

        try:
            gateway.state.apply_topology(
                {
                    "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False}}],
                    "groups": [],
                    "rooms": [],
                    "scenes": [],
                }
            )
            gateway.add_property_listener(changes.append)
            await gateway.connect()
            await asyncio.sleep(0.05)
        finally:
            await gateway.close()

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].before.params["p"], False)
        self.assertEqual(changes[0].after.params["p"], True)
        self.assertEqual(changes[0].after.params["l"], 70)

    async def test_state_listener_receives_topology_push(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                writer.write(
                    b'{"method":"gateway_post.topology","nodes":[{"id":"new-light","nt":2,"type":3,"params":{"p":true}}],"groups":[],"rooms":[],"scenes":[]}\r\n'
                )
                await writer.drain()
                await reader.readline()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)
        messages: list[Any] = []

        try:
            gateway.add_state_listener(messages.append)
            await gateway.connect()
            await asyncio.sleep(0.05)
        finally:
            await gateway.close()

        self.assertEqual(messages[0]["method"], "gateway_post.topology")
        self.assertEqual(gateway.state.nodes["new-light"].type, 3)
        self.assertEqual(gateway.state.nodes["new-light"].params["p"], True)

    async def test_set_prop_uses_gateway_payload(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readline())
                received.append(request)
                writer.write(json.dumps({"id": request["id"], "result": "ok"}, separators=(",", ":")).encode("utf-8"))
                writer.write(b"\r\n")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)

        try:
            await gateway.connect()
            response = await gateway.set_prop([NodeSet(id="light-1", nt=2, props={"p": True})])
        finally:
            await gateway.close()

        self.assertEqual(response["result"], "ok")
        self.assertEqual(received[0]["method"], "gateway_set.prop")
        self.assertEqual(received[0]["nodes"], [{"id": "light-1", "nt": 2, "set": {"p": True}}])

    async def test_scene_and_event_methods_are_explicit_payloads(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                for _ in range(2):
                    request = parse_line(await reader.readline())
                    received.append(request)
                    writer.write(
                        json.dumps({"id": request["id"], "result": "ok"}, separators=(",", ":")).encode("utf-8")
                    )
                    writer.write(b"\r\n")
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)

        try:
            await gateway.connect()
            await gateway.set_scenes([{"id": "scene-1"}])
            await gateway.set_event([{"id": "virtual-1", "nt": 2, "value": "motion.true"}])
        finally:
            await gateway.close()

        self.assertEqual(received[0]["method"], "gateway_set.prop")
        self.assertEqual(received[0]["scenes"], [{"id": "scene-1"}])
        self.assertEqual(received[1]["method"], "gateway_set.event")
        self.assertEqual(received[1]["nodes"], [{"id": "virtual-1", "nt": 2, "value": "motion.true"}])

    async def test_collection_getters_use_reference_id_zero_payload(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                for _ in range(3):
                    request = parse_line(await reader.readline())
                    received.append(request)
                    key = request["method"].rsplit(".", 1)[-1] + "s"
                    writer.write(json.dumps({"id": request["id"], key: []}, separators=(",", ":")).encode("utf-8"))
                    writer.write(b"\r\n")
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)

        try:
            await gateway.connect()
            await gateway.get_group()
            await gateway.get_room()
            await gateway.get_scene()
        finally:
            await gateway.close()

        self.assertEqual([request["params"] for request in received], [{"id": 0}, {"id": 0}, {"id": 0}])

    async def test_sync_waits_for_protocol_full_property_push(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readuntil(b"\r\n"))
                writer.write(
                    json.dumps(
                        {
                            "id": request["id"],
                            "nodes": [{"id": "light-1", "nt": 2, "type": 3, "name": "Light"}],
                            "groups": [],
                            "rooms": [],
                            "scenes": [],
                        },
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\r\n"
                )
                writer.write(
                    b'{"method":"gateway_post.prop","nodes":[{"id":"light-1","nt":2,"params":{"p":true},"o":true}]}\r\n'
                )
                await writer.drain()
                await reader.readline()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)

        try:
            await gateway.connect()
            await gateway.sync()
        finally:
            await gateway.close()

        self.assertEqual(gateway.state.nodes["light-1"].params, {"p": True})
        self.assertTrue(gateway.state.nodes["light-1"].online)
        self.assertIsNotNone(gateway.last_full_sync_at)

    async def test_concurrent_requests_are_serialized_and_matched_by_id(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                first = parse_line(await reader.readuntil(b"\r\n"))
                second = parse_line(await reader.readuntil(b"\r\n"))
                for request in (second, first):
                    writer.write(
                        json.dumps(
                            {"id": request["id"], "method_seen": request["method"]},
                            separators=(",", ":"),
                        ).encode("utf-8")
                        + b"\r\n"
                    )
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        rpc = GatewayRPC(host, port=port)

        try:
            await rpc.connect()
            first, second = await asyncio.gather(
                rpc.request("gateway_get.topology"),
                rpc.request("gateway_get.room", {"params": {"id": 0}}),
            )
        finally:
            await rpc.close()

        self.assertEqual(first["method_seen"], "gateway_get.topology")
        self.assertEqual(second["method_seen"], "gateway_get.room")

    async def test_large_crlf_framed_json_response_is_accepted(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readuntil(b"\r\n"))
                writer.write(
                    json.dumps(
                        {"id": request["id"], "blob": "x" * (70 * 1024)},
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\r\n"
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        rpc = GatewayRPC(host, port=port)

        try:
            await rpc.connect()
            response = await rpc.request("gateway_get.node", {"params": {"id": 0}})
        finally:
            await rpc.close()

        self.assertEqual(len(response["blob"]), 70 * 1024)

    async def test_request_timeout_marks_connection_closed(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readline()
                await asyncio.sleep(1)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, request_timeout=1.0)

        try:
            await gateway.connect()
            gateway.rpc.request_timeout = 0.05
            with self.assertRaises(RequestTimeout):
                await gateway.get_topology()
            await asyncio.wait_for(gateway.wait_closed(), timeout=0.5)
            self.assertFalse(gateway.is_connected)
            self.assertIsInstance(gateway.last_disconnect_error, RequestTimeout)
            self.assertIn("gateway_get.topology", str(gateway.last_disconnect_error))
        finally:
            await gateway.close()

    async def test_connect_timeout_reports_gateway_endpoint(self) -> None:
        async def stalled_open_connection(
            *_args: Any, **_kwargs: Any
        ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            await asyncio.sleep(1)
            raise AssertionError("unreachable")

        rpc = GatewayRPC("192.0.2.1", port=65443, request_timeout=0.01)
        with patch("yeelight_pro.session.rpc.asyncio.open_connection", side_effect=stalled_open_connection):
            with self.assertRaises(RequestTimeout) as ctx:
                await rpc.connect()

        self.assertIn("timed out connecting to 192.0.2.1:65443", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
