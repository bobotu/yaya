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

from yeelight_pro.core import (  # noqa: E402
    ConnectionClosed,
    NodeSet,
    ProtocolError,
    ProtocolFrameTooLarge,
    RequestTimeout,
    parse_line,
)
from yeelight_pro.session import (
    GatewayRPC,  # noqa: E402
    YeelightProGateway,  # noqa: E402
)
from yeelight_pro.session.messages import RpcPushEvent  # noqa: E402
from yeelight_pro.session.model import MOTOR_TRACKING_TARGET_POSITION  # noqa: E402


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

        self.assertEqual(messages[0].message["method"], "gateway_post.topology")
        self.assertEqual(gateway.state.nodes["new-light"].type, 3)
        self.assertEqual(gateway.state.nodes["new-light"].params["p"], True)

    async def test_send_node_command_uses_gateway_payload(self) -> None:
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
            response = await gateway.send_node_command(NodeSet(id="light-1", nt=2, props={"p": True}))
        finally:
            await gateway.close()

        self.assertEqual(response["result"], "ok")
        self.assertEqual(received[0]["method"], "gateway_set.prop")
        self.assertEqual(received[0]["nodes"], [{"id": "light-1", "nt": 2, "set": {"p": True}}])

    async def test_set_prop_batcher_merges_non_conflicting_requests_and_fans_out_ack(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readline())
                received.append(request)
                writer.write(json.dumps({"id": request["id"], "result": "ok"}, separators=(",", ":")).encode("utf-8"))
                writer.write(b"\r\n")
                await writer.drain()
                await asyncio.sleep(0.05)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, set_prop_batch_delay=0.02)

        try:
            gateway.state.apply_topology(
                {
                    "nodes": [
                        {"id": "light-1", "nt": 2, "type": 3, "params": {"p": False}},
                        {"id": "light-2", "nt": 2, "type": 3, "params": {"p": True}},
                    ],
                    "groups": [],
                    "rooms": [],
                    "scenes": [],
                }
            )
            await gateway.connect()
            first, second = await asyncio.gather(
                gateway.set_node_props("light-1", {"p": True}, state_targets={"p": True}),
                gateway.set_node_props("light-2", {"p": False}, state_targets={"p": False}),
            )
        finally:
            await gateway.close()

        self.assertEqual(first["result"], "ok")
        self.assertEqual(second["result"], "ok")
        self.assertEqual([request["method"] for request in received], ["gateway_set.prop"])
        self.assertEqual(
            received[0]["nodes"],
            [
                {"id": "light-1", "nt": 2, "set": {"p": True}},
                {"id": "light-2", "nt": 2, "set": {"p": False}},
            ],
        )

    async def test_set_prop_batcher_merges_same_node_non_conflicting_properties(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readline())
                received.append(request)
                writer.write(json.dumps({"id": request["id"], "result": "ok"}, separators=(",", ":")).encode("utf-8"))
                writer.write(b"\r\n")
                await writer.drain()
                await asyncio.sleep(0.05)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, set_prop_batch_delay=0.02)

        try:
            gateway.state.apply_topology(
                {
                    "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False, "l": 80}}],
                    "groups": [],
                    "rooms": [],
                    "scenes": [],
                }
            )
            await gateway.connect()
            await asyncio.gather(
                gateway.set_node_props("light-1", {"p": True}, state_targets={"p": True}),
                gateway.set_node_props("light-1", {"l": 42}, state_targets={"l": 42}),
            )
        finally:
            await gateway.close()

        self.assertEqual([request["method"] for request in received], ["gateway_set.prop"])
        self.assertEqual(received[0]["nodes"], [{"id": "light-1", "nt": 2, "set": {"p": True, "l": 42}}])

    async def test_set_prop_batcher_splits_conflicting_property_targets(self) -> None:
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
                await asyncio.sleep(0.05)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, set_prop_batch_delay=0.02)

        try:
            gateway.state.apply_topology(
                {
                    "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False}}],
                    "groups": [],
                    "rooms": [],
                    "scenes": [],
                }
            )
            await gateway.connect()
            await asyncio.gather(
                gateway.set_node_props("light-1", {"p": True}, state_targets={"p": True}),
                gateway.set_node_props("light-1", {"p": False}, state_targets={"p": False}),
            )
        finally:
            await gateway.close()

        self.assertEqual([request["method"] for request in received], ["gateway_set.prop", "gateway_set.prop"])
        self.assertEqual(received[0]["nodes"], [{"id": "light-1", "nt": 2, "set": {"p": True}}])
        self.assertEqual(received[1]["nodes"], [{"id": "light-1", "nt": 2, "set": {"p": False}}])

    async def test_set_prop_batcher_propagates_rpc_failure_to_each_caller(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readline())
                received.append(request)
                writer.write(json.dumps({"id": request["id"], "result": "fail"}, separators=(",", ":")).encode())
                writer.write(b"\r\n")
                await writer.drain()
                await asyncio.sleep(0.05)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, set_prop_batch_delay=0.02)

        try:
            await gateway.connect()
            results = await asyncio.gather(
                gateway.set_node_props("light-1", {"p": True}, state_targets={"p": True}),
                gateway.set_node_props("light-2", {"p": False}, state_targets={"p": False}),
                return_exceptions=True,
            )
        finally:
            await gateway.close()

        self.assertEqual([request["method"] for request in received], ["gateway_set.prop"])
        self.assertEqual(
            received[0]["nodes"],
            [
                {"id": "light-1", "nt": 2, "set": {"p": True}},
                {"id": "light-2", "nt": 2, "set": {"p": False}},
            ],
        )
        self.assertTrue(all(isinstance(result, ProtocolError) for result in results))
        self.assertEqual(
            {str(result) for result in results},
            {"gateway_set.prop did not return a successful acknowledgement"},
        )

    async def test_gateway_rejects_empty_payload_collections_and_invalid_curtain_position(self) -> None:
        gateway = YeelightProGateway("127.0.0.1", port=1)

        with self.assertRaises(ValueError):
            await gateway._send_node_commands([])
        with self.assertRaises(ValueError):
            await gateway.set_scenes([])
        with self.assertRaises(ValueError):
            await gateway.set_event([])
        with self.assertRaises(ValueError):
            await gateway.set_curtain_position("curtain-1", 101)

    async def test_curtain_position_command_tracks_target_without_overwriting_current(self) -> None:
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
            gateway.state.apply_topology(
                {
                    "nodes": [{"id": "curtain-1", "nt": 2, "type": 6, "params": {"cp": 20, "tp": 20}}],
                    "groups": [],
                    "rooms": [],
                    "scenes": [],
                }
            )
            await gateway.connect()
            await gateway.set_curtain_position("curtain-1", 80)
            self.assertEqual(gateway.state.nodes["curtain-1"].params["cp"], 20)
            self.assertEqual(gateway.visible_node("curtain-1").params["cp"], 20)
            self.assertEqual(gateway.visible_node("curtain-1").params[MOTOR_TRACKING_TARGET_POSITION], 80)
        finally:
            await gateway.close()

        self.assertEqual(received[0]["nodes"], [{"id": "curtain-1", "nt": 2, "set": {"tp": 80}}])

    async def test_rpc_request_requires_connected_transport(self) -> None:
        rpc = GatewayRPC("127.0.0.1", port=1)

        with self.assertRaises(ConnectionClosed):
            await rpc.request("gateway_get.topology")

    async def test_rpc_write_callback_exception_does_not_fail_request(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readline())
                writer.write(json.dumps({"id": request["id"], "result": "ok"}, separators=(",", ":")).encode("utf-8"))
                writer.write(b"\r\n")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        rpc = GatewayRPC(host, port=port)

        def broken_callback() -> None:
            raise RuntimeError("write callback failed")

        try:
            await rpc.connect()
            with self.assertLogs("yeelight_pro.session.transport.rpc", level="ERROR"):
                response = await rpc.request("gateway_get.topology", on_written=broken_callback)
        finally:
            await rpc.close()

        self.assertEqual(response["result"], "ok")

    async def test_set_node_props_holds_observed_state_until_target_push(self) -> None:
        allow_target = asyncio.Event()

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readline())
                writer.write(json.dumps({"id": request["id"], "result": "ok"}, separators=(",", ":")).encode())
                writer.write(b"\r\n")
                await writer.drain()
                writer.write(
                    b'{"method":"gateway_post.prop","nodes":[{"id":"light-1","nt":2,"params":{"p":true}}]}\r\n'
                )
                await writer.drain()
                await allow_target.wait()
                writer.write(
                    b'{"method":"gateway_post.prop","nodes":[{"id":"light-1","nt":2,"params":{"p":false}}]}\r\n'
                )
                await writer.drain()
                await asyncio.sleep(0.02)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, set_prop_batch_delay=0)
        gateway.state.apply_topology(
            {
                "nodes": [{"id": "light-1", "nt": 2, "type": 3, "o": True, "params": {"p": True}}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )

        try:
            await gateway.connect()
            await gateway.set_node_props("light-1", {"p": False}, state_targets={"p": False})
            await asyncio.sleep(0.02)

            self.assertTrue(gateway.visible_node("light-1").params["p"])
            self.assertTrue(gateway.has_pending_write("light-1", ["p"]))

            allow_target.set()
            await asyncio.sleep(0.04)

            self.assertFalse(gateway.visible_node("light-1").params["p"])
            self.assertFalse(gateway.has_pending_write("light-1"))
        finally:
            allow_target.set()
            await gateway.close()

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

    async def test_sync_falls_back_to_poll_when_full_property_push_is_missing(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                topology_request = parse_line(await reader.readuntil(b"\r\n"))
                received.append(topology_request)
                writer.write(
                    json.dumps(
                        {
                            "id": topology_request["id"],
                            "nodes": [{"id": "light-1", "nt": 2, "type": 3, "name": "Light"}],
                            "groups": [],
                            "rooms": [],
                            "scenes": [],
                        },
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\r\n"
                )
                await writer.drain()

                poll_request = parse_line(await reader.readuntil(b"\r\n"))
                received.append(poll_request)
                writer.write(
                    json.dumps(
                        {
                            "id": poll_request["id"],
                            "nodes": [{"id": "light-1", "nt": 2, "params": {"p": True}}],
                        },
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\r\n"
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)
        gateway.full_prop_timeout = 0.01

        try:
            await gateway.connect()
            await gateway.sync()
        finally:
            await gateway.close()

        self.assertEqual([request["method"] for request in received], ["gateway_get.topology", "gateway_get.node"])
        self.assertEqual(received[1]["params"], {"id": 0})
        self.assertEqual(gateway.state.nodes["light-1"].params, {"p": True})
        self.assertEqual(gateway.last_full_sync_source, "poll")

    async def test_topology_push_full_property_wait_falls_back_to_poll(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                writer.write(
                    b'{"method":"gateway_post.topology",'
                    b'"nodes":[{"id":"light-1","nt":2,"type":3,"name":"Light"}],'
                    b'"groups":[],"rooms":[],"scenes":[]}\r\n'
                )
                await writer.drain()

                poll_request = parse_line(await reader.readuntil(b"\r\n"))
                received.append(poll_request)
                writer.write(
                    json.dumps(
                        {
                            "id": poll_request["id"],
                            "nodes": [{"id": "light-1", "nt": 2, "params": {"p": True}}],
                        },
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\r\n"
                )
                await writer.drain()
                await asyncio.sleep(0.05)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)
        gateway.full_prop_timeout = 0.01

        try:
            await gateway.connect()
            await asyncio.wait_for(gateway._session.wait_ready(), timeout=0.5)
        finally:
            await gateway.close()

        self.assertEqual([request["method"] for request in received], ["gateway_get.node"])
        self.assertEqual(received[0]["params"], {"id": 0})
        self.assertEqual(gateway.state.nodes["light-1"].params, {"p": True})
        self.assertEqual(gateway.last_full_sync_source, "poll")

    async def test_concurrent_sync_requests_join_current_sync(self) -> None:
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                topology_request = parse_line(await reader.readuntil(b"\r\n"))
                received.append(topology_request)
                writer.write(
                    json.dumps(
                        {
                            "id": topology_request["id"],
                            "nodes": [{"id": "light-1", "nt": 2, "type": 3, "name": "Light"}],
                            "groups": [],
                            "rooms": [],
                            "scenes": [],
                        },
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\r\n"
                )
                await writer.drain()

                poll_request = parse_line(await reader.readuntil(b"\r\n"))
                received.append(poll_request)
                writer.write(
                    json.dumps(
                        {
                            "id": poll_request["id"],
                            "nodes": [{"id": "light-1", "nt": 2, "params": {"p": True}}],
                        },
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\r\n"
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)
        gateway.full_prop_timeout = 0.01

        try:
            await gateway.connect()
            await asyncio.gather(gateway.sync(), gateway.sync())
        finally:
            await gateway.close()

        self.assertEqual([request["method"] for request in received], ["gateway_get.topology", "gateway_get.node"])

    async def test_start_propagates_initial_sync_failure(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readuntil(b"\r\n")
                await asyncio.sleep(1)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, request_timeout=0.05)

        try:
            with self.assertRaises(RequestTimeout):
                await asyncio.wait_for(gateway.start(), timeout=0.5)
        finally:
            await gateway.close()

    async def test_stale_epoch_push_is_ignored_after_reconnect(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readline()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port)

        try:
            gateway.state.apply_topology(
                {
                    "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False}}],
                    "groups": [],
                    "rooms": [],
                    "scenes": [],
                }
            )
            await gateway.connect()
            await gateway._session.ref.tell(
                RpcPushEvent(
                    epoch=0,
                    message={"method": "gateway_post.prop", "nodes": [{"id": "light-1", "params": {"p": True}}]},
                )
            )
            await asyncio.sleep(0)
        finally:
            await gateway.close()

        self.assertEqual(gateway.state.nodes["light-1"].params["p"], False)

    async def test_start_runs_connection_supervision_and_initial_sync(self) -> None:
        statuses: list[Any] = []

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
        gateway = YeelightProGateway(host, port=port, reconnect_delay=0.01)
        gateway.add_session_listener(statuses.append)

        try:
            await gateway.start()
        finally:
            await gateway.close()

        self.assertEqual(gateway.state.nodes["light-1"].params, {"p": True})
        self.assertEqual(gateway.session_state, "disconnected")
        self.assertIn("ready", [getattr(status, "current", None) for status in statuses])

    async def test_refresh_reconnect_preserves_background_auto_sync(self) -> None:
        requests_by_connection: list[list[str]] = []
        third_auto_sync = asyncio.Event()
        attempts = 0

        async def handle_sync(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
            *,
            close_after_sync: bool,
            power: bool,
        ) -> None:
            request = parse_line(await reader.readuntil(b"\r\n"))
            requests_by_connection[-1].append(request["method"])
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
                json.dumps(
                    {
                        "method": "gateway_post.prop",
                        "nodes": [{"id": "light-1", "nt": 2, "params": {"p": power}, "o": True}],
                    },
                    separators=(",", ":"),
                ).encode("utf-8")
                + b"\r\n"
            )
            await writer.drain()
            if close_after_sync:
                await asyncio.sleep(0.1)
                writer.close()
                await writer.wait_closed()
                return
            third_auto_sync.set()
            await reader.readline()

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal attempts
            attempts += 1
            requests_by_connection.append([])
            try:
                await handle_sync(
                    reader,
                    writer,
                    close_after_sync=attempts < 3,
                    power=attempts % 2 == 1,
                )
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, reconnect_delay=1.0)

        try:
            await gateway.start()
            await asyncio.wait_for(gateway.wait_closed(), timeout=0.5)

            await gateway.reconnect()
            await gateway.sync()
            await asyncio.wait_for(gateway.wait_closed(), timeout=0.5)

            await asyncio.wait_for(third_auto_sync.wait(), timeout=2.0)
        finally:
            await gateway.close()

        self.assertGreaterEqual(attempts, 3)
        self.assertEqual(requests_by_connection[0], ["gateway_get.topology"])
        self.assertEqual(requests_by_connection[1], ["gateway_get.topology"])
        self.assertEqual(requests_by_connection[2], ["gateway_get.topology"])

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

    async def test_invalid_json_response_closes_connection_and_fails_pending_request(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readuntil(b"\r\n")
                writer.write(b"not-json\r\n")
                await writer.drain()
                await asyncio.sleep(0.05)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        rpc = GatewayRPC(host, port=port)

        try:
            await rpc.connect()
            with self.assertRaises(ProtocolError) as ctx:
                await rpc.request("gateway_get.node", {"params": {"id": 0}})
            await asyncio.wait_for(rpc.wait_closed(), timeout=0.5)
        finally:
            await rpc.close()

        self.assertFalse(rpc.is_connected)
        self.assertIsInstance(rpc.last_disconnect_error, ProtocolError)
        self.assertIn("valid JSON", str(ctx.exception))

    async def test_oversized_response_frame_closes_connection_and_fails_pending_request(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                request = parse_line(await reader.readuntil(b"\r\n"))
                writer.write(
                    json.dumps(
                        {"id": request["id"], "blob": "x" * 256},
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\r\n"
                )
                await writer.drain()
                await asyncio.sleep(0.05)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        rpc = GatewayRPC(host, port=port)
        rpc.max_frame_bytes = 64

        try:
            await rpc.connect()
            with self.assertRaises(ProtocolFrameTooLarge) as ctx:
                await rpc.request("gateway_get.node", {"params": {"id": 0}})
            await asyncio.wait_for(rpc.wait_closed(), timeout=0.5)
        finally:
            await rpc.close()

        self.assertFalse(rpc.is_connected)
        self.assertIsInstance(rpc.last_disconnect_error, ProtocolFrameTooLarge)
        self.assertIn("exceeded 64 bytes", str(ctx.exception))

    async def test_rpc_push_listener_exception_does_not_stop_dispatch(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                writer.write(b'{"method":"gateway_post.prop","nodes":[{"id":"light-1","params":{"p":true}}]}\r\n')
                writer.write(b'{"method":"gateway_post.event","nodes":[{"id":"panel-1","value":"panel.click"}]}\r\n')
                await writer.drain()
                await asyncio.sleep(0.05)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        rpc = GatewayRPC(host, port=port)
        messages: list[Any] = []

        def broken_listener(_message: Any) -> None:
            raise RuntimeError("listener failed")

        try:
            rpc.add_push_listener(broken_listener)
            rpc.add_push_listener(messages.append)
            with self.assertLogs("yeelight_pro.session.transport.rpc", level="ERROR"):
                await rpc.connect()
                await asyncio.sleep(0.1)
        finally:
            await rpc.close()

        self.assertEqual([message["method"] for message in messages], ["gateway_post.prop", "gateway_post.event"])

    async def test_writer_loop_exception_closes_connection_and_fails_request(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readline()
                await asyncio.sleep(1)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        rpc = GatewayRPC(host, port=port, request_timeout=0.2)

        try:
            await rpc.connect()
            with self.assertRaises(ConnectionClosed):
                await rpc.request("gateway_get.node", {"id": 1})
            await asyncio.wait_for(rpc.wait_closed(), timeout=0.5)
            self.assertFalse(rpc.is_connected)
            self.assertIsInstance(rpc.last_disconnect_error, ConnectionClosed)
        finally:
            await rpc.close()

    async def test_request_timeout_marks_connection_closed(self) -> None:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readline()
                await asyncio.sleep(1)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        gateway = YeelightProGateway(host, port=port, request_timeout=0.2)

        try:
            await gateway.connect()
            with self.assertRaises(RequestTimeout):
                await gateway.get_topology()
            await asyncio.wait_for(gateway.wait_closed(), timeout=0.5)
            self.assertFalse(gateway.is_connected)
            self.assertIsInstance(gateway.last_disconnect_error, RequestTimeout)
            self.assertIn("gateway_get.topology", str(gateway.last_disconnect_error))
        finally:
            await gateway.close()

    async def test_rpc_reconnects_after_request_timeout(self) -> None:
        attempts = 0

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal attempts
            attempts += 1
            try:
                request = parse_line(await reader.readuntil(b"\r\n"))
                if attempts == 1:
                    await asyncio.sleep(1)
                    return
                writer.write(
                    json.dumps({"id": request["id"], "result": "ok"}, separators=(",", ":")).encode("utf-8") + b"\r\n"
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        rpc = GatewayRPC(host, port=port, request_timeout=0.05)

        try:
            await rpc.connect()
            with self.assertRaises(RequestTimeout):
                await rpc.request("gateway_get.topology")
            self.assertFalse(rpc.is_connected)

            await rpc.connect()
            response = await rpc.request("gateway_get.topology")
        finally:
            await rpc.close()

        self.assertEqual(response["result"], "ok")

    async def test_connect_timeout_reports_gateway_endpoint(self) -> None:
        async def stalled_open_connection(
            *_args: Any, **_kwargs: Any
        ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            await asyncio.sleep(1)
            raise AssertionError("unreachable")

        rpc = GatewayRPC("192.0.2.1", port=65443, request_timeout=0.01)
        with patch("yeelight_pro.session.transport.rpc.asyncio.open_connection", side_effect=stalled_open_connection):
            with self.assertRaises(RequestTimeout) as ctx:
                await rpc.connect()

        self.assertIn("timed out connecting to 192.0.2.1:65443", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
