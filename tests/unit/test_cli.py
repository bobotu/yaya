from __future__ import annotations

import asyncio
import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "custom_components"))

from dev_tools.yeelight_pro_cli import async_main, main, parse_value  # noqa: E402
from yeelight_pro.gateway import parse_line  # noqa: E402


class CliEntryPointTests(unittest.TestCase):
    def test_parse_value(self) -> None:
        self.assertIs(parse_value("true"), True)
        self.assertIs(parse_value("off"), False)
        self.assertIsNone(parse_value("null"))
        self.assertEqual(parse_value("42"), 42)
        self.assertEqual(parse_value("3.5"), 3.5)
        self.assertEqual(parse_value("bedroom"), "bedroom")

    def test_main_returns_130_on_keyboard_interrupt(self) -> None:
        def interrupted(coro: Any) -> int:
            coro.close()
            raise KeyboardInterrupt

        with patch("dev_tools.yeelight_pro_cli.asyncio.run", side_effect=interrupted):
            self.assertEqual(main(["watch", "--host", "127.0.0.1"]), 130)


class CliTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_list_json_against_fake_gateway(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    line = await reader.readline()
                    if line == b"":
                        return
                    request = parse_line(line)
                    await _write_gateway_response(writer, request, fixture)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = await async_main(["list", "--host", host, "--port", str(port), "--json"])

        devices = json.loads(output.getvalue())
        self.assertEqual(rc, 0)
        self.assertIn("curtain-1", {device["id"] for device in devices})
        self.assertIn("multi_switch", {device["category"] for device in devices})
        self.assertNotIn("group-node-1", {device["id"] for device in devices})
        light = next(device for device in devices if device["id"] == "light-1")
        self.assertEqual(light["room_id"], "room-1")
        self.assertEqual(light["room"], "Kitchen")

    async def test_list_fetches_rooms_when_topology_omits_room_records(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())
        topology_fixture = {**fixture, "rooms": []}
        methods: list[str] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    line = await reader.readline()
                    if line == b"":
                        return
                    request = parse_line(line)
                    methods.append(request["method"])
                    if request["method"] == "gateway_get.room":
                        await _write_json_line(writer, {"id": request["id"], "rooms": fixture["rooms"]})
                    else:
                        await _write_gateway_response(writer, request, topology_fixture)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = await async_main(["list", "--host", host, "--port", str(port), "--json"])

        devices = json.loads(output.getvalue())
        light = next(device for device in devices if device["id"] == "light-1")
        self.assertEqual(rc, 0)
        self.assertIn("gateway_get.room", methods)
        self.assertEqual(light["room"], "Kitchen")

    async def test_list_can_include_group_nodes(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    line = await reader.readline()
                    if line == b"":
                        return
                    request = parse_line(line)
                    await _write_gateway_response(writer, request, fixture)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = await async_main(["list", "--host", host, "--port", str(port), "--include-groups", "--json"])

        devices = json.loads(output.getvalue())
        group = next(device for device in devices if device["id"] == "group-node-1")
        self.assertEqual(rc, 0)
        self.assertEqual(group["nt"], 4)
        self.assertEqual(group["room"], "Kitchen")

    async def test_command_set_channel_sends_expected_payload(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    line = await reader.readline()
                    if line == b"":
                        return
                    request = parse_line(line)
                    received.append(request)
                    if request["method"] == "gateway_set.prop":
                        response = {"id": request["id"], "result": "ok"}
                        await _write_json_line(writer, response)
                    else:
                        await _write_gateway_response(writer, request, fixture)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = await async_main(
                [
                    "command",
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "--id",
                    "switch-1",
                    "set-channel",
                    "--channel",
                    "2",
                    "--value",
                    "false",
                    "--json",
                ]
            )

        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(output.getvalue()), {"result": "ok", "id": received[-1]["id"]})
        self.assertEqual(received[-1]["method"], "gateway_set.prop")
        self.assertEqual(received[-1]["nodes"], [{"id": "switch-1", "nt": 2, "set": {"2-sp": False}}])

    async def test_command_blink_sends_expected_payload(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())
        received: list[dict[str, Any]] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    line = await reader.readline()
                    if line == b"":
                        return
                    request = parse_line(line)
                    received.append(request)
                    if request["method"] == "gateway_set.prop":
                        await _write_json_line(writer, {"id": request["id"], "result": "ok"})
                    else:
                        await _write_gateway_response(writer, request, fixture)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = await async_main(
                [
                    "command",
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "--id",
                    "light-1",
                    "blink",
                    "--blink-type",
                    "urgent",
                    "--repeat",
                    "2",
                    "--json",
                ]
            )

        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(output.getvalue()), {"result": "ok", "id": received[-1]["id"]})
        self.assertEqual(received[-1]["method"], "gateway_set.prop")
        self.assertEqual(
            received[-1]["nodes"],
            [{"id": "light-1", "nt": 2, "action": {"blink": {"repeat": 2, "type": "urgent"}}}],
        )

    async def test_describe_shows_panel_button_count(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    line = await reader.readline()
                    if line == b"":
                        return
                    request = parse_line(line)
                    await _write_gateway_response(writer, request, fixture)
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = await async_main(["describe", "--host", host, "--port", str(port), "--id", "panel-1"])

        text = output.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("button_count: 4", text)
        self.assertIn("component_type_ids: 1, 2, 3, 4", text)
        self.assertIn("button_event_key: params.key", text)

    async def test_listen_prints_gateway_events(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    line = await reader.readline()
                    if line == b"":
                        return
                    request = parse_line(line)
                    await _write_gateway_response(writer, request, fixture)
                    if request["method"] == "gateway_get.topology":
                        writer.write(
                            b'{"method":"gateway_post.event","nodes":[{"id":"panel-1","nt":2,"value":"panel.click","params":{"key":2,"count":1}}]}\r\n'
                        )
                        await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = await async_main(
                ["listen", "--host", host, "--port", str(port), "--id", "panel-1", "--duration", "1", "--json"]
            )

        event = json.loads(output.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(event["id"], "panel-1")
        self.assertEqual(event["value"], "panel.click")
        self.assertEqual(event["key"], 2)

    async def test_watch_prints_property_changes_and_events(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            async def send_pushes() -> None:
                await asyncio.sleep(0.05)
                writer.write(
                    b'{"method":"gateway_post.prop","nodes":[{"id":"curtain-1","nt":2,"params":{"cp":25},"o":true}]}\r\n'
                )
                writer.write(
                    b'{"method":"gateway_post.event","nodes":[{"id":"panel-1","nt":2,"value":"panel.click","params":{"key":2,"count":1}}]}\r\n'
                )
                with contextlib.suppress(ConnectionError, OSError):
                    await writer.drain()

            try:
                while True:
                    line = await reader.readline()
                    if line == b"":
                        return
                    request = parse_line(line)
                    await _write_gateway_response(writer, request, fixture)
                    if request["method"] == "gateway_get.room":
                        asyncio.create_task(send_pushes())
            finally:
                writer.close()
                await writer.wait_closed()

        host, port = await self.start_gateway(handler)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = await async_main(["watch", "--host", host, "--port", str(port), "--duration", "0.2", "--settle", "0"])

        text = output.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("Watching gateway", text)
        self.assertIn("[property] Dream curtain (id=curtain-1, room_id=room-2", text)
        self.assertIn('"cp":{"after":25,"before":20}', text)
        self.assertIn('"online":{"after":true,"before":null}', text)
        self.assertIn('update:  {"id":"curtain-1","nt":2,"o":true,"params":{"cp":25}}', text)
        self.assertIn('"cp":20', text)
        self.assertIn('"cp":25', text)
        self.assertIn("[event] Scene panel (id=panel-1, room_id=none, nt=2) value=panel.click", text)


def _response_for(request: dict[str, Any], fixture: dict[str, Any]) -> dict[str, Any]:
    method = request["method"]
    if method == "gateway_get.topology":
        return {"id": request["id"], **fixture}
    if method == "gateway_get.group":
        return {"id": request["id"], "groups": fixture["groups"]}
    if method == "gateway_get.room":
        return {"id": request["id"], "rooms": fixture["rooms"]}
    if method == "gateway_get.scene":
        return {"id": request["id"], "scenes": fixture["scenes"]}
    if method == "gateway_get.node":
        node_id = request.get("params", {}).get("id")
        if node_id == 0:
            return {"id": request["id"], "nodes": fixture["nodes"]}
        return {"id": request["id"], "nodes": [node for node in fixture["nodes"] if node["id"] == node_id]}
    return {"id": request["id"], "result": "ok"}


async def _write_gateway_response(
    writer: asyncio.StreamWriter,
    request: dict[str, Any],
    fixture: dict[str, Any],
) -> None:
    await _write_json_line(writer, _response_for(request, fixture))
    if request["method"] == "gateway_get.topology":
        await _write_json_line(writer, {"method": "gateway_post.prop", "nodes": fixture["nodes"]})


async def _write_json_line(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
    writer.write(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    writer.write(b"\r\n")
    await writer.drain()


if __name__ == "__main__":
    unittest.main()
