from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from yeelight_pro.gateway.capabilities import capabilities_for_node
from yeelight_pro.gateway.commands import BlinkType, MotorAction
from yeelight_pro.gateway.devices import (
    AirConditionDevice,
    BathHeaterDevice,
    CurtainDevice,
    DoubleSwitchDevice,
    DreamCurtainDevice,
    LightDevice,
)
from yeelight_pro.gateway.discovery import discover_gateways
from yeelight_pro.gateway.topology import NodeType, TopologyNode
from yeelight_pro.gateway.updates import PropertyChange
from yeelight_pro.session import YeelightProGateway


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        return 130


async def async_main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return await args.func(args)
    except asyncio.CancelledError:
        return 130
    except KeyboardInterrupt:
        return 130


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="yeelight-pro",
        description="Inspect and test Yeelight Pro gateways over the local LAN protocol.",
    )
    subparsers = parser.add_subparsers(dest="command_name", required=True)

    discover = subparsers.add_parser("discover", help="Broadcast a read-only gateway discovery probe")
    discover.add_argument("--timeout", type=float, default=3.0)
    discover.add_argument("--target-host", default="255.255.255.255")
    discover.add_argument("--json", action="store_true")
    discover.set_defaults(func=_cmd_discover)

    list_devices = subparsers.add_parser("list", aliases=["list-devices"], help="List devices from a gateway")
    _add_gateway_args(list_devices)
    list_devices.add_argument(
        "--raw-devices-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only show raw mesh devices and hide groups; enabled by default",
    )
    list_devices.add_argument(
        "--include-groups",
        action="store_false",
        dest="raw_devices_only",
        help="Show group nodes as well",
    )
    list_devices.add_argument("--json", action="store_true")
    list_devices.set_defaults(func=_cmd_list)

    describe = subparsers.add_parser("describe", help="Describe one device and its supported actions")
    _add_gateway_args(describe)
    describe.add_argument("--id", required=True, help="Device node id")
    describe.add_argument("--json", action="store_true")
    describe.set_defaults(func=_cmd_describe)

    listen = subparsers.add_parser("listen", help="Listen for gateway_post.event pushes over the TCP connection")
    _add_gateway_args(listen)
    listen.add_argument("--id", help="Only print events from one device node id")
    listen.add_argument("--duration", type=float, help="Stop after this many seconds")
    listen.add_argument("--json", action="store_true")
    listen.set_defaults(func=_cmd_listen)

    watch = subparsers.add_parser("watch", help="Watch gateway property updates and events")
    _add_gateway_args(watch)
    watch.add_argument("--duration", type=float, help="Stop after this many seconds")
    watch.add_argument(
        "--settle",
        type=float,
        default=1.0,
        help="Absorb startup property pushes for this many seconds before printing",
    )
    watch.set_defaults(func=_cmd_watch)

    command = subparsers.add_parser("command", help="Send an explicit test command to one device")
    _add_gateway_args(command)
    command.add_argument("--id", required=True, help="Device node id")
    command.add_argument("action", help="Action name shown by the describe command")
    command.add_argument("--prop", action="append", default=[], metavar="KEY=VALUE", help="Generic property write")
    command.add_argument("--value", help="Action value")
    command.add_argument("--channel", type=int, help="Switch channel number")
    command.add_argument("--position", type=int, help="Curtain position 0-100")
    command.add_argument("--angle", type=int, help="Dream curtain angle 0-180")
    command.add_argument("--brightness", type=int, help="Light brightness 1-100")
    command.add_argument("--color-temperature", type=int, help="Light color temperature")
    command.add_argument("--color", type=int, help="Light color as RGB integer")
    command.add_argument("--duration", type=int, help="Transition duration in milliseconds")
    command.add_argument("--blink-type", choices=[item.value for item in BlinkType], help="Native blink action type")
    command.add_argument("--repeat", type=int, default=4, help="Native blink repeat count")
    command.add_argument("--index", type=int, default=1, help="Air conditioner channel index")
    command.add_argument(
        "--motor-action", choices=[item.value for item in MotorAction], help="Native motorAdjust action"
    )
    command.add_argument("--json", action="store_true")
    command.set_defaults(func=_cmd_command)

    return parser


def _add_gateway_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", required=True, help="Gateway IP address")
    parser.add_argument("--port", type=int, default=65443)
    parser.add_argument("--timeout", type=float, default=5.0, help="Request timeout in seconds")


async def _cmd_discover(args: argparse.Namespace) -> int:
    gateways = await discover_gateways(timeout=args.timeout, target_host=args.target_host)
    rows = [
        {
            "host": gateway.host,
            "ip": gateway.ip,
            "pid": gateway.pid,
            "mac": gateway.mac,
            "did": gateway.did,
            "fields": gateway.fields,
        }
        for gateway in gateways
    ]
    if args.json:
        _print_json(rows)
    else:
        _print_table(rows, ("ip", "mac", "did", "pid"))
    return 0


async def _cmd_list(args: argparse.Namespace) -> int:
    async with YeelightProGateway(args.host, port=args.port, request_timeout=args.timeout) as gateway:
        await gateway.sync(include_groups=not args.raw_devices_only, include_rooms=True)
        rows = [
            _node_summary(node, gateway)
            for node in gateway.visible_nodes()
            if _should_list_node(node, raw_devices_only=args.raw_devices_only)
        ]

    if args.json:
        _print_json(rows)
    else:
        _print_table(rows, ("id", "name", "room", "type", "pt", "nt", "category", "online"))
    return 0


async def _cmd_describe(args: argparse.Namespace) -> int:
    async with YeelightProGateway(args.host, port=args.port, request_timeout=args.timeout) as gateway:
        await gateway.sync(include_groups=True, include_rooms=True)
        node_id = _lookup_node_id(gateway, args.id)
        if gateway.visible_node(node_id) is not None:
            await gateway.readback_node(node_id)
        node = gateway.visible_node(node_id)
        if node is None:
            raise SystemExit(f"device not found: {args.id}")
        summary = _node_detail(node, gateway)

    if args.json:
        _print_json(summary)
    else:
        _print_device_detail(summary)
    return 0


async def _cmd_listen(args: argparse.Namespace) -> int:
    event_seen = asyncio.Event()

    async with YeelightProGateway(args.host, port=args.port, request_timeout=args.timeout) as gateway:

        def on_event(event: Any) -> None:
            if args.id is not None and str(event.id) != str(args.id):
                return
            event_seen.set()
            if args.json:
                print(json.dumps(event.as_dict(), ensure_ascii=False, sort_keys=True), flush=True)
            else:
                params = json.dumps(dict(event.params), ensure_ascii=False, sort_keys=True)
                print(
                    f"id={event.id} nt={event.nt} value={event.value} params={params}",
                    flush=True,
                )

        gateway.add_event_listener(on_event)
        await gateway.sync(include_rooms=True)
        if args.duration is None:
            print("Listening for events. Press Ctrl+C to stop.", flush=True)
            await asyncio.Event().wait()
        elif args.duration <= 0:
            return 0
        else:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(event_seen.wait(), timeout=args.duration)
    return 0


async def _cmd_watch(args: argparse.Namespace) -> int:
    watching = False

    async with YeelightProGateway(args.host, port=args.port, request_timeout=args.timeout) as gateway:

        def on_property(change: PropertyChange) -> None:
            if not watching:
                return
            _print_property_change(change, gateway)

        def on_event(event: Any) -> None:
            if not watching:
                return
            _print_gateway_event(event, gateway)

        gateway.add_property_listener(on_property)
        gateway.add_event_listener(on_event)
        await gateway.sync(include_rooms=True)

        if args.settle > 0:
            print(f"Initial sync complete. Absorbing startup pushes for {args.settle:g}s.", flush=True)
            await asyncio.sleep(args.settle)
        watching = True
        print(f"Watching gateway {args.host}. Press Ctrl+C to stop.", flush=True)
        if args.duration is None:
            await asyncio.Event().wait()
        elif args.duration > 0:
            await asyncio.sleep(args.duration)
    return 0


async def _cmd_command(args: argparse.Namespace) -> int:
    async with YeelightProGateway(args.host, port=args.port, request_timeout=args.timeout) as gateway:
        await gateway.sync(include_rooms=True)
        device = gateway.device(_lookup_node_id(gateway, args.id))
        if device is None:
            raise SystemExit(f"device not found: {args.id}")
        response = await _send_device_command(device, args)

    if args.json:
        _print_json(response)
    else:
        _print_json(response)
    return 0


async def _send_device_command(device: Any, args: argparse.Namespace) -> dict[str, Any]:
    action = args.action
    if action == "set-prop":
        return await device.set_props(_parse_props(args.prop), duration=args.duration)

    if isinstance(device, LightDevice):
        if action == "turn-on":
            return await device.turn_on(
                brightness=args.brightness,
                color_temperature=args.color_temperature,
                color=args.color,
                duration=args.duration,
            )
        if action == "turn-off":
            return await device.turn_off(duration=args.duration)
        if action == "set-brightness":
            return await device.set_brightness(_required_int(args.value, "value"), duration=args.duration)
        if action == "set-color-temperature":
            return await device.set_color_temperature(_required_int(args.value, "value"), duration=args.duration)
        if action == "set-color":
            return await device.set_color(_required_int(args.value, "value"), duration=args.duration)
        if action == "blink":
            return await device.blink(blink_type=args.blink_type or args.value or BlinkType.NOTIFY, repeat=args.repeat)

    if isinstance(device, CurtainDevice):
        if action == "open":
            return await device.open(duration=args.duration)
        if action == "close":
            return await device.close(duration=args.duration)
        if action == "stop":
            return await device.stop()
        if action == "set-position":
            position = args.position if args.position is not None else _required_int(args.value, "value")
            return await device.set_position(position, duration=args.duration)
        if action == "motor-adjust":
            motor_action = args.motor_action or args.value
            if motor_action is None:
                raise SystemExit("motor-adjust requires --motor-action or --value")
            return await device.motor_adjust(motor_action)

    if isinstance(device, DreamCurtainDevice):
        if action == "set-angle":
            angle = args.angle if args.angle is not None else _required_int(args.value, "value")
            return await device.set_angle(angle, duration=args.duration)
        if action == "open-tilt":
            return await device.open_tilt(duration=args.duration)
        if action == "close-tilt":
            return await device.close_tilt(duration=args.duration)
        if action == "stop-tilt":
            return await device.stop_tilt()

    if hasattr(device, "set_channel") and action == "set-channel":
        if args.channel is None:
            raise SystemExit("set-channel requires --channel")
        return await device.set_channel(args.channel, _required_bool(args.value, "value"))

    if isinstance(device, DoubleSwitchDevice) and action == "set-all":
        return await device.set_all(_required_bool(args.value, "value"))

    if isinstance(device, AirConditionDevice):
        if action == "ac-power":
            return await device.set_power(_required_bool(args.value, "value"), index=args.index)
        if action == "ac-mode":
            return await device.set_mode(_required_int(args.value, "value"), index=args.index)
        if action == "ac-temp":
            return await device.set_target_temperature(_required_int(args.value, "value"), index=args.index)
        if action == "ac-fan":
            return await device.set_fan_speed(_required_int(args.value, "value"), index=args.index)
        if action == "ac-delay":
            return await device.set_delay(_required_int(args.value, "value"), index=args.index)
        if action == "ac-deflector":
            return await device.set_deflector(_required_int(args.value, "value"), index=args.index)
        if action == "ac-remote":
            return await device.set_remote_controller(_required_bool(args.value, "value"), index=args.index)

    if isinstance(device, BathHeaterDevice):
        if action == "bath-power":
            return await device.set_power(_required_bool(args.value, "value"))
        if action == "bath-mode":
            return await device.set_mode(_required_int(args.value, "value"))
        if action == "bath-delay-off":
            return await device.set_delay_off(_required_int(args.value, "value"))
        if action == "bath-ventilation":
            return await device.set_ventilation(_required_int(args.value, "value"))
        if action == "bath-fan":
            return await device.set_fan(_required_int(args.value, "value"))
        if action == "bath-heat":
            return await device.set_heat(_required_int(args.value, "value"))
        if action == "bath-temp":
            return await device.set_target_temperature(_required_int(args.value, "value"))

    raise SystemExit(f"action {action!r} is not supported by device {device.id}")


def _node_summary(node: TopologyNode, gateway: YeelightProGateway | None = None) -> dict[str, Any]:
    capabilities = capabilities_for_node(node)
    room_id = gateway.room_id_for_node(node) if gateway is not None else node.room_id
    return {
        "id": node.id,
        "name": node.name,
        "room_id": room_id,
        "room": gateway.room_name(room_id) if gateway is not None else None,
        "type": node.type,
        "pt": node.property_type,
        "nt": node.nt,
        "category": capabilities.category,
        "online": node.online,
        "channel_count": node.channel_count,
        "component_type_ids": list(node.component_type_ids),
    }


def _print_property_change(change: PropertyChange, gateway: YeelightProGateway) -> None:
    node = change.after
    room_id = gateway.room_id_for_node(node)
    print(
        f"[property] {_node_display_name(node)} (id={node.id}, room_id={_display_optional(room_id)}, "
        f"nt={node.nt}, type={node.type}, pt={_display_optional(node.property_type)})",
        flush=True,
    )
    print(f"changed: {_compact_json(_node_diff(change.before, change.after))}", flush=True)
    print(f"update:  {_compact_json(dict(change.update))}", flush=True)
    print(f"before: {_compact_json(_node_state(change.before))}", flush=True)
    print(f"after:  {_compact_json(_node_state(change.after))}", flush=True)


def _print_gateway_event(event: Any, gateway: YeelightProGateway) -> None:
    node = gateway.visible_node(event.id)
    room_id = gateway.room_id_for_node(node) if node is not None else None
    name = _node_display_name(node) if node is not None else "<unknown>"
    print(
        f"[event] {name} (id={event.id}, room_id={_display_optional(room_id)}, nt={event.nt}) "
        f"value={event.value} params={_compact_json(dict(event.params))}",
        flush=True,
    )


def _node_state(node: TopologyNode | None) -> dict[str, Any] | None:
    if node is None:
        return None
    return {
        "name": node.name,
        "nt": node.nt,
        "type": node.type,
        "pt": node.property_type,
        "online": node.online,
        "params": dict(node.params),
    }


def _node_diff(before: TopologyNode | None, after: TopologyNode) -> dict[str, Any]:
    if before is None:
        return {"created": True, "params": dict(after.params)}

    diff: dict[str, Any] = {}
    for key, before_value, after_value in (
        ("name", before.name, after.name),
        ("nt", before.nt, after.nt),
        ("type", before.type, after.type),
        ("pt", before.property_type, after.property_type),
        ("room_id", before.room_id, after.room_id),
        ("online", before.online, after.online),
    ):
        if before_value != after_value:
            diff[key] = {"before": before_value, "after": after_value}

    params: dict[str, Any] = {}
    for key in sorted(set(before.params) | set(after.params)):
        before_value = before.params.get(key)
        after_value = after.params.get(key)
        if before_value != after_value:
            params[key] = {"before": before_value, "after": after_value}
    if params:
        diff["params"] = params
    return diff


def _node_display_name(node: TopologyNode) -> str:
    return node.name or "<unnamed>"


def _display_optional(value: object) -> str:
    return "none" if value is None else str(value)


def _compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _should_list_node(node: TopologyNode, *, raw_devices_only: bool) -> bool:
    if not raw_devices_only:
        return True
    return node.nt == NodeType.MESH_SUBDEVICE


def _node_detail(node: TopologyNode, gateway: YeelightProGateway | None = None) -> dict[str, Any]:
    return {
        **_node_summary(node, gateway),
        "params": dict(node.params),
        "capabilities": capabilities_for_node(node).as_dict(),
    }


def _lookup_node(gateway: YeelightProGateway, raw_id: str) -> TopologyNode | None:
    node_id = _lookup_node_id(gateway, raw_id)
    return gateway.visible_node(node_id)


def _lookup_node_id(gateway: YeelightProGateway, raw_id: str) -> str | int:
    if gateway.visible_node(raw_id) is not None:
        return raw_id
    try:
        int_id = int(raw_id)
    except ValueError:
        return raw_id
    return int_id if gateway.visible_node(int_id) is not None else raw_id


def _parse_props(values: Sequence[str]) -> dict[str, Any]:
    if not values:
        raise SystemExit("set-prop requires at least one --prop KEY=VALUE")
    props: dict[str, Any] = {}
    for item in values:
        if "=" not in item:
            raise SystemExit(f"invalid property assignment: {item}")
        key, value = item.split("=", 1)
        if not key:
            raise SystemExit(f"invalid property assignment: {item}")
        props[key] = parse_value(value)
    return props


def parse_value(value: str) -> Any:
    lowered = value.strip().lower()
    if lowered in {"true", "on", "yes"}:
        return True
    if lowered in {"false", "off", "no"}:
        return False
    if lowered in {"none", "null"}:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _required_int(value: str | None, name: str) -> int:
    if value is None:
        raise SystemExit(f"{name} is required")
    parsed = parse_value(value)
    if isinstance(parsed, bool) or not isinstance(parsed, int):
        raise SystemExit(f"{name} must be an integer")
    return parsed


def _required_bool(value: str | None, name: str) -> bool:
    if value is None:
        raise SystemExit(f"{name} is required")
    parsed = parse_value(value)
    if not isinstance(parsed, bool):
        raise SystemExit(f"{name} must be true/false")
    return parsed


def _print_device_detail(summary: dict[str, Any]) -> None:
    print(f"id: {summary['id']}")
    print(f"name: {summary['name']}")
    print(f"room: {summary['room'] or '-'}")
    print(f"room_id: {summary['room_id'] if summary['room_id'] is not None else '-'}")
    print(f"type: {summary['type']}")
    print(f"pt: {summary['pt'] if summary['pt'] is not None else '-'}")
    print(f"nt: {summary['nt']}")
    print(f"category: {summary['category']}")
    print(f"button_count: {summary['channel_count'] if summary['channel_count'] is not None else '-'}")
    print("component_type_ids:", ", ".join(str(item) for item in summary["component_type_ids"]) or "-")
    if summary["capabilities"]["events"]:
        print("button_event_key: params.key")
    print("readable:", ", ".join(summary["capabilities"]["readable_properties"]) or "-")
    print("writable:", ", ".join(summary["capabilities"]["writable_properties"]) or "-")
    print("events:", ", ".join(summary["capabilities"]["events"]) or "-")
    print("commands:")
    for command in summary["capabilities"]["commands"]:
        args = ", ".join(command["arguments"])
        suffix = f" ({args})" if args else ""
        print(f"  {command['name']}{suffix}: {command['description']}")


def _print_table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
    if not rows:
        print("No results.")
        return
    widths = {column: len(column) for column in columns}
    for row in rows:
        for column in columns:
            widths[column] = max(widths[column], len(str(row.get(column, ""))))
    print("  ".join(column.ljust(widths[column]) for column in columns))
    print("  ".join("-" * widths[column] for column in columns))
    for row in rows:
        print("  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
