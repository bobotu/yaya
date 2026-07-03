from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "custom_components"))

from yeelight_pro.core import (  # noqa: E402
    BlinkType,
    MotorAction,
    NodeCommand,
    Topology,
    TopologyNode,
    capabilities_for_node,
)
from yeelight_pro.core.devices import (  # noqa: E402
    AirConditionDevice,
    BathHeaterDevice,
    DoubleSwitchDevice,
    DreamCurtainDevice,
    KnobDevice,
    LightDevice,
    MotionSensorDevice,
    MultiSwitchDevice,
    ProgrammableSwitchDevice,
    create_device,
)


class FakeExecutor:
    def __init__(self) -> None:
        self.commands: list[NodeCommand] = []

    async def send_node_command(self, command: NodeCommand) -> dict[str, Any]:
        self.commands.append(command)
        return {"result": "ok"}


class DeviceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())
        self.nodes = {node.id: node for node in Topology.from_message(fixture).nodes}
        self.executor = FakeExecutor()

    async def test_factory_covers_home_device_types(self) -> None:
        expected = {
            "light-1": LightDevice,
            "curtain-1": DreamCurtainDevice,
            "switch-1": MultiSwitchDevice,
            "panel-1": ProgrammableSwitchDevice,
            "knob-1": KnobDevice,
            "air-1": AirConditionDevice,
            "bath-1": BathHeaterDevice,
            "sensor-1": MotionSensorDevice,
            "double-switch-1": DoubleSwitchDevice,
        }

        for node_id, device_class in expected.items():
            with self.subTest(node_id=node_id):
                self.assertIsInstance(create_device(self.nodes[node_id], self.executor), device_class)

    async def test_control_panel_with_knob_property_type_is_knob_capable(self) -> None:
        node = TopologyNode.from_mapping(
            {
                "id": "knob-panel-1",
                "nt": 2,
                "type": 128,
                "pt": 137,
                "name": "Knob panel",
                "ch_num": 4,
            }
        )

        device = create_device(node, self.executor)
        capabilities = capabilities_for_node(node)

        self.assertIsInstance(device, KnobDevice)
        self.assertEqual(capabilities.category, "knob")
        self.assertIn("knob.spin", capabilities.events)

    async def test_curtain_with_dream_property_type_has_tilt_capabilities(self) -> None:
        node = TopologyNode.from_mapping(
            {
                "id": "dream-curtain-1",
                "nt": 2,
                "type": 6,
                "pt": 22,
                "name": "Dream curtain",
                "params": {"cp": 0, "tp": 0, "cra": 90, "tra": 90},
            }
        )

        device = create_device(node, self.executor)
        capabilities = capabilities_for_node(node)

        self.assertIsInstance(device, DreamCurtainDevice)
        self.assertEqual(capabilities.category, "cover")
        self.assertIn("tra", capabilities.writable_properties)

    async def test_multi_switch_capabilities_only_advertise_present_relay_props(self) -> None:
        node = TopologyNode.from_mapping(
            {
                "id": "wireless-switch-1",
                "nt": 2,
                "type": 13,
                "pt": 13,
                "name": "Wireless switch",
                "ch_num": 3,
                "params": {"0-blp": True},
            }
        )

        capabilities = capabilities_for_node(node)

        self.assertEqual(capabilities.readable_properties, ("0-blp",))
        self.assertEqual(capabilities.writable_properties, ("0-blp",))
        self.assertEqual(capabilities.events, ("panel.click", "panel.hold", "panel.release"))

    async def test_double_switch_capabilities_include_panel_events(self) -> None:
        node = TopologyNode.from_mapping(
            {
                "id": "double-switch-2",
                "nt": 2,
                "type": 7,
                "pt": 7,
                "name": "Double switch",
                "params": {"1-p": True, "2-p": False},
            }
        )

        capabilities = capabilities_for_node(node)

        self.assertEqual(capabilities.category, "relay_switch")
        self.assertEqual(capabilities.events, ("panel.click", "panel.hold", "panel.release"))

    async def test_light_commands(self) -> None:
        light = create_device(self.nodes["light-1"], self.executor)
        assert isinstance(light, LightDevice)

        await light.turn_on(brightness=55, color_temperature=3000, duration=1500)
        await light.turn_off()
        await light.set_color_temperature(2700)
        await light.set_color_temperature(6500)
        await light.blink(blink_type=BlinkType.URGENT, repeat=3)

        self.assertEqual(self.executor.commands[0].to_payload()["set"], {"p": True, "l": 55, "ct": 3000})
        self.assertEqual(self.executor.commands[0].to_payload()["duration"], 1500)
        self.assertEqual(self.executor.commands[1].to_payload()["set"], {"p": False})
        self.assertEqual(self.executor.commands[2].to_payload()["set"], {"ct": 2700})
        self.assertEqual(self.executor.commands[3].to_payload()["set"], {"ct": 6500})
        self.assertEqual(
            self.executor.commands[4].to_payload()["action"],
            {"blink": {"repeat": 3, "type": str(BlinkType.URGENT)}},
        )
        with self.assertRaises(ValueError):
            await light.set_color_temperature(2600)
        with self.assertRaises(ValueError):
            await light.set_color_temperature(6501)

    async def test_dream_curtain_position_angle_and_stop(self) -> None:
        curtain = create_device(self.nodes["curtain-1"], self.executor)
        assert isinstance(curtain, DreamCurtainDevice)

        await curtain.set_position(40)
        await curtain.set_angle(90)
        await curtain.stop()

        self.assertEqual(curtain.current_position, 20)
        self.assertTrue(curtain.is_route_calibrated)
        self.assertEqual(self.executor.commands[0].to_payload()["set"], {"tp": 40})
        self.assertEqual(self.executor.commands[1].to_payload()["set"], {"tra": 90})
        self.assertEqual(
            self.executor.commands[2].to_payload()["action"],
            {"motorAdjust": {"type": str(MotorAction.PAUSE)}},
        )

    async def test_switch_air_condition_and_sensor_models(self) -> None:
        switch = create_device(self.nodes["switch-1"], self.executor)
        air = create_device(self.nodes["air-1"], self.executor)
        sensor = create_device(self.nodes["sensor-1"], self.executor)
        assert isinstance(switch, MultiSwitchDevice)
        assert isinstance(air, AirConditionDevice)
        assert isinstance(sensor, MotionSensorDevice)

        await switch.set_channel(2, False)
        await air.set_target_temperature(23)
        await air.set_fan_speed(0)

        self.assertEqual(switch.channels, ("1-sp", "2-sp"))
        self.assertTrue(switch.backlight)
        self.assertTrue(sensor.is_motion_detected)
        self.assertEqual(sensor.battery_percent, 91)
        self.assertEqual(self.executor.commands[0].to_payload()["set"], {"2-sp": False})
        self.assertEqual(self.executor.commands[1].to_payload()["set"], {"1-actt": 23})
        self.assertEqual(self.executor.commands[2].to_payload()["set"], {"1-acf": 0})


if __name__ == "__main__":
    unittest.main()
