from __future__ import annotations

import importlib
import sys
import types
import unittest
from enum import StrEnum
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CUSTOM_COMPONENTS = ROOT / "custom_components"
INTEGRATION = CUSTOM_COMPONENTS / "yeelight_pro"


class _Platform(StrEnum):
    BUTTON = "button"
    CLIMATE = "climate"
    LIGHT = "light"
    COVER = "cover"
    SWITCH = "switch"
    FAN = "fan"
    NUMBER = "number"
    SELECT = "select"
    SENSOR = "sensor"
    BINARY_SENSOR = "binary_sensor"
    EVENT = "event"


_homeassistant = sys.modules.get("homeassistant")
_homeassistant_const = sys.modules.get("homeassistant.const")
_test_package = types.ModuleType("_yeelight_pro_helper_test")
_test_package.__path__ = [str(INTEGRATION)]  # type: ignore[attr-defined]
sys.modules["_yeelight_pro_helper_test"] = _test_package

try:
    homeassistant = types.ModuleType("homeassistant")
    homeassistant_const = types.ModuleType("homeassistant.const")
    homeassistant_const.ATTR_DEVICE_ID = "device_id"
    homeassistant_const.Platform = _Platform
    sys.modules["homeassistant"] = homeassistant
    sys.modules["homeassistant.const"] = homeassistant_const

    TopologyNode = importlib.import_module("_yeelight_pro_helper_test.core.topology").TopologyNode
    helpers = importlib.import_module("_yeelight_pro_helper_test.helpers")
finally:
    if _homeassistant is None:
        sys.modules.pop("homeassistant", None)
    else:
        sys.modules["homeassistant"] = _homeassistant
    if _homeassistant_const is None:
        sys.modules.pop("homeassistant.const", None)
    else:
        sys.modules["homeassistant.const"] = _homeassistant_const

button_count = helpers.button_count
bool_param = helpers.bool_param
event_subtypes_for_node = helpers.event_subtypes_for_node
event_types_for_node = helpers.event_types_for_node
indexed_props = helpers.indexed_props
int_param = helpers.int_param
relay_channel_numbers = helpers.relay_channel_numbers
should_import_node = helpers.should_import_node
switch_mode_for_node = helpers.switch_mode_for_node
switch_node_is_relay_mode = helpers.switch_node_is_relay_mode
switch_node_is_wireless_mode = helpers.switch_node_is_wireless_mode
true_bool_param = helpers.true_bool_param


class HomeAssistantHelperTests(unittest.TestCase):
    def test_platform_param_helpers_preserve_bool_and_int_semantics(self) -> None:
        node = TopologyNode.from_mapping(
            {
                "id": "helpers",
                "nt": 2,
                "type": 3,
                "params": {
                    "level": 42,
                    "bool_value": True,
                    "int_true": 1,
                    "int_false": 0,
                    "text": "1",
                },
            }
        )

        self.assertEqual(int_param(node, "level"), 42)
        self.assertIsNone(int_param(node, "bool_value"))
        self.assertIsNone(int_param(node, "text"))
        self.assertIs(bool_param(node, "bool_value"), True)
        self.assertIs(bool_param(node, "int_true"), True)
        self.assertIs(bool_param(node, "int_false"), False)
        self.assertIsNone(bool_param(node, "text"))
        self.assertIs(true_bool_param(node, "bool_value"), True)
        self.assertIs(true_bool_param(node, "int_true"), False)

    def test_indexed_props_sorts_numeric_prefixes(self) -> None:
        node = TopologyNode.from_mapping(
            {
                "id": "indexed",
                "nt": 2,
                "type": 10,
                "params": {
                    "10-acrc": True,
                    "2-acrc": True,
                    "not-acrc": True,
                    "3-other": True,
                },
            }
        )

        self.assertEqual(indexed_props(node, "acrc"), ("2-acrc", "10-acrc"))

    def test_knob_panel_without_channel_metadata_exports_wide_idx_fallback(self) -> None:
        node = TopologyNode.from_mapping(
            {
                "id": "knob-panel",
                "nt": 2,
                "type": 128,
                "pt": 137,
                "name": "Knob panel",
            }
        )

        self.assertEqual(button_count(node), 6)
        self.assertIn("knob_spin", event_types_for_node(node))
        self.assertEqual(
            event_subtypes_for_node(node, "knob_spin"),
            ("idx_1", "idx_2", "idx_3", "idx_4", "idx_5", "idx_6"),
        )

    def test_event_types_follow_updated_property_type(self) -> None:
        node = TopologyNode.from_mapping(
            {
                "id": "panel",
                "nt": 2,
                "type": 128,
                "name": "Panel",
            }
        )
        updated = node.merge_update({"pt": 137})

        self.assertNotIn("knob_spin", event_types_for_node(node))
        self.assertIn("knob_spin", event_types_for_node(updated))

    def test_multi_switch_relay_channels_use_explicit_topology_or_present_props(self) -> None:
        topology_channels = TopologyNode.from_mapping(
            {
                "id": "three-key",
                "nt": 2,
                "type": 13,
                "ch_num": 3,
                "params": {"0-blp": True},
            }
        )
        prop_channels = TopologyNode.from_mapping(
            {
                "id": "prop-key",
                "nt": 2,
                "type": 13,
                "params": {"1-sp": True, "3-sp": False},
            }
        )

        self.assertEqual(relay_channel_numbers(topology_channels), (1, 2, 3))
        self.assertEqual(relay_channel_numbers(prop_channels), (1, 3))

    def test_switch_modes_default_to_relay_and_wireless_is_explicit(self) -> None:
        node = TopologyNode.from_mapping({"id": "three-key", "nt": 2, "type": 13, "ch_num": 3})

        self.assertEqual(switch_mode_for_node(node, {}), "relay")
        self.assertTrue(switch_node_is_relay_mode(node, {}))
        self.assertFalse(switch_node_is_wireless_mode(node, {}))

        modes = {"three-key": "wireless"}
        self.assertEqual(switch_mode_for_node(node, modes), "wireless")
        self.assertFalse(switch_node_is_relay_mode(node, modes))
        self.assertTrue(switch_node_is_wireless_mode(node, modes))

        self.assertEqual(switch_mode_for_node(node, {"three-key": "invalid"}), "relay")

    def test_light_groups_are_imported_and_non_light_groups_are_not_imported(self) -> None:
        light_group = TopologyNode.from_mapping({"id": "group-light", "nt": 4, "type": 3})
        scene_group = TopologyNode.from_mapping({"id": "group-scene", "nt": 4, "type": 128})
        subdevice = TopologyNode.from_mapping({"id": "subdevice", "nt": 2, "type": 128})

        self.assertTrue(should_import_node(light_group))
        self.assertFalse(should_import_node(scene_group))
        self.assertTrue(should_import_node(subdevice))

    def test_room_filter_only_imports_selected_explicit_room(self) -> None:
        kitchen = TopologyNode.from_mapping({"id": "kitchen-light", "nt": 2, "type": 3, "roomId": "room-1"})
        bedroom = TopologyNode.from_mapping({"id": "bedroom-light", "nt": 2, "type": 3, "roomId": "room-2"})
        unassigned = TopologyNode.from_mapping({"id": "unassigned-light", "nt": 2, "type": 3})

        self.assertTrue(should_import_node(kitchen, import_room_ids=["room-1"]))
        self.assertFalse(should_import_node(bedroom, import_room_ids=["room-1"]))
        self.assertFalse(should_import_node(unassigned, import_room_ids=["room-1"]))
        self.assertTrue(should_import_node(unassigned, import_room_ids=[]))


if __name__ == "__main__":
    unittest.main()
