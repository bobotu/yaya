from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "custom_components"))

from yeelight_pro.core import (  # noqa: E402
    ProtocolError,
    Topology,
    build_request,
    iter_gateway_events,
    parse_discovery_response,
    parse_line,
)
from yeelight_pro.session.state import GatewayState  # noqa: E402


class ProtocolAndStateTests(unittest.TestCase):
    def test_build_request_uses_gateway_wire_format(self) -> None:
        payload = build_request("gateway_get.topology", request_id=7)

        self.assertTrue(payload.endswith(b"\r\n"))
        self.assertEqual(
            json.loads(payload),
            {"version": "1.0", "id": 7, "method": "gateway_get.topology"},
        )

    def test_parse_line_rejects_invalid_json(self) -> None:
        with self.assertRaises(ProtocolError):
            parse_line(b"not-json\r\n")

    def test_topology_accepts_direct_gateway_shape(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())

        topology = Topology.from_message(fixture)

        self.assertEqual(len(topology.nodes), 10)
        self.assertEqual(topology.groups[0]["nt"], 4)
        self.assertEqual(topology.nodes[1].type, 22)

    def test_state_merges_property_push(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "topology-direct.json").read_text())
        state = GatewayState()
        state.apply_topology(fixture)
        state.apply_properties(
            {
                "method": "gateway_post.prop",
                "nodes": [
                    {
                        "id": "curtain-1",
                        "nt": 2,
                        "params": {"cp": 25},
                    }
                ],
            }
        )

        self.assertEqual(state.nodes["curtain-1"].params["cp"], 25)
        self.assertEqual(state.nodes["curtain-1"].params["tra"], 45)

    def test_state_keeps_unknown_property_nodes_out_of_topology_nodes(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [
                    {
                        "id": "group-a",
                        "nt": 4,
                        "type": 3,
                        "n": "Kitchen spots",
                        "roomid": "room-1",
                    }
                ],
                "rooms": [{"id": "room-1", "n": "Kitchen"}],
            }
        )
        changes = state.apply_properties(
            {
                "method": "gateway_post.prop",
                "nodes": [
                    {
                        "id": "raw-light-1",
                        "nt": 2,
                        "pt": 3,
                        "n": "Kitchen spots 1",
                        "params": {"p": True, "l": 80, "ct": 4000},
                    }
                ],
            }
        )

        self.assertEqual(changes, [])
        self.assertNotIn("raw-light-1", state.nodes)
        unknown = state.unknown_property_nodes["raw-light-1"]
        self.assertEqual(unknown.property_type, 3)
        self.assertEqual(unknown.params, {"p": True, "l": 80, "ct": 4000})

    def test_topology_claims_previously_unknown_property_node(self) -> None:
        state = GatewayState()
        state.apply_properties(
            {
                "method": "gateway_post.prop",
                "nodes": [{"id": "light-1", "nt": 2, "pt": 3, "params": {"p": True}}],
            }
        )
        state.apply_topology(
            {
                "nodes": [{"id": "light-1", "nt": 2, "type": 3, "name": "Light"}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )

        self.assertNotIn("light-1", state.unknown_property_nodes)
        self.assertEqual(state.nodes["light-1"].params, {"p": True})

    def test_room_id_falls_back_to_room_membership(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [{"id": "light-1", "nt": 2, "type": 3, "name": "Light"}],
                "rooms": [{"id": "room-1", "n": "Kitchen", "nodes": [{"id": "light-1"}]}],
            }
        )

        self.assertEqual(state.room_id_for_node(state.nodes["light-1"]), "room-1")
        self.assertEqual(state.room_name_for_node(state.nodes["light-1"]), "Kitchen")

    def test_room_id_falls_back_to_room_group_membership(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [{"id": "light-1", "nt": 2, "type": 3, "name": "Light"}],
                "groups": [{"id": "group-1", "nt": 4, "nodes": [{"id": "light-1"}]}],
                "rooms": [{"id": "room-1", "n": "Kitchen", "groups": [{"id": "group-1"}]}],
            }
        )

        self.assertEqual(state.room_id_for_node(state.nodes["light-1"]), "room-1")

    def test_gateway_event_normalization_for_programmable_switches(self) -> None:
        events = list(
            iter_gateway_events(
                {
                    "method": "gateway_post.event",
                    "nodes": [
                        {
                            "id": "switch-1",
                            "nt": 2,
                            "value": "panel.click",
                            "params": {"key": 2, "count": 1},
                        },
                        {
                            "id": "knob-1",
                            "nt": 2,
                            "value": "knob.spin",
                            "params": {"idx": 1, "free_spin": -5},
                        },
                        {
                            "id": "knob-2",
                            "nt": 2,
                            "value": "knob.spin",
                            "params": {"idx": 3, "3-free_spin": 4},
                        },
                    ],
                }
            )
        )

        self.assertEqual(events[0].event_type, "panel_click")
        self.assertEqual(events[0].key, 2)
        self.assertEqual(events[0].count, 1)
        self.assertEqual(events[1].event_type, "knob_spin")
        self.assertEqual(events[1].index, 1)
        self.assertEqual(events[1].spin_delta, -5)
        self.assertEqual(events[1].spin_mode, "free")
        self.assertEqual(events[1].spin_direction, "counterclockwise")
        self.assertEqual(events[2].index, 3)
        self.assertEqual(events[2].spin_delta, 4)
        self.assertEqual(events[2].spin_mode, "free")
        self.assertEqual(events[2].spin_direction, "clockwise")

    def test_parse_discovery_response(self) -> None:
        gateway = parse_discovery_response("pid:1\r\nmac:aa:bb:cc:dd:ee:ff\r\ndid:gateway-1\r\nip:192.0.2.10\r\n")

        self.assertEqual(gateway.pid, "1")
        self.assertEqual(gateway.mac, "aa:bb:cc:dd:ee:ff")
        self.assertEqual(gateway.did, "gateway-1")
        self.assertEqual(gateway.ip, "192.0.2.10")


if __name__ == "__main__":
    unittest.main()
