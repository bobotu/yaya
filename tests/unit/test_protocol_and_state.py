from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "custom_components"))

from yeelight_pro.core import (  # noqa: E402
    ProtocolError,
    Topology,
    TopologyNode,
    build_request,
    iter_gateway_events,
    parse_discovery_response,
    parse_line,
)
from yeelight_pro.session.model import (  # noqa: E402
    MOTOR_TRACKING_POSITION_MOTION,
    MOTOR_TRACKING_TARGET_POSITION,
    GatewayState,
    MotorStateTracker,
    MotorTargetIntent,
    PendingRefresh,
    PendingWriteTracker,
)


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
        self.assertIsNone(topology.nodes[0].product_id)

    def test_topology_preserves_product_id(self) -> None:
        node = TopologyNode.from_mapping({"id": "light-1", "nt": 2, "type": 3, "pid": 198672})

        self.assertEqual(node.product_id, 198672)

        updated = node.merge_update({"id": "light-1", "pid": 198666})

        self.assertEqual(updated.product_id, 198666)

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

    def test_topology_push_merges_without_removing_existing_nodes(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [
                    {"id": "light-1", "nt": 2, "type": 3, "params": {"p": False}},
                    {"id": "switch-1", "nt": 2, "type": 13, "params": {"1-sp": True}},
                ],
                "rooms": [{"id": "room-1", "n": "Kitchen"}],
            }
        )

        state.apply_topology(
            {
                "method": "gateway_post.topology",
                "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"l": 42}}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            },
            replace=False,
        )

        self.assertIn("switch-1", state.nodes)
        self.assertEqual(state.nodes["light-1"].params, {"p": False, "l": 42})
        self.assertEqual(state.rooms["room-1"]["n"], "Kitchen")

    def test_full_topology_sync_retains_missing_nodes_as_unavailable(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [
                    {"id": "light-1", "nt": 2, "type": 3, "params": {"p": True}},
                    {"id": "switch-1", "nt": 2, "type": 13},
                ],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )

        state.apply_topology(
            {
                "nodes": [{"id": "switch-1", "nt": 2, "type": 13}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )

        self.assertIn("light-1", state.nodes)
        self.assertFalse(state.nodes["light-1"].online)
        self.assertEqual(state.topology_node_ids, {"switch-1"})
        self.assertTrue(
            state.full_property_coverage(
                {"method": "gateway_post.prop", "nodes": [{"id": "switch-1", "params": {"1-sp": False}, "o": True}]}
            )
        )

    def test_full_property_coverage_requires_full_snapshot_marker(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [
                    {"id": "light-1", "nt": 2, "type": 3},
                    {"id": "light-2", "nt": 2, "type": 3},
                ],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )

        self.assertFalse(
            state.full_property_coverage(
                {
                    "method": "gateway_post.prop",
                    "nodes": [
                        {"id": "light-1", "params": {"p": True}},
                        {"id": "light-2", "params": {"p": False}},
                    ],
                }
            )
        )
        self.assertTrue(
            state.full_property_coverage(
                {
                    "method": "gateway_post.prop",
                    "nodes": [
                        {"id": "light-1", "params": {"p": True}, "o": True},
                        {"id": "light-2", "params": {"p": False}, "o": True},
                    ],
                }
            )
        )

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

    def test_state_updates_unknown_property_node_summary(self) -> None:
        state = GatewayState()
        state.apply_properties(
            {
                "method": "gateway_post.prop",
                "nodes": [{"id": "raw-light-1", "nt": "2", "pt": "3", "params": {"p": True}}],
            }
        )
        state.apply_properties(
            {
                "method": "gateway_post.prop",
                "nodes": [{"id": "raw-light-1", "nt": 2, "pt": "bad", "params": {"l": 42}}],
            }
        )

        unknown = state.unknown_property_nodes["raw-light-1"]
        self.assertEqual(unknown.count, 2)
        self.assertEqual(unknown.nt, 2)
        self.assertEqual(unknown.property_type, None)
        self.assertEqual(unknown.params, {"l": 42})
        self.assertEqual(
            state.unknown_summary(),
            {"count": 1, "by_shape": {"nt=2;pt=None;params=l": 1}},
        )

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

    def test_room_name_looks_up_string_and_integer_ids(self) -> None:
        state = GatewayState()
        state.apply_topology({"nodes": [], "rooms": [{"id": 1, "n": "Kitchen"}, {"id": "2", "name": "Office"}]})

        self.assertEqual(state.room_name("1"), "Kitchen")
        self.assertEqual(state.room_name(2), "Office")
        self.assertIsNone(state.room_name("bad"))
        self.assertIsNone(state.room_name(None))

    def test_pending_write_tracker_holds_until_not_before_and_quiet_window(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False}}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )
        tracker = PendingWriteTracker(report_grace=5.0, quiet_window=2.5)
        tracker.prepare_writes(
            1,
            {"light-1": {"p": True}},
            nodes=state.nodes,
            now=10.0,
            transition_delays={"light-1": {"p": 2.0}},
        )
        tracker.accept_writes((1,), now=11.0)

        state.apply_properties({"nodes": [{"id": "light-1", "params": {"p": True}}]})
        tracker.apply_observation({"nodes": [{"id": "light-1", "params": {"p": True}}]}, now=12.9)
        self.assertFalse(tracker.project_visible(state.nodes["light-1"]).params["p"])
        self.assertTrue(tracker.has_pending("light-1", ["p"]))

        tracker.apply_observation({"nodes": [{"id": "light-1", "params": {"p": True}}]}, now=13.0)
        self.assertTrue(tracker.has_pending("light-1", ["p"]))
        self.assertEqual(tracker.tick(now=15.49).visible_affected, set())

        result = tracker.tick(now=15.5)
        self.assertEqual(result.visible_affected, {"light-1"})
        self.assertFalse(tracker.has_pending("light-1", ["p"]))
        self.assertTrue(tracker.project_visible(state.nodes["light-1"]).params["p"])

    def test_pending_write_refresh_match_starts_its_own_quiet_window(self) -> None:
        state = GatewayState()
        state.apply_topology({"nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False}}]})
        tracker = PendingWriteTracker(report_grace=1.0, quiet_window=2.5)
        tracker.prepare_writes(7, {"light-1": {"p": True}}, nodes=state.nodes, now=1.0)
        tracker.accept_writes((7,), now=2.0)

        refresh = tracker.tick(now=3.0).refreshes[0]
        response = {"nodes": [{"id": "light-1", "params": {"p": True}}]}
        self.assertEqual(tracker.complete_refresh(refresh, response, failed=False, now=3.1), set())
        diagnostics = tracker.diagnostics(now=3.1)["properties"][0]
        self.assertTrue(diagnostics["matched"])
        self.assertFalse(diagnostics["refreshing"])
        self.assertEqual(tracker.tick(now=5.59).visible_affected, set())
        self.assertEqual(tracker.tick(now=5.6).visible_affected, {"light-1"})

    def test_pending_write_refresh_mismatch_releases_only_matching_write_id(self) -> None:
        state = GatewayState()
        state.apply_topology({"nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False, "l": 10}}]})
        tracker = PendingWriteTracker(report_grace=1.0, quiet_window=2.5)
        tracker.prepare_writes(1, {"light-1": {"p": True, "l": 20}}, nodes=state.nodes, now=1.0)
        tracker.accept_writes((1,), now=1.0)
        tracker.tick(now=2.0)
        tracker.prepare_writes(2, {"light-1": {"p": False}}, nodes=state.nodes, now=2.1)

        affected = tracker.complete_refresh(
            PendingRefresh("light-1", {"p": 1, "l": 1}),
            {"nodes": [{"id": "light-1", "params": {"p": False, "l": 10}}]},
            failed=False,
            now=2.2,
        )

        self.assertEqual(affected, {"light-1"})
        self.assertTrue(tracker.has_pending("light-1", ["p"]))
        self.assertFalse(tracker.has_pending("light-1", ["l"]))
        self.assertEqual(tracker.diagnostics(now=2.2)["properties"][0]["write_id"], 2)

    def test_pending_write_does_not_create_missing_visible_property(self) -> None:
        state = GatewayState()
        state.apply_topology({"nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": True}}]})
        tracker = PendingWriteTracker()

        tracker.prepare_writes(1, {"light-1": {"ct": 3000}}, nodes=state.nodes, now=1.0)

        self.assertNotIn("ct", tracker.project_visible(state.nodes["light-1"]).params)

    def test_motor_tracking_uses_target_without_overwriting_current_position(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [{"id": "curtain-1", "nt": 2, "type": 6, "params": {"cp": 20, "tp": 20}}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )
        tracker = MotorStateTracker(ttl=30.0)

        affected = tracker.set_target(
            MotorTargetIntent("curtain-1", "cp", "tp", 80),
            current_value=20,
            now=1.0,
        )
        visible = tracker.visible_node(state.nodes["curtain-1"])

        self.assertEqual(affected, {"curtain-1"})
        self.assertEqual(visible.params["cp"], 20)
        self.assertEqual(visible.params[MOTOR_TRACKING_TARGET_POSITION], 80)
        self.assertEqual(visible.params[MOTOR_TRACKING_POSITION_MOTION], "opening")

    def test_motor_tracking_authoritative_push_updates_direction_and_completion(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [{"id": "curtain-1", "nt": 2, "type": 6, "params": {"cp": 80, "tp": 80}}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )
        tracker = MotorStateTracker(ttl=30.0)

        changes = state.apply_properties(
            {"method": "gateway_post.prop", "nodes": [{"id": "curtain-1", "params": {"cp": 60, "tp": 20}}]}
        )
        self.assertEqual(
            tracker.apply_authoritative_changes(changes, state.nodes, now=1.0),
            {"curtain-1"},
        )
        visible = tracker.visible_node(state.nodes["curtain-1"])
        self.assertEqual(visible.params["cp"], 60)
        self.assertEqual(visible.params[MOTOR_TRACKING_TARGET_POSITION], 20)
        self.assertEqual(visible.params[MOTOR_TRACKING_POSITION_MOTION], "closing")

        changes = state.apply_properties(
            {"method": "gateway_post.prop", "nodes": [{"id": "curtain-1", "params": {"cp": 20}}]}
        )
        self.assertEqual(
            tracker.apply_authoritative_changes(changes, state.nodes, now=2.0),
            {"curtain-1"},
        )
        visible = tracker.visible_node(state.nodes["curtain-1"])
        self.assertEqual(visible.params["cp"], 20)
        self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, visible.params)

    def test_motor_tracking_stop_and_expiry_clear_visible_target(self) -> None:
        state = GatewayState()
        state.apply_topology(
            {
                "nodes": [{"id": "curtain-1", "nt": 2, "type": 6, "params": {"cp": 10}}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            }
        )
        tracker = MotorStateTracker(ttl=5.0)

        tracker.set_target(MotorTargetIntent("curtain-1", "cp", "tp", 90), current_value=10, now=1.0)
        self.assertTrue(tracker.has_tracking("curtain-1"))
        self.assertEqual(tracker.clear_node("curtain-1"), {"curtain-1"})
        self.assertFalse(tracker.has_tracking("curtain-1"))

        tracker.set_target(MotorTargetIntent("curtain-1", "cp", "tp", 90), current_value=10, now=10.0)
        self.assertEqual(tracker.expire_pending(now=14.9), ())
        expired = tracker.expire_pending(now=15.0)
        self.assertEqual(tuple(track.node_id for track in expired), ("curtain-1",))
        self.assertFalse(tracker.has_tracking("curtain-1"))

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
