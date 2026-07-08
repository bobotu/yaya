from __future__ import annotations

import asyncio
import sys
import unittest
from dataclasses import dataclass
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "custom_components"))

from yeelight_pro.session.actors import (  # noqa: E402
    Actor,
    ActorClosed,
    ActorReentrancyError,
    ActorRef,
    DeviceStateActor,
    create_actor_task,
)
from yeelight_pro.session.messages import (  # noqa: E402
    ApplyGroupsCommand,
    ApplyPropertiesCommand,
    ApplyTopologyCommand,
    RecordCommandIntentCommand,
    SessionStatusChanged,
    StateSnapshotChanged,
    SyncStartedEvent,
)
from yeelight_pro.session.model import (  # noqa: E402
    MOTOR_TRACKING_POSITION_MOTION,
    MOTOR_TRACKING_TARGET_POSITION,
    GatewaySessionState,
    MotorTargetIntent,
)


class SessionRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_actor_mailbox_serializes_concurrent_asks(self) -> None:
        class RecordingActor(Actor):
            def __init__(self) -> None:
                super().__init__("test-recording-actor")
                self.active = 0
                self.max_active = 0
                self.handled: list[int] = []

            async def handle(self, message: object) -> object:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                await asyncio.sleep(0)
                self.handled.append(int(message))
                self.active -= 1
                return message

        actor = RecordingActor()
        ref = ActorRef(actor)
        try:
            results = await asyncio.gather(*(ref.ask(index) for index in range(5)))
        finally:
            await actor.close()

        self.assertEqual(results, [0, 1, 2, 3, 4])
        self.assertEqual(actor.handled, [0, 1, 2, 3, 4])
        self.assertEqual(actor.max_active, 1)

    async def test_actor_only_exposes_messaging_through_ref(self) -> None:
        class RefOnlyActor(Actor):
            async def handle(self, message: object) -> object:
                return message

        actor = RefOnlyActor("test-ref-only-actor")
        ref = ActorRef(actor)
        try:
            self.assertFalse(hasattr(actor, "ask"))
            self.assertFalse(hasattr(actor, "tell"))
            self.assertFalse(hasattr(actor, "ref"))
            self.assertEqual(await ref.ask("message"), "message")
        finally:
            await actor.close()

    async def test_actor_rejects_messages_after_close(self) -> None:
        class ClosedActor(Actor):
            async def handle(self, message: object) -> object:
                return message

        actor = ClosedActor("test-closed-actor")
        ref = ActorRef(actor)
        await actor.close()

        with self.assertRaises(ActorClosed):
            await ref.ask("message")
        with self.assertRaises(ActorClosed):
            await ref.tell("message")

    async def test_actor_background_task_does_not_reenter_mailbox(self) -> None:
        class SpawnWorker:
            def __init__(self, ref: ActorRef[object]) -> None:
                self.target_ref = ref

            def __str__(self) -> str:
                return "spawn"

            async def run(self) -> None:
                await self.target_ref.tell("inner")

        class SpawningActor(Actor):
            def __init__(self) -> None:
                super().__init__("test-spawning-actor")
                self.active = 0
                self.max_active = 0
                self.handled: list[str] = []

            async def handle(self, message: object) -> object:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                if isinstance(message, SpawnWorker):
                    create_actor_task(message.run(), name="test-spawning-actor-inner")
                    await asyncio.sleep(0.01)
                self.handled.append(str(message))
                self.active -= 1
                return message

        actor = SpawningActor()
        ref = ActorRef(actor)
        try:
            await ref.ask(SpawnWorker(ref))
            await asyncio.sleep(0.02)
        finally:
            await actor.close()

        self.assertEqual(actor.handled, ["spawn", "inner"])
        self.assertEqual(actor.max_active, 1)

    async def test_actor_defer_queues_after_current_message(self) -> None:
        class DeferActor(Actor):
            def __init__(self) -> None:
                super().__init__("test-defer-actor")
                self.handled: list[str] = []

            async def handle(self, message: object) -> object:
                if message == "outer":
                    await self.defer("inner")
                    self.handled.append("outer-done")
                    return None
                self.handled.append(str(message))
                return None

        actor = DeferActor()
        ref = ActorRef(actor)
        try:
            await ref.ask("outer")
            await asyncio.sleep(0)
        finally:
            await actor.close()

        self.assertEqual(actor.handled, ["outer-done", "inner"])

    async def test_actor_self_ask_is_rejected_instead_of_deadlocking(self) -> None:
        @dataclass(frozen=True)
        class AskSelf:
            target_ref: ActorRef[object]

        class SelfAskActor(Actor):
            async def handle(self, message: object) -> object:
                if isinstance(message, AskSelf):
                    await message.target_ref.ask("inner")
                return message

        actor = SelfAskActor("test-self-ask-actor")
        ref = ActorRef(actor)
        try:
            with self.assertRaises(ActorReentrancyError):
                await ref.ask(AskSelf(ref))
        finally:
            await actor.close()

    async def test_actor_self_ref_tell_is_rejected(self) -> None:
        @dataclass(frozen=True)
        class TellSelf:
            target_ref: ActorRef[object]

        class SelfTellActor(Actor):
            async def handle(self, message: object) -> object:
                if isinstance(message, TellSelf):
                    await message.target_ref.tell("inner")
                return message

        actor = SelfTellActor("test-self-tell-actor")
        ref = ActorRef(actor)
        try:
            with self.assertRaises(ActorReentrancyError):
                await ref.ask(TellSelf(ref))
        finally:
            await actor.close()

    async def test_device_state_listener_exception_does_not_block_other_subscribers(self) -> None:
        state = DeviceStateActor()
        state_ref = ActorRef(state)
        received: list[str] = []

        def broken_listener(_event: StateSnapshotChanged) -> None:
            raise RuntimeError("listener failed")

        state.add_state_listener(broken_listener)
        state.add_state_listener(lambda event: received.append(event.reason))

        with self.assertLogs("yeelight_pro.session.actors.device_state", level="ERROR"):
            await state_ref.ask(
                ApplyTopologyCommand(
                    payload=_topology(False),
                    reason="topology sync",
                    message={"method": "gateway_sync.topology"},
                )
            )

        self.assertEqual(received, ["topology sync"])
        await state.close()

    async def test_device_state_clears_intents_on_sync_and_disconnect_events(self) -> None:
        state = DeviceStateActor()
        state_ref = ActorRef(state)
        snapshots: list[StateSnapshotChanged] = []
        state.add_state_listener(snapshots.append)

        await state_ref.ask(
            ApplyTopologyCommand(
                payload=_topology(False),
                reason="topology sync",
                message={"method": "gateway_sync.topology"},
            )
        )
        await state_ref.ask(RecordCommandIntentCommand({"light-1": {"p": True}}))
        self.assertTrue(state.has_pending("light-1", ["p"]))

        await state_ref.tell(SyncStartedEvent(reason="manual sync"))
        await asyncio.sleep(0.01)
        self.assertFalse(state.has_pending("light-1", ["p"]))
        self.assertIn("gateway_intent.clear", [snapshot.message["method"] for snapshot in snapshots])

        await state_ref.ask(RecordCommandIntentCommand({"light-1": {"p": True}}))
        await state_ref.tell(
            SessionStatusChanged(
                previous=GatewaySessionState.READY,
                current=GatewaySessionState.DISCONNECTED,
                error=RuntimeError("closed"),
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(state.has_pending("light-1", ["p"]))
        await state.close()

    async def test_device_state_keeps_intent_on_mismatched_push_and_requests_refresh_on_expiry(self) -> None:
        state = DeviceStateActor(ttl=0.01)
        state_ref = ActorRef(state)
        refreshes: list[str | int] = []
        state.set_refresh_requester(lambda event: refreshes.append(event.node_id))

        await state_ref.ask(
            ApplyTopologyCommand(
                payload=_topology(False),
                reason="topology sync",
                message={"method": "gateway_sync.topology"},
            )
        )
        await state_ref.ask(RecordCommandIntentCommand({"light-1": {"p": True}}))
        self.assertEqual(state.visible_node("light-1").params["p"], True)

        await state_ref.ask(
            ApplyPropertiesCommand(
                payload={"method": "gateway_post.prop", "nodes": [{"id": "light-1", "params": {"p": False}}]},
                reason="property push",
            )
        )
        self.assertTrue(state.has_pending("light-1", ["p"]))
        self.assertEqual(state.visible_node("light-1").params["p"], True)

        await state_ref.ask(RecordCommandIntentCommand({"light-1": {"p": True}}))
        await asyncio.sleep(0.05)
        self.assertEqual(refreshes, ["light-1"])
        self.assertFalse(state.has_pending("light-1", ["p"]))
        self.assertIs(state.visible_node("light-1").online, False)
        await state.close()

    async def test_device_state_expiry_marks_node_unavailable_until_fresh_state_arrives(self) -> None:
        state = DeviceStateActor(ttl=0.01)
        state_ref = ActorRef(state)
        snapshots: list[StateSnapshotChanged] = []
        refreshes: list[str | int] = []
        state.add_state_listener(snapshots.append)
        state.set_refresh_requester(lambda event: refreshes.append(event.node_id))

        await state_ref.ask(
            ApplyTopologyCommand(
                payload=_topology(True),
                reason="topology sync",
                message={"method": "gateway_sync.topology"},
            )
        )
        await state_ref.ask(RecordCommandIntentCommand({"light-1": {"p": False}}))
        self.assertEqual(state.visible_node("light-1").params["p"], False)

        snapshots.clear()
        await asyncio.sleep(0.05)

        self.assertEqual(refreshes, ["light-1"])
        self.assertFalse(state.has_pending("light-1", ["p"]))
        self.assertIs(state.visible_node("light-1").online, False)
        self.assertIn("gateway_intent.expired", [snapshot.message["method"] for snapshot in snapshots])

        await state_ref.ask(
            ApplyPropertiesCommand(
                payload={"method": "gateway_get.node", "nodes": [{"id": "light-1", "params": {"p": True}}]},
                reason="node refresh",
            )
        )
        self.assertIsNot(state.visible_node("light-1").online, False)
        self.assertEqual(state.visible_node("light-1").params["p"], True)
        await state.close()

    async def test_device_state_group_refresh_clears_stale_mesh_group_node(self) -> None:
        state = DeviceStateActor(ttl=0.01)
        state_ref = ActorRef(state)
        refreshes: list[tuple[str | int, int | None]] = []
        state.set_refresh_requester(lambda event: refreshes.append((event.node_id, event.node_type)))

        await state_ref.ask(
            ApplyTopologyCommand(
                payload={
                    "nodes": [{"id": 265461, "nt": 4, "type": 3, "params": {"p": False, "l": 20}}],
                    "groups": [{"id": 265461, "nt": 4, "params": {"p": False, "l": 20}}],
                    "rooms": [],
                    "scenes": [],
                },
                reason="topology sync",
                message={"method": "gateway_sync.topology"},
            )
        )
        await state_ref.ask(RecordCommandIntentCommand({265461: {"p": True}}))
        self.assertEqual(state.visible_node(265461).params["p"], True)

        await asyncio.sleep(0.05)

        self.assertEqual(refreshes, [(265461, 4)])
        self.assertFalse(state.has_pending(265461, ["p"]))
        self.assertIs(state.visible_node(265461).online, False)

        await state_ref.ask(
            ApplyGroupsCommand(
                payload={"method": "gateway_get.group", "groups": [{"id": 265461, "nt": 4, "params": {"p": False}}]},
                reason="node refresh",
            )
        )
        self.assertIsNot(state.visible_node(265461).online, False)
        self.assertEqual(state.visible_node(265461).params["p"], False)
        await state.close()

    async def test_device_state_masks_transition_intermediate_values_until_targets_confirm(self) -> None:
        state = DeviceStateActor()
        state_ref = ActorRef(state)

        await state_ref.ask(
            ApplyTopologyCommand(
                payload={
                    "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": False, "l": 10, "ct": 2700}}],
                    "groups": [],
                    "rooms": [],
                    "scenes": [],
                },
                reason="topology sync",
                message={"method": "gateway_sync.topology"},
            )
        )
        await state_ref.ask(RecordCommandIntentCommand({"light-1": {"p": True, "l": 80, "ct": 4000}}))

        await state_ref.ask(
            ApplyPropertiesCommand(
                payload={
                    "method": "gateway_post.prop",
                    "nodes": [{"id": "light-1", "params": {"p": True, "l": 20, "ct": 3000}}],
                },
                reason="property push",
            )
        )
        visible = state.visible_node("light-1")
        self.assertEqual(visible.params["p"], True)
        self.assertEqual(visible.params["l"], 80)
        self.assertEqual(visible.params["ct"], 4000)
        self.assertFalse(state.has_pending("light-1", ["p"]))
        self.assertTrue(state.has_pending("light-1", ["l", "ct"]))

        await state_ref.ask(
            ApplyPropertiesCommand(
                payload={
                    "method": "gateway_post.prop",
                    "nodes": [{"id": "light-1", "params": {"l": 80, "ct": 4000}}],
                },
                reason="property push",
            )
        )
        self.assertFalse(state.has_pending("light-1", ["p", "l", "ct"]))
        self.assertEqual(state.visible_node("light-1").params["l"], 80)
        self.assertEqual(state.visible_node("light-1").params["ct"], 4000)
        await state.close()

    async def test_device_state_tracks_motor_target_push_stop_disconnect_and_expiry(self) -> None:
        state = DeviceStateActor(motor_tracking_ttl=0.02)
        state_ref = ActorRef(state)
        refreshes: list[str | int] = []
        state.set_refresh_requester(lambda event: refreshes.append(event.node_id))

        await state_ref.ask(
            ApplyTopologyCommand(
                payload={
                    "nodes": [{"id": "curtain-1", "nt": 2, "type": 6, "params": {"cp": 20, "tp": 20}}],
                    "groups": [],
                    "rooms": [],
                    "scenes": [],
                },
                reason="topology sync",
                message={"method": "gateway_sync.topology"},
            )
        )
        await state_ref.ask(
            RecordCommandIntentCommand({}, motor_targets=(MotorTargetIntent("curtain-1", "cp", "tp", 80),))
        )
        visible = state.visible_node("curtain-1")
        self.assertEqual(visible.params["cp"], 20)
        self.assertEqual(visible.params[MOTOR_TRACKING_TARGET_POSITION], 80)
        self.assertEqual(visible.params[MOTOR_TRACKING_POSITION_MOTION], "opening")

        await state_ref.ask(
            ApplyPropertiesCommand(
                payload={"method": "gateway_post.prop", "nodes": [{"id": "curtain-1", "params": {"cp": 45}}]},
                reason="property push",
            )
        )
        visible = state.visible_node("curtain-1")
        self.assertEqual(visible.params["cp"], 45)
        self.assertEqual(visible.params[MOTOR_TRACKING_TARGET_POSITION], 80)

        await state_ref.ask(
            ApplyPropertiesCommand(
                payload={"method": "gateway_post.prop", "nodes": [{"id": "curtain-1", "params": {"cp": 80}}]},
                reason="property push",
            )
        )
        self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, state.visible_node("curtain-1").params)

        await state_ref.ask(
            RecordCommandIntentCommand({}, motor_targets=(MotorTargetIntent("curtain-1", "cp", "tp", 10),))
        )
        await state_ref.ask(RecordCommandIntentCommand({}, motor_stops=("curtain-1",)))
        self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, state.visible_node("curtain-1").params)

        await state_ref.ask(
            RecordCommandIntentCommand({}, motor_targets=(MotorTargetIntent("curtain-1", "cp", "tp", 10),))
        )
        await state_ref.tell(
            SessionStatusChanged(
                previous=GatewaySessionState.READY,
                current=GatewaySessionState.DISCONNECTED,
                error=RuntimeError("closed"),
            )
        )
        await asyncio.sleep(0)
        self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, state.visible_node("curtain-1").params)

        await state_ref.ask(
            RecordCommandIntentCommand({}, motor_targets=(MotorTargetIntent("curtain-1", "cp", "tp", 10),))
        )
        await asyncio.sleep(0.05)
        self.assertEqual(refreshes, ["curtain-1"])
        self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, state.visible_node("curtain-1").params)
        await state.close()


def _topology(power: bool) -> dict[str, object]:
    return {
        "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": power}}],
        "groups": [],
        "rooms": [],
        "scenes": [],
    }


if __name__ == "__main__":
    unittest.main()
