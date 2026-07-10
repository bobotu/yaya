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
    AcceptPendingWritesCommand,
    ApplyPropertiesCommand,
    ApplyTopologyCommand,
    CaptureWriteWatermarkCommand,
    PreparePendingWritesCommand,
    SessionStatusChanged,
    StateSnapshotChanged,
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
        @dataclass(frozen=True)
        class SpawnWorker:
            target_ref: ActorRef[object]

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
                    value = "spawn"
                else:
                    value = str(message)
                self.handled.append(value)
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
        ref = ActorRef(state)
        received: list[str] = []

        def broken_listener(_event: StateSnapshotChanged) -> None:
            raise RuntimeError("listener failed")

        state.add_state_listener(broken_listener)
        state.add_state_listener(lambda event: received.append(event.reason))
        try:
            with self.assertLogs("yeelight_pro.session.actors.device_state", level="ERROR"):
                await ref.ask(
                    ApplyTopologyCommand(
                        payload=_topology(False),
                        reason="topology sync",
                        message={"method": "gateway_sync.topology"},
                    )
                )
                await asyncio.sleep(0)
            self.assertEqual(received, ["topology sync"])
        finally:
            await state.close()

    async def test_pending_write_holds_before_and_after_ack_then_publishes_once_when_stable(self) -> None:
        state, ref, snapshots = await self._state(power=False, quiet_window=0.01)
        try:
            snapshots.clear()
            await ref.ask(PreparePendingWritesCommand(1, {"light-1": {"p": True}}))
            await ref.ask(
                ApplyPropertiesCommand(
                    {"method": "gateway_post.prop", "nodes": [{"id": "light-1", "params": {"p": True}}]},
                    "property push",
                )
            )
            self.assertFalse(state.visible_node("light-1").params["p"])
            self.assertEqual(snapshots, [])

            await ref.ask(AcceptPendingWritesCommand((1,)))
            self.assertFalse(state.visible_node("light-1").params["p"])
            self.assertEqual(snapshots, [])

            await ref.ask(
                ApplyPropertiesCommand(
                    {"method": "gateway_post.prop", "nodes": [{"id": "light-1", "params": {"p": True}}]},
                    "property push",
                )
            )
            self.assertTrue(state.has_pending("light-1", ["p"]))
            self.assertEqual(snapshots, [])
            await asyncio.sleep(0.03)

            self.assertFalse(state.has_pending("light-1", ["p"]))
            self.assertTrue(state.visible_node("light-1").params["p"])
            self.assertEqual(len(snapshots), 1)
        finally:
            await state.close()

    async def test_confirmation_waits_for_not_before_and_quiet_window(self) -> None:
        state, ref, _snapshots = await self._state(power=False, quiet_window=0.02)
        try:
            await ref.ask(
                PreparePendingWritesCommand(
                    1,
                    {"light-1": {"p": True}},
                    transition_delays={"light-1": {"p": 0.03}},
                )
            )
            await ref.ask(AcceptPendingWritesCommand((1,)))
            await ref.ask(
                ApplyPropertiesCommand(
                    {"method": "gateway_post.prop", "nodes": [{"id": "light-1", "params": {"p": True}}]},
                    "property push",
                )
            )
            await asyncio.sleep(0.04)
            self.assertTrue(state.has_pending("light-1", ["p"]))

            await ref.ask(
                ApplyPropertiesCommand(
                    {"method": "gateway_post.prop", "nodes": [{"id": "light-1", "params": {"p": True}}]},
                    "property push",
                )
            )
            await asyncio.sleep(0.01)
            self.assertTrue(state.has_pending("light-1", ["p"]))
            await asyncio.sleep(0.05)
            self.assertFalse(state.has_pending("light-1", ["p"]))
        finally:
            await state.close()

    async def test_reverse_tail_resets_quiet_confirmation(self) -> None:
        state, ref, _snapshots = await self._state(power=False, quiet_window=0.02)
        try:
            await ref.ask(PreparePendingWritesCommand(1, {"light-1": {"p": True}}))
            await ref.ask(AcceptPendingWritesCommand((1,)))
            await ref.ask(
                ApplyPropertiesCommand({"nodes": [{"id": "light-1", "params": {"p": True}}]}, "property push")
            )
            await asyncio.sleep(0.01)
            await ref.ask(
                ApplyPropertiesCommand({"nodes": [{"id": "light-1", "params": {"p": False}}]}, "property push")
            )
            await asyncio.sleep(0.02)
            self.assertTrue(state.has_pending("light-1", ["p"]))
            self.assertFalse(state.visible_node("light-1").params["p"])
        finally:
            await state.close()

    async def test_mismatch_refresh_releases_to_latest_raw_without_unavailable(self) -> None:
        state, ref, _snapshots = await self._state(power=True, report_grace=0.01, quiet_window=0.005)
        refreshes: list[object] = []

        async def refresh(event: object) -> dict[str, object]:
            refreshes.append(event)
            response = {"method": "gateway_get.node", "nodes": [{"id": event.node_id, "params": {"p": True}}]}
            await ref.ask(
                ApplyPropertiesCommand(response, "node refresh", captured_write_ids={event.node_id: event.write_ids})
            )
            return response

        state.set_refresh_requester(refresh)
        try:
            await ref.ask(PreparePendingWritesCommand(1, {"light-1": {"p": False}}))
            await ref.ask(AcceptPendingWritesCommand((1,)))
            await asyncio.sleep(0.04)
            self.assertEqual(len(refreshes), 1)
            self.assertFalse(state.has_pending("light-1", ["p"]))
            self.assertTrue(state.visible_node("light-1").params["p"])
            self.assertIsNot(state.visible_node("light-1").online, False)
        finally:
            await state.close()

    async def test_matching_refresh_enters_quiet_confirmation_and_completes(self) -> None:
        state, ref, _snapshots = await self._state(power=False, report_grace=0.01, quiet_window=0.01)

        async def refresh(event: object) -> dict[str, object]:
            response = {"method": "gateway_get.node", "nodes": [{"id": event.node_id, "params": {"p": True}}]}
            await ref.ask(
                ApplyPropertiesCommand(response, "node refresh", captured_write_ids={event.node_id: event.write_ids})
            )
            return response

        state.set_refresh_requester(refresh)
        try:
            await ref.ask(PreparePendingWritesCommand(1, {"light-1": {"p": True}}))
            await ref.ask(AcceptPendingWritesCommand((1,)))
            await asyncio.sleep(0.015)
            self.assertTrue(state.has_pending("light-1", ["p"]))
            self.assertFalse(state.visible_node("light-1").params["p"])
            for _ in range(20):
                if not state.has_pending("light-1", ["p"]):
                    break
                await asyncio.sleep(0.01)
            self.assertFalse(state.has_pending("light-1", ["p"]))
            self.assertTrue(state.visible_node("light-1").params["p"])
        finally:
            await state.close()

    async def test_same_target_write_still_blocks_conflicting_push(self) -> None:
        state, ref, snapshots = await self._state(power=True)
        try:
            snapshots.clear()
            await ref.ask(PreparePendingWritesCommand(1, {"light-1": {"p": True}}))
            await ref.ask(
                ApplyPropertiesCommand({"nodes": [{"id": "light-1", "params": {"p": False}}]}, "property push")
            )
            self.assertTrue(state.visible_node("light-1").params["p"])
            self.assertTrue(state.has_pending("light-1", ["p"]))
            self.assertEqual(snapshots, [])
        finally:
            await state.close()

    async def test_aba_stale_ack_and_refresh_cannot_affect_newest_write(self) -> None:
        state, ref, _snapshots = await self._state(power=True, report_grace=0.01)
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()

        async def refresh(event: object) -> dict[str, object]:
            refresh_started.set()
            await release_refresh.wait()
            response = {"nodes": [{"id": event.node_id, "params": {"p": False}}]}
            await ref.ask(
                ApplyPropertiesCommand(response, "node refresh", captured_write_ids={event.node_id: event.write_ids})
            )
            return response

        state.set_refresh_requester(refresh)
        try:
            await ref.ask(PreparePendingWritesCommand(1, {"light-1": {"p": False}}))
            await ref.ask(AcceptPendingWritesCommand((1,)))
            await asyncio.wait_for(refresh_started.wait(), 1.0)
            await ref.ask(PreparePendingWritesCommand(2, {"light-1": {"p": True}}))
            await ref.ask(AcceptPendingWritesCommand((2,)))
            await ref.ask(PreparePendingWritesCommand(3, {"light-1": {"p": False}}))
            await ref.ask(AcceptPendingWritesCommand((1,)))
            release_refresh.set()
            await asyncio.sleep(0.02)

            diagnostics = state.diagnostics()["properties"]
            self.assertEqual([(item["write_id"], item["target"]) for item in diagnostics], [(3, False)])
            self.assertTrue(state.visible_node("light-1").params["p"])
        finally:
            await state.close()

    async def test_pull_started_before_new_write_cannot_overwrite_touched_raw_property(self) -> None:
        state, ref, _snapshots = await self._state(power=False)
        try:
            watermark = await ref.ask(CaptureWriteWatermarkCommand())
            await ref.ask(PreparePendingWritesCommand(1, {"light-1": {"p": True}}))
            await ref.ask(
                ApplyPropertiesCommand(
                    {"nodes": [{"id": "light-1", "params": {"p": True}}]},
                    "poll full properties",
                    captured_write_ids=watermark,
                )
            )
            self.assertFalse(state.state.nodes["light-1"].params["p"])
            self.assertTrue(state.has_pending("light-1", ["p"]))
        finally:
            await state.close()

    async def test_full_sync_preserves_pending_but_disconnect_clears(self) -> None:
        state, ref, _snapshots = await self._state(power=False)
        try:
            await ref.ask(PreparePendingWritesCommand(1, {"light-1": {"p": True}}))
            captured = await ref.ask(CaptureWriteWatermarkCommand())
            await ref.ask(
                ApplyTopologyCommand(
                    payload=_topology(False),
                    reason="topology sync",
                    message={"method": "gateway_sync.topology"},
                    captured_write_ids=captured,
                )
            )
            self.assertTrue(state.has_pending("light-1", ["p"]))

            await ref.tell(
                SessionStatusChanged(
                    previous=GatewaySessionState.READY,
                    current=GatewaySessionState.DISCONNECTED,
                    error=RuntimeError("closed"),
                )
            )
            await asyncio.sleep(0)
            self.assertFalse(state.has_pending("light-1", ["p"]))
        finally:
            await state.close()

    async def test_motor_state_tracker_preserves_target_push_stop_disconnect_and_expiry(self) -> None:
        state, ref, _snapshots = await self._state(
            topology={
                "nodes": [{"id": "curtain-1", "nt": 2, "type": 6, "params": {"cp": 20, "tp": 20}}],
                "groups": [],
                "rooms": [],
                "scenes": [],
            },
            motor_tracking_ttl=0.02,
        )
        refreshes: list[str | int] = []
        state.set_refresh_requester(lambda event: refreshes.append(event.node_id) or {})
        try:
            await ref.ask(
                AcceptPendingWritesCommand(
                    (),
                    motor_targets=(MotorTargetIntent("curtain-1", "cp", "tp", 80),),
                )
            )
            visible = state.visible_node("curtain-1")
            self.assertEqual(visible.params["cp"], 20)
            self.assertEqual(visible.params[MOTOR_TRACKING_TARGET_POSITION], 80)
            self.assertEqual(visible.params[MOTOR_TRACKING_POSITION_MOTION], "opening")

            await ref.ask(
                ApplyPropertiesCommand({"nodes": [{"id": "curtain-1", "params": {"cp": 45}}]}, "property push")
            )
            visible = state.visible_node("curtain-1")
            self.assertEqual(visible.params["cp"], 45)
            self.assertEqual(visible.params[MOTOR_TRACKING_TARGET_POSITION], 80)

            await ref.ask(
                ApplyPropertiesCommand({"nodes": [{"id": "curtain-1", "params": {"cp": 80}}]}, "property push")
            )
            self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, state.visible_node("curtain-1").params)

            await ref.ask(
                AcceptPendingWritesCommand(
                    (),
                    motor_targets=(MotorTargetIntent("curtain-1", "cp", "tp", 10),),
                )
            )
            await ref.ask(AcceptPendingWritesCommand((), motor_stops=("curtain-1",)))
            self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, state.visible_node("curtain-1").params)

            await ref.ask(
                AcceptPendingWritesCommand(
                    (),
                    motor_targets=(MotorTargetIntent("curtain-1", "cp", "tp", 10),),
                )
            )
            await ref.tell(
                SessionStatusChanged(
                    previous=GatewaySessionState.READY,
                    current=GatewaySessionState.DISCONNECTED,
                    error=RuntimeError("closed"),
                )
            )
            await asyncio.sleep(0)
            self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, state.visible_node("curtain-1").params)

            await ref.ask(
                AcceptPendingWritesCommand(
                    (),
                    motor_targets=(MotorTargetIntent("curtain-1", "cp", "tp", 10),),
                )
            )
            await asyncio.sleep(0.05)
            self.assertEqual(refreshes, ["curtain-1"])
            self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, state.visible_node("curtain-1").params)
        finally:
            await state.close()

    async def _state(
        self,
        *,
        power: bool = False,
        topology: dict[str, object] | None = None,
        report_grace: float | None = None,
        quiet_window: float | None = None,
        motor_tracking_ttl: float | None = None,
    ) -> tuple[DeviceStateActor, ActorRef, list[StateSnapshotChanged]]:
        state = DeviceStateActor(
            report_grace=report_grace,
            quiet_window=quiet_window,
            motor_tracking_ttl=motor_tracking_ttl,
        )
        ref = ActorRef(state)
        snapshots: list[StateSnapshotChanged] = []
        state.add_state_listener(snapshots.append)
        await ref.ask(
            ApplyTopologyCommand(
                payload=topology or _topology(power),
                reason="topology sync",
                message={"method": "gateway_sync.topology"},
            )
        )
        await asyncio.sleep(0)
        return state, ref, snapshots


def _topology(power: bool) -> dict[str, object]:
    return {
        "nodes": [{"id": "light-1", "nt": 2, "type": 3, "params": {"p": power}}],
        "groups": [],
        "rooms": [],
        "scenes": [],
    }


if __name__ == "__main__":
    unittest.main()
