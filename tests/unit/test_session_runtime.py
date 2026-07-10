from __future__ import annotations

import asyncio
import sys
import unittest
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "custom_components"))

from yeelight_pro.core import ConnectionClosed, NodeCommand, ProtocolError  # noqa: E402
from yeelight_pro.core.commands import MotorAction, motor_adjust_action  # noqa: E402
from yeelight_pro.session.actor import (  # noqa: E402
    Actor,
    ActorClosed,
    ActorReentrancyError,
    ActorRef,
    create_actor_task,
)
from yeelight_pro.session.connection import (  # noqa: E402
    ConnectionLostEvent,
    GatewayRpcRequest,
    RpcPushEvent,
)
from yeelight_pro.session.motor import (  # noqa: E402
    MOTOR_TRACKING_POSITION_MOTION,
    MOTOR_TRACKING_TARGET_POSITION,
)
from yeelight_pro.session.runtime import GatewaySession  # noqa: E402


class FakeConnectionRef:
    def __init__(self) -> None:
        self.requests: list[GatewayRpcRequest] = []
        self.write_response: Mapping[str, Any] | BaseException = {"result": "ok"}
        self.topology_response: Mapping[str, Any] = _topology()
        self.node_responses: dict[str, Mapping[str, Any] | BaseException] = {}
        self.group_responses: dict[str, Mapping[str, Any] | BaseException] = {}

    async def ask(self, message: Any) -> dict[str, Any] | None:
        if not isinstance(message, GatewayRpcRequest):
            return None
        self.requests.append(message)
        if message.method == "gateway_set.prop":
            return _response_or_raise(self.write_response)
        if message.method == "gateway_get.topology":
            return dict(self.topology_response)
        params = {} if message.payload is None else message.payload.get("params", {})
        node_id = str(params.get("id")) if isinstance(params, Mapping) else "None"
        if message.method == "gateway_get.node":
            return _response_or_raise(self.node_responses.get(node_id, {"nodes": []}))
        if message.method == "gateway_get.group":
            return _response_or_raise(self.group_responses.get(node_id, {"groups": []}))
        return {}


def _response_or_raise(value: Mapping[str, Any] | BaseException) -> dict[str, Any]:
    if isinstance(value, BaseException):
        raise value
    return dict(value)


def _topology(*nodes: tuple[str, int, dict[str, object]]) -> dict[str, object]:
    return {
        "nodes": [
            {"id": node_id, "nt": node_type, "type": 3 if node_type in {2, 4} else 6, "o": True, "params": params}
            for node_id, node_type, params in nodes
        ],
        "groups": [],
        "rooms": [],
        "scenes": [],
    }


def _push(node_id: str, **params: object) -> dict[str, object]:
    return {"method": "gateway_post.prop", "nodes": [{"id": node_id, "nt": 2, "params": params}]}


class SessionRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        for session in getattr(self, "sessions", []):
            await session.close()

    def session(self, connection: FakeConnectionRef, **kwargs: Any) -> GatewaySession:
        session = GatewaySession(connection_ref=connection, **kwargs)  # type: ignore[arg-type]
        self.sessions = [*getattr(self, "sessions", []), session]
        return session

    async def test_actor_mailbox_serializes_concurrent_asks(self) -> None:
        class RecordingActor(Actor[int]):
            def __init__(self) -> None:
                super().__init__("test-recording-actor")
                self.active = 0
                self.max_active = 0
                self.handled: list[int] = []

            async def handle(self, message: int) -> int:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                await asyncio.sleep(0)
                self.handled.append(message)
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

    async def test_actor_rejects_messages_after_close(self) -> None:
        class ClosedActor(Actor[object]):
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

            async def run(self) -> None:
                await self.target_ref.tell("inner")

        class SpawningActor(Actor[object]):
            def __init__(self) -> None:
                super().__init__("test-spawning-actor")
                self.active = 0
                self.max_active = 0
                self.handled: list[str] = []

            async def handle(self, message: object) -> object:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                if isinstance(message, SpawnWorker):
                    create_actor_task(message.run(), name="test-spawn-worker")
                    await asyncio.sleep(0.01)
                    self.handled.append("spawn")
                else:
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
        class DeferActor(Actor[str]):
            def __init__(self) -> None:
                super().__init__("test-defer-actor")
                self.handled: list[str] = []

            async def handle(self, message: str) -> None:
                if message == "outer":
                    await self.defer("inner")
                    self.handled.append("outer")
                    return
                self.handled.append(message)

        actor = DeferActor()
        ref = ActorRef(actor)
        try:
            await ref.ask("outer")
            await asyncio.sleep(0)
        finally:
            await actor.close()

        self.assertEqual(actor.handled, ["outer", "inner"])

    async def test_actor_self_messages_are_rejected(self) -> None:
        @dataclass(frozen=True)
        class AskSelf:
            target_ref: ActorRef[object]

        class SelfActor(Actor[object]):
            async def handle(self, message: object) -> object:
                if isinstance(message, AskSelf):
                    await message.target_ref.ask("inner")
                return message

        actor = SelfActor("test-self-actor")
        ref = ActorRef(actor)
        try:
            with self.assertRaises(ActorReentrancyError):
                await ref.ask(AskSelf(ref))
        finally:
            await actor.close()

    async def test_write_ack_holds_visible_state_until_target_push(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(connection, set_prop_batch_delay=0)
        session.store.apply_topology(_topology(("light-a", 2, {"p": True})))
        events: list[Any] = []
        session.add_state_listener(events.append)

        response = await session.submit_commands(
            [NodeCommand(id="light-a", nt=2, props={"p": False})],
        )

        self.assertEqual(response, {"result": "ok"})
        self.assertTrue(session.has_pending_write("light-a", ["p"]))
        self.assertTrue(session.visible_node("light-a").params["p"])  # type: ignore[union-attr]

        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-a", p=True)))
        await asyncio.sleep(0)
        self.assertEqual(events, [])

        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-a", p=False)))
        await asyncio.sleep(0)

        self.assertFalse(session.has_pending_write("light-a"))
        self.assertFalse(session.visible_node("light-a").params["p"])  # type: ignore[union-attr]
        self.assertEqual(len(events), 1)

    async def test_gateway_batch_releases_members_in_one_state_event(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(connection, set_prop_batch_delay=0)
        session.store.apply_topology(
            _topology(
                ("light-a", 2, {"l": 20}),
                ("light-b", 2, {"l": 20}),
            )
        )
        events: list[Any] = []
        session.add_state_listener(events.append)

        await session.submit_commands(
            [
                NodeCommand(id="light-a", nt=2, props={"l": 80}),
                NodeCommand(id="light-b", nt=2, props={"l": 80}),
            ],
        )
        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-a", p=True, l=80)))
        await asyncio.sleep(0)

        self.assertEqual(events, [])
        self.assertEqual(session.visible_node("light-a").params["l"], 20)  # type: ignore[union-attr]

        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-b", p=True, l=80)))
        await asyncio.sleep(0)

        self.assertEqual(len(events), 1)
        self.assertEqual({change.id for change in events[0].changes}, {"light-a", "light-b"})

    async def test_delayed_readback_uses_group_api_and_confirms_batch(self) -> None:
        connection = FakeConnectionRef()
        connection.group_responses["group-a"] = {
            "groups": [{"id": "group-a", "nt": 4, "o": True, "params": {"p": False}}]
        }
        session = self.session(
            connection,
            set_prop_batch_delay=0,
            state_readback_delay=0.01,
            state_deadline=0.1,
        )
        session.store.apply_topology(_topology(("group-a", 4, {"p": True})))

        await session.submit_commands(
            [NodeCommand(id="group-a", nt=4, props={"p": False})],
        )
        await asyncio.sleep(0.04)

        self.assertFalse(session.has_pending_write("group-a"))
        self.assertFalse(session.visible_node("group-a").params["p"])  # type: ignore[union-attr]
        self.assertIn("gateway_get.group", [request.method for request in connection.requests])
        self.assertNotIn("gateway_get.node", [request.method for request in connection.requests])

    async def test_deadline_releases_latest_raw_without_unavailable(self) -> None:
        connection = FakeConnectionRef()
        connection.node_responses["light-a"] = {"nodes": [{"id": "light-a", "nt": 2, "o": True, "params": {"l": 40}}]}
        session = self.session(
            connection,
            set_prop_batch_delay=0,
            state_readback_delay=0.01,
            state_deadline=0.03,
        )
        session.store.apply_topology(_topology(("light-a", 2, {"l": 20})))

        await session.submit_commands(
            [NodeCommand(id="light-a", nt=2, props={"l": 80})],
        )
        await asyncio.sleep(0.06)

        node = session.visible_node("light-a")
        self.assertIsNotNone(node)
        self.assertEqual(node.params["l"], 40)  # type: ignore[union-attr]
        self.assertTrue(node.online)  # type: ignore[union-attr]
        self.assertFalse(session.has_pending_write("light-a"))

    async def test_rpc_failure_releases_hold_and_fails_caller(self) -> None:
        connection = FakeConnectionRef()
        connection.write_response = {"result": "fail"}
        session = self.session(connection, set_prop_batch_delay=0)
        session.store.apply_topology(_topology(("light-a", 2, {"p": False})))

        with self.assertRaises(ProtocolError):
            await session.submit_commands(
                [NodeCommand(id="light-a", nt=2, props={"p": True})],
            )

        self.assertFalse(session.has_pending_write("light-a"))
        self.assertFalse(session.visible_node("light-a").params["p"])  # type: ignore[union-attr]

    async def test_connection_loss_clears_pending_hold(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(connection, set_prop_batch_delay=0)
        session.store.apply_topology(_topology(("light-a", 2, {"p": True})))
        await session.submit_commands(
            [NodeCommand(id="light-a", nt=2, props={"p": False})],
        )

        await session.ref.ask(ConnectionLostEvent(epoch=0, error=ConnectionClosed("closed")))

        self.assertFalse(session.has_pending_write("light-a"))
        self.assertTrue(session.visible_node("light-a").params["p"])  # type: ignore[union-attr]

    async def test_routine_sync_preserves_pending_hold(self) -> None:
        connection = FakeConnectionRef()
        connection.topology_response = _topology(("light-a", 2, {"p": True}))
        session = self.session(connection, set_prop_batch_delay=0)
        session.full_prop_timeout = 60
        session.store.apply_topology(connection.topology_response)

        await session.submit_commands([NodeCommand(id="light-a", nt=2, props={"p": False})])
        sync_task = asyncio.create_task(session.sync(include_groups=False, include_rooms=False, include_scenes=False))
        for _ in range(20):
            await asyncio.sleep(0)
            if any(request.method == "gateway_get.topology" for request in connection.requests):
                break

        await session.ref.ask(
            RpcPushEvent(
                epoch=0,
                message={
                    "method": "gateway_post.prop",
                    "nodes": [{"id": "light-a", "nt": 2, "o": True, "params": {"p": True}}],
                },
            )
        )
        await sync_task

        self.assertTrue(session.has_pending_write("light-a", ["p"]))
        self.assertTrue(session.visible_node("light-a").params["p"])  # type: ignore[union-attr]

        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-a", p=False)))

        self.assertFalse(session.has_pending_write("light-a"))
        self.assertFalse(session.visible_node("light-a").params["p"])  # type: ignore[union-attr]

    async def test_partial_execution_releases_once_at_deadline(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(
            connection,
            set_prop_batch_delay=0,
            state_readback_delay=0.01,
            state_deadline=0.03,
        )
        session.store.apply_topology(
            _topology(
                ("light-a", 2, {"p": True}),
                ("light-b", 2, {"p": True}),
            )
        )
        events: list[Any] = []
        session.add_state_listener(events.append)

        await session.submit_commands(
            [
                NodeCommand(id="light-a", nt=2, props={"p": False}),
                NodeCommand(id="light-b", nt=2, props={"p": False}),
            ]
        )
        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-a", p=False)))
        await asyncio.sleep(0.06)

        self.assertEqual(len(events), 1)
        self.assertEqual({change.id for change in events[0].changes}, {"light-a"})
        self.assertFalse(session.visible_node("light-a").params["p"])  # type: ignore[union-attr]
        self.assertTrue(session.visible_node("light-b").params["p"])  # type: ignore[union-attr]
        self.assertFalse(session.has_pending_write("light-a"))
        self.assertFalse(session.has_pending_write("light-b"))
        self.assertEqual(session.write_diagnostics()["outcomes"]["deadline"], 1)

    async def test_property_write_derives_implicit_power_target(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(connection, set_prop_batch_delay=0)
        session.store.apply_topology(_topology(("light-a", 2, {"p": False, "l": 20})))

        await session.submit_commands([NodeCommand(id="light-a", nt=2, props={"l": 80})])

        self.assertTrue(session.has_pending_write("light-a", ["p", "l"]))
        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-a", l=80)))
        self.assertEqual(session.visible_node("light-a").params, {"p": False, "l": 20})  # type: ignore[union-attr]

        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-a", p=True)))

        self.assertEqual(session.visible_node("light-a").params, {"p": True, "l": 80})  # type: ignore[union-attr]
        self.assertFalse(session.has_pending_write("light-a"))

    async def test_shutdown_clears_holds_tasks_and_sanitizes_diagnostics(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(
            connection,
            set_prop_batch_delay=0,
            state_readback_delay=60,
            state_deadline=60,
        )
        session.store.apply_topology(_topology(("private-node", 2, {"p": True})))

        await session.submit_commands([NodeCommand(id="private-node", nt=2, props={"p": False})])
        diagnostics = session.write_diagnostics()

        self.assertEqual(diagnostics["active_batches"], 1)
        self.assertEqual(diagnostics["pending_nodes"], 1)
        self.assertEqual(diagnostics["pending_properties"], 1)
        self.assertEqual(diagnostics["scheduled_readbacks"], 1)
        self.assertNotIn("batches", diagnostics)
        self.assertNotIn("private-node", repr(diagnostics))

        await session.close()

        diagnostics = session.write_diagnostics()
        self.assertFalse(session.has_pending_write("private-node"))
        self.assertEqual(diagnostics["active_batches"], 0)
        self.assertEqual(diagnostics["scheduled_readbacks"], 0)
        self.assertIsNone(diagnostics["oldest_pending_age"])
        self.assertEqual(diagnostics["outcomes"]["shutdown"], 1)
        self.assertIsNone(session._write_deadline_task)
        self.assertEqual(session._readback_tasks, {})

    async def test_motor_projection_remains_domain_specific(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(connection, set_prop_batch_delay=0)
        session.store.apply_topology(_topology(("curtain-a", 6, {"cp": 20, "tp": 20})))

        await session.submit_commands(
            [NodeCommand(id="curtain-a", nt=2, props={"tp": 80})],
        )

        moving = session.visible_node("curtain-a")
        self.assertEqual(moving.params["cp"], 20)  # type: ignore[union-attr]
        self.assertEqual(moving.params[MOTOR_TRACKING_TARGET_POSITION], 80)  # type: ignore[union-attr]
        self.assertEqual(moving.params[MOTOR_TRACKING_POSITION_MOTION], "opening")  # type: ignore[union-attr]
        self.assertFalse(session.has_pending_write("curtain-a"))

        await session.submit_commands(
            [NodeCommand(id="curtain-a", nt=2, action=motor_adjust_action(MotorAction.PAUSE))],
        )

        stopped = session.visible_node("curtain-a")
        self.assertNotIn(MOTOR_TRACKING_TARGET_POSITION, stopped.params)  # type: ignore[union-attr]

    async def test_listener_failure_does_not_block_other_subscribers(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(connection)
        session.store.apply_topology(_topology(("light-a", 2, {"p": False})))
        delivered: list[Any] = []

        def broken(_event: Any) -> None:
            raise RuntimeError("listener failed")

        session.add_state_listener(broken)
        session.add_state_listener(delivered.append)

        await session.ref.ask(RpcPushEvent(epoch=0, message=_push("light-a", p=True)))
        await asyncio.sleep(0.02)

        self.assertEqual(len(delivered), 1)

    async def test_drain_flushes_queued_writes_and_rejects_new_submissions(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(connection, set_prop_batch_delay=60)
        session.store.apply_topology(_topology(("light-a", 2, {"p": False})))
        queued = asyncio.create_task(
            session.submit_commands(
                [NodeCommand(id="light-a", nt=2, props={"p": True})],
            )
        )
        for _ in range(10):
            await asyncio.sleep(0)
            if session._pending_writes:
                break

        await session.drain_writes()

        self.assertEqual(await queued, {"result": "ok"})
        self.assertEqual(session._pending_writes, [])
        with self.assertRaises(ActorClosed):
            await session.submit_commands(
                [NodeCommand(id="light-a", nt=2, props={"p": False})],
            )

    async def test_power_off_and_level_commands_are_never_merged(self) -> None:
        connection = FakeConnectionRef()
        session = self.session(connection, set_prop_batch_delay=0.01)

        await asyncio.gather(
            session.submit_commands(
                [NodeCommand(id="light-a", nt=2, props={"p": False})],
            ),
            session.submit_commands(
                [NodeCommand(id="light-a", nt=2, props={"l": 40})],
            ),
        )

        writes = [request for request in connection.requests if request.method == "gateway_set.prop"]
        self.assertEqual(len(writes), 2)
        self.assertEqual(writes[0].payload["nodes"][0]["set"], {"p": False})  # type: ignore[index]
        self.assertEqual(writes[1].payload["nodes"][0]["set"], {"l": 40})  # type: ignore[index]

        with self.assertRaises(ValueError):
            await session.submit_commands(
                [NodeCommand(id="light-a", nt=2, props={"p": False, "l": 40})],
            )
