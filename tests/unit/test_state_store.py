from __future__ import annotations

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "custom_components"))

from yeelight_pro.session.state import StateResult, StateStore  # noqa: E402


def _topology(*nodes: tuple[str | int, int, dict[str, object], bool]) -> dict[str, object]:
    return {
        "nodes": [
            {"id": node_id, "nt": node_type, "type": 3, "o": online, "params": params}
            for node_id, node_type, params, online in nodes
        ],
        "groups": [],
        "rooms": [],
        "scenes": [],
    }


def _props(*nodes: tuple[str | int, dict[str, object], bool | None]) -> dict[str, object]:
    payload: list[dict[str, object]] = []
    for node_id, params, online in nodes:
        item: dict[str, object] = {"id": node_id, "nt": 2, "params": params}
        if online is not None:
            item["o"] = online
        payload.append(item)
    return {"method": "gateway_post.prop", "nodes": payload}


def _groups(*nodes: tuple[str | int, dict[str, object], bool | None]) -> dict[str, object]:
    payload: list[dict[str, object]] = []
    for node_id, params, online in nodes:
        item: dict[str, object] = {"id": node_id, "nt": 4, "params": params}
        if online is not None:
            item["o"] = online
        payload.append(item)
    return {"groups": payload}


def _state(*nodes: tuple[str | int, int, dict[str, object], bool]) -> StateStore:
    store = StateStore()
    _parsed, result = store.apply_topology(_topology(*nodes))
    assert result.visible_changed
    return store


def _prepare(
    store: StateStore,
    targets: dict[str, dict[str, object]],
    *,
    deadline: float,
) -> tuple[int, StateResult]:
    batch_id, result = store.prepare_batch(targets, deadline=deadline)
    assert batch_id is not None
    return batch_id, result


def test_ack_projects_target_until_it_is_observed() -> None:
    store = _state(("light-a", 2, {"p": True}, True))

    batch_id, prepared = _prepare(store, {"light-a": {"p": False}}, deadline=10.0)
    accepted = store.accept_batch(batch_id)

    assert not prepared.visible_changed
    assert accepted.changed_node_ids == frozenset({"light-a"})
    assert store.raw.nodes["light-a"].params["p"] is True
    assert store.nodes["light-a"].params["p"] is False

    conflict = store.apply_properties(_props(("light-a", {"p": True}, None)))

    assert not conflict.visible_changed
    assert store.has_pending("light-a", ["p"])
    assert store.nodes["light-a"].params["p"] is False

    confirmed = store.apply_properties(_props(("light-a", {"p": False}, None)))

    assert not confirmed.visible_changed
    assert confirmed.ended_batch_ids == (batch_id,)
    assert store.nodes["light-a"].params["p"] is False
    assert not store.has_pending("light-a")


def test_equal_baseline_still_creates_a_hold_against_late_opposite_state() -> None:
    store = _state(("light-a", 2, {"p": True}, True))

    batch_id, _result = _prepare(store, {"light-a": {"p": True}}, deadline=10.0)
    store.accept_batch(batch_id)
    late_off = store.apply_properties(_props(("light-a", {"p": False}, None)))

    assert not late_off.visible_changed
    assert store.raw.nodes["light-a"].params["p"] is False
    assert store.nodes["light-a"].params["p"] is True
    assert store.has_pending("light-a", ["p"])

    observed = store.apply_properties(_props(("light-a", {"p": True}, None)))

    assert observed.ended_batch_ids == (batch_id,)
    assert not observed.visible_changed
    assert not store.has_pending("light-a")


def test_repeated_accepted_target_does_not_extend_its_deadline() -> None:
    store = _state(("light-a", 2, {"p": False}, True))
    batch_id, _result = _prepare(store, {"light-a": {"p": True}}, deadline=5.0)
    store.accept_batch(batch_id)

    repeated_id, repeated = store.prepare_batch({"light-a": {"p": True}}, deadline=20.0)

    assert repeated_id is None
    assert not repeated.visible_changed
    assert store.next_deadline() == 5.0
    assert store.diagnostics(now=1.0)["active_batches"] == 1
    assert store.nodes["light-a"].params["p"] is True

    confirmed = store.apply_properties(_props(("light-a", {"p": True}, None)))

    assert confirmed.ended_batch_ids == (batch_id,)
    assert not store.has_pending("light-a")


def test_target_observed_before_ack_is_released_only_after_ack() -> None:
    store = _state(("light-a", 2, {"p": False}, True))
    batch_id, _result = _prepare(store, {"light-a": {"p": True}}, deadline=10.0)

    early = store.apply_properties(_props(("light-a", {"p": True}, None)))

    assert not early.visible_changed
    assert store.nodes["light-a"].params["p"] is False

    accepted = store.accept_batch(batch_id)

    assert accepted.changed_node_ids == frozenset({"light-a"})
    assert accepted.ended_batch_ids == (batch_id,)
    assert store.nodes["light-a"].params["p"] is True


def test_gateway_batch_projects_all_members_together_on_ack() -> None:
    store = _state(
        ("ordinary", 2, {"p": True, "l": 20}, True),
        ("group", 4, {"p": True, "l": 20}, True),
    )
    batch_id, _result = _prepare(
        store,
        {"ordinary": {"l": 80}, "group": {"l": 80}},
        deadline=10.0,
    )
    accepted = store.accept_batch(batch_id)

    assert accepted.changed_node_ids == frozenset({"ordinary", "group"})
    assert store.nodes["ordinary"].params["l"] == 80
    assert store.nodes["group"].params["l"] == 80

    first = store.apply_properties(_props(("ordinary", {"l": 80}, None)))

    assert not first.visible_changed
    assert store.raw.nodes["ordinary"].params["l"] == 80

    second = store.apply_groups(_groups(("group", {"l": 80}, None)))

    assert not second.changed_node_ids
    assert second.ended_batch_ids == (batch_id,)
    assert store.nodes["ordinary"].params["l"] == 80
    assert store.nodes["group"].params["l"] == 80


def test_unrelated_property_publishes_while_target_property_is_held() -> None:
    store = _state(("light-a", 2, {"p": True, "l": 20, "ct": 3000}, True))
    batch_id, _result = _prepare(store, {"light-a": {"l": 80}}, deadline=10.0)
    store.accept_batch(batch_id)

    changed = store.apply_properties(_props(("light-a", {"l": 40, "ct": 4000}, None)))

    assert changed.changed_node_ids == frozenset({"light-a"})
    assert store.raw.nodes["light-a"].params == {"p": True, "l": 40, "ct": 4000}
    assert store.nodes["light-a"].params == {"p": True, "l": 80, "ct": 4000}


def test_deadline_releases_current_raw_without_marking_node_offline() -> None:
    store = _state(("light-a", 2, {"p": True}, True))
    batch_id, _result = _prepare(store, {"light-a": {"p": False}}, deadline=5.0)
    store.accept_batch(batch_id)
    store.apply_properties(_props(("light-a", {"p": True}, None)))

    before = store.expire_due(now=4.9)
    expired = store.expire_due(now=5.0)

    assert not before.visible_changed
    assert expired.ended_batch_ids == (batch_id,)
    assert expired.changed_node_ids == frozenset({"light-a"})
    assert store.nodes["light-a"].online is True
    assert store.nodes["light-a"].params["p"] is True


def test_deadline_publishes_observed_offline_state() -> None:
    store = _state(("group", 4, {"p": True}, True))
    batch_id, _result = _prepare(store, {"group": {"p": False}}, deadline=5.0)
    store.accept_batch(batch_id)

    offline = store.apply_groups(_groups(("group", {}, False)))

    assert offline.changed_node_ids == frozenset({"group"})
    assert store.nodes["group"].online is False
    assert store.nodes["group"].params["p"] is False

    expired = store.expire_batch(batch_id)

    assert expired.ended_batch_ids == (batch_id,)
    assert store.nodes["group"].online is False
    assert store.nodes["group"].params["p"] is True


def test_rpc_failure_removes_only_failed_batch_owners() -> None:
    store = _state(("light-a", 2, {"p": False, "l": 20}, True))
    first_id, _result = _prepare(store, {"light-a": {"p": True}}, deadline=10.0)
    store.accept_batch(first_id)
    second_id, _result = _prepare(store, {"light-a": {"l": 80}}, deadline=11.0)
    store.apply_properties(_props(("light-a", {"l": 40}, None)))

    failed = store.fail_batch(second_id)

    assert failed.changed_node_ids == frozenset({"light-a"})
    assert store.nodes["light-a"].params == {"p": True, "l": 40}
    assert store.has_pending("light-a", ["p"])
    assert not store.has_pending("light-a", ["l"])


def test_newer_batch_fences_old_readback_and_timer() -> None:
    store = _state(("light-a", 2, {"p": False}, True))
    first_id, _result = _prepare(store, {"light-a": {"p": True}}, deadline=5.0)
    store.accept_batch(first_id)
    second_id, _result = _prepare(store, {"light-a": {"p": False}}, deadline=10.0)
    store.accept_batch(second_id)

    old_readback = store.apply_properties(
        _props(("light-a", {"p": False}, None)),
        match_batch_id=first_id,
    )
    old_timer = store.expire_batch(first_id)

    assert not old_readback.visible_changed
    assert not old_timer.visible_changed
    assert store.has_pending("light-a", ["p"])

    current_readback = store.apply_properties(
        _props(("light-a", {"p": False}, None)),
        match_batch_id=second_id,
    )

    assert current_readback.ended_batch_ids == (second_id,)
    assert not store.has_pending("light-a")


def test_a_b_a_supersession_never_exposes_old_b() -> None:
    store = _state(("light-a", 2, {"p": True}, True))
    off_id, _result = _prepare(store, {"light-a": {"p": False}}, deadline=5.0)
    store.accept_batch(off_id)
    on_id, _result = _prepare(store, {"light-a": {"p": True}}, deadline=10.0)
    store.accept_batch(on_id)

    late_off = store.apply_properties(_props(("light-a", {"p": False}, None)))

    assert not late_off.visible_changed
    assert store.nodes["light-a"].params["p"] is True

    final_on = store.apply_properties(_props(("light-a", {"p": True}, None)))

    assert final_on.ended_batch_ids == (on_id,)
    assert store.nodes["light-a"].params["p"] is True
    assert not store.has_pending("light-a")


def test_superseding_one_property_can_release_remaining_old_target() -> None:
    store = _state(("light-a", 2, {"p": False, "l": 20}, True))
    first_id, _result = _prepare(store, {"light-a": {"p": True, "l": 80}}, deadline=5.0)
    store.accept_batch(first_id)
    store.apply_properties(_props(("light-a", {"p": True}, None)))

    second_id, prepared = _prepare(store, {"light-a": {"l": 40}}, deadline=10.0)

    assert prepared.ended_batch_ids == (first_id,)
    assert not prepared.visible_changed
    assert store.nodes["light-a"].params == {"p": True, "l": 80}
    assert store.has_pending("light-a", ["l"])
    assert second_id != first_id

    accepted = store.accept_batch(second_id)

    assert accepted.changed_node_ids == frozenset({"light-a"})
    assert store.nodes["light-a"].params == {"p": True, "l": 40}


def test_disconnect_clears_holds_and_exposes_last_observation() -> None:
    store = _state(("light-a", 2, {"p": True}, True))
    batch_id, _result = _prepare(store, {"light-a": {"p": False}}, deadline=10.0)
    store.accept_batch(batch_id)
    store.apply_properties(_props(("light-a", {"p": True}, None)))

    cleared = store.clear_pending()

    assert cleared.ended_batch_ids == (batch_id,)
    assert store.nodes["light-a"].params["p"] is True
    assert not store.has_pending("light-a")


def test_property_before_topology_is_retained_as_raw_observation() -> None:
    store = StateStore()
    store.apply_properties(_props(("late-light", {"p": True, "l": 60}, True)))

    assert "late-light" not in store.nodes
    assert store.unknown_summary()["count"] == 1

    _topology_result, applied = store.apply_topology(
        _topology(("late-light", 2, {}, True)),
    )

    assert applied.changed_node_ids == frozenset({"late-light"})
    assert store.nodes["late-light"].params == {"p": True, "l": 60}
    assert store.unknown_summary()["count"] == 0


def test_randomized_sequence_never_fabricates_visible_values() -> None:
    rng = random.Random(20260710)
    store = _state(("light-a", 2, {"p": False, "l": 10}, True))
    known: dict[str, set[object]] = {"p": {False}, "l": {10}}
    active_ids: list[int] = []

    for step in range(500):
        operation = rng.randrange(5)
        if operation == 0:
            prop = rng.choice(("p", "l"))
            value: object = rng.choice((False, True)) if prop == "p" else rng.randrange(1, 101)
            known[prop].add(value)
            batch_id, _result = store.prepare_batch({"light-a": {prop: value}}, deadline=float(step + 5))
            if batch_id is not None:
                active_ids.append(batch_id)
        elif operation == 1 and active_ids:
            batch_id = rng.choice(active_ids)
            store.accept_batch(batch_id)
        elif operation == 2:
            prop = rng.choice(("p", "l"))
            value = rng.choice((False, True)) if prop == "p" else rng.randrange(1, 101)
            known[prop].add(value)
            store.apply_properties(_props(("light-a", {prop: value}, None)))
        elif operation == 3 and active_ids:
            store.expire_batch(rng.choice(active_ids))
        else:
            store.expire_due(now=float(step))

        visible = store.nodes["light-a"].params
        raw = store.raw.nodes["light-a"].params
        assert visible["p"] in known["p"]
        assert visible["l"] in known["l"]
        assert raw["p"] in known["p"]
        assert raw["l"] in known["l"]
