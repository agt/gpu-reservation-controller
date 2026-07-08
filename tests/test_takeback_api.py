"""Reclaim-block take-back — ControllerState.take_back_blocks + POST /api/reservations/take-back.

Two layers of coverage, mirroring test_push_api.py:

* the pure, synchronous state logic (idle checks, all-or-nothing, merge
  truncation/detachment, tombstones) with no Kubernetes or HTTP calls;
* the HTTP endpoint via ``TestClient`` (auth, status-code mapping, the
  snapshot fail-closed path, and the push-restore round trip).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.controller import ControllerState
from app.k8s_client import ToleratedPodInfo
from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, block as _block
from tests.test_push_api import _patched_main

GUARD_MINUTES = 240  # wide enough to commit both future blocks in one pass


def _expiry(now: datetime) -> datetime:
    """A stand-in for the endpoint's unknown-id fallback expiry."""
    return now + timedelta(minutes=10)


def _state(*reservations, fetch_at: datetime | None = None) -> ControllerState:
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.reclaim_preempt_guard_minutes = GUARD_MINUTES
    state.last_reservation_fetch_at = fetch_at or datetime.now(timezone.utc)
    state.reservations = list(reservations)
    return state


def _merged_state(now: datetime):
    """Subject #1 (open) with future reclaim stubs #2 and #3 merged onto it.

    Windows: subject t0–t1, #2 t1–t2, #3 t2–t3 (all abutting, equal capacity).
    Returns ``(state, subject, (t0, t1, t2, t3))``.
    """
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(minutes=60)
    t3 = t2 + timedelta(minutes=60)
    subject = _block(1, t0, t1)
    state = _state(subject, _block(2, t1, t2), _block(3, t2, t3), fetch_at=now)
    state.reconcile_reclaim_merges(now)
    assert state.merged_stub_ids == {2, 3}
    assert state.reclaim_merges[1].absorbed_ids == [2, 3]
    return state, subject, (t0, t1, t2, t3)


# ---------------------------------------------------------------------------
# take_back_blocks: standalone blocks
# ---------------------------------------------------------------------------


def test_idle_block_taken_and_tombstoned():
    now = datetime.now(timezone.utc)
    b = _block(1, now - timedelta(hours=1), now + timedelta(hours=1))
    state = _state(b)

    result = state.take_back_blocks({1}, {}, now, _expiry(now))

    assert result.granted
    assert result.taken_back == [1]
    assert state.reservations == []
    assert state.taken_back == {1: b.end_utc}


def test_occupied_block_conflicts_and_nothing_mutates():
    now = datetime.now(timezone.utc)
    b = _block(1, now - timedelta(hours=1), now + timedelta(hours=1))
    state = _state(b)

    result = state.take_back_blocks({1}, {1: [None]}, now, _expiry(now))

    assert not result.granted
    assert [(c.reservation_id, c.reason) for c in result.conflicts] == [(1, "occupied")]
    assert [r.id for r in state.reservations] == [1]
    assert state.taken_back == {}


def test_all_or_nothing_rejects_idle_blocks_too():
    now = datetime.now(timezone.utc)
    busy = _block(1, now - timedelta(hours=1), now + timedelta(hours=1))
    idle = _block(2, now - timedelta(hours=1), now + timedelta(hours=1))
    state = _state(busy, idle)

    result = state.take_back_blocks({1, 2}, {1: [None]}, now, _expiry(now))

    assert not result.granted
    assert [(c.reservation_id, c.reason) for c in result.conflicts] == [(1, "occupied")]
    # The idle block was NOT taken, tombstoned, or removed.
    assert {r.id for r in state.reservations} == {1, 2}
    assert state.taken_back == {}


def test_claimed_block_conflicts():
    now = datetime.now(timezone.utc)
    state = _state(_block(1, now - timedelta(hours=1), now + timedelta(hours=1)))
    state.claimed_reservation_ids = {1}

    result = state.take_back_blocks({1}, {}, now, _expiry(now))

    assert [(c.reservation_id, c.reason) for c in result.conflicts] == [(1, "claimed")]
    assert [r.id for r in state.reservations] == [1]


def test_booking_id_is_invalid_target():
    now = datetime.now(timezone.utc)
    booking = _block(1, now - timedelta(hours=1), now + timedelta(hours=1),
                     kind="booking", user=True)
    cancelled = _block(2, now - timedelta(hours=1), now + timedelta(hours=1),
                       kind="booking", status="cancelled", user=True)
    state = _state(booking)
    state.cancelled_reservations = {2: cancelled}

    result = state.take_back_blocks({1, 2}, {}, now, _expiry(now))

    assert not result.granted
    assert {(c.reservation_id, c.reason) for c in result.invalid} == {
        (1, "not-a-reclaim-block"),
        (2, "not-a-reclaim-block"),
    }
    assert [r.id for r in state.reservations] == [1]
    assert 2 in state.cancelled_reservations
    assert state.taken_back == {}


def test_cancelled_reclaim_block_can_be_taken():
    now = datetime.now(timezone.utc)
    b = _block(1, now - timedelta(hours=1), now + timedelta(hours=1), status="cancelled")
    state = _state()
    state.cancelled_reservations = {1: b}

    result = state.take_back_blocks({1}, {}, now, _expiry(now))

    assert result.granted and result.taken_back == [1]
    assert state.cancelled_reservations == {}
    assert state.taken_back == {1: b.end_utc}


def test_idempotent_retry_reports_already_taken_back():
    now = datetime.now(timezone.utc)
    state = _state(
        _block(1, now - timedelta(hours=1), now + timedelta(hours=1)),
        _block(2, now - timedelta(hours=1), now + timedelta(hours=1)),
    )
    assert state.take_back_blocks({1}, {}, now, _expiry(now)).granted

    result = state.take_back_blocks({1, 2}, {}, now, _expiry(now))

    assert result.granted
    assert result.taken_back == [2]
    assert result.already_taken_back == [1]
    assert state.reservations == []


# ---------------------------------------------------------------------------
# take_back_blocks: unknown ids
# ---------------------------------------------------------------------------


def test_unknown_id_granted_and_tombstoned_with_fallback_expiry():
    now = datetime.now(timezone.utc)
    state = _state()

    result = state.take_back_blocks({99}, {}, now, _expiry(now))

    assert result.granted
    assert result.unknown == [99] and result.taken_back == []
    assert state.taken_back == {99: _expiry(now)}


def test_unknown_id_with_orphan_occupancy_conflicts():
    """Occupancy rebuilt from pod annotations (e.g. after a restart) can hold
    ids the reservation list does not — those still block a take-back."""
    now = datetime.now(timezone.utc)
    state = _state()

    result = state.take_back_blocks({99}, {99: [None]}, now, _expiry(now))

    assert [(c.reservation_id, c.reason) for c in result.conflicts] == [(99, "occupied")]
    assert state.taken_back == {}


# ---------------------------------------------------------------------------
# take_back_blocks: merged stubs and subjects
# ---------------------------------------------------------------------------


def test_tail_stub_truncates_merge():
    now = datetime.now(timezone.utc)
    state, subject, (t0, t1, t2, t3) = _merged_state(now)

    result = state.take_back_blocks({3}, {}, now, _expiry(now))

    assert result.granted and result.taken_back == [3] and result.detached == []
    assert state.reclaim_merges[1].absorbed_ids == [2]
    assert state.reclaim_merges[1].extended_end == t2
    assert state.merged_stub_ids == {2}
    assert {r.id for r in state.reservations} == {1, 2}
    assert state.effective_end(subject) == t2
    assert state.taken_back == {3: t3}
    # On-demand placement now caps at the truncated end.
    found = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
    assert found is not None and found.id == 1 and state.effective_end(found) == t2


def test_middle_stub_take_back_detaches_tail():
    now = datetime.now(timezone.utc)
    state, subject, (t0, t1, t2, t3) = _merged_state(now)

    result = state.take_back_blocks({2}, {}, now, _expiry(now))

    assert result.granted and result.taken_back == [2]
    assert result.detached == [3]
    assert state.reclaim_merges == {}          # nothing kept before the cut
    assert state.merged_stub_ids == set()
    # The detached tail is back as a standalone block; the taken one is gone.
    assert {r.id for r in state.reservations} == {1, 3}
    assert state.effective_end(subject) == t1  # subject back to its own window


def test_subject_take_back_dissolves_merge_and_detaches_stubs():
    now = datetime.now(timezone.utc)
    state, subject, (t0, t1, t2, t3) = _merged_state(now)

    result = state.take_back_blocks({1}, {}, now, _expiry(now))

    assert result.granted and result.taken_back == [1]
    assert result.detached == [2, 3]
    assert state.reclaim_merges == {} and state.merged_stub_ids == set()
    assert {r.id for r in state.reservations} == {2, 3}
    assert state.taken_back == {1: t1}


def test_stub_in_use_when_pod_projects_into_it():
    now = datetime.now(timezone.utc)
    state, _, (t0, t1, t2, t3) = _merged_state(now)
    # A pod on the subject whose deadline reaches into stub #2 (but not #3).
    pod_ends = {1: [t1 + timedelta(minutes=5)]}

    result = state.take_back_blocks({2}, pod_ends, now, _expiry(now))

    assert [(c.reservation_id, c.reason) for c in result.conflicts] == [
        (2, "extension-in-use")
    ]
    assert state.merged_stub_ids == {2, 3}  # nothing mutated


def test_tail_stub_free_when_pod_ends_before_it():
    """The marquee case: a job extended a little into stub #2 must not block
    taking back the untouched tail stub #3."""
    now = datetime.now(timezone.utc)
    state, _, (t0, t1, t2, t3) = _merged_state(now)
    pod_ends = {1: [t1 + timedelta(minutes=5)]}

    result = state.take_back_blocks({3}, pod_ends, now, _expiry(now))

    assert result.granted and result.taken_back == [3]
    assert state.reclaim_merges[1].absorbed_ids == [2]
    assert state.merged_stub_ids == {2}


def test_stub_free_when_pod_ends_within_subject():
    """A pod admitted before the merge (deadline ≤ the subject's own end)
    never blocks taking back an absorbed stub."""
    now = datetime.now(timezone.utc)
    state, _, (t0, t1, t2, t3) = _merged_state(now)
    pod_ends = {1: [t1 - timedelta(minutes=5)]}

    result = state.take_back_blocks({2, 3}, pod_ends, now, _expiry(now))

    assert result.granted and result.taken_back == [2, 3]
    assert state.reclaim_merges == {} and state.merged_stub_ids == set()


def test_unbounded_pod_on_subject_blocks_all_stubs():
    now = datetime.now(timezone.utc)
    state, _, _ = _merged_state(now)

    result = state.take_back_blocks({3}, {1: [None]}, now, _expiry(now))

    assert [(c.reservation_id, c.reason) for c in result.conflicts] == [
        (3, "extension-in-use")
    ]


def test_claimed_subject_stub_take_back_keeps_earlier_stub():
    """B4 shape: subject claimed by a reserved holder, an on-demand job's
    extended deadline reaches stub #2.  Taking back tail #3 must keep #2
    stubbed — both here and through the next reconcile pass."""
    now = datetime.now(timezone.utc)
    state, subject, (t0, t1, t2, t3) = _merged_state(now)
    state.claimed_reservation_ids = {1}
    pod_ends = {1: [t1 + timedelta(minutes=5)]}

    result = state.take_back_blocks({3}, pod_ends, now, _expiry(now))

    assert result.granted and result.taken_back == [3]
    assert state.reclaim_merges[1].absorbed_ids == [2]
    assert state.merged_stub_ids == {2}

    # The claimed-subject validation branch retains the truncated merge.
    state.reconcile_reclaim_merges(now)
    assert state.reclaim_merges[1].absorbed_ids == [2]
    assert state.merged_stub_ids == {2}
    assert subject.end_utc == t1  # claimed subject's own window never mutated


# ---------------------------------------------------------------------------
# Tombstones: fetch filter, pinning, push restore
# ---------------------------------------------------------------------------


def test_filter_taken_back_drops_fetched_entry_until_expiry():
    now = datetime.now(timezone.utc)
    end = now + timedelta(hours=1)
    b = _block(1, now - timedelta(hours=1), end)
    state = _state(b)
    assert state.take_back_blocks({1}, {}, now, _expiry(now)).granted

    # A (stale) fetch still returns the block — it must be filtered out.
    fresh = _block(1, now - timedelta(hours=1), end)
    assert state.filter_taken_back([fresh], now) == []
    assert state.taken_back == {1: end}  # pinned to the observed window

    # Once the window has passed the tombstone is pruned and entries flow again.
    later = end + timedelta(seconds=1)
    assert state.filter_taken_back([fresh], later) == [fresh]
    assert state.taken_back == {}


def test_unknown_tombstone_pinned_to_observed_window():
    now = datetime.now(timezone.utc)
    state = _state()
    assert state.take_back_blocks({99}, {}, now, _expiry(now)).granted
    assert state.taken_back == {99: _expiry(now)}

    observed_end = now + timedelta(hours=3)  # well past the fallback expiry
    entry = _block(99, now + timedelta(hours=2), observed_end)
    assert state.filter_taken_back([entry], now) == []
    assert state.taken_back == {99: observed_end}


def test_clear_taken_back_restores_pushed_id():
    now = datetime.now(timezone.utc)
    b = _block(1, now - timedelta(hours=1), now + timedelta(hours=1))
    state = _state(b)
    assert state.take_back_blocks({1}, {}, now, _expiry(now)).granted

    state.clear_taken_back({1})

    assert state.taken_back == {}
    fresh = _block(1, now - timedelta(hours=1), now + timedelta(hours=1))
    assert state.filter_taken_back([fresh], now) == [fresh]


# ---------------------------------------------------------------------------
# HTTP endpoint
# ---------------------------------------------------------------------------


def _open_block(res_id: int, **kw):
    now = datetime.now(timezone.utc)
    return _block(res_id, now - timedelta(hours=1), now + timedelta(hours=1), **kw)


def _post(client, ids, token="secret"):
    return client.post(
        "/api/reservations/take-back",
        json={"reclaim_ids": ids},
        headers={"Authorization": f"Bearer {token}"},
    )


async def _empty_snapshot(_key):
    return []


def test_takeback_disabled_returns_503_when_token_unset(monkeypatch):
    main = _patched_main(monkeypatch, token=None)
    with TestClient(main.app) as client:
        resp = client.post("/api/reservations/take-back", json={"reclaim_ids": [1]})
    assert resp.status_code == 503


@pytest.mark.parametrize("headers", [
    {},                                   # missing
    {"Authorization": "Bearer wrong"},    # mismatched
    {"Authorization": "secret"},          # no scheme
])
def test_takeback_bad_or_missing_bearer_returns_401(monkeypatch, headers):
    main = _patched_main(monkeypatch, token="secret")
    with TestClient(main.app) as client:
        resp = client.post(
            "/api/reservations/take-back", json={"reclaim_ids": [1]}, headers=headers
        )
    assert resp.status_code == 401


def test_takeback_happy_path(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    monkeypatch.setattr(main, "snapshot_tolerated_pods", _empty_snapshot)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_open_block(1)]

        resp = _post(client, [1, 99])

        assert resp.status_code == 200
        assert resp.json() == {
            "taken_back": [1],
            "already_taken_back": [],
            "unknown": [99],
            "detached": [],
            "total_active": 0,
        }
        assert state.reservations == []
        assert set(state.taken_back) == {1, 99}


def test_takeback_occupied_returns_409(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")

    pod = ToleratedPodInfo(
        namespace="alice", name="pod-1", uid="uid-1", gpu_class=GPU_CLASS_LABEL,
        booking_reference="ondemand-1", reservation_id=1, gpu_count=2,
        phase="Running", scheduled_false=False,
    )

    async def _snapshot(_key):
        return [pod]

    monkeypatch.setattr(main, "snapshot_tolerated_pods", _snapshot)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_open_block(1)]

        resp = _post(client, [1])

        assert resp.status_code == 409
        assert resp.json()["detail"]["in_use"] == [{"id": 1, "reason": "occupied"}]
        assert [r.id for r in state.reservations] == [1]
        assert state.taken_back == {}


def test_takeback_snapshot_failure_fails_closed_503(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")

    async def _boom(_key):
        raise RuntimeError("apiserver down")

    monkeypatch.setattr(main, "snapshot_tolerated_pods", _boom)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_open_block(1)]

        resp = _post(client, [1])

        assert resp.status_code == 503
        assert [r.id for r in state.reservations] == [1]
        assert state.taken_back == {}


def test_takeback_inflight_occupancy_blocks(monkeypatch):
    """A placement recorded in memory but not yet visible to the LIST (patch
    in flight) must still block the take-back."""
    main = _patched_main(monkeypatch, token="secret")
    monkeypatch.setattr(main, "snapshot_tolerated_pods", _empty_snapshot)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_open_block(1)]
        state.record_placement(1, "uid-inflight", 2)

        resp = _post(client, [1])

        assert resp.status_code == 409
        assert resp.json()["detail"]["in_use"] == [{"id": 1, "reason": "occupied"}]


def test_takeback_booking_id_returns_400(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    monkeypatch.setattr(main, "snapshot_tolerated_pods", _empty_snapshot)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_open_block(1, kind="booking", user=True)]

        resp = _post(client, [1])

        assert resp.status_code == 400
        assert resp.json()["detail"]["invalid"] == [
            {"id": 1, "reason": "not-a-reclaim-block"}
        ]
        assert [r.id for r in state.reservations] == [1]


def test_push_restores_taken_back_block(monkeypatch):
    """The sanctioned restore path: pushing the id active again clears the
    tombstone and re-adds the block."""
    main = _patched_main(monkeypatch, token="secret")
    monkeypatch.setattr(main, "snapshot_tolerated_pods", _empty_snapshot)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_open_block(1)]

        assert _post(client, [1]).status_code == 200
        assert state.reservations == [] and 1 in state.taken_back

        resp = client.post(
            "/api/reservations/push",
            json={"reservations": [_open_block(1).model_dump(mode="json")]},
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.status_code == 200
        assert [r.id for r in state.reservations] == [1]
        assert state.taken_back == {}
