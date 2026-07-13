"""Unit tests for no-show reservation conversion logic.

Covers: update_noshow_tracking (init + new), reconcile_noshow,
check_noshow_deadlines, mark_pod_seen_for_noshow, enqueue_pod deadline clearing,
find_best_reservation skipping no-shows, and reconcile_occupancy including
no-shows.  Guarantee-arithmetic no-show skipping (compute_guaranteed_until) is
covered in test_guarantees.py.

No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from app.controller import slot_end, slot_start
from app.schemas import ReservationResponse

from tests.conftest import (
    FIXED_DATE,
    FUTURE_DATE,
    GPU_CLASS_LABEL,
    OTHER_CLASS_ID,
    USERNAME,
)
from tests.conftest import make_state as _state
from tests.conftest import reclaim_reservation as _reclaim_reservation
from tests.conftest import user_reservation as _user_reservation


# ---------------------------------------------------------------------------
# Shared constants & factories
# ---------------------------------------------------------------------------

TIMEOUT = 15
GRACE = 30


def _window_start_dt(start_time: str = "08:00:00", reservation_date: date = FIXED_DATE) -> datetime:
    h, m, _ = start_time.split(":")
    midnight = datetime.combine(reservation_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    return midnight + timedelta(hours=int(h), minutes=int(m))


def _window_open_now(
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
) -> datetime:
    """Return a UTC datetime that falls inside the given window (at the midpoint)."""
    return _window_start_dt(start_time, reservation_date) + timedelta(
        minutes=duration_minutes // 2
    )


# ---------------------------------------------------------------------------
# TestInitializeNoshowTracking
# ---------------------------------------------------------------------------


class TestInitializeNoshowTracking:
    def test_future_reservation_deadline_is_slot_start_plus_timeout(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        state.update_noshow_tracking(now, TIMEOUT, GRACE, reason="init")
        assert state.noshow_deadlines[1] == slot_start(r) + timedelta(minutes=TIMEOUT)

    def test_midwindow_deadline_is_now_plus_grace(self):
        r = _user_reservation(1)  # window: FIXED_DATE 08:00–10:00 UTC
        now = _window_open_now()   # 09:00 UTC — inside the window
        state = _state(r)
        state.update_noshow_tracking(now, TIMEOUT, GRACE, reason="init")
        assert state.noshow_deadlines[1] == now + timedelta(minutes=GRACE)

    def test_expired_reservation_not_tracked(self):
        r = _user_reservation(1)  # window ended on FIXED_DATE in the past
        now = _window_start_dt() + timedelta(hours=3)  # after slot_end
        state = _state(r)
        state.update_noshow_tracking(now, TIMEOUT, GRACE, reason="init")
        assert 1 not in state.noshow_deadlines

    def test_reclaim_reservation_not_tracked(self):
        r = _reclaim_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        state.update_noshow_tracking(now, TIMEOUT, GRACE, reason="init")
        assert 1 not in state.noshow_deadlines

    def test_user_none_not_tracked(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.reservations[0] = ReservationResponse(
            **{**r.model_dump(), "user": None, "user_id": None}
        )
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        state.update_noshow_tracking(now, TIMEOUT, GRACE, reason="init")
        assert 1 not in state.noshow_deadlines

    def test_multiple_reservations_all_tracked(self):
        r1 = _user_reservation(1, reservation_date=FUTURE_DATE, slot_index=0)
        r2 = _user_reservation(2, reservation_date=FUTURE_DATE, slot_index=1)
        state = _state(r1, r2)
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        state.update_noshow_tracking(now, TIMEOUT, GRACE, reason="init")
        assert 1 in state.noshow_deadlines
        assert 2 in state.noshow_deadlines

    def test_does_not_overwrite_existing_deadline(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        sentinel = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.noshow_deadlines[1] = sentinel
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        state.update_noshow_tracking(now, TIMEOUT, GRACE, reason="init")
        assert state.noshow_deadlines[1] == sentinel


# ---------------------------------------------------------------------------
# TestUpdateNoshowTracking
# ---------------------------------------------------------------------------


class TestUpdateNoshowTracking:
    def test_new_reservation_gets_deadline(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        state.update_noshow_tracking(now, TIMEOUT, GRACE)
        assert 1 in state.noshow_deadlines

    def test_existing_deadline_not_overwritten(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        sentinel = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.noshow_deadlines[1] = sentinel
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        state.update_noshow_tracking(now, TIMEOUT, GRACE)
        assert state.noshow_deadlines[1] == sentinel

    def test_declared_noshow_not_resurrected(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        state.update_noshow_tracking(now, TIMEOUT, GRACE)
        assert 1 not in state.noshow_deadlines

    def test_expired_reservation_not_added(self):
        r = _user_reservation(1)  # FIXED_DATE — window already over
        now = _window_start_dt() + timedelta(hours=3)
        state = _state(r)
        state.update_noshow_tracking(now, TIMEOUT, GRACE)
        assert 1 not in state.noshow_deadlines

    def test_midwindow_uses_grace(self):
        r = _user_reservation(1)
        now = _window_open_now()
        state = _state(r)
        state.update_noshow_tracking(now, TIMEOUT, GRACE)
        assert state.noshow_deadlines[1] == now + timedelta(minutes=GRACE)


# ---------------------------------------------------------------------------
# TestReconcileNoshow
# ---------------------------------------------------------------------------


class TestReconcileNoshow:
    def test_deadline_pruned_for_missing_reservation(self):
        state = _state()
        state.noshow_deadlines[99] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.reconcile_noshow()
        assert 99 not in state.noshow_deadlines

    def test_noshow_id_pruned_for_missing_reservation(self):
        state = _state()
        state.noshow_reservation_ids.add(99)
        state.reconcile_noshow()
        assert 99 not in state.noshow_reservation_ids

    def test_active_reservation_deadline_preserved(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        deadline = datetime(2099, 6, 1, tzinfo=timezone.utc)
        state.noshow_deadlines[1] = deadline
        state.reconcile_noshow()
        assert state.noshow_deadlines[1] == deadline

    def test_active_noshow_id_preserved(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        state.reconcile_noshow()
        assert 1 in state.noshow_reservation_ids

    def test_pending_cancel_pruned_once_cancel_has_landed(self):
        # Once the controller-issued cancel lands app-side, the id drops out
        # of the fetched active list — no restart-safe re-arm is needed, so
        # the retry bookkeeping should not linger either.
        state = _state()
        state.pending_noshow_cancels.add(99)
        state.reconcile_noshow()
        assert 99 not in state.pending_noshow_cancels

    def test_pending_cancel_preserved_while_still_active(self):
        # The cancel hasn't landed yet (a prior attempt failed) — the id is
        # still active, so the retry must be preserved for the next tick.
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.pending_noshow_cancels.add(1)
        state.reconcile_noshow()
        assert 1 in state.pending_noshow_cancels


# ---------------------------------------------------------------------------
# TestCheckNoshowDeadlines
# ---------------------------------------------------------------------------


class TestCheckNoshowDeadlines:
    def test_expired_deadline_moves_to_noshow_ids(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        deadline = datetime(2024, 6, 1, 9, 0, tzinfo=timezone.utc)
        state.noshow_deadlines[1] = deadline
        state.check_noshow_deadlines(deadline + timedelta(seconds=1))
        assert 1 in state.noshow_reservation_ids
        assert 1 not in state.noshow_deadlines
        assert 1 in state.pending_noshow_cancels

    def test_window_already_ended_not_queued_for_cancel(self):
        # A reservation whose window ended before the deadline check must not
        # be declared no-show at all, so it must not be queued for a cancel
        # either (there's nothing to reclaim).
        r = _user_reservation(1, reservation_date=FIXED_DATE, duration_minutes=60)
        state = _state(r)
        deadline = datetime(2024, 1, 15, 8, 30, tzinfo=timezone.utc)
        state.noshow_deadlines[1] = deadline
        after_window_end = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        state.check_noshow_deadlines(after_window_end)
        assert 1 not in state.noshow_reservation_ids
        assert 1 not in state.pending_noshow_cancels

    def test_future_deadline_not_moved(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        deadline = datetime(2099, 6, 1, 9, 0, tzinfo=timezone.utc)
        state.noshow_deadlines[1] = deadline
        state.check_noshow_deadlines(deadline - timedelta(seconds=1))
        assert 1 not in state.noshow_reservation_ids
        assert 1 in state.noshow_deadlines

    def test_exactly_at_deadline_is_noshow(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        deadline = datetime(2024, 6, 1, 9, 0, tzinfo=timezone.utc)
        state.noshow_deadlines[1] = deadline
        state.check_noshow_deadlines(deadline)
        assert 1 in state.noshow_reservation_ids
        assert 1 not in state.noshow_deadlines

    def test_only_expired_moves_when_mixed(self):
        r1 = _user_reservation(1, reservation_date=FUTURE_DATE, slot_index=0)
        r2 = _user_reservation(2, reservation_date=FUTURE_DATE, slot_index=1)
        state = _state(r1, r2)
        now = datetime(2024, 6, 1, 9, 30, tzinfo=timezone.utc)
        state.noshow_deadlines[1] = datetime(2024, 6, 1, 9, 0, tzinfo=timezone.utc)   # expired
        state.noshow_deadlines[2] = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)  # future
        state.check_noshow_deadlines(now)
        assert 1 in state.noshow_reservation_ids
        assert 2 not in state.noshow_reservation_ids
        assert 2 in state.noshow_deadlines

    def test_empty_deadlines_is_noop(self):
        state = _state()
        state.check_noshow_deadlines(datetime.now(timezone.utc))  # must not raise


# ---------------------------------------------------------------------------
# TestMarkPodSeenForNoshow
# ---------------------------------------------------------------------------


class TestMarkPodSeenForNoshow:
    def test_clears_deadline_for_matching_reservation(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)
        assert 1 not in state.noshow_deadlines

    def test_wrong_namespace_leaves_deadline(self):
        r = _user_reservation(1, username="bob", reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.mark_pod_seen_for_noshow("alice", GPU_CLASS_LABEL)
        assert 1 in state.noshow_deadlines

    def test_wrong_gpu_class_leaves_deadline(self):
        r = _user_reservation(1, gpu_class_id=OTHER_CLASS_ID, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.gpu_class_labels[OTHER_CLASS_ID] = "a100"
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)
        assert 1 in state.noshow_deadlines

    def test_not_in_deadlines_is_noop(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        # noshow_deadlines[1] not set — must not raise
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)
        assert 1 not in state.noshow_deadlines

    def test_picks_soonest_slot_start_when_multiple(self):
        # r1 slot_index=0 → 08:00 UTC, r2 slot_index=1 → 10:00 UTC
        r1 = _user_reservation(1, reservation_date=FUTURE_DATE, slot_index=0)
        r2 = _user_reservation(2, reservation_date=FUTURE_DATE, slot_index=1)
        state = _state(r1, r2)
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.noshow_deadlines[2] = datetime(2099, 1, 2, tzinfo=timezone.utc)
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)
        assert 1 not in state.noshow_deadlines  # soonest cleared
        assert 2 in state.noshow_deadlines       # later one untouched

    def test_noop_when_no_reservations(self):
        state = _state()
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)  # must not raise


# ---------------------------------------------------------------------------
# TestEnqueuePodClearsDeadline
# ---------------------------------------------------------------------------


class TestEnqueuePodClearsDeadline:
    def test_enqueue_clears_noshow_deadline(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        assert 1 not in state.noshow_deadlines

    def test_enqueue_no_match_leaves_deadlines_unchanged(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        # Wrong namespace — find_best_reservation returns None
        state.enqueue_pod("uid-1", "pod-1", "nobody", GPU_CLASS_LABEL, 1)
        assert 1 in state.noshow_deadlines


# ---------------------------------------------------------------------------
# TestFindBestReservationSkipsNoshow
# ---------------------------------------------------------------------------


class TestFindBestReservationSkipsNoshow:
    def test_noshow_reservation_skipped(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL) is None

    def test_non_noshow_returned_normally(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        result = state.find_best_reservation(USERNAME, GPU_CLASS_LABEL)
        assert result is not None
        assert result.id == 1

    def test_noshow_skipped_other_returned(self):
        r1 = _user_reservation(1, reservation_date=FUTURE_DATE, slot_index=0)
        r2 = _user_reservation(2, reservation_date=FUTURE_DATE, slot_index=1)
        state = _state(r1, r2)
        state.noshow_reservation_ids.add(1)
        result = state.find_best_reservation(USERNAME, GPU_CLASS_LABEL)
        assert result is not None
        assert result.id == 2


# ---------------------------------------------------------------------------
# TestReconcileOccupancyNoshow
# ---------------------------------------------------------------------------


class TestReconcileOccupancyNoshow:
    def test_noshow_block_occupancy_rebuilt_from_snapshot(self):
        # Occupancy is keyed by reservation id regardless of kind: a noshow-1
        # squatter present in the snapshot is retained; a vanished one is dropped.
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        state.occupancy[1] = {"uid-gone": 1}
        state.reconcile_occupancy([(1, "uid-a", 1)])
        assert state.occupancy == {1: {"uid-a": 1}}


# ---------------------------------------------------------------------------
# main._cancel_pending_noshows — no-show declared -> durable app-side cancel
# ---------------------------------------------------------------------------


class _FakeCancelClient:
    def __init__(self, *, cancel_result=True):
        self.cancel_result = cancel_result
        self.cancel_calls: list = []

    async def cancel_reservation(self, reservation_id, reason):
        self.cancel_calls.append((reservation_id, reason))
        return self.cancel_result


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-noshow-cancel")
    import app.main as main_module

    return main_module


def _pod_info(reservation_id, phase="Running"):
    from types import SimpleNamespace

    return SimpleNamespace(reservation_id=reservation_id, phase=phase)


class TestCancelPendingNoshows:
    def test_declared_noshow_is_cancelled_and_cleared(self, monkeypatch):
        import asyncio

        m = _main_module(monkeypatch)
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.pending_noshow_cancels = {1}
        client = _FakeCancelClient(cancel_result=True)

        asyncio.run(m._cancel_pending_noshows(state, client, []))

        assert client.cancel_calls == [(1, "no-show")]
        assert 1 not in state.pending_noshow_cancels
        assert [x.id for x in state.reservations] == []

    def test_failed_cancel_retried_next_tick(self, monkeypatch):
        import asyncio

        m = _main_module(monkeypatch)
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.pending_noshow_cancels = {1}
        client = _FakeCancelClient(cancel_result=False)

        asyncio.run(m._cancel_pending_noshows(state, client, []))

        assert client.cancel_calls == [(1, "no-show")]
        assert 1 in state.pending_noshow_cancels  # left pending for retry
        assert [x.id for x in state.reservations] == [1]  # not removed

    def test_live_pod_recheck_suppresses_cancel(self, monkeypatch):
        import asyncio

        m = _main_module(monkeypatch)
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.pending_noshow_cancels = {1}
        client = _FakeCancelClient(cancel_result=True)
        snapshot = [_pod_info(1, phase="Running")]  # last-second arrival

        asyncio.run(m._cancel_pending_noshows(state, client, snapshot))

        assert client.cancel_calls == []
        assert 1 in state.pending_noshow_cancels  # untouched; may retry later

    def test_terminating_pod_does_not_suppress_cancel(self, monkeypatch):
        # A pod snapshot entry only counts as "occupied" when Running/Pending;
        # phases outside that (e.g. already gone) must not block the cancel.
        import asyncio

        m = _main_module(monkeypatch)
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.pending_noshow_cancels = {1}
        client = _FakeCancelClient(cancel_result=True)
        snapshot = [_pod_info(1, phase="Succeeded")]

        asyncio.run(m._cancel_pending_noshows(state, client, snapshot))

        assert client.cancel_calls == [(1, "no-show")]
        assert 1 not in state.pending_noshow_cancels

    def test_no_pending_ids_makes_no_client_calls(self, monkeypatch):
        import asyncio

        m = _main_module(monkeypatch)
        state = _state()
        client = _FakeCancelClient()

        asyncio.run(m._cancel_pending_noshows(state, client, []))

        assert client.cancel_calls == []

    def test_multiple_pending_ids_each_attempted(self, monkeypatch):
        import asyncio

        m = _main_module(monkeypatch)
        r1 = _user_reservation(1, reservation_date=FUTURE_DATE, slot_index=0)
        r2 = _user_reservation(2, reservation_date=FUTURE_DATE, slot_index=1)
        state = _state(r1, r2)
        state.pending_noshow_cancels = {1, 2}
        client = _FakeCancelClient(cancel_result=True)

        asyncio.run(m._cancel_pending_noshows(state, client, []))

        assert sorted(client.cancel_calls) == [(1, "no-show"), (2, "no-show")]
        assert state.pending_noshow_cancels == set()
        assert state.reservations == []


# ---------------------------------------------------------------------------
# Restart path: a controller-cancelled reservation is never re-armed
# ---------------------------------------------------------------------------


class TestNoshowRestartSafety:
    def test_cancelled_id_absent_from_fetch_is_never_rearmed(self):
        """After a restart, a reservation the controller already cancelled for
        no-show is simply absent from the next fetch's active list — a fresh
        ControllerState has no memory of it, so update_noshow_tracking has
        nothing to re-arm and it is not treated as claimed either."""
        state = _state()  # fresh, restart-simulating state; cancelled id 1 is gone
        state.update_noshow_tracking(datetime.now(timezone.utc), 15, 30, reason="init")
        assert 1 not in state.noshow_deadlines
        assert 1 not in state.noshow_reservation_ids
        assert 1 not in state.pending_noshow_cancels
