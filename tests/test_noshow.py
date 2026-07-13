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
from tests.conftest import reclaim_reservation as _ondemand_reservation
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

    def test_ondemand_reservation_not_tracked(self):
        r = _ondemand_reservation(1, reservation_date=FUTURE_DATE)
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
