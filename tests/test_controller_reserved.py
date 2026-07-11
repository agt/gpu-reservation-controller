"""Unit tests for the reserved-path logic in controller.py.

Covers slot_start/slot_end accessors, find_best_reservation, enqueue_pod,
dequeue_pod, and reconcile_queue.  Guarantee arithmetic
(compute_guaranteed_until) is covered in test_guarantees.py.

No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import datetime, timezone

from app.controller import ControllerState, QueueEntry, slot_end, slot_start
from app.schemas import ReservationResponse

from tests.conftest import (
    FIXED_DATE,
    FUTURE_DATE,
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    OTHER_CLASS_ID,
    OTHER_CLASS_LABEL,
    USERNAME,
)
from tests.conftest import make_state as _state
from tests.conftest import reservation, user_reservation as _user_reservation


# ---------------------------------------------------------------------------
# Shared constants & factories
# ---------------------------------------------------------------------------


def _queued_entry(uid: str, reservation: ReservationResponse, pod_name: str = "pod-1") -> QueueEntry:
    return QueueEntry(
        pod_uid=uid,
        pod_name=pod_name,
        pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_requested=1,
        reservation=reservation,
        next_attempt_at=datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# slot_start / slot_end
# ---------------------------------------------------------------------------


class TestSlotArithmetic:
    def test_accessors_return_window_fields(self):
        # slot_start / slot_end are trivial accessors: they return start_utc /
        # end_utc unchanged (no local-time conversion).  Nothing else to test
        # here now that window construction lives in the shared factory (T2).
        start = datetime(2024, 1, 15, 8, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)
        res = reservation(1, start_utc=start, end_utc=end)
        assert slot_start(res) == start
        assert slot_end(res) == end


# ---------------------------------------------------------------------------
# find_best_reservation
# ---------------------------------------------------------------------------


class TestFindBestReservation:
    def test_basic_match(self):
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(res)
        result = state.find_best_reservation(USERNAME, GPU_CLASS_LABEL)
        assert result is not None
        assert result.id == 1

    def test_no_match_wrong_namespace(self):
        res = _user_reservation(1, username="other-user", reservation_date=FUTURE_DATE)
        state = _state(res)
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL) is None

    def test_no_match_wrong_gpu_class_label(self):
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(res)
        assert state.find_best_reservation(USERNAME, "a100") is None

    def test_no_match_gpu_class_not_resolved(self):
        """gpu_class_id absent from label map → no match."""
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = ControllerState()
        state.reservations = [res]
        state.gpu_class_labels = {}
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL) is None

    def test_no_match_expired_window(self):
        # slot_end = 2024-01-15 10:00 UTC, well in the past
        res = _user_reservation(1, reservation_date=FIXED_DATE)
        state = _state(res)
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL) is None

    def test_no_match_ondemand_kind(self):
        """kind='reclaim' reservations are skipped."""
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        od = res.model_copy(update={"kind": "reclaim", "user": None, "user_id": None})
        state = ControllerState()
        state.reservations = [od]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL) is None

    def test_no_match_user_none(self):
        """Reservation with user=None is excluded even for kind='booking'."""
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        no_user = res.model_copy(update={"user": None})
        state = ControllerState()
        state.reservations = [no_user]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL) is None

    def test_returns_soonest_start(self):
        """When multiple reservations match, return the one with the earliest slot_start."""
        later = _user_reservation(1, start_time="10:00:00", reservation_date=FUTURE_DATE)
        sooner = _user_reservation(2, start_time="08:00:00", reservation_date=FUTURE_DATE)
        state = _state(later, sooner)
        result = state.find_best_reservation(USERNAME, GPU_CLASS_LABEL)
        assert result is not None
        assert result.id == 2  # 08:00 < 10:00

    def test_expired_excluded_when_active_also_present(self):
        expired = _user_reservation(1, reservation_date=FIXED_DATE)
        active = _user_reservation(2, reservation_date=FUTURE_DATE)
        state = _state(expired, active)
        result = state.find_best_reservation(USERNAME, GPU_CLASS_LABEL)
        assert result is not None
        assert result.id == 2


# ---------------------------------------------------------------------------
# enqueue_pod / dequeue_pod
# ---------------------------------------------------------------------------


class TestEnqueueDequeue:
    def test_enqueue_adds_to_queue(self):
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(res)
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        assert "uid-1" in state.task_queue
        assert state.task_queue["uid-1"].reservation.id == 1

    def test_enqueue_stores_correct_fields(self):
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(res)
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 2)
        entry = state.task_queue["uid-1"]
        assert entry.pod_name == "pod-1"
        assert entry.pod_namespace == USERNAME
        assert entry.gpu_class_label == GPU_CLASS_LABEL
        assert entry.gpu_requested == 2

    def test_enqueue_no_match_is_noop(self):
        state = ControllerState()
        state.reservations = []
        state.gpu_class_labels = {}
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        assert "uid-1" not in state.task_queue

    def test_enqueue_idempotent_same_reservation(self):
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(res)
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        first_entry = state.task_queue["uid-1"]
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        assert len(state.task_queue) == 1
        assert state.task_queue["uid-1"] is first_entry

    def test_enqueue_replaces_stale_reservation(self):
        res_old = _user_reservation(1, reservation_date=FUTURE_DATE)
        res_new = _user_reservation(2, reservation_date=FUTURE_DATE)
        state = _state(res_old)
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        assert state.task_queue["uid-1"].reservation.id == 1
        state.reservations = [res_new]
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        assert state.task_queue["uid-1"].reservation.id == 2

    def test_dequeue_removes_entry(self):
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(res)
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        state.dequeue_pod("uid-1")
        assert "uid-1" not in state.task_queue

    def test_dequeue_unknown_uid_is_noop(self):
        state = ControllerState()
        state.dequeue_pod("ghost-uid")  # must not raise


# ---------------------------------------------------------------------------
# reconcile_queue
# ---------------------------------------------------------------------------


class TestReconcileQueue:
    def test_preserves_active_entry(self):
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(res)
        state.task_queue["uid-1"] = _queued_entry("uid-1", res)
        state.reconcile_queue()
        assert "uid-1" in state.task_queue
        assert state.task_queue["uid-1"].reservation.id == 1

    def test_removes_stale_entry_no_replacement(self):
        """Entry whose reservation is no longer in the active list is dropped."""
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(res)
        state.task_queue["uid-1"] = _queued_entry("uid-1", res)
        state.reservations = []
        state.reconcile_queue()
        assert "uid-1" not in state.task_queue

    def test_requeues_with_replacement(self):
        """Entry's reservation was cancelled but a new one exists; re-bind it."""
        res_old = _user_reservation(1, reservation_date=FUTURE_DATE)
        res_new = _user_reservation(2, reservation_date=FUTURE_DATE)
        state = _state(res_old)
        state.task_queue["uid-1"] = _queued_entry("uid-1", res_old)
        state.reservations = [res_new]
        state.reconcile_queue()
        assert "uid-1" in state.task_queue
        assert state.task_queue["uid-1"].reservation.id == 2

    def test_all_stale_entries_processed(self):
        """Multiple stale entries are all dropped in a single reconcile call."""
        res1 = _user_reservation(1, reservation_date=FUTURE_DATE)
        res2 = _user_reservation(2, reservation_date=FUTURE_DATE)
        state = _state(res1, res2)
        state.task_queue["uid-a"] = _queued_entry("uid-a", res1, pod_name="pod-a")
        state.task_queue["uid-b"] = _queued_entry("uid-b", res2, pod_name="pod-b")
        state.reservations = []
        state.reconcile_queue()
        assert len(state.task_queue) == 0
