"""Unit tests for the reserved-path logic in controller.py.

Covers slot_start/slot_end accessors, find_best_reservation, enqueue_pod,
dequeue_pod, compute_max_deadline_seconds, and reconcile_queue.

No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from app.controller import ControllerState, QueueEntry, slot_end, slot_start
from app.schemas import GpuClassBrief, PolicyBrief, ReservationResponse, UserBrief


# ---------------------------------------------------------------------------
# Shared constants & factories
# ---------------------------------------------------------------------------

GPU_CLASS_ID = 10
GPU_CLASS_LABEL = "h100"
OTHER_CLASS_ID = 20
OTHER_CLASS_LABEL = "a100"
FIXED_DATE = date(2024, 1, 15)   # Past date; window timing controlled via explicit `now`
FUTURE_DATE = date(2099, 6, 15)  # Far future; slot_end always > datetime.now(utc)
USERNAME = "student1"


def _compute_window(
    date_val: date,
    start_time: str,
    slot_index: int,
    duration_minutes: int,
) -> tuple[datetime, datetime]:
    """Return (start_utc, end_utc) from policy fields, tagged as UTC."""
    parts = start_time.split(":")
    minutes = int(parts[0]) * 60 + int(parts[1]) + slot_index * duration_minutes
    midnight = datetime.combine(date_val, datetime.min.time()).replace(tzinfo=timezone.utc)
    start = midnight + timedelta(minutes=minutes)
    return start, start + timedelta(minutes=duration_minutes)


def _user_reservation(
    res_id: int,
    *,
    username: str = USERNAME,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_count: int = 2,
    slot_index: int = 0,
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
) -> ReservationResponse:
    start_utc, end_utc = _compute_window(reservation_date, start_time, slot_index, duration_minutes)
    return ReservationResponse(
        id=res_id,
        user_id=1,
        user=UserBrief(id=1, username=username),
        group_id=None,
        group=None,
        gpu_class_id=gpu_class_id,
        gpu_class=GpuClassBrief(id=gpu_class_id, name="H100"),
        policy_id=1,
        slot_index=slot_index,
        policy=PolicyBrief(
            id=1,
            name="Test policy",
            start_time=start_time,
            duration_minutes=duration_minutes,
            repeat_count=1,
        ),
        date=reservation_date,
        start_utc=start_utc,
        end_utc=end_utc,
        gpu_count=gpu_count,
        status="active",
        kind="user",
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 1, 1),
    )


def _state(*reservations: ReservationResponse) -> ControllerState:
    state = ControllerState()
    state.reservations = list(reservations)
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


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
    def test_slot_start_index_0(self):
        res = _user_reservation(1, start_time="08:00:00", slot_index=0)
        assert slot_start(res) == datetime(2024, 1, 15, 8, 0, tzinfo=timezone.utc)

    def test_slot_start_index_1_advances_by_duration(self):
        # slot_start = midnight + (480 + 1×120) minutes = 10:00 UTC
        res = _user_reservation(1, start_time="08:00:00", duration_minutes=120, slot_index=1)
        assert slot_start(res) == datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)

    def test_slot_start_index_2(self):
        # slot_start = midnight + (480 + 2×120) minutes = 12:00 UTC
        res = _user_reservation(1, start_time="08:00:00", duration_minutes=120, slot_index=2)
        assert slot_start(res) == datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)

    def test_slot_start_with_minute_offset(self):
        res = _user_reservation(1, start_time="08:30:00", slot_index=0)
        assert slot_start(res) == datetime(2024, 1, 15, 8, 30, tzinfo=timezone.utc)

    def test_slot_end_equals_start_plus_duration(self):
        res = _user_reservation(1, start_time="08:00:00", duration_minutes=90, slot_index=0)
        assert slot_end(res) == slot_start(res) + timedelta(minutes=90)

    def test_slot_end_crosses_midnight(self):
        # 23:30 + 90 min = 01:00 next day
        res = _user_reservation(1, start_time="23:30:00", duration_minutes=90, slot_index=0)
        assert slot_start(res) == datetime(2024, 1, 15, 23, 30, tzinfo=timezone.utc)
        assert slot_end(res) == datetime(2024, 1, 16, 1, 0, tzinfo=timezone.utc)


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
        """kind='ondemand' reservations are skipped."""
        res = _user_reservation(1, reservation_date=FUTURE_DATE)
        od = res.model_copy(update={"kind": "ondemand", "user": None, "user_id": None})
        state = ControllerState()
        state.reservations = [od]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL) is None

    def test_no_match_user_none(self):
        """Reservation with user=None is excluded even for kind='user'."""
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
# compute_max_deadline_seconds
# ---------------------------------------------------------------------------


class TestComputeMaxDeadline:
    """
    All tests use FIXED_DATE (2024-01-15) with an 08:00 UTC start so that `now`
    can be constructed explicitly without depending on the real wall clock.
    """

    def test_no_chain_returns_remaining_time(self):
        res = _user_reservation(1, start_time="08:00:00", duration_minutes=120)
        state = _state(res)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)  # 1 hour into the 2-hour window
        assert state.compute_max_deadline_seconds(now, res) == 3600

    def test_window_expired_returns_zero(self):
        res = _user_reservation(1, start_time="08:00:00", duration_minutes=120)
        state = _state(res)
        now = datetime(2024, 1, 15, 11, 0, tzinfo=timezone.utc)  # 1 hour after window ended
        assert state.compute_max_deadline_seconds(now, res) == 0

    def test_one_backtoback_adds_full_duration(self):
        # res1: 08:00–10:00  res2: 10:00–12:00 (slot_index=1, same policy params)
        res1 = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, slot_index=1, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)  # 1 h into res1 → 1 h remaining
        # 3600 (remaining in res1) + 7200 (full res2) = 10800
        assert state.compute_max_deadline_seconds(now, res1) == 10800

    def test_two_backtoback_adds_both_durations(self):
        res1 = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, slot_index=1, start_time="08:00:00", duration_minutes=120)
        res3 = _user_reservation(3, slot_index=2, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2, res3)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        # 3600 + 7200 + 7200 = 18000
        assert state.compute_max_deadline_seconds(now, res1) == 18000

    def test_gap_breaks_chain(self):
        res1 = _user_reservation(1, start_time="08:00:00", duration_minutes=120)
        # 10:05 start creates a 5-minute gap after res1's 10:00 end
        res2 = _user_reservation(2, start_time="10:05:00", duration_minutes=120)
        state = _state(res1, res2)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        assert state.compute_max_deadline_seconds(now, res1) == 3600

    def test_different_gpu_count_breaks_chain(self):
        res1 = _user_reservation(1, gpu_count=2, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, gpu_count=1, slot_index=1, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        assert state.compute_max_deadline_seconds(now, res1) == 3600

    def test_different_username_breaks_chain(self):
        res1 = _user_reservation(1, username="student1", slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, username="student2", slot_index=1, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        assert state.compute_max_deadline_seconds(now, res1) == 3600

    def test_different_gpu_class_breaks_chain(self):
        res1 = _user_reservation(1, gpu_class_id=GPU_CLASS_ID, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, gpu_class_id=OTHER_CLASS_ID, slot_index=1, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2)
        state.gpu_class_labels[OTHER_CLASS_ID] = OTHER_CLASS_LABEL
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        assert state.compute_max_deadline_seconds(now, res1) == 3600


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
