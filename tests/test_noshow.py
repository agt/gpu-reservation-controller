"""Unit tests for no-show reservation conversion logic.

Covers: initialize_noshow_tracking, update_noshow_tracking, reconcile_noshow,
check_noshow_deadlines, mark_pod_seen_for_noshow, enqueue_pod deadline clearing,
find_best_reservation skipping no-shows, find_ondemand_block including no-shows,
reconcile_ondemand including no-shows, and compute_max_deadline_seconds skipping
no-shows.

No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from app.controller import ControllerState, slot_end, slot_start
from app.schemas import GpuClassBrief, PolicyBrief, ReservationResponse, UserBrief


# ---------------------------------------------------------------------------
# Shared constants & factories
# ---------------------------------------------------------------------------

GPU_CLASS_ID = 10
GPU_CLASS_LABEL = "h100"
OTHER_CLASS_ID = 20
FIXED_DATE = date(2024, 1, 15)   # Past date; window timing controlled via explicit `now`
FUTURE_DATE = date(2099, 6, 15)  # Far future; slot_end always > datetime.now()
USERNAME = "alice"

TIMEOUT = 15
GRACE = 30


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
        gpu_count=gpu_count,
        status="active",
        kind="user",
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 1, 1),
    )


def _ondemand_reservation(
    res_id: int,
    *,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_count: int = 2,
    slot_index: int = 0,
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
) -> ReservationResponse:
    return ReservationResponse(
        id=res_id,
        user_id=None,
        user=None,
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
        gpu_count=gpu_count,
        status="active",
        kind="ondemand",
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 1, 1),
    )


def _state(*reservations: ReservationResponse) -> ControllerState:
    state = ControllerState()
    state.reservations = list(reservations)
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


def _window_start_dt(start_time: str = "08:00:00", reservation_date: date = FIXED_DATE) -> datetime:
    h, m, _ = start_time.split(":")
    return datetime.combine(reservation_date, datetime.min.time()) + timedelta(
        hours=int(h), minutes=int(m)
    )


def _window_open_now(
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
) -> datetime:
    """Return a datetime that falls inside the given window (at the midpoint)."""
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
        now = datetime(2024, 1, 1, 0, 0)
        state.initialize_noshow_tracking(now, TIMEOUT, GRACE)
        assert state.noshow_deadlines[1] == slot_start(r) + timedelta(minutes=TIMEOUT)

    def test_midwindow_deadline_is_now_plus_grace(self):
        r = _user_reservation(1)  # window: FIXED_DATE 08:00–10:00
        now = _window_open_now()   # 09:00 — inside the window
        state = _state(r)
        state.initialize_noshow_tracking(now, TIMEOUT, GRACE)
        assert state.noshow_deadlines[1] == now + timedelta(minutes=GRACE)

    def test_expired_reservation_not_tracked(self):
        r = _user_reservation(1)  # window ended on FIXED_DATE in the past
        now = _window_start_dt() + timedelta(hours=3)  # after slot_end
        state = _state(r)
        state.initialize_noshow_tracking(now, TIMEOUT, GRACE)
        assert 1 not in state.noshow_deadlines

    def test_ondemand_reservation_not_tracked(self):
        r = _ondemand_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        now = datetime(2024, 1, 1, 0, 0)
        state.initialize_noshow_tracking(now, TIMEOUT, GRACE)
        assert 1 not in state.noshow_deadlines

    def test_user_none_not_tracked(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.reservations[0] = ReservationResponse(
            **{**r.model_dump(), "user": None, "user_id": None}
        )
        now = datetime(2024, 1, 1, 0, 0)
        state.initialize_noshow_tracking(now, TIMEOUT, GRACE)
        assert 1 not in state.noshow_deadlines

    def test_multiple_reservations_all_tracked(self):
        r1 = _user_reservation(1, reservation_date=FUTURE_DATE, slot_index=0)
        r2 = _user_reservation(2, reservation_date=FUTURE_DATE, slot_index=1)
        state = _state(r1, r2)
        now = datetime(2024, 1, 1, 0, 0)
        state.initialize_noshow_tracking(now, TIMEOUT, GRACE)
        assert 1 in state.noshow_deadlines
        assert 2 in state.noshow_deadlines

    def test_does_not_overwrite_existing_deadline(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        sentinel = datetime(2099, 1, 1)
        state.noshow_deadlines[1] = sentinel
        now = datetime(2024, 1, 1, 0, 0)
        state.initialize_noshow_tracking(now, TIMEOUT, GRACE)
        assert state.noshow_deadlines[1] == sentinel


# ---------------------------------------------------------------------------
# TestUpdateNoshowTracking
# ---------------------------------------------------------------------------


class TestUpdateNoshowTracking:
    def test_new_reservation_gets_deadline(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        now = datetime(2024, 1, 1, 0, 0)
        state.update_noshow_tracking(now, TIMEOUT, GRACE)
        assert 1 in state.noshow_deadlines

    def test_existing_deadline_not_overwritten(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        sentinel = datetime(2099, 1, 1)
        state.noshow_deadlines[1] = sentinel
        now = datetime(2024, 1, 1, 0, 0)
        state.update_noshow_tracking(now, TIMEOUT, GRACE)
        assert state.noshow_deadlines[1] == sentinel

    def test_declared_noshow_not_resurrected(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        now = datetime(2024, 1, 1, 0, 0)
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
        state.noshow_deadlines[99] = datetime(2099, 1, 1)
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
        deadline = datetime(2099, 6, 1)
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
        deadline = datetime(2024, 6, 1, 9, 0)
        state.noshow_deadlines[1] = deadline
        state.check_noshow_deadlines(deadline + timedelta(seconds=1))
        assert 1 in state.noshow_reservation_ids
        assert 1 not in state.noshow_deadlines

    def test_future_deadline_not_moved(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        deadline = datetime(2099, 6, 1, 9, 0)
        state.noshow_deadlines[1] = deadline
        state.check_noshow_deadlines(deadline - timedelta(seconds=1))
        assert 1 not in state.noshow_reservation_ids
        assert 1 in state.noshow_deadlines

    def test_exactly_at_deadline_is_noshow(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        deadline = datetime(2024, 6, 1, 9, 0)
        state.noshow_deadlines[1] = deadline
        state.check_noshow_deadlines(deadline)
        assert 1 in state.noshow_reservation_ids
        assert 1 not in state.noshow_deadlines

    def test_only_expired_moves_when_mixed(self):
        r1 = _user_reservation(1, reservation_date=FUTURE_DATE, slot_index=0)
        r2 = _user_reservation(2, reservation_date=FUTURE_DATE, slot_index=1)
        state = _state(r1, r2)
        now = datetime(2024, 6, 1, 9, 30)
        state.noshow_deadlines[1] = datetime(2024, 6, 1, 9, 0)   # expired
        state.noshow_deadlines[2] = datetime(2024, 6, 1, 10, 0)  # future
        state.check_noshow_deadlines(now)
        assert 1 in state.noshow_reservation_ids
        assert 2 not in state.noshow_reservation_ids
        assert 2 in state.noshow_deadlines

    def test_empty_deadlines_is_noop(self):
        state = _state()
        state.check_noshow_deadlines(datetime.now())  # must not raise


# ---------------------------------------------------------------------------
# TestMarkPodSeenForNoshow
# ---------------------------------------------------------------------------


class TestMarkPodSeenForNoshow:
    def test_clears_deadline_for_matching_reservation(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_deadlines[1] = datetime(2099, 1, 1)
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)
        assert 1 not in state.noshow_deadlines

    def test_wrong_namespace_leaves_deadline(self):
        r = _user_reservation(1, username="bob", reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_deadlines[1] = datetime(2099, 1, 1)
        state.mark_pod_seen_for_noshow("alice", GPU_CLASS_LABEL)
        assert 1 in state.noshow_deadlines

    def test_wrong_gpu_class_leaves_deadline(self):
        r = _user_reservation(1, gpu_class_id=OTHER_CLASS_ID, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.gpu_class_labels[OTHER_CLASS_ID] = "a100"
        state.noshow_deadlines[1] = datetime(2099, 1, 1)
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)
        assert 1 in state.noshow_deadlines

    def test_not_in_deadlines_is_noop(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        # noshow_deadlines[1] not set — must not raise
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)
        assert 1 not in state.noshow_deadlines

    def test_picks_soonest_slot_start_when_multiple(self):
        # r1 slot_index=0 → 08:00, r2 slot_index=1 → 10:00
        r1 = _user_reservation(1, reservation_date=FUTURE_DATE, slot_index=0)
        r2 = _user_reservation(2, reservation_date=FUTURE_DATE, slot_index=1)
        state = _state(r1, r2)
        state.noshow_deadlines[1] = datetime(2099, 1, 1)
        state.noshow_deadlines[2] = datetime(2099, 1, 2)
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
        state.noshow_deadlines[1] = datetime(2099, 1, 1)
        state.enqueue_pod("uid-1", "pod-1", USERNAME, GPU_CLASS_LABEL, 1)
        assert 1 not in state.noshow_deadlines

    def test_enqueue_no_match_leaves_deadlines_unchanged(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_deadlines[1] = datetime(2099, 1, 1)
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
# TestFindOndemandBlockIncludesNoshow
# ---------------------------------------------------------------------------


class TestFindOndemandBlockIncludesNoshow:
    def test_noshow_block_matched_when_window_open(self):
        r = _user_reservation(1)
        now = _window_open_now()
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 1)
        assert result is not None
        assert result.id == 1

    def test_noshow_block_not_matched_when_window_closed(self):
        r = _user_reservation(1)
        now = _window_start_dt() + timedelta(hours=3)  # past slot_end
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        assert state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 1) is None

    def test_noshow_respects_capacity(self):
        r = _user_reservation(1, gpu_count=2)
        now = _window_open_now()
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        state.record_ondemand_placement(1, "uid-a", 2)  # fills all capacity
        assert state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 1) is None

    def test_noshow_respects_min_runtime(self):
        r = _user_reservation(1, duration_minutes=30)
        # Only 1 second left in the window
        end = _window_start_dt() + timedelta(minutes=30)
        now = end - timedelta(seconds=1)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        assert state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60) is None

    def test_prefers_latest_slot_end(self):
        # noshow:   09:00–12:00 (start=09:00, dur=180) — ends later
        # ondemand: 08:00–10:00 (start=08:00, dur=120) — ends sooner
        # Both are open at 09:30
        r_noshow = _user_reservation(
            1, slot_index=0, start_time="09:00:00", duration_minutes=180
        )
        r_ondemand = _ondemand_reservation(
            2, slot_index=0, start_time="08:00:00", duration_minutes=120
        )
        now = _window_start_dt("09:00:00") + timedelta(minutes=30)  # 09:30
        state = _state(r_noshow, r_ondemand)
        state.noshow_reservation_ids.add(1)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 1)
        assert result is not None
        assert result.id == 1  # later slot_end (12:00) preferred over 10:00

    def test_regular_ondemand_still_matched(self):
        r = _ondemand_reservation(1)
        now = _window_open_now()
        state = _state(r)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 1)
        assert result is not None
        assert result.id == 1


# ---------------------------------------------------------------------------
# TestReconcileOndemandIncludesNoshow
# ---------------------------------------------------------------------------


class TestReconcileOndemandIncludesNoshow:
    def test_active_noshow_occupancy_preserved(self):
        r = _user_reservation(1, reservation_date=FUTURE_DATE)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        state.ondemand_occupancy[1] = {"uid-a": 1}
        state.reconcile_ondemand()
        assert 1 in state.ondemand_occupancy

    def test_expired_noshow_occupancy_pruned(self):
        # FIXED_DATE window is in the past, so slot_end < datetime.now()
        r = _user_reservation(1)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        state.ondemand_occupancy[1] = {"uid-a": 1}
        state.reconcile_ondemand()
        assert 1 not in state.ondemand_occupancy


# ---------------------------------------------------------------------------
# TestComputeMaxDeadlineSkipsNoshow
# ---------------------------------------------------------------------------


class TestComputeMaxDeadlineSkipsNoshow:
    def test_noshow_back_to_back_not_chained(self):
        # r1: 08:00–10:00, r2: 10:00–12:00 (directly back-to-back)
        r1 = _user_reservation(1, slot_index=0, duration_minutes=120, reservation_date=FUTURE_DATE)
        r2 = _user_reservation(2, slot_index=1, duration_minutes=120, reservation_date=FUTURE_DATE)
        state = _state(r1, r2)
        state.noshow_reservation_ids.add(2)
        now = slot_start(r1) + timedelta(minutes=1)
        result = state.compute_max_deadline_seconds(now, r1)
        # Only r1's remaining time — r2 is no-show
        expected = int((slot_end(r1) - now).total_seconds())
        assert result == expected

    def test_non_noshow_back_to_back_still_chained(self):
        r1 = _user_reservation(1, slot_index=0, duration_minutes=120, reservation_date=FUTURE_DATE)
        r2 = _user_reservation(2, slot_index=1, duration_minutes=120, reservation_date=FUTURE_DATE)
        state = _state(r1, r2)
        # r2 is NOT a no-show — should chain
        now = slot_start(r1) + timedelta(minutes=1)
        result = state.compute_max_deadline_seconds(now, r1)
        expected = int((slot_end(r1) - now).total_seconds()) + 120 * 60
        assert result == expected
