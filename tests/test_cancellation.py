"""Unit tests for mid-window reservation cancellation detection and on-demand reclaim.

Covers:
- detect_cancelled_in_window: various conditions that should and should not trigger
- record_cancelled_reservation / cleanup_cancelled_reservations lifecycle
- find_ondemand_block picking up freed capacity from cancelled_reservations
- _canceller_description helper in main.py
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from app.controller import ControllerState, canceller_description, slot_end
from app.schemas import GpuClassBrief, PolicyBrief, ReservationResponse, UserBrief


# ---------------------------------------------------------------------------
# Constants & factories
# ---------------------------------------------------------------------------

GPU_CLASS_ID = 10
GPU_CLASS_LABEL = "h100"
FIXED_DATE = date(2024, 1, 15)
USERNAME = "alice"
ADMIN_USERNAME = "sysadmin"


def _window(
    minutes_from_midnight_start: int,
    duration_minutes: int,
    day: date = FIXED_DATE,
) -> tuple[datetime, datetime]:
    midnight = datetime.combine(day, datetime.min.time()).replace(tzinfo=timezone.utc)
    start = midnight + timedelta(minutes=minutes_from_midnight_start)
    return start, start + timedelta(minutes=duration_minutes)


def _user_res(
    res_id: int,
    *,
    start_offset: int = 480,   # 08:00 UTC
    duration: int = 120,
    gpu_count: int = 2,
    user_id: int = 1,
    username: str = USERNAME,
    cancelled_by_id: int | None = None,
    cancelled_by: UserBrief | None = None,
    day: date = FIXED_DATE,
) -> ReservationResponse:
    s, e = _window(start_offset, duration, day)
    return ReservationResponse(
        id=res_id,
        user_id=user_id,
        user=UserBrief(id=user_id, username=username),
        group_id=None,
        group=None,
        gpu_class_id=GPU_CLASS_ID,
        gpu_class=GpuClassBrief(id=GPU_CLASS_ID, name="H100", label_value=GPU_CLASS_LABEL),
        policy_id=1,
        slot_index=0,
        policy=PolicyBrief(
            id=1,
            name="Test",
            start_time="08:00:00",
            duration_minutes=duration,
            repeat_count=1,
        ),
        date=day,
        start_utc=s,
        end_utc=e,
        gpu_count=gpu_count,
        status="cancelled" if cancelled_by_id else "active",
        kind="user",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        cancelled_by_id=cancelled_by_id,
        cancelled_by=cancelled_by,
    )


def _state_with_label() -> ControllerState:
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


# ---------------------------------------------------------------------------
# detect_cancelled_in_window
# ---------------------------------------------------------------------------


class TestDetectCancelledInWindow:
    def test_detects_mid_window_cancellation(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=120)
        # Seed previous cycle with res
        state._prev_reservations = {1: res}
        # New fetch has no reservations
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([], mid_window)
        assert [r.id for r in result] == [1]

    def test_ignores_naturally_expired(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=60)
        state._prev_reservations = {1: res}
        # now is after the window ended
        after_end = slot_end(res) + timedelta(minutes=1)
        result = state.detect_cancelled_in_window([], after_end)
        assert result == []

    def test_ignores_still_active(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=120)
        state._prev_reservations = {1: res}
        mid_window = res.start_utc + timedelta(minutes=30)
        # Same reservation still in the new list
        result = state.detect_cancelled_in_window([res], mid_window)
        assert result == []

    def test_ignores_already_noshow(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=120)
        state._prev_reservations = {1: res}
        state.noshow_reservation_ids.add(1)
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([], mid_window)
        assert result == []

    def test_ignores_already_recorded_cancellation(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=120)
        state._prev_reservations = {1: res}
        state.cancelled_reservations = {1: res}
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([], mid_window)
        assert result == []

    def test_updates_prev_reservations(self):
        state = _state_with_label()
        res_old = _user_res(1, start_offset=0, duration=120)
        res_new = _user_res(2, start_offset=0, duration=120)
        state._prev_reservations = {1: res_old}
        state.detect_cancelled_in_window([res_new], res_new.start_utc + timedelta(minutes=10))
        assert set(state._prev_reservations.keys()) == {2}

    def test_empty_prev_no_detections(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=120)
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([], mid_window)
        assert result == []


# ---------------------------------------------------------------------------
# record_cancelled_reservation / cleanup_cancelled_reservations
# ---------------------------------------------------------------------------


class TestCancelledReservationLifecycle:
    def test_record_stores_reservation(self):
        state = _state_with_label()
        res = _user_res(42, start_offset=0, duration=120)
        state.record_cancelled_reservation(res)
        assert 42 in state.cancelled_reservations
        assert state.cancelled_reservations[42] is res

    def test_cleanup_removes_expired(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=60)
        state.cancelled_reservations = {1: res}
        after_end = slot_end(res) + timedelta(seconds=1)
        state.cleanup_cancelled_reservations(after_end)
        assert 1 not in state.cancelled_reservations

    def test_cleanup_keeps_open(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=120)
        state.cancelled_reservations = {1: res}
        mid_window = res.start_utc + timedelta(minutes=30)
        state.cleanup_cancelled_reservations(mid_window)
        assert 1 in state.cancelled_reservations

    def test_cleanup_removes_only_expired(self):
        state = _state_with_label()
        short = _user_res(1, start_offset=0, duration=30)
        long_ = _user_res(2, start_offset=0, duration=180)
        state.cancelled_reservations = {1: short, 2: long_}
        after_short = slot_end(short) + timedelta(seconds=1)
        state.cleanup_cancelled_reservations(after_short)
        assert 1 not in state.cancelled_reservations
        assert 2 in state.cancelled_reservations


# ---------------------------------------------------------------------------
# find_ondemand_block — cancelled reservation as on-demand source
# ---------------------------------------------------------------------------


class TestFindOndemandBlockCancelledReservation:
    def _make_state(self, res: ReservationResponse) -> ControllerState:
        state = _state_with_label()
        # No active reservations — the cancelled one is the only source.
        state.reservations = []
        state.cancelled_reservations = {res.id: res}
        return state

    def test_cancelled_res_used_as_ondemand_block(self):
        res = _user_res(1, start_offset=0, duration=120, gpu_count=4)
        state = self._make_state(res)
        now = res.start_utc + timedelta(minutes=30)
        block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 2, 0)
        assert block is not None
        assert block.id == 1

    def test_cancelled_res_wrong_gpu_class_skipped(self):
        res = _user_res(1, start_offset=0, duration=120, gpu_count=4)
        state = self._make_state(res)
        now = res.start_utc + timedelta(minutes=30)
        block = state.find_ondemand_block("a100", now, 2, 0)
        assert block is None

    def test_cancelled_res_window_not_open_skipped(self):
        res = _user_res(1, start_offset=0, duration=120, gpu_count=4)
        state = self._make_state(res)
        before_start = res.start_utc - timedelta(seconds=1)
        block = state.find_ondemand_block(GPU_CLASS_LABEL, before_start, 2, 0)
        assert block is None

    def test_cancelled_res_insufficient_capacity_skipped(self):
        res = _user_res(1, start_offset=0, duration=120, gpu_count=1)
        state = self._make_state(res)
        now = res.start_utc + timedelta(minutes=30)
        block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 2, 0)
        assert block is None

    def test_cancelled_res_min_runtime_not_met_skipped(self):
        res = _user_res(1, start_offset=0, duration=60, gpu_count=4)
        state = self._make_state(res)
        # 59 minutes remaining but min_runtime = 61 min
        now = res.start_utc + timedelta(minutes=1)
        block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 2, 61 * 60)
        assert block is None

    def test_cancelled_res_claimed_skipped(self):
        res = _user_res(1, start_offset=0, duration=120, gpu_count=4)
        state = self._make_state(res)
        state.claimed_reservation_ids.add(1)
        now = res.start_utc + timedelta(minutes=30)
        block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 2, 0)
        assert block is None

    def test_cancelled_res_capacity_respects_occupancy(self):
        res = _user_res(1, start_offset=0, duration=120, gpu_count=2)
        state = self._make_state(res)
        # Fill all capacity with an existing on-demand pod.
        state.occupancy = {1: {"pod-uid-existing": 2}}
        now = res.start_utc + timedelta(minutes=30)
        block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 0)
        assert block is None

    def test_cancelled_res_label_fallback_when_not_in_cache(self):
        """label_value from GpuClassBrief used when gpu_class_labels cache misses."""
        res = _user_res(1, start_offset=0, duration=120, gpu_count=4)
        state = ControllerState()
        # Empty labels cache — no entry for GPU_CLASS_ID
        state.gpu_class_labels = {}
        state.cancelled_reservations = {res.id: res}
        now = res.start_utc + timedelta(minutes=30)
        # Should still match via r.gpu_class.label_value = GPU_CLASS_LABEL
        block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 2, 0)
        assert block is not None
        assert block.id == 1


# ---------------------------------------------------------------------------
# _canceller_description
# ---------------------------------------------------------------------------


class TestCancellerDescription:
    """Tests for the _canceller_description helper in main.py."""

    def _make_res(
        self,
        user_id: int | None,
        cancelled_by_id: int | None,
        cancelled_by: UserBrief | None,
    ) -> ReservationResponse:
        return _user_res(
            1,
            user_id=user_id or 1,
            cancelled_by_id=cancelled_by_id,
            cancelled_by=cancelled_by,
        )

    def test_self_cancel_returns_by_user(self):
        res = self._make_res(
            user_id=7,
            cancelled_by_id=7,
            cancelled_by=UserBrief(id=7, username=USERNAME),
        )
        assert canceller_description(res) == "by user"

    def test_admin_cancel_with_brief_returns_username(self):
        res = self._make_res(
            user_id=7,
            cancelled_by_id=99,
            cancelled_by=UserBrief(id=99, username=ADMIN_USERNAME),
        )
        assert canceller_description(res) == f"by {ADMIN_USERNAME}"

    def test_different_id_no_brief_returns_by_another_user(self):
        res = self._make_res(user_id=7, cancelled_by_id=99, cancelled_by=None)
        assert canceller_description(res) == "by another user"

    def test_no_canceller_info_returns_by_user(self):
        res = self._make_res(user_id=7, cancelled_by_id=None, cancelled_by=None)
        assert canceller_description(res) == "by user"
