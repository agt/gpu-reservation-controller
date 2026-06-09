"""Unit tests for on-demand pod placement logic in ControllerState.

These tests exercise only the pure-Python logic in controller.py.
No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from app.controller import ControllerState, OnDemandCandidate, slot_end, slot_start
from app.schemas import (
    GpuClassBrief,
    PolicyBrief,
    ReservationResponse,
    UserBrief,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

GPU_CLASS_ID = 10
GPU_CLASS_LABEL = "h100"


def _policy(duration_minutes: int = 120) -> PolicyBrief:
    return PolicyBrief(
        id=1,
        name="Test policy",
        start_time="08:00:00",
        duration_minutes=duration_minutes,
        repeat_count=1,
    )


def _ondemand_block(
    block_id: int,
    gpu_count: int = 2,
    slot_index: int = 0,
    duration_minutes: int = 120,
    date_offset_days: int = 0,
) -> ReservationResponse:
    """Return a kind='ondemand' reservation whose window is *today* at 08:00."""
    today = date.today() if date_offset_days == 0 else (
        datetime.now() + timedelta(days=date_offset_days)
    ).date()
    return ReservationResponse(
        id=block_id,
        user_id=None,
        user=None,
        group_id=None,
        group=None,
        gpu_class_id=GPU_CLASS_ID,
        gpu_class=GpuClassBrief(id=GPU_CLASS_ID, name="H100"),
        policy_id=1,
        slot_index=slot_index,
        policy=PolicyBrief(
            id=1,
            name="Test policy",
            start_time="08:00:00",
            duration_minutes=duration_minutes,
            repeat_count=1,
        ),
        date=today,
        gpu_count=gpu_count,
        status="active",
        kind="ondemand",
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )


def _state_with_block(
    block: ReservationResponse,
) -> ControllerState:
    """Return a ControllerState pre-loaded with one on-demand block."""
    state = ControllerState()
    state.reservations = [block]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


def _now_inside_block(block: ReservationResponse) -> datetime:
    """Return a datetime 1 minute after the block's slot_start."""
    return slot_start(block) + timedelta(minutes=1)


def _currently_open_block(
    block_id: int,
    gpu_count: int = 2,
    duration_minutes: int = 120,
) -> ReservationResponse:
    """Return an on-demand block whose window is open at the real current time.

    The window started 30 minutes ago and lasts *duration_minutes*, so it will
    not expire for at least (duration_minutes - 30) more minutes.
    """
    now = datetime.now()
    start_dt = now - timedelta(minutes=30)
    start_time = f"{start_dt.hour:02d}:{start_dt.minute:02d}:00"
    return ReservationResponse(
        id=block_id,
        user_id=None,
        user=None,
        group_id=None,
        group=None,
        gpu_class_id=GPU_CLASS_ID,
        gpu_class=GpuClassBrief(id=GPU_CLASS_ID, name="H100"),
        policy_id=1,
        slot_index=0,
        policy=PolicyBrief(
            id=1,
            name="Test policy",
            start_time=start_time,
            duration_minutes=duration_minutes,
            repeat_count=1,
        ),
        date=start_dt.date(),
        gpu_count=gpu_count,
        status="active",
        kind="ondemand",
        created_at=now,
        updated_at=now,
    )


# ---------------------------------------------------------------------------
# find_ondemand_block — matching
# ---------------------------------------------------------------------------


class TestFindOndemandBlock:
    def test_matches_open_block(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        now = _now_inside_block(block)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert result is not None
        assert result.id == 1

    def test_no_match_wrong_class(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        now = _now_inside_block(block)
        result = state.find_ondemand_block("a100", now, 1, 60)
        assert result is None

    def test_no_match_window_not_open(self):
        block = _ondemand_block(1)
        state = _state_with_block(block)
        # Use a time well before the window opens
        now = slot_start(block) - timedelta(hours=1)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert result is None

    def test_no_match_window_ended(self):
        block = _ondemand_block(1)
        state = _state_with_block(block)
        now = slot_end(block) + timedelta(seconds=1)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert result is None

    def test_no_match_min_runtime_exceeds_remaining(self):
        block = _ondemand_block(1, duration_minutes=30)
        state = _state_with_block(block)
        # 1 minute before end → only 1 minute remaining
        now = slot_end(block) - timedelta(minutes=1)
        # Requires 600 seconds (10 min) but only 60 s remain
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 600)
        assert result is None

    def test_no_match_capacity_exhausted(self):
        block = _ondemand_block(1, gpu_count=1)
        state = _state_with_block(block)
        state.record_ondemand_placement(1, "pod-a", 1)
        now = _now_inside_block(block)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert result is None

    def test_skips_user_kind_blocks(self):
        """kind='user' reservations must not be matched as on-demand blocks."""
        block = _ondemand_block(1)
        # Mutate kind to simulate a user reservation
        user_block = block.model_copy(
            update={
                "kind": "user",
                "user": UserBrief(id=99, username="student1"),
                "user_id": 99,
            }
        )
        state = ControllerState()
        state.reservations = [user_block]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        now = _now_inside_block(user_block)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert result is None

    def test_prefers_latest_slot_end(self):
        """When two blocks are available, pick the one that ends latest."""
        # Block A: 2-hour window starting at 08:00 (slot_index=0)
        block_a = _ondemand_block(1, duration_minutes=120, slot_index=0)
        # Block B: same class, 4-hour window; use a later slot_index to push its end
        block_b_policy = PolicyBrief(
            id=2,
            name="Long policy",
            start_time="08:00:00",
            duration_minutes=240,
            repeat_count=1,
        )
        today = date.today()
        block_b = ReservationResponse(
            id=2,
            user_id=None,
            user=None,
            group_id=None,
            group=None,
            gpu_class_id=GPU_CLASS_ID,
            gpu_class=GpuClassBrief(id=GPU_CLASS_ID, name="H100"),
            policy_id=2,
            slot_index=0,
            policy=block_b_policy,
            date=today,
            gpu_count=2,
            status="active",
            kind="ondemand",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        state = ControllerState()
        state.reservations = [block_a, block_b]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}

        # Both windows are open; pick one minute into block_a's window
        now = slot_start(block_a) + timedelta(minutes=1)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert result is not None
        assert result.id == 2  # 4-hour block ends later


# ---------------------------------------------------------------------------
# Occupancy — record, available, release
# ---------------------------------------------------------------------------


class TestOccupancy:
    def test_available_decreases_on_record(self):
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        assert state.ondemand_available(block) == 4
        state.record_ondemand_placement(1, "pod-a", 2)
        assert state.ondemand_available(block) == 2

    def test_available_increases_on_release(self):
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        state.record_ondemand_placement(1, "pod-a", 2)
        state.release_ondemand_pod("pod-a")
        assert state.ondemand_available(block) == 4

    def test_release_returns_block_id(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_ondemand_placement(1, "pod-a", 1)
        result = state.release_ondemand_pod("pod-a")
        assert result == 1

    def test_release_unknown_pod_returns_none(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        result = state.release_ondemand_pod("ghost-pod")
        assert result is None

    def test_multi_tenant_fills_to_limit(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_ondemand_placement(1, "pod-a", 1)
        state.record_ondemand_placement(1, "pod-b", 1)
        now = _now_inside_block(block)
        # Block is now full; a third request should not find it
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert result is None

    def test_multi_tenant_partial_release_allows_new(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_ondemand_placement(1, "pod-a", 1)
        state.record_ondemand_placement(1, "pod-b", 1)
        state.release_ondemand_pod("pod-a")
        now = _now_inside_block(block)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert result is not None

    def test_occupancy_cleaned_up_when_empty(self):
        block = _ondemand_block(1, gpu_count=1)
        state = _state_with_block(block)
        state.record_ondemand_placement(1, "pod-a", 1)
        state.release_ondemand_pod("pod-a")
        assert 1 not in state.ondemand_occupancy


# ---------------------------------------------------------------------------
# reconcile_ondemand
# ---------------------------------------------------------------------------


class TestReconcileOndemand:
    def test_prunes_cancelled_block(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_ondemand_placement(1, "pod-a", 1)
        # Remove the block from active reservations (simulates cancellation)
        state.reservations = []
        state.reconcile_ondemand()
        assert 1 not in state.ondemand_occupancy

    def test_prunes_expired_block(self):
        """A block whose window has ended should be pruned."""
        block = _ondemand_block(99, duration_minutes=30)
        state = _state_with_block(block)
        state.record_ondemand_placement(99, "pod-a", 1)
        # Simulate the block's window ending by using a past date
        past_date = (datetime.now() - timedelta(days=1)).date()
        expired_block = block.model_copy(update={"date": past_date})
        state.reservations = [expired_block]
        state.reconcile_ondemand()
        assert 99 not in state.ondemand_occupancy

    def test_preserves_active_block(self):
        # Use a block whose window is open at the actual current time so that
        # reconcile_ondemand (which calls datetime.now() internally) sees it
        # as still active.
        block = _currently_open_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_ondemand_placement(1, "pod-a", 1)
        state.reconcile_ondemand()
        assert 1 in state.ondemand_occupancy


# ---------------------------------------------------------------------------
# add_ondemand_candidate / remove_ondemand_candidate
# ---------------------------------------------------------------------------


class TestCandidateManagement:
    def test_add_candidate(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600)
        assert "uid-1" in state.ondemand_candidates
        c = state.ondemand_candidates["uid-1"]
        assert c.gpu_class_label == "h100"
        assert c.min_runtime_seconds == 600

    def test_add_is_idempotent(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600)
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600)
        assert len(state.ondemand_candidates) == 1

    def test_already_placed_pod_not_re_added(self):
        state = ControllerState()
        state.ondemand_occupancy[42] = {"uid-1": 1}
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600)
        assert "uid-1" not in state.ondemand_candidates

    def test_remove_candidate(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600)
        state.remove_ondemand_candidate("uid-1")
        assert "uid-1" not in state.ondemand_candidates

    def test_remove_nonexistent_is_noop(self):
        state = ControllerState()
        state.remove_ondemand_candidate("ghost")  # should not raise


# ---------------------------------------------------------------------------
# Deadline arithmetic (slot_end - now, not chained)
# ---------------------------------------------------------------------------


class TestDeadlineArithmetic:
    def test_remaining_is_not_chained(self):
        """On-demand deadline must not chain across blocks (unlike reserved path)."""
        block = _ondemand_block(1, duration_minutes=60)
        now = _now_inside_block(block)
        remaining = int((slot_end(block) - now).total_seconds())
        # Should be approximately 59 minutes (1 minute elapsed inside the 60-min block)
        assert 59 * 60 - 5 <= remaining <= 59 * 60 + 5

    def test_remaining_approaches_zero_near_end(self):
        block = _ondemand_block(1, duration_minutes=60)
        now = slot_end(block) - timedelta(seconds=10)
        remaining = int((slot_end(block) - now).total_seconds())
        assert 8 <= remaining <= 12
