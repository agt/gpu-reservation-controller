"""Unit tests for occupancy accounting and on-demand candidate bookkeeping in
ControllerState.

These tests exercise only the pure-Python logic in controller.py.
No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.controller import ControllerState, slot_start
from app.schemas import ReservationResponse

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL
from tests.conftest import ondemand_block as _ondemand_block


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _state_with_block(block: ReservationResponse) -> ControllerState:
    """Return a ControllerState pre-loaded with one reservation."""
    state = ControllerState()
    state.reservations = [block]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


def _now_inside_block(block: ReservationResponse) -> datetime:
    """Return a UTC datetime 1 minute after the block's slot_start."""
    return slot_start(block) + timedelta(minutes=1)


# ---------------------------------------------------------------------------
# Occupancy — record, available, release
# ---------------------------------------------------------------------------


class TestOccupancy:
    def test_available_decreases_on_record(self):
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        assert state.available(block) == 4
        state.record_placement(1, "pod-a", 2)
        assert state.available(block) == 2

    def test_available_increases_on_release(self):
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 2)
        state.release_pod("pod-a")
        assert state.available(block) == 4

    def test_available_excludes_named_uid(self):
        # The reserved path passes exclude_uid so a retry doesn't count itself.
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 2)
        state.record_placement(1, "pod-b", 1)
        assert state.available(block) == 1
        assert state.available(block, exclude_uid="pod-a") == 3

    def test_release_returns_block_id(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        result = state.release_pod("pod-a")
        assert result == 1

    def test_release_unknown_pod_returns_none(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        result = state.release_pod("ghost-pod")
        assert result is None

    def test_multi_tenant_fills_to_limit(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.record_placement(1, "pod-b", 1)
        assert state.available(block) == 0

    def test_multi_tenant_partial_release_allows_new(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.record_placement(1, "pod-b", 1)
        state.release_pod("pod-a")
        assert state.available(block) == 1

    def test_occupancy_cleaned_up_when_empty(self):
        block = _ondemand_block(1, gpu_count=1)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.release_pod("pod-a")
        assert 1 not in state.occupancy


# ---------------------------------------------------------------------------
# reconcile_occupancy — rebuild from a live cluster snapshot
# ---------------------------------------------------------------------------


class TestReconcileOccupancy:
    def test_rebuilds_from_snapshot(self):
        state = ControllerState()
        state.reconcile_occupancy(
            [(1, "pod-a", 2), (1, "pod-b", 1), (2, "pod-c", 1)]
        )
        assert state.occupancy == {1: {"pod-a": 2, "pod-b": 1}, 2: {"pod-c": 1}}

    def test_empty_snapshot_clears(self):
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1}}
        state.reconcile_occupancy([])
        assert state.occupancy == {}

    def test_prunes_stale_pod(self):
        # pod-gone is no longer in the snapshot → dropped (self-heal on a missed
        # DELETE event); pod-a survives.
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1, "pod-gone": 1}}
        state.reconcile_occupancy([(1, "pod-a", 1)])
        assert state.occupancy == {1: {"pod-a": 1}}

    def test_overwrites_gpu_count(self):
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1}}
        state.reconcile_occupancy([(1, "pod-a", 3)])
        assert state.occupancy == {1: {"pod-a": 3}}


# ---------------------------------------------------------------------------
# add_ondemand_candidate / remove_ondemand_candidate
# ---------------------------------------------------------------------------


class TestCandidateManagement:
    def test_add_candidate(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert "uid-1" in state.ondemand_candidates
        c = state.ondemand_candidates["uid-1"]
        assert c.gpu_class_label == "h100"
        assert c.min_runtime_seconds == 600

    def test_add_is_idempotent(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert len(state.ondemand_candidates) == 1

    def test_already_placed_pod_not_re_added(self):
        state = ControllerState()
        state.occupancy[42] = {"uid-1": 1}
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert "uid-1" not in state.ondemand_candidates

    def test_remove_candidate(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        state.remove_ondemand_candidate("uid-1")
        assert "uid-1" not in state.ondemand_candidates

    def test_remove_nonexistent_is_noop(self):
        state = ControllerState()
        state.remove_ondemand_candidate("ghost")  # should not raise
