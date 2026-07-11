"""Unit tests for the live guarantee-arithmetic helpers in controller.py.

``compute_guaranteed_until`` and ``guarantee_end`` are the demand-driven-
preemption replacement for ``compute_max_deadline_seconds``: instead of a
seconds count anchored to the moment of admission, they return the absolute
UTC instant a pod's runtime guarantee currently ends, recomputed live from
window arithmetic on every call.

No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.controller import ReclaimMerge

from tests.conftest import (
    FIXED_DATE,
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    OTHER_CLASS_ID,
    OTHER_CLASS_LABEL,
    USERNAME,
)
from tests.conftest import make_state as _state
from tests.conftest import reclaim_reservation as _reclaim
from tests.conftest import user_reservation as _user_reservation

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)  # 1h into an 08:00-10:00 window


# ---------------------------------------------------------------------------
# compute_guaranteed_until (reserved-path chain arithmetic)
# ---------------------------------------------------------------------------


class TestComputeGuaranteedUntil:
    def test_no_chain_returns_window_end(self):
        res = _user_reservation(1, start_time="08:00:00", duration_minutes=120)
        state = _state(res)
        assert state.compute_guaranteed_until(NOW, res) == datetime(
            2024, 1, 15, 10, 0, tzinfo=timezone.utc
        )

    def test_expired_window_returns_past_end_no_floor(self):
        # Unlike compute_max_deadline_seconds, there is no floor: an absolute
        # end may legitimately be in the past (a stale-data race); the caller
        # is responsible for treating that as "past guarantee".
        res = _user_reservation(1, start_time="08:00:00", duration_minutes=120)
        state = _state(res)
        later = datetime(2024, 1, 15, 11, 0, tzinfo=timezone.utc)  # 1h after end
        assert state.compute_guaranteed_until(later, res) == datetime(
            2024, 1, 15, 10, 0, tzinfo=timezone.utc
        )

    def test_one_backtoback_extends_to_chain_end(self):
        res1 = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, slot_index=1, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2)
        assert state.compute_guaranteed_until(NOW, res1) == datetime(
            2024, 1, 15, 12, 0, tzinfo=timezone.utc
        )

    def test_two_backtoback_extends_to_last_chain_end(self):
        res1 = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, slot_index=1, start_time="08:00:00", duration_minutes=120)
        res3 = _user_reservation(3, slot_index=2, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2, res3)
        assert state.compute_guaranteed_until(NOW, res1) == datetime(
            2024, 1, 15, 14, 0, tzinfo=timezone.utc
        )

    def test_gap_breaks_chain(self):
        res1 = _user_reservation(1, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, start_time="10:05:00", duration_minutes=120)
        state = _state(res1, res2)
        assert state.compute_guaranteed_until(NOW, res1) == datetime(
            2024, 1, 15, 10, 0, tzinfo=timezone.utc
        )

    def test_no_show_chain_link_excluded(self):
        res1 = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, slot_index=1, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2)
        state.noshow_reservation_ids.add(2)
        assert state.compute_guaranteed_until(NOW, res1) == datetime(
            2024, 1, 15, 10, 0, tzinfo=timezone.utc
        )

    def test_mismatched_gpu_count_breaks_chain(self):
        res1 = _user_reservation(1, gpu_count=2, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, gpu_count=1, slot_index=1, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2)
        assert state.compute_guaranteed_until(NOW, res1) == datetime(
            2024, 1, 15, 10, 0, tzinfo=timezone.utc
        )


# ---------------------------------------------------------------------------
# guarantee_end (live, restart-safe, dispatches on reserved_path)
# ---------------------------------------------------------------------------


class TestGuaranteeEndReservedPath:
    def test_resolves_to_chain_end(self):
        res1 = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        res2 = _user_reservation(2, slot_index=1, start_time="08:00:00", duration_minutes=120)
        state = _state(res1, res2)
        end = state.guarantee_end(1, reserved_path=True, now=NOW)
        assert end == datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)

    def test_absent_reservation_id_returns_none(self):
        state = _state()
        assert state.guarantee_end(999, reserved_path=True, now=NOW) is None

    def test_none_reservation_id_returns_none(self):
        state = _state()
        assert state.guarantee_end(None, reserved_path=True, now=NOW) is None

    def test_extends_when_abutting_booking_lands(self):
        # The defining property of live guarantees: booking a follow-on slot
        # after admission extends a currently-running pod's guarantee, which a
        # frozen activeDeadlineSeconds could never do (Kubernetes forbids
        # raising an existing deadline).
        res1 = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        state = _state(res1)
        assert state.guarantee_end(1, reserved_path=True, now=NOW) == datetime(
            2024, 1, 15, 10, 0, tzinfo=timezone.utc
        )
        res2 = _user_reservation(2, slot_index=1, start_time="08:00:00", duration_minutes=120)
        state.reservations.append(res2)
        assert state.guarantee_end(1, reserved_path=True, now=NOW) == datetime(
            2024, 1, 15, 12, 0, tzinfo=timezone.utc
        )


class TestGuaranteeEndOnDemandPath:
    def test_resolves_to_effective_end_of_active_block(self):
        block = _reclaim(42, slot_index=0, start_time="08:00:00", duration_minutes=120)
        state = _state(block)
        end = state.guarantee_end(42, reserved_path=False, now=NOW)
        assert end == datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)

    def test_resolves_through_reclaim_merge_extension(self):
        subject = _reclaim(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        state = _state(subject)
        extended = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
        state.reclaim_merges[1] = ReclaimMerge(
            subject_id=1, absorbed_ids=[2], extended_end=extended
        )
        assert state.guarantee_end(1, reserved_path=False, now=NOW) == extended

    def test_resolves_from_cancelled_reservations_when_not_active(self):
        cancelled = _user_reservation(7, start_time="08:00:00", duration_minutes=120)
        state = _state()
        state.record_cancelled_reservation(cancelled)
        end = state.guarantee_end(7, reserved_path=False, now=NOW)
        assert end == datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)

    def test_absent_block_returns_none(self):
        state = _state()
        assert state.guarantee_end(999, reserved_path=False, now=NOW) is None


class TestGuaranteeEndForeignPod:
    def test_no_booking_reference_never_returns_a_guarantee(self):
        state = _state()
        assert state.guarantee_end(None, reserved_path=False, now=NOW) is None
