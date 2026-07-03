"""Unit tests for chain-aware no-show protection (the #3 double-count fix).

Covers refresh_claimed_reservations and the claimed-set guards added to
check_noshow_deadlines, update_noshow_tracking, find_ondemand_block, and the
chain-aware branch of mark_pod_seen_for_noshow.

No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.schemas import GpuClassBrief, ReservationResponse, UserBrief

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state
from tests.conftest import reclaim_reservation as _ondemand_reservation
from tests.conftest import user_reservation as _user_reservation

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)  # 1 h into res #1's 08:00–10:00 UTC window
TIMEOUT = 15
GRACE = 30


def _open_chain_pair() -> tuple[ReservationResponse, ReservationResponse]:
    """Two back-to-back user reservations whose chain is active at wall-clock now (UTC).

    res #1 spans [now-30m, now+30m]; res #2 (slot_index=1) spans [now+30m,
    now+90m].  Used where the code under test computes the chain against
    ``datetime.now(timezone.utc)`` (e.g. mark_pod_seen_for_noshow), which would
    filter out past-dated fixtures.
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=30)
    start_time = f"{start.hour:02d}:{start.minute:02d}:00"
    slot_duration = 60

    # Compute start_utc/end_utc from the policy fields the same way _compute_window does.
    parts = start_time.split(":")
    base_minutes = int(parts[0]) * 60 + int(parts[1])
    midnight = datetime.combine(start.date(), datetime.min.time()).replace(tzinfo=timezone.utc)

    def _res(res_id: int, slot_index: int) -> ReservationResponse:
        s_utc = midnight + timedelta(minutes=base_minutes + slot_index * slot_duration)
        e_utc = s_utc + timedelta(minutes=slot_duration)
        return ReservationResponse(
            id=res_id,
            user_id=1,
            user=UserBrief(id=1, username=USERNAME),
            group_id=None,
            group=None,
            gpu_class_id=GPU_CLASS_ID,
            gpu_class=GpuClassBrief(id=GPU_CLASS_ID, name="H100"),
            date=start.date(),
            start_utc=s_utc,
            end_utc=e_utc,
            gpu_count=2,
            status="active",
            kind="booking",
            created_at=datetime(2024, 1, 1),
            updated_at=datetime(2024, 1, 1),
        )

    return _res(1, 0), _res(2, 1)


# ---------------------------------------------------------------------------
# refresh_claimed_reservations
# ---------------------------------------------------------------------------


class TestRefreshClaimedReservations:
    def test_single_holder_no_chain(self):
        state = _state(_user_reservation(1))
        state.refresh_claimed_reservations([1], NOW)
        assert state.claimed_reservation_ids == {1}

    def test_holder_claims_full_chain(self):
        state = _state(
            _user_reservation(1, slot_index=0),
            _user_reservation(2, slot_index=1),
            _user_reservation(3, slot_index=2),
        )
        state.refresh_claimed_reservations([1], NOW)
        assert state.claimed_reservation_ids == {1, 2, 3}

    def test_clears_deadlines_for_claimed(self):
        state = _state(
            _user_reservation(1, slot_index=0),
            _user_reservation(2, slot_index=1),
        )
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.noshow_deadlines[2] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.refresh_claimed_reservations([1], NOW)
        assert 1 not in state.noshow_deadlines
        assert 2 not in state.noshow_deadlines  # chained window also cleared

    def test_empty_holders_resets_claimed(self):
        state = _state(_user_reservation(1))
        state.claimed_reservation_ids = {5}
        state.refresh_claimed_reservations([], NOW)
        assert state.claimed_reservation_ids == set()

    def test_duplicate_holder_ids_deduped(self):
        state = _state(_user_reservation(1))
        state.refresh_claimed_reservations([1, 1, 1], NOW)
        assert state.claimed_reservation_ids == {1}


# ---------------------------------------------------------------------------
# check_noshow_deadlines respects the claimed set
# ---------------------------------------------------------------------------


class TestCheckNoshowDeadlinesRespectsClaimed:
    def test_claimed_not_declared_even_if_expired(self):
        state = _state(_user_reservation(1))
        state.noshow_deadlines[1] = datetime(2024, 1, 1, tzinfo=timezone.utc)  # long expired
        state.claimed_reservation_ids = {1}
        state.check_noshow_deadlines(NOW)
        assert 1 not in state.noshow_reservation_ids
        assert 1 in state.noshow_deadlines  # left intact for when it unclaims

    def test_unclaimed_expired_still_declared(self):
        state = _state(_user_reservation(1))
        state.noshow_deadlines[1] = datetime(2024, 1, 1, tzinfo=timezone.utc)
        state.check_noshow_deadlines(NOW)
        assert 1 in state.noshow_reservation_ids


# ---------------------------------------------------------------------------
# update_noshow_tracking skips claimed
# ---------------------------------------------------------------------------


class TestUpdateNoshowTrackingSkipsClaimed:
    def test_claimed_not_armed(self):
        r = _user_reservation(1)
        state = _state(r)
        state.claimed_reservation_ids = {1}
        # Window open at NOW; without the claim this would arm a grace deadline.
        state.update_noshow_tracking(NOW, TIMEOUT, GRACE)
        assert 1 not in state.noshow_deadlines

    def test_unclaimed_armed(self):
        r = _user_reservation(1)
        state = _state(r)
        state.update_noshow_tracking(NOW, TIMEOUT, GRACE)
        assert 1 in state.noshow_deadlines


# ---------------------------------------------------------------------------
# find_ondemand_block excludes claimed (defense in depth)
# ---------------------------------------------------------------------------


class TestFindOndemandBlockExcludesClaimed:
    def test_claimed_noshow_block_not_lent(self):
        r = _user_reservation(1)
        state = _state(r)
        state.noshow_reservation_ids.add(1)  # declared no-show...
        state.claimed_reservation_ids = {1}  # ...but a chained holder occupies it
        assert state.find_ondemand_block(GPU_CLASS_LABEL, NOW, 1, 1) is None

    def test_unclaimed_noshow_block_is_lent(self):
        r = _user_reservation(1)
        state = _state(r)
        state.noshow_reservation_ids.add(1)
        result = state.find_ondemand_block(GPU_CLASS_LABEL, NOW, 1, 1)
        assert result is not None
        assert result.id == 1


# ---------------------------------------------------------------------------
# mark_pod_seen_for_noshow — chain-aware branch
# ---------------------------------------------------------------------------


class TestMarkPodSeenChainAware:
    def test_holder_clears_full_chain(self):
        # mark_pod_seen_for_noshow walks the chain against wall-clock now (UTC),
        # so use a pair whose windows are open now.
        r1, r2 = _open_chain_pair()
        state = _state(r1, r2)
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.noshow_deadlines[2] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL, booking_reservation_id=1)
        assert 1 not in state.noshow_deadlines
        assert 2 not in state.noshow_deadlines

    def test_ondemand_booking_clears_nothing(self):
        state = _state(_ondemand_reservation(1))
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL, booking_reservation_id=1)
        assert 1 in state.noshow_deadlines  # squatter vouches for nothing

    def test_fallback_when_no_booking_id(self):
        # Two matching reservations; fallback clears the soonest by slot_start.
        state = _state(
            _user_reservation(1, slot_index=0),
            _user_reservation(2, slot_index=1),
        )
        state.noshow_deadlines[1] = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.noshow_deadlines[2] = datetime(2099, 1, 2, tzinfo=timezone.utc)
        state.mark_pod_seen_for_noshow(USERNAME, GPU_CLASS_LABEL)
        assert 1 not in state.noshow_deadlines  # soonest cleared
        assert 2 in state.noshow_deadlines


# ---------------------------------------------------------------------------
# End-to-end #3 regression: a chained holder protects the next window
# ---------------------------------------------------------------------------


class TestChainedHolderBlocksNoshowConversion:
    def test_chained_window_not_converted_while_holder_runs(self):
        # alice holds back-to-back 08:00–10:00 (res 1) and 10:00–12:00 (res 2),
        # both 2 GPUs, and runs ONE pod booked res-1 chained through res 2.
        # res 2 has an expired no-show deadline (no pod booked res-2), but must
        # not convert while the res-1 holder is live.
        state = _state(
            _user_reservation(1, slot_index=0, gpu_count=2),
            _user_reservation(2, slot_index=1, gpu_count=2),
        )
        state.noshow_deadlines[2] = datetime(2024, 1, 1, tzinfo=timezone.utc)  # already past

        # Per-tick refresh sees the live res-1 holder.
        state.refresh_claimed_reservations([1], NOW)
        state.check_noshow_deadlines(NOW)

        assert 2 not in state.noshow_reservation_ids
        # And res 2 is not offered as an on-demand block while claimed.
        assert state.find_ondemand_block(GPU_CLASS_LABEL, NOW, 1, 1) is None

    def test_window_converts_once_holder_gone(self):
        state = _state(
            _user_reservation(1, slot_index=0, gpu_count=2),
            _user_reservation(2, slot_index=1, gpu_count=2),
        )
        state.noshow_deadlines[2] = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # Holder vanished — no holder ids this tick.
        state.refresh_claimed_reservations([], NOW)
        state.check_noshow_deadlines(NOW)

        assert 2 in state.noshow_reservation_ids  # now reclaimable
