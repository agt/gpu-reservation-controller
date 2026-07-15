"""Unit tests for the pure preemption-risk forecast planning functions.

Covers ``forecast_buckets``, ``overlapping_bucket_indices``, ``combine_risks``,
``ControllerState.forecast_boundaries``, and the assembled
``ControllerState.forecast_preemption_risk``.  All pure / no I/O.  The HTTP
endpoint riding on these is covered in ``tests/test_forecast_api.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.controller import (
    OnDemandCandidate,
    PodRuntimeView,
    combine_risks,
    forecast_buckets,
    overlapping_bucket_indices,
)

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    OTHER_CLASS_LABEL,
    USERNAME,
)
from tests.conftest import make_state as _state
from tests.conftest import reservation

NOW = datetime(2024, 1, 15, 9, 20, tzinfo=timezone.utc)
LEAD = timedelta(minutes=15)


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 15, hour, minute, tzinfo=timezone.utc)


def _view(
    uid: str,
    *,
    gpu_class: str = GPU_CLASS_LABEL,
    gpu_count: int = 1,
    reservation_id: int | None = None,
    node_resident: bool = True,
    terminating: bool = False,
    namespace: str = USERNAME,
) -> PodRuntimeView:
    return PodRuntimeView(
        uid=uid,
        namespace=namespace,
        name=f"pod-{uid}",
        gpu_class=gpu_class,
        gpu_count=gpu_count,
        reservation_id=reservation_id,
        node_resident=node_resident,
        terminating=terminating,
    )


def _booking(
    res_id: int,
    start: datetime,
    end: datetime,
    *,
    username: str = "bob",
    user_id: int = 9,
    gpu_count: int = 4,
):
    """A competitor booking (different owner than the holder by default)."""
    return reservation(
        res_id,
        start_utc=start,
        end_utc=end,
        username=username,
        user_id=user_id,
        gpu_count=gpu_count,
    )


def _forecast(state, pods, *, capacity=None, pending=None, now=NOW, lead=LEAD):
    return state.forecast_preemption_risk(
        capacity if capacity is not None else {GPU_CLASS_LABEL: 4},
        pods,
        pending or [],
        now,
        lead,
    )


def _pod(forecast, uid):
    return next(pf for pf in forecast.pods if pf.view.uid == uid)


def _risks(pod_forecast) -> list[float]:
    return [pb.risk for pb in pod_forecast.buckets]


def _states(pod_forecast) -> list[str]:
    return [pb.state for pb in pod_forecast.buckets]


def _assert_guarantee_invariant(forecast) -> None:
    """A bucket labelled guaranteed must carry exactly zero risk."""
    for pf in forecast.pods:
        for pb in pf.buckets:
            if pb.state == "guaranteed":
                assert pb.risk == 0.0


# ---------------------------------------------------------------------------
# Bucket / attribution / combination helpers
# ---------------------------------------------------------------------------


class TestForecastBuckets:
    def test_mid_hour_start_clips_bucket_zero(self):
        assert forecast_buckets(NOW) == [
            (NOW, _t(10)),
            (_t(10), _t(11)),
            (_t(11), _t(12)),
        ]

    def test_exactly_on_the_hour_gives_three_full_hours(self):
        assert forecast_buckets(_t(9)) == [
            (_t(9), _t(10)),
            (_t(10), _t(11)),
            (_t(11), _t(12)),
        ]


class TestOverlappingBucketIndices:
    BUCKETS = [(NOW, _t(10)), (_t(10), _t(11)), (_t(11), _t(12))]

    def test_interval_straddling_an_edge_hits_both(self):
        assert overlapping_bucket_indices(_t(9, 50), _t(10, 5), self.BUCKETS) == [0, 1]

    def test_interval_ending_exactly_at_a_bucket_start_includes_it(self):
        # A kill landing at instant 10:00 happens inside [10:00, 11:00).
        assert overlapping_bucket_indices(_t(9, 45), _t(10), self.BUCKETS) == [0, 1]

    def test_interval_starting_exactly_at_a_bucket_end_excludes_it(self):
        assert overlapping_bucket_indices(_t(10), _t(10, 10), self.BUCKETS) == [1]


class TestCombineRisks:
    def test_empty_is_zero(self):
        assert combine_risks([]) == 0.0

    def test_two_halves_combine_to_three_quarters(self):
        assert combine_risks([0.5, 0.5]) == 0.75

    def test_certainty_dominates(self):
        assert combine_risks([1.0, 0.2]) == 1.0

    def test_inputs_clamped(self):
        assert combine_risks([-0.5]) == 0.0
        assert combine_risks([1.5]) == 1.0


# ---------------------------------------------------------------------------
# forecast_boundaries
# ---------------------------------------------------------------------------


class TestForecastBoundaries:
    def test_bounds_are_exclusive_start_inclusive_end(self):
        state = _state(
            _booking(1, NOW, _t(10, 20)),            # exactly at now — excluded
            _booking(2, _t(10), _t(11)),             # inside — included
            _booking(3, _t(12), _t(13)),             # exactly at horizon_end — included
            _booking(4, _t(12, 1), _t(13)),          # beyond — excluded
        )
        assert state.forecast_boundaries(NOW, _t(12)) == [_t(10), _t(12)]

    def test_noshow_and_non_booking_excluded_and_deduped(self):
        state = _state(
            _booking(1, _t(10), _t(11)),
            _booking(2, _t(10), _t(11), username="carol", user_id=8),  # dup instant
            _booking(3, _t(11), _t(12)),
            reservation(4, start_utc=_t(10, 30), end_utc=_t(11), kind="reclaim"),
        )
        state.noshow_reservation_ids.add(3)
        assert state.forecast_boundaries(NOW, _t(12)) == [_t(10)]


# ---------------------------------------------------------------------------
# forecast_preemption_risk — per-pod risk semantics
# ---------------------------------------------------------------------------


class TestGuaranteedPodHasZeroRisk:
    def test_zero_risk_everywhere_despite_shortfall(self):
        # Holder's own booking spans the whole horizon; bob's 8-GPU booking at
        # 11:00 is short by 5 GPUs — but the guaranteed holder is never at risk.
        state = _state(
            reservation(1, start_utc=_t(8), end_utc=_t(12), gpu_count=1),
            _booking(2, _t(11), _t(12), gpu_count=8),
        )
        f = _forecast(state, [_view("u1", reservation_id=1)])
        pf = _pod(f, "u1")
        assert pf.guarantee_end == _t(12)
        assert _risks(pf) == [0.0, 0.0, 0.0]
        assert _states(pf) == ["guaranteed", "guaranteed", "guaranteed"]
        _assert_guarantee_invariant(f)

    def test_shortfall_with_empty_pool_is_still_reported(self):
        state = _state(
            reservation(1, start_utc=_t(8), end_utc=_t(12), gpu_count=1),
            _booking(2, _t(11), _t(12), gpu_count=8),
        )
        f = _forecast(state, [_view("u1", reservation_id=1)])
        # 11:00 boundary (kill window 10:45–11:00) lands in buckets 1 and 2:
        # demand 8, free 3 → shortfall 5, and nobody is eligible to supply it.
        b1 = f.buckets[1]
        assert b1.shortfall_by_class[GPU_CLASS_LABEL] == 5
        assert b1.eligible_pool_gpus_by_class[GPU_CLASS_LABEL] == 0


class TestOverstayRisk:
    def test_risk_lands_in_buckets_overlapping_the_kill_window(self):
        # Holder's window ended at 08:00; bob's 4-GPU booking at 11:00 against
        # capacity 4 with 1 GPU in use → shortfall 1 over a 1-GPU pool → 1.0,
        # attributed to [10:45, 11:00] → buckets 1 and 2.
        state = _state(
            reservation(1, start_utc=_t(7), end_utc=_t(8), gpu_count=1),
            _booking(2, _t(11), _t(12), gpu_count=4),
        )
        f = _forecast(state, [_view("u1", reservation_id=1)])
        pf = _pod(f, "u1")
        assert pf.guarantee_end == _t(8)
        assert _risks(pf) == [0.0, 1.0, 1.0]
        assert _states(pf) == ["overstay", "overstay", "overstay"]

    def test_zero_risk_when_free_capacity_covers_demand(self):
        state = _state(
            reservation(1, start_utc=_t(7), end_utc=_t(8), gpu_count=1),
            _booking(2, _t(10), _t(11), gpu_count=4),
        )
        f = _forecast(state, [_view("u1", reservation_id=1)], capacity={GPU_CLASS_LABEL: 8})
        assert _risks(_pod(f, "u1")) == [0.0, 0.0, 0.0]
        assert _states(_pod(f, "u1")) == ["overstay", "overstay", "overstay"]
        assert f.buckets[1].demand_by_class[GPU_CLASS_LABEL] == 4
        assert f.buckets[1].shortfall_by_class[GPU_CLASS_LABEL] == 0

    def test_two_overstayers_share_the_pool_and_risks_combine(self):
        # Boundaries at 10:00 (bob) and 10:30 (carol), each short 1 GPU against
        # a 2-GPU overstay pool → 0.5 per boundary.  10:00's kill window
        # (09:45–10:00) covers buckets 0+1; 10:30's (10:15–10:30) covers
        # bucket 1 only → bucket 1 combines to 1 − 0.5·0.5 = 0.75.
        state = _state(
            reservation(1, start_utc=_t(7), end_utc=_t(8), gpu_count=2),
            _booking(2, _t(10), _t(11), gpu_count=4),
            _booking(3, _t(10, 30), _t(11, 30), username="carol", user_id=8, gpu_count=4),
        )
        pods = [_view("u1", reservation_id=1), _view("u2", reservation_id=1)]
        f = _forecast(state, pods, capacity={GPU_CLASS_LABEL: 5})
        for uid in ("u1", "u2"):
            assert _risks(_pod(f, uid)) == [0.5, 0.75, 0.0]
        # Summary: demand/shortfall sum over attributed boundaries, pool is a max.
        assert f.buckets[0].demand_by_class[GPU_CLASS_LABEL] == 4
        assert f.buckets[1].demand_by_class[GPU_CLASS_LABEL] == 8
        assert f.buckets[1].shortfall_by_class[GPU_CLASS_LABEL] == 2
        assert f.buckets[1].eligible_pool_gpus_by_class[GPU_CLASS_LABEL] == 2
        assert f.buckets[2].demand_by_class[GPU_CLASS_LABEL] == 0
        for b in f.buckets:
            assert b.capacity_by_class[GPU_CLASS_LABEL] == 5
            assert b.free_by_class[GPU_CLASS_LABEL] == 3

    def test_boundary_just_past_the_horizon_reaches_back_into_bucket_two(self):
        # 12:10 boundary is beyond the last bucket but within lead of it: its
        # kill window (11:55–12:10) overlaps bucket 2 — without the tail
        # extension the end of bucket 2 would read falsely safe.
        state = _state(
            reservation(1, start_utc=_t(7), end_utc=_t(8), gpu_count=1),
            _booking(2, _t(12, 10), _t(13, 10), gpu_count=4),
        )
        f = _forecast(state, [_view("u1", reservation_id=1)])
        assert _risks(_pod(f, "u1")) == [0.0, 0.0, 1.0]

    def test_risk_never_precedes_the_pods_own_guarantee_end(self):
        # Guarantee ends 10:30 (mid bucket 1).  The 10:00 boundary precedes it
        # → no contribution (and its pool is empty).  The 11:00 boundary makes
        # the pod eligible; its window clamps to [10:45, 11:00] → buckets 1+2.
        state = _state(
            reservation(1, start_utc=_t(8), end_utc=_t(10, 30), gpu_count=1),
            _booking(2, _t(10), _t(11), gpu_count=4),
            _booking(3, _t(11), _t(12), username="carol", user_id=8, gpu_count=4),
        )
        f = _forecast(state, [_view("u1", reservation_id=1)])
        pf = _pod(f, "u1")
        assert pf.guarantee_end == _t(10, 30)
        assert _states(pf) == ["guaranteed", "mixed", "overstay"]
        assert _risks(pf) == [0.0, 1.0, 1.0]
        _assert_guarantee_invariant(f)


class TestChainIntegrity:
    def test_chained_holder_is_guaranteed_through_the_chain_and_credits_demand(self):
        # Back-to-back chain 08:00→09:30→10:30→11:30 (same user/class/count).
        # The regression this guards: evaluating eligibility "as of" boundary
        # 10:30 would sever the chain at the member ending exactly there,
        # collapsing the guarantee and marking the holder a phantom victim of
        # bob's short 10:30 booking.
        state = _state(
            reservation(1, start_utc=_t(8), end_utc=_t(9, 30), gpu_count=1),
            reservation(2, start_utc=_t(9, 30), end_utc=_t(10, 30), gpu_count=1),
            reservation(3, start_utc=_t(10, 30), end_utc=_t(11, 30), gpu_count=1),
            _booking(4, _t(10, 30), _t(11, 30), gpu_count=4),
        )
        f = _forecast(state, [_view("u1", reservation_id=1)])
        pf = _pod(f, "u1")
        assert pf.guarantee_end == _t(11, 30)          # chain walked intact
        assert _risks(pf) == [0.0, 0.0, 0.0]
        assert _states(pf) == ["guaranteed", "guaranteed", "mixed"]
        # The chained holder supplies its own windows: reservations #2/#3 add
        # no demand, so the only shortfall is bob's (unmet — pool is empty).
        assert f.buckets[1].shortfall_by_class[GPU_CLASS_LABEL] == 1
        assert f.buckets[1].eligible_pool_gpus_by_class[GPU_CLASS_LABEL] == 0
        _assert_guarantee_invariant(f)


class TestPodPopulationAndEdgeCases:
    def test_zero_gpu_pod_is_reported_but_never_pooled(self):
        state = _state(
            reservation(1, start_utc=_t(7), end_utc=_t(8), gpu_count=1),
            _booking(2, _t(10), _t(11), gpu_count=4),
        )
        f = _forecast(
            state, [_view("u0", reservation_id=1, gpu_count=0)],
            capacity={GPU_CLASS_LABEL: 3},
        )
        pf = _pod(f, "u0")
        assert _risks(pf) == [0.0, 0.0, 0.0]
        assert f.buckets[1].shortfall_by_class[GPU_CLASS_LABEL] == 1
        assert f.buckets[1].eligible_pool_gpus_by_class[GPU_CLASS_LABEL] == 0

    def test_non_admitted_pod_absent_from_output_but_consumes_free(self):
        state = _state(_booking(2, _t(10), _t(11), gpu_count=4))
        f = _forecast(state, [_view("sq", reservation_id=None, gpu_count=2)])
        assert f.pods == []
        assert f.buckets[1].free_by_class[GPU_CLASS_LABEL] == 2
        assert f.buckets[1].shortfall_by_class[GPU_CLASS_LABEL] == 2

    def test_negative_free_and_unresolvable_guarantee(self):
        # reservation_id 99 is not in state → guarantee unresolvable → overstay;
        # 4 GPUs in use on a 2-GPU class → free −2 → shortfall exceeds demand.
        state = _state(_booking(2, _t(10), _t(11), gpu_count=2))
        f = _forecast(
            state, [_view("v", reservation_id=99, gpu_count=4)],
            capacity={GPU_CLASS_LABEL: 2},
        )
        pf = _pod(f, "v")
        assert pf.guarantee_end is None
        assert _states(pf) == ["overstay", "overstay", "overstay"]
        assert _risks(pf) == [1.0, 1.0, 0.0]
        assert f.buckets[0].free_by_class[GPU_CLASS_LABEL] == -2
        assert f.buckets[1].shortfall_by_class[GPU_CLASS_LABEL] == 4

    def test_unresolvable_gpu_class_label_contributes_no_demand(self):
        state = _state(
            reservation(1, start_utc=_t(7), end_utc=_t(8), gpu_count=1),
            _booking(2, _t(10), _t(11), gpu_count=4),
            labels={},
        )
        f = _forecast(state, [_view("u1", reservation_id=1)])
        assert _risks(_pod(f, "u1")) == [0.0, 0.0, 0.0]
        for b in f.buckets:
            assert b.demand_by_class[GPU_CLASS_LABEL] == 0
            assert b.shortfall_by_class[GPU_CLASS_LABEL] == 0


class TestPendingJit:
    def test_reported_on_bucket_zero_only_and_never_folded_into_shortfall(self):
        def _candidate(uid, label, gpus):
            return OnDemandCandidate(
                pod_uid=uid,
                pod_name=f"pod-{uid}",
                pod_namespace=USERNAME,
                gpu_class_label=label,
                gpu_requested=gpus,
                min_runtime_seconds=600,
                pod_created_at=NOW,
                next_attempt_at=NOW,
            )

        f = _forecast(
            _state(),
            [],
            pending=[
                _candidate("j1", GPU_CLASS_LABEL, 2),
                _candidate("j2", GPU_CLASS_LABEL, 1),
                _candidate("j3", OTHER_CLASS_LABEL, 2),
            ],
        )
        assert f.buckets[0].pending_jit_gpus_by_class[GPU_CLASS_LABEL] == 3
        assert f.buckets[0].pending_jit_gpus_by_class[OTHER_CLASS_LABEL] == 2
        assert f.buckets[0].capacity_by_class[OTHER_CLASS_LABEL] == 0
        for b in f.buckets[1:]:
            assert b.pending_jit_gpus_by_class[GPU_CLASS_LABEL] == 0
        for b in f.buckets:
            assert b.shortfall_by_class[GPU_CLASS_LABEL] == 0


class TestForecastMetadata:
    def test_generated_at_and_lead_minutes(self):
        f = _forecast(_state(), [])
        assert f.generated_at == NOW
        assert f.lead_minutes == 15
        assert len(f.buckets) == 3
        assert (f.buckets[0].start, f.buckets[0].end) == (NOW, _t(10))
