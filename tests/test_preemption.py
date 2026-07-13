"""Unit tests for the pure demand-driven-preemption planning functions.

Covers ``free_capacity_by_class``, ``ControllerState.upcoming_boundaries``,
``ControllerState.boundary_demand``, ``ControllerState.plan_boundary_preemption``,
and ``ControllerState.prune_preemption_marks``.  All pure / no I/O — no
Kubernetes or HTTP calls are made.  The async sweep loop
(``main._run_preemption_sweep``) is covered separately once it exists.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from app.controller import PodRuntimeView, free_capacity_by_class

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    OTHER_CLASS_ID,
    OTHER_CLASS_LABEL,
    USERNAME,
)
from tests.conftest import make_state as _state
from tests.conftest import reservation, user_reservation as _user_reservation

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)


def _view(
    uid: str,
    *,
    gpu_class: str = GPU_CLASS_LABEL,
    gpu_count: int = 1,
    reservation_id: int | None = None,
    node_resident: bool = True,
    terminating: bool = False,
    namespace: str = USERNAME,
    name: str | None = None,
) -> PodRuntimeView:
    return PodRuntimeView(
        uid=uid,
        namespace=namespace,
        name=name or f"pod-{uid}",
        gpu_class=gpu_class,
        gpu_count=gpu_count,
        reservation_id=reservation_id,
        node_resident=node_resident,
        terminating=terminating,
    )


# ---------------------------------------------------------------------------
# free_capacity_by_class
# ---------------------------------------------------------------------------


class TestFreeCapacityByClass:
    def test_running_pod_counted(self):
        pods = [_view("p1", gpu_count=2, node_resident=True)]
        free = free_capacity_by_class({GPU_CLASS_LABEL: 4}, pods)
        assert free == {GPU_CLASS_LABEL: 2}

    def test_pending_not_scheduled_excluded(self):
        pods = [_view("p1", gpu_count=2, node_resident=False)]
        free = free_capacity_by_class({GPU_CLASS_LABEL: 4}, pods)
        assert free == {GPU_CLASS_LABEL: 4}

    def test_terminating_pod_excluded(self):
        pods = [_view("p1", gpu_count=2, node_resident=True, terminating=True)]
        free = free_capacity_by_class({GPU_CLASS_LABEL: 4}, pods)
        assert free == {GPU_CLASS_LABEL: 4}

    def test_unknown_class_ignored(self):
        pods = [_view("p1", gpu_class="unknown-class", gpu_count=2)]
        free = free_capacity_by_class({GPU_CLASS_LABEL: 4}, pods)
        assert free == {GPU_CLASS_LABEL: 4}

    def test_negative_free_signals_overcommit(self):
        pods = [_view("p1", gpu_count=6, node_resident=True)]
        free = free_capacity_by_class({GPU_CLASS_LABEL: 4}, pods)
        assert free == {GPU_CLASS_LABEL: -2}

    def test_multiple_classes_independent(self):
        pods = [
            _view("p1", gpu_class=GPU_CLASS_LABEL, gpu_count=1),
            _view("p2", gpu_class=OTHER_CLASS_LABEL, gpu_count=2),
        ]
        free = free_capacity_by_class(
            {GPU_CLASS_LABEL: 4, OTHER_CLASS_LABEL: 4}, pods
        )
        assert free == {GPU_CLASS_LABEL: 3, OTHER_CLASS_LABEL: 2}


# ---------------------------------------------------------------------------
# upcoming_boundaries
# ---------------------------------------------------------------------------


class TestUpcomingBoundaries:
    def test_boundary_within_lead_before_included_phase_a(self):
        res = _user_reservation(1, start_time="09:10:00", duration_minutes=60)
        state = _state(res)
        result = state.upcoming_boundaries(NOW, timedelta(minutes=15))
        assert result == [datetime(2024, 1, 15, 9, 10, tzinfo=timezone.utc)]

    def test_boundary_just_passed_within_lead_included_phase_b(self):
        res = _user_reservation(1, start_time="08:50:00", duration_minutes=60)
        state = _state(res)
        result = state.upcoming_boundaries(NOW, timedelta(minutes=15))
        assert result == [datetime(2024, 1, 15, 8, 50, tzinfo=timezone.utc)]

    def test_boundary_outside_lead_excluded(self):
        res = _user_reservation(1, start_time="10:00:00", duration_minutes=60)
        state = _state(res)
        result = state.upcoming_boundaries(NOW, timedelta(minutes=15))
        assert result == []

    def test_boundary_far_in_past_excluded(self):
        res = _user_reservation(1, start_time="07:00:00", duration_minutes=60)
        state = _state(res)
        result = state.upcoming_boundaries(NOW, timedelta(minutes=15))
        assert result == []

    def test_reclaim_kind_excluded(self):
        from tests.conftest import reclaim_reservation as _reclaim

        res = _reclaim(1, start_time="09:10:00", duration_minutes=60)
        state = _state(res)
        result = state.upcoming_boundaries(NOW, timedelta(minutes=15))
        assert result == []

    def test_duplicate_start_times_deduplicated_and_sorted(self):
        res1 = _user_reservation(1, start_time="09:10:00", duration_minutes=60, user_id=1, username="a")
        res2 = _user_reservation(2, start_time="09:10:00", duration_minutes=60, user_id=2, username="b")
        res3 = _user_reservation(3, start_time="08:55:00", duration_minutes=60, user_id=3, username="c")
        state = _state(res1, res2, res3)
        result = state.upcoming_boundaries(NOW, timedelta(minutes=15))
        assert result == [
            datetime(2024, 1, 15, 8, 55, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 9, 10, tzinfo=timezone.utc),
        ]


# ---------------------------------------------------------------------------
# boundary_demand
# ---------------------------------------------------------------------------


BOUNDARY = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)


class TestBoundaryDemand:
    def test_unclaimed_booking_demands_full_gpu_count(self):
        res = reservation(
            1,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=2,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state = _state(res)
        demand = state.boundary_demand(BOUNDARY, [], NOW)
        assert demand == {GPU_CLASS_LABEL: 2}

    def test_chained_holder_subtracts_partial_coverage(self):
        # Chaining (_chain_for) requires the two reservations' declared
        # gpu_count to match — the "partial coverage" comes from the holder's
        # POD only consuming 1 of the 4 GPUs its chain-origin reservation
        # reserved, not from a gpu_count mismatch between the linked windows.
        # A 1-GPU holder chained into a 4-GPU boundary reservation still
        # leaves 3 GPUs of genuine demand (subtracting per-reservation rather
        # than excluding the whole claimed reservation from demand).
        prior = reservation(
            1,
            start_utc=BOUNDARY - timedelta(hours=2),
            end_utc=BOUNDARY,
            gpu_count=4,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        boundary_res = reservation(
            2,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=4,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state = _state(prior, boundary_res)
        holder = _view("h1", reservation_id=1, gpu_count=1)
        demand = state.boundary_demand(BOUNDARY, [holder], NOW)
        assert demand == {GPU_CLASS_LABEL: 3}

    def test_fully_chained_reservation_has_no_demand(self):
        prior = reservation(
            1,
            start_utc=BOUNDARY - timedelta(hours=2),
            end_utc=BOUNDARY,
            gpu_count=2,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        boundary_res = reservation(
            2,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=2,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state = _state(prior, boundary_res)
        holder = _view("h1", reservation_id=1, gpu_count=2)
        demand = state.boundary_demand(BOUNDARY, [holder], NOW)
        assert demand == {}

    def test_noshow_reservation_excluded(self):
        res = reservation(
            1,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=2,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state = _state(res)
        state.noshow_reservation_ids.add(1)
        demand = state.boundary_demand(BOUNDARY, [], NOW)
        assert demand == {}

    def test_off_boundary_reservation_excluded(self):
        res = reservation(
            1,
            start_utc=BOUNDARY + timedelta(minutes=5),
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=2,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state = _state(res)
        demand = state.boundary_demand(BOUNDARY, [], NOW)
        assert demand == {}

    def test_two_classes_split_independently(self):
        res1 = reservation(
            1,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=2,
            gpu_class_id=GPU_CLASS_ID,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        res2 = reservation(
            2,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=3,
            gpu_class_id=OTHER_CLASS_ID,
            gpu_class_label=OTHER_CLASS_LABEL,
            user_id=2,
            username="bob",
        )
        state = _state(res1, res2)
        state.gpu_class_labels[OTHER_CLASS_ID] = OTHER_CLASS_LABEL
        demand = state.boundary_demand(BOUNDARY, [], NOW)
        assert demand == {GPU_CLASS_LABEL: 2, OTHER_CLASS_LABEL: 3}

    def test_unresolvable_label_skipped(self):
        res = reservation(
            1,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=2,
            gpu_class_id=999,
        )
        state = _state(res)
        demand = state.boundary_demand(BOUNDARY, [], NOW)
        assert demand == {}


# ---------------------------------------------------------------------------
# plan_boundary_preemption
# ---------------------------------------------------------------------------


class TestPlanBoundaryPreemption:
    def _demand_state(self, gpu_count=4):
        res = reservation(
            1,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=gpu_count,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        return _state(res)

    def test_kills_needed_selects_enough_victims(self):
        state = self._demand_state(gpu_count=2)
        # reservation_id set to ids absent from state.reservations so
        # guarantee_end resolves to None (unresolvable → treated as past
        # guarantee → eligible).
        victims = [
            _view(f"v{i}", reservation_id=100 + i, gpu_count=1)
            for i in range(3)
        ]
        # capacity_by_class is TOTAL node capacity, not pre-computed free —
        # free_capacity_by_class subtracts the victims' own node-resident
        # usage from it.  capacity=3 (all 3 victims' usage) → free=0.
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 3}, victims, NOW, rng=random.Random(0)
        )
        # demand=2, free=0 → kills_needed=2; each victim is 1 GPU → 2 selected.
        assert len(plan.victims) == 2
        assert plan.unmet_by_class == {}

    def test_within_guarantee_pod_never_selected(self):
        state = self._demand_state(gpu_count=2)
        # A live reservation whose window has not yet ended: guarantee is in
        # the future relative to NOW, so this pod must never be a victim even
        # though it is the only candidate.
        holder_res = reservation(
            2,
            start_utc=NOW - timedelta(minutes=30),
            end_utc=NOW + timedelta(hours=1),
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state.reservations.append(holder_res)
        within_guarantee = _view("in-guarantee", reservation_id=2, gpu_count=1)
        # capacity == the pod's own usage → free=0 → kills_needed=demand=2.
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 1}, [within_guarantee], NOW, rng=random.Random(0)
        )
        assert plan.victims == []
        assert plan.unmet_by_class == {GPU_CLASS_LABEL: 2}

    def test_past_guarantee_pod_eligible(self):
        state = self._demand_state(gpu_count=1)
        # Reservation #2's window already ended relative to NOW.
        expired_res = reservation(
            2,
            start_utc=NOW - timedelta(hours=2),
            end_utc=NOW - timedelta(minutes=5),
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state.reservations.append(expired_res)
        overstayer = _view("overstayer", reservation_id=2, gpu_count=1)
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 1}, [overstayer], NOW, rng=random.Random(0)
        )
        assert plan.victims == [overstayer]

    def test_unresolvable_guarantee_treated_as_past(self):
        # reservation_id not in state.reservations at all → guarantee_end
        # returns None → treated as past guarantee → eligible.
        state = self._demand_state(gpu_count=1)
        overstayer = _view("gone", reservation_id=999, gpu_count=1)
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 1}, [overstayer], NOW, rng=random.Random(0)
        )
        assert plan.victims == [overstayer]

    def test_foreign_pod_excluded_from_victims(self):
        state = self._demand_state(gpu_count=1)
        # A foreign pod (not admitted by this controller) still occupies
        # physical capacity — capacity=5 matches its usage so free=0 — but is
        # never eligible as a victim (reservation_id is None).
        foreign = _view("foreign", reservation_id=None, gpu_count=5)
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 5}, [foreign], NOW, rng=random.Random(0)
        )
        assert plan.victims == []
        assert plan.unmet_by_class == {GPU_CLASS_LABEL: 1}

    def test_terminating_pod_excluded_from_victims(self):
        state = self._demand_state(gpu_count=1)
        term = _view(
            "term", reservation_id=None, gpu_count=5, terminating=True
        )
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 0}, [term], NOW, rng=random.Random(0)
        )
        assert plan.victims == []

    def test_zero_gpu_pod_excluded_from_victims(self):
        state = self._demand_state(gpu_count=1)
        zero = _view("zero", reservation_id=None, gpu_count=0)
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 0}, [zero], NOW, rng=random.Random(0)
        )
        assert plan.victims == []

    def test_no_shortfall_selects_nothing(self):
        state = self._demand_state(gpu_count=2)
        # capacity=7, usage=5 → free=2 == demand → kills_needed=0.
        overstayer = _view("v1", reservation_id=None, gpu_count=5)
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 7}, [overstayer], NOW, rng=random.Random(0)
        )
        assert plan.victims == []
        assert plan.unmet_by_class == {}

    def test_overshoot_when_victim_larger_than_shortfall(self):
        state = self._demand_state(gpu_count=1)
        # reservation_id set but absent from state → unresolvable guarantee →
        # treated as past → eligible.  capacity == usage (4) → free=0.
        big_victim = _view("big", reservation_id=55, gpu_count=4)
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 4}, [big_victim], NOW, rng=random.Random(0)
        )
        assert plan.victims == [big_victim]
        assert plan.unmet_by_class == {}

    def test_unmet_when_no_eligible_victims_cover_shortfall(self):
        state = self._demand_state(gpu_count=4)
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 0}, [], NOW, rng=random.Random(0)
        )
        assert plan.victims == []
        assert plan.unmet_by_class == {GPU_CLASS_LABEL: 4}

    def test_two_classes_planned_independently(self):
        res1 = reservation(
            1,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=1,
            gpu_class_id=GPU_CLASS_ID,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        res2 = reservation(
            2,
            start_utc=BOUNDARY,
            end_utc=BOUNDARY + timedelta(hours=2),
            gpu_count=1,
            gpu_class_id=OTHER_CLASS_ID,
            gpu_class_label=OTHER_CLASS_LABEL,
            user_id=2,
            username="bob",
        )
        state = _state(res1, res2)
        state.gpu_class_labels[OTHER_CLASS_ID] = OTHER_CLASS_LABEL
        v1 = _view("v1", gpu_class=GPU_CLASS_LABEL, reservation_id=101, gpu_count=1)
        v2 = _view("v2", gpu_class=OTHER_CLASS_LABEL, reservation_id=102, gpu_count=1)
        plan = state.plan_boundary_preemption(
            BOUNDARY,
            {GPU_CLASS_LABEL: 1, OTHER_CLASS_LABEL: 1},
            [v1, v2],
            NOW,
            rng=random.Random(0),
        )
        assert set(plan.victims) == {v1, v2}
        assert plan.unmet_by_class == {}

    def test_seeded_rng_is_deterministic(self):
        state = self._demand_state(gpu_count=1)
        victims = [
            _view(f"v{i}", reservation_id=100 + i, gpu_count=1)
            for i in range(5)
        ]
        plan1 = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 5}, list(victims), NOW, rng=random.Random(42)
        )
        plan2 = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 5}, list(victims), NOW, rng=random.Random(42)
        )
        assert plan1.victims == plan2.victims

    def test_phase_a_before_boundary(self):
        state = self._demand_state()
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 999}, [], NOW, rng=random.Random(0)
        )
        assert plan.phase == "A"

    def test_phase_b_at_or_after_boundary(self):
        state = self._demand_state()
        plan = state.plan_boundary_preemption(
            BOUNDARY, {GPU_CLASS_LABEL: 999}, [], BOUNDARY, rng=random.Random(0)
        )
        assert plan.phase == "B"


# ---------------------------------------------------------------------------
# prune_preemption_marks
# ---------------------------------------------------------------------------


class TestPrunePreemptionMarks:
    def test_stale_boundary_pruned(self):
        state = _state()
        lead = timedelta(minutes=15)
        stale_boundary = NOW - lead  # exactly at the prune threshold
        state.preemption_fired[stale_boundary] = {"A", "B"}
        state.prune_preemption_marks(NOW, lead)
        assert stale_boundary not in state.preemption_fired

    def test_recent_boundary_kept(self):
        state = _state()
        lead = timedelta(minutes=15)
        recent_boundary = NOW - timedelta(minutes=1)
        state.preemption_fired[recent_boundary] = {"B"}
        state.prune_preemption_marks(NOW, lead)
        assert recent_boundary in state.preemption_fired
