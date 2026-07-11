"""Unit tests for pod adoption (re-linking a running pod to a new booking).

Covers the pure planning surface for two triggers:

  - a reserved-path pod running **past its runtime guarantee** is rescued into
    a fresh booking its user has since made (the "user re-books while their pod
    is still running" scenario)
  - an on-demand pod, still **within its own guarantee**, is proactively
    upgraded to a guaranteed reservation the moment one is open — no waiting
    for it to overstay, and no "shrink guard" (see ``TestPlanPodAdoptions``)

Test surface:

  - ``ControllerState.find_open_booking_for`` — open-booking selection
  - ``ControllerState.plan_pod_adoptions`` — which pods adopt where, and
    whether each was a rescue (``was_overstay=True``) or a proactive upgrade
    (``was_overstay=False``)
  - ``ControllerState.relink_occupancy`` — occupancy re-home
  - a regression proving adoption closes the preemption race, and a doc test
    pinning that the *abutting* case is already handled by chaining (no adoption)

All pure / no I/O — the async orchestrator (``main._adopt_pods``) is not
exercised here.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from app.controller import PodRuntimeView, free_capacity_by_class

from tests.conftest import (
    GPU_CLASS_LABEL,
    OTHER_CLASS_ID,
    OTHER_CLASS_LABEL,
    USERNAME,
)
from tests.conftest import block
from tests.conftest import make_state as _state
from tests.conftest import reservation

# 15:15 UTC — "now"; the pod's original window (12:00–15:00) has already closed.
NOW = datetime(2024, 1, 15, 15, 15, tzinfo=timezone.utc)
NOON = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
THREE_PM = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)
FIVE_PM = datetime(2024, 1, 15, 17, 0, tzinfo=timezone.utc)


def _view(
    uid: str,
    *,
    gpu_class: str = GPU_CLASS_LABEL,
    gpu_count: int = 1,
    reservation_id: int | None = None,
    reserved_path: bool = True,
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
        reserved_path=reserved_path,
        node_resident=node_resident,
        terminating=terminating,
    )


def _old_booking(res_id: int = 100, *, gpu_count: int = 1):
    """The concluded 12:00–15:00 reservation the overstay pod was admitted under."""
    return reservation(
        res_id,
        start_utc=NOON,
        end_utc=THREE_PM,
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
    )


def _open_booking(
    res_id: int = 200,
    *,
    start_utc: datetime,
    end_utc: datetime = FIVE_PM,
    gpu_count: int = 1,
    username: str = USERNAME,
    user_id: int = 1,
    gpu_class_id=None,
    gpu_class_label: str = GPU_CLASS_LABEL,
):
    kwargs = {}
    if gpu_class_id is not None:
        kwargs["gpu_class_id"] = gpu_class_id
    return reservation(
        res_id,
        start_utc=start_utc,
        end_utc=end_utc,
        gpu_count=gpu_count,
        gpu_class_label=gpu_class_label,
        username=username,
        user_id=user_id,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# find_open_booking_for
# ---------------------------------------------------------------------------


class TestFindOpenBookingFor:
    def test_selects_open_same_user_same_class_booking(self):
        # Non-abutting: starts 15:10, open at 15:15.
        b = _open_booking(200, start_utc=THREE_PM + timedelta(minutes=10))
        state = _state(b)
        got = state.find_open_booking_for(USERNAME, GPU_CLASS_LABEL, NOW, 1)
        assert got is not None and got.id == 200

    def test_future_window_rejected(self):
        b = _open_booking(200, start_utc=NOW + timedelta(minutes=30), end_utc=FIVE_PM)
        state = _state(b)
        assert state.find_open_booking_for(USERNAME, GPU_CLASS_LABEL, NOW, 1) is None

    def test_past_window_rejected(self):
        b = _open_booking(200, start_utc=NOON, end_utc=THREE_PM)  # closed at 15:00
        state = _state(b)
        assert state.find_open_booking_for(USERNAME, GPU_CLASS_LABEL, NOW, 1) is None

    def test_wrong_user_rejected(self):
        b = _open_booking(
            200, start_utc=THREE_PM, username="bob", user_id=2
        )
        state = _state(b)
        assert state.find_open_booking_for(USERNAME, GPU_CLASS_LABEL, NOW, 1) is None

    def test_wrong_class_rejected(self):
        b = _open_booking(
            200, start_utc=THREE_PM, gpu_class_id=OTHER_CLASS_ID,
            gpu_class_label=OTHER_CLASS_LABEL,
        )
        state = _state(b)
        state.gpu_class_labels[OTHER_CLASS_ID] = OTHER_CLASS_LABEL
        # Pod is h100; the only open booking is a100.
        assert state.find_open_booking_for(USERNAME, GPU_CLASS_LABEL, NOW, 1) is None

    def test_noshow_id_rejected(self):
        b = _open_booking(200, start_utc=THREE_PM)
        state = _state(b)
        state.noshow_reservation_ids.add(200)
        assert state.find_open_booking_for(USERNAME, GPU_CLASS_LABEL, NOW, 1) is None

    def test_insufficient_budget_rejected(self):
        b = _open_booking(200, start_utc=THREE_PM, gpu_count=1)
        state = _state(b)
        state.record_placement(200, "other-pod", 1)  # fills the single GPU
        assert state.find_open_booking_for(USERNAME, GPU_CLASS_LABEL, NOW, 1) is None

    def test_prefers_latest_ending(self):
        short = _open_booking(200, start_utc=THREE_PM, end_utc=FIVE_PM)
        longer = _open_booking(
            201, start_utc=THREE_PM, end_utc=FIVE_PM + timedelta(hours=1)
        )
        state = _state(short, longer)
        got = state.find_open_booking_for(USERNAME, GPU_CLASS_LABEL, NOW, 1)
        assert got is not None and got.id == 201

    def test_extra_used_reserves_budget(self):
        b = _open_booking(200, start_utc=THREE_PM, gpu_count=1)
        state = _state(b)
        # One GPU, already tentatively assigned this pass → nothing left.
        got = state.find_open_booking_for(
            USERNAME, GPU_CLASS_LABEL, NOW, 1, extra_used={200: 1}
        )
        assert got is None


# ---------------------------------------------------------------------------
# plan_pod_adoptions
# ---------------------------------------------------------------------------


class TestPlanPodAdoptions:
    def test_non_abutting_booking_adopts_overstay_pod(self):
        old = _old_booking(100)
        new = _open_booking(200, start_utc=THREE_PM + timedelta(minutes=10))
        state = _state(old, new)
        pod = _view("p1", reservation_id=100, reserved_path=True)
        plan = state.plan_pod_adoptions([pod], NOW)
        assert len(plan) == 1
        assigned_pod, target, was_overstay = plan[0]
        assert assigned_pod.uid == "p1" and target.id == 200
        assert was_overstay is True

    def test_pod_within_guarantee_left_alone(self):
        # Abutting follow-on (15:00–17:00): chaining already guarantees the pod
        # to 17:00, so it is not overstay and is not adopted. Reserved-path pods
        # are only rescued once past guarantee — unlike on-demand pods, there is
        # no proactive-upgrade trigger for them.
        old = _old_booking(100)
        abutting = _open_booking(200, start_utc=THREE_PM)  # abuts old's 15:00 end
        state = _state(old, abutting)
        pod = _view("p1", reservation_id=100, reserved_path=True)
        assert state.plan_pod_adoptions([pod], NOW) == []

    def test_pod_with_no_open_booking_left_alone(self):
        old = _old_booking(100)
        state = _state(old)  # no new booking for alice
        pod = _view("p1", reservation_id=100, reserved_path=True)
        assert state.plan_pod_adoptions([pod], NOW) == []

    def test_on_demand_overstay_pod_adopted(self):
        # Pod ran under an on-demand block (id 300) that has since ended and is
        # gone from the reservation set → guarantee_end is None → overstay.
        new = _open_booking(200, start_utc=THREE_PM + timedelta(minutes=10))
        state = _state(new)
        pod = _view("p1", reservation_id=300, reserved_path=False)
        plan = state.plan_pod_adoptions([pod], NOW)
        assert len(plan) == 1
        _, target, was_overstay = plan[0]
        assert target.id == 200
        assert was_overstay is True

    def test_ondemand_pod_within_guarantee_adopted_immediately(self):
        # The on-demand block (300) is still active and ends well after NOW —
        # the pod is comfortably within its own on-demand guarantee. It should
        # still be proactively upgraded to the new booking the moment one is
        # open, rather than waiting for the block to end.
        ondemand = block(300, start_utc=NOON, end_utc=NOW + timedelta(hours=3))
        new = _open_booking(200, start_utc=THREE_PM + timedelta(minutes=10))
        state = _state(ondemand, new)
        pod = _view("p1", reservation_id=300, reserved_path=False)
        plan = state.plan_pod_adoptions([pod], NOW)
        assert len(plan) == 1
        _, target, was_overstay = plan[0]
        assert target.id == 200
        assert was_overstay is False

    def test_ondemand_upgrade_applies_even_if_it_shortens_guarantee(self):
        # The on-demand block runs until 20:00 (long past the new booking's own
        # end at 16:00, with no further chaining). Converting to the new
        # booking shortens the pod's protection window, but there is no shrink
        # guard — the upgrade happens unconditionally per the chosen design.
        ondemand = block(300, start_utc=NOON, end_utc=NOW + timedelta(hours=5))
        new = _open_booking(
            200, start_utc=THREE_PM + timedelta(minutes=10), end_utc=NOW + timedelta(minutes=45)
        )
        state = _state(ondemand, new)
        pod = _view("p1", reservation_id=300, reserved_path=False)
        plan = state.plan_pod_adoptions([pod], NOW)
        assert len(plan) == 1
        _, target, was_overstay = plan[0]
        assert target.id == 200
        assert was_overstay is False
        assert state.compute_guaranteed_until(NOW, target) < state.effective_end(ondemand)

    def test_reserved_pod_within_guarantee_not_proactively_upgraded(self):
        # Contrast with the on-demand case: a reserved-path pod within guarantee
        # is never touched, even though a matching open booking exists — only
        # on-demand pods get the proactive-upgrade trigger.
        # Give reservation 100 a longer window than usual so it's still open
        # (and the pod still within guarantee) at NOW, unlike `_old_booking`.
        current = _open_booking(100, start_utc=NOON, end_utc=NOW + timedelta(hours=1))
        new = _open_booking(200, start_utc=THREE_PM + timedelta(minutes=10))
        state = _state(current, new)
        pod = _view("p1", reservation_id=100, reserved_path=True)
        assert state.plan_pod_adoptions([pod], NOW) == []

    def test_budget_respected_across_pods(self):
        old = _old_booking(100)
        new = _open_booking(200, start_utc=THREE_PM + timedelta(minutes=10), gpu_count=1)
        state = _state(old, new)
        p1 = _view("p1", reservation_id=100, reserved_path=True)
        p2 = _view("p2", reservation_id=100, reserved_path=True)
        plan = state.plan_pod_adoptions([p1, p2], NOW)
        # Only one GPU of budget on the new booking → only one pod adopted.
        assert len(plan) == 1

    def test_terminating_pod_skipped(self):
        old = _old_booking(100)
        new = _open_booking(200, start_utc=THREE_PM + timedelta(minutes=10))
        state = _state(old, new)
        pod = _view("p1", reservation_id=100, terminating=True)
        assert state.plan_pod_adoptions([pod], NOW) == []


# ---------------------------------------------------------------------------
# relink_occupancy
# ---------------------------------------------------------------------------


class TestRelinkOccupancy:
    def test_moves_pod_between_reservations(self):
        state = _state(_old_booking(100), _open_booking(200, start_utc=THREE_PM))
        state.record_placement(100, "p1", 1)
        state.relink_occupancy("p1", 200, 1)
        assert state.occupancy.get(100) in (None, {})
        assert state.occupancy[200] == {"p1": 1}


# ---------------------------------------------------------------------------
# Regression: adoption closes the preemption race
# ---------------------------------------------------------------------------


class TestAdoptionClosesPreemptionRace:
    def test_pod_no_longer_preempted_after_adoption(self):
        old = _old_booking(100, gpu_count=1)
        new = _open_booking(200, start_utc=THREE_PM + timedelta(minutes=10), gpu_count=1)
        state = _state(old, new)
        boundary = THREE_PM + timedelta(minutes=10)  # new booking's start
        capacity = {GPU_CLASS_LABEL: 1}

        pod = _view("p1", reservation_id=100, reserved_path=True, gpu_count=1)
        state.record_placement(100, "p1", 1)

        # Before adoption, the overstay pod WOULD be preempted for the new booking.
        before = state.plan_boundary_preemption(boundary, capacity, [pod], NOW)
        assert [v.uid for v in before.victims] == ["p1"]

        # Apply the adoption the orchestrator would perform.
        plan = state.plan_pod_adoptions([pod], NOW)
        assert len(plan) == 1
        assert plan[0][2] is True  # was_overstay
        state.relink_occupancy("p1", 200, 1)
        adopted = replace(pod, reservation_id=200, reserved_path=True)

        # After adoption: demand is satisfied by the pod itself → no victims.
        after = state.plan_boundary_preemption(boundary, capacity, [adopted], NOW)
        assert after.victims == []
        assert after.demand_by_class == {}

    def test_abutting_case_needs_no_adoption(self):
        # Documents the invariant the design relies on: when the new window abuts
        # the old (same user/class/gpu_count), chaining already extends the
        # guarantee, so the pod is neither overstay nor a preemption victim and
        # no adoption is planned.
        old = _old_booking(100, gpu_count=1)
        abutting = _open_booking(200, start_utc=THREE_PM, gpu_count=1)
        state = _state(old, abutting)
        boundary = THREE_PM
        capacity = {GPU_CLASS_LABEL: 1}

        pod = _view("p1", reservation_id=100, reserved_path=True, gpu_count=1)
        state.record_placement(100, "p1", 1)

        assert state.plan_pod_adoptions([pod], NOW) == []
        plan = state.plan_boundary_preemption(boundary, capacity, [pod], NOW)
        assert plan.victims == []


# A sanity check that free_capacity accounting used by the regression holds.
def test_free_capacity_zero_when_single_gpu_occupied():
    pod = _view("p1", gpu_count=1, node_resident=True)
    assert free_capacity_by_class({GPU_CLASS_LABEL: 1}, [pod]) == {GPU_CLASS_LABEL: 0}
