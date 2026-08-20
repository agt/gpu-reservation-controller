"""Unit tests for re-stamping reservation facts when a reservation mutates in place.

``annotate_runtime_guarantee`` stamps ``galends/reservation-kind`` / ``-start`` /
``-end`` / ``-gpu-count`` and ``galends/gpu-class-name`` at admission, and every
**re-link** path (adoption, lease-to-booking merge) re-stamps them by calling it
again with the new reservation.  That covers every way a pod's reservation can
change *identity*.

It does not cover the reservation changing *content* underneath the pod: an
on-demand lease whose window is extended in place keeps its id, so nothing
re-links and none of those callers run.  The pod would go on advertising a
``galends/reservation-end`` that has since moved, while ``guaranteed-until`` —
recomputed live from chain-aware state on every queue tick — moved forward.  A
consumer reading both would see them contradict.

Three layers, all without a real cluster:

- ``ControllerState.plan_reservation_facts`` — the pure planner.
- ``get_pod_reservation_facts`` / ``annotate_reservation_facts`` — the reader and
  the write patch, against a capturing fake ``_core_v1``.
- ``main._apply_reservation_facts`` — the diff-and-skip reconcile, whose
  skip-when-unchanged property is what keeps this off the per-tick API budget.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app import k8s_client
from app.controller import PodRuntimeView
from app.k8s_client import (
    GPU_CLASS_NAME,
    RESERVATION_END,
    RESERVATION_GPU_COUNT,
    RESERVATION_KIND,
    RESERVATION_START,
    ReservationFacts,
    ToleratedPodInfo,
    annotate_reservation_facts,
    get_pod_reservation_facts,
    utc_iso,
)

from tests.conftest import GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state
from tests.conftest import reservation

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
START = NOW - timedelta(minutes=30)
END = NOW + timedelta(hours=2)
EXTENDED_END = NOW + timedelta(hours=6)

# The display name the shared ``reservation`` fixture stamps on every row; it is
# what lands in ``galends/gpu-class-name``, as distinct from the label_value used
# for matching.
CLASS_NAME = "H100"


def _res(res_id: int, *, start=START, end=END, gpu_count=1, kind="on_demand"):
    # with_user is forced on: the fixture couples it to kind="booking", but a JIT
    # lease is owned by the user the controller minted it for, and these rows
    # stand in for one.
    return reservation(
        res_id,
        start_utc=start,
        end_utc=end,
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
        user_id=1,
        kind=kind,
        with_user=True,
    )


def _view(uid: str, *, reservation_id: int | None = 1) -> PodRuntimeView:
    return PodRuntimeView(
        uid=uid,
        namespace=USERNAME,
        name=f"pod-{uid}",
        gpu_class=GPU_CLASS_LABEL,
        gpu_count=1,
        reservation_id=reservation_id,
        node_resident=True,
        terminating=False,
    )


# ---------------------------------------------------------------------------
# plan_reservation_facts (pure)
# ---------------------------------------------------------------------------


class TestPlanReservationFacts:
    def test_returns_the_pods_current_reservation_keyed_by_id(self):
        res = _res(1)
        state = _state(res)
        plan = state.plan_reservation_facts([_view("p", reservation_id=1)])
        assert set(plan) == {1}
        assert plan[1].id == res.id

    def test_reflects_an_extension_applied_in_place(self):
        """The whole point: same id, moved window, and the planner sees the new one.

        This is what a push carrying an extended lease produces — ``apply_push_to_active``
        upserts by id, so the row is replaced rather than added alongside.
        """
        state = _state(_res(1, end=EXTENDED_END))
        plan = state.plan_reservation_facts([_view("p", reservation_id=1)])
        assert plan[1].end_utc == EXTENDED_END

    def test_pod_without_reservation_id_is_skipped(self):
        state = _state(_res(1))
        assert state.plan_reservation_facts([_view("p", reservation_id=None)]) == {}

    def test_pod_whose_reservation_is_gone_is_skipped(self):
        """No entry rather than a cleared one — the pod keeps its last-known stamps.

        ``state.reservations`` holds only active rows, so an absent id means
        cancelled or expired.  There is deliberately no clear path: these
        annotations describe what the pod was admitted under, and a reservation
        ending is not new information about that.
        """
        state = _state(_res(1))
        assert state.plan_reservation_facts([_view("p", reservation_id=999)]) == {}

    def test_several_pods_on_one_reservation_yield_one_entry(self):
        state = _state(_res(1))
        plan = state.plan_reservation_facts(
            [_view("a", reservation_id=1), _view("b", reservation_id=1)]
        )
        assert list(plan) == [1]


# ---------------------------------------------------------------------------
# get_pod_reservation_facts (pure reader)
# ---------------------------------------------------------------------------


def _pod_with_annotations(annotations: dict | None):
    return SimpleNamespace(metadata=SimpleNamespace(annotations=annotations))


class TestGetPodReservationFacts:
    def test_returns_all_five_in_reservation_facts_order(self):
        pod = _pod_with_annotations({
            RESERVATION_KIND: "on_demand",
            RESERVATION_START: "2024-01-15T08:30:00Z",
            RESERVATION_END: "2024-01-15T11:00:00Z",
            RESERVATION_GPU_COUNT: "2",
            GPU_CLASS_NAME: CLASS_NAME,
        })
        assert get_pod_reservation_facts(pod) == (
            "on_demand", "2024-01-15T08:30:00Z", "2024-01-15T11:00:00Z", "2", CLASS_NAME,
        )

    def test_absent_returns_all_none(self):
        assert get_pod_reservation_facts(_pod_with_annotations(None)) == (None,) * 5
        assert get_pod_reservation_facts(_pod_with_annotations({})) == (None,) * 5


# ---------------------------------------------------------------------------
# annotate_reservation_facts (write patch)
# ---------------------------------------------------------------------------


class _CapturingCore:
    def __init__(self):
        self.patches: list[tuple[str, str, dict]] = []

    def patch_namespaced_pod(self, name, namespace, body):
        self.patches.append((name, namespace, body))


_FACTS = ReservationFacts(
    kind="on_demand",
    start_utc=START,
    end_utc=EXTENDED_END,
    gpu_count=2,
    gpu_class_name=CLASS_NAME,
)


class TestAnnotateReservationFacts:
    def test_writes_the_five_fact_keys(self, monkeypatch):
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(annotate_reservation_facts("pod-1", USERNAME, _FACTS))

        (name, namespace, body) = core.patches[0]
        assert (name, namespace) == ("pod-1", USERNAME)
        assert body["metadata"]["annotations"] == {
            RESERVATION_KIND: "on_demand",
            RESERVATION_START: utc_iso(START),
            RESERVATION_END: utc_iso(EXTENDED_END),
            RESERVATION_GPU_COUNT: "2",
            GPU_CLASS_NAME: CLASS_NAME,
        }

    def test_does_not_touch_guarantee_or_admitted_at_keys(self, monkeypatch):
        """Those are owned elsewhere and must not be collateral damage.

        ``guaranteed-until`` / ``guarantee-status`` are reconciled from live
        chain-aware state, and ``admitted-at`` is write-once — restarting a
        session-elapsed clock mid-job is exactly what the re-link paths take care
        to avoid.
        """
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(annotate_reservation_facts("pod-1", USERNAME, _FACTS))

        written = set(core.patches[0][2]["metadata"]["annotations"])
        assert "galends/admitted-at" not in written
        assert "galends/guaranteed-until" not in written
        assert "galends/guarantee-status" not in written


# ---------------------------------------------------------------------------
# _apply_reservation_facts (reconcile)
# ---------------------------------------------------------------------------


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://reservation.test")
    monkeypatch.setenv("RESERVATION_API_KEY", "gpures_test")
    import app.main

    return app.main


def _pod(
    uid="1",
    *,
    reservation_id: int | None = 1,
    kind=None,
    start=None,
    end=None,
    gpu_count=None,
    class_name=None,
) -> ToleratedPodInfo:
    return ToleratedPodInfo(
        namespace=USERNAME,
        name=f"pod-{uid}",
        uid=uid,
        gpu_class=GPU_CLASS_LABEL,
        booking_reference=f"res-{reservation_id}",
        reservation_id=reservation_id,
        gpu_count=1,
        phase="Running",
        scheduled_false=False,
        reservation_kind=kind,
        reservation_start=start,
        reservation_end=end,
        reservation_gpu_count=gpu_count,
        gpu_class_name=class_name,
    )


def _stamped(**overrides) -> ToleratedPodInfo:
    """A pod carrying exactly the facts of ``_res(1)`` — i.e. already up to date."""
    base = dict(
        kind="on_demand",
        start=utc_iso(START),
        end=utc_iso(END),
        gpu_count="1",
        class_name=CLASS_NAME,
    )
    base.update(overrides)
    return _pod(**base)


def _patch_writer(monkeypatch, m):
    writes: list[tuple] = []

    async def _annotate(name, namespace, facts):
        writes.append((namespace, name, facts))

    monkeypatch.setattr(m, "annotate_reservation_facts", _annotate)
    return writes


class TestApplyReservationFacts:
    def test_rewrites_the_stamps_when_the_window_moved(self, monkeypatch):
        """The regression this whole path exists for."""
        m = _main_module(monkeypatch)
        writes = _patch_writer(monkeypatch, m)
        state = _state(_res(1, end=EXTENDED_END))
        plan = state.plan_reservation_facts([_view("1", reservation_id=1)])

        asyncio.run(m._apply_reservation_facts([_stamped()], plan))

        assert len(writes) == 1
        assert writes[0][2].end_utc == EXTENDED_END

    def test_skips_a_pod_whose_stamps_already_match(self, monkeypatch):
        """No-op ticks must cost no API call.

        This is the load-bearing half: the reconcile runs on every queue tick
        against every admitted pod, so a comparison that drifted from the writer's
        formatting would re-patch the whole cluster every tick.
        """
        m = _main_module(monkeypatch)
        writes = _patch_writer(monkeypatch, m)
        state = _state(_res(1))
        plan = state.plan_reservation_facts([_view("1", reservation_id=1)])

        asyncio.run(m._apply_reservation_facts([_stamped()], plan))

        assert writes == []

    def test_rewrites_when_gpu_count_changed(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_writer(monkeypatch, m)
        state = _state(_res(1, gpu_count=4))
        plan = state.plan_reservation_facts([_view("1", reservation_id=1)])

        asyncio.run(m._apply_reservation_facts([_stamped()], plan))

        assert len(writes) == 1
        assert writes[0][2].gpu_count == 4

    def test_rewrites_when_kind_changed(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_writer(monkeypatch, m)
        state = _state(_res(1, kind="booking"))
        plan = state.plan_reservation_facts([_view("1", reservation_id=1)])

        asyncio.run(m._apply_reservation_facts([_stamped()], plan))

        assert len(writes) == 1
        assert writes[0][2].kind == "booking"

    def test_stamps_a_pod_carrying_no_facts_at_all(self, monkeypatch):
        """A pod admitted before these keys existed, or whose patch was lost."""
        m = _main_module(monkeypatch)
        writes = _patch_writer(monkeypatch, m)
        state = _state(_res(1))
        plan = state.plan_reservation_facts([_view("1", reservation_id=1)])

        asyncio.run(m._apply_reservation_facts([_pod()], plan))

        assert len(writes) == 1

    def test_pod_absent_from_the_plan_is_left_alone(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_writer(monkeypatch, m)

        asyncio.run(m._apply_reservation_facts([_stamped()], {}))

        assert writes == []

    def test_pod_without_reservation_id_is_left_alone(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_writer(monkeypatch, m)
        state = _state(_res(1))
        plan = state.plan_reservation_facts([_view("1", reservation_id=1)])

        asyncio.run(m._apply_reservation_facts([_pod(reservation_id=None)], plan))

        assert writes == []

    def test_one_failure_does_not_stop_the_other_pods(self, monkeypatch):
        """Best-effort per pod, mirroring _apply_guarantee_status."""
        m = _main_module(monkeypatch)
        seen: list[str] = []

        async def _annotate(name, namespace, facts):
            seen.append(name)
            if name == "pod-a":
                raise RuntimeError("patch rejected")

        monkeypatch.setattr(m, "annotate_reservation_facts", _annotate)
        state = _state(_res(1, end=EXTENDED_END))
        plan = state.plan_reservation_facts([_view("a", reservation_id=1)])

        asyncio.run(m._apply_reservation_facts(
            [_stamped(uid="a"), _stamped(uid="b")], plan
        ))

        assert seen == ["pod-a", "pod-b"]


# ---------------------------------------------------------------------------
# Wiring: the queue tick must actually call the reconcile
# ---------------------------------------------------------------------------


class TestQueueTickWiring:
    """Every unit above passes whether or not the tick calls any of it.

    The reconcile is per-tick housekeeping with no other trigger, so an
    unwired one fails silently and forever — a pod's stamps simply never
    catch up.  These two assert the seam itself.
    """

    def _tick(self, monkeypatch, snapshot, state):
        import app.main as main_module
        from tests.conftest import make_config

        async def _snapshot(*a, **kw):
            return snapshot

        async def _inventory(*a, **kw):
            return {}

        seen: list[tuple] = []

        async def _apply(snap, plan):
            seen.append((snap, plan))

        monkeypatch.setattr(main_module, "snapshot_tolerated_pods", _snapshot)
        monkeypatch.setattr(main_module, "snapshot_node_gpu_inventory", _inventory)
        monkeypatch.setattr(main_module, "_apply_reservation_facts", _apply)
        asyncio.run(main_module._run_queue_tick(state, None, make_config()))
        return seen

    def test_tick_reconciles_facts_from_its_own_snapshot(self, monkeypatch):
        state = _state(_res(1, end=EXTENDED_END))
        pod = _stamped()
        seen = self._tick(monkeypatch, [pod], state)

        assert len(seen) == 1, "the queue tick never reconciled reservation facts"
        snap, plan = seen[0]
        # The tick's own snapshot is threaded through — no second pod LIST.
        assert snap == [pod]
        # And the plan carries the extended window the pod has not yet caught up to.
        assert plan[1].end_utc == EXTENDED_END

    def test_tick_skips_the_reconcile_when_the_snapshot_failed(self, monkeypatch):
        """Fail-safe: never act on a pod set we could not read."""
        import app.main as main_module
        from tests.conftest import make_config

        async def _fail(*a, **kw):
            raise RuntimeError("api down")

        async def _inventory(*a, **kw):
            return {}

        seen: list[tuple] = []

        async def _apply(snap, plan):
            seen.append((snap, plan))

        monkeypatch.setattr(main_module, "snapshot_tolerated_pods", _fail)
        monkeypatch.setattr(main_module, "snapshot_node_gpu_inventory", _inventory)
        monkeypatch.setattr(main_module, "_apply_reservation_facts", _apply)
        asyncio.run(main_module._run_queue_tick(
            _state(_res(1)), None, make_config()
        ))

        assert seen == []
