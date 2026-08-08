"""Unit tests for the live guarantee-status annotations.

Covers three layers, all without a real cluster:

- ``ControllerState.plan_guarantee_status`` — the pure planner that maps each
  admitted pod to ``guaranteed`` (+ live end) or ``overstay`` from its live
  guarantee.
- ``get_pod_guarantee_status`` / ``annotate_guarantee_status`` /
  ``annotate_runtime_guarantee`` — the reader and write patches, against a
  capturing fake ``_core_v1``.
- ``main._apply_guarantee_status`` — the diff-and-skip reconcile.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app import k8s_client
from app.controller import GuaranteeStatus, PodRuntimeView
from app.k8s_client import (
    GUARANTEE_STATUS,
    ToleratedPodInfo,
    annotate_guarantee_status,
    annotate_runtime_guarantee,
    get_pod_guarantee_status,
)

from tests.conftest import GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state
from tests.conftest import reservation

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
FUTURE = NOW + timedelta(hours=2)
FUTURE_ISO = "2024-01-15T11:00:00Z"


def _view(uid: str, *, reservation_id: int | None = None) -> PodRuntimeView:
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


def _res(res_id: int, *, start: datetime, end: datetime):
    return reservation(
        res_id,
        start_utc=start,
        end_utc=end,
        gpu_count=1,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
        user_id=1,
    )


# ---------------------------------------------------------------------------
# plan_guarantee_status (pure)
# ---------------------------------------------------------------------------


class TestPlanGuaranteeStatus:
    def test_in_guarantee_pod_reports_guaranteed_with_future_end(self):
        state = _state(_res(1, start=NOW - timedelta(minutes=30), end=FUTURE))
        plan = state.plan_guarantee_status([_view("p", reservation_id=1)], NOW)
        assert set(plan) == {"p"}
        assert plan["p"].status == "guaranteed"
        assert plan["p"].guaranteed_until == FUTURE

    def test_past_window_reports_overstay_with_none(self):
        # Reservation still in the set but its window has fully passed.
        state = _state(
            _res(2, start=NOW - timedelta(hours=3), end=NOW - timedelta(hours=1))
        )
        plan = state.plan_guarantee_status([_view("p", reservation_id=2)], NOW)
        assert plan["p"].status == "overstay"
        assert plan["p"].guaranteed_until is None

    def test_absent_reservation_reports_overstay(self):
        # reservation_id resolves to nothing (window over / cancelled) → overstay.
        state = _state(_res(1, start=NOW - timedelta(minutes=30), end=FUTURE))
        plan = state.plan_guarantee_status([_view("p", reservation_id=999)], NOW)
        assert plan["p"].status == "overstay"
        assert plan["p"].guaranteed_until is None

    def test_pod_without_reservation_id_is_skipped(self):
        state = _state(_res(1, start=NOW - timedelta(minutes=30), end=FUTURE))
        plan = state.plan_guarantee_status([_view("p", reservation_id=None)], NOW)
        assert plan == {}

    def test_guarantee_grows_across_back_to_back_chain(self):
        # A follow-on abutting window extends the live guarantee end.
        first = _res(1, start=NOW - timedelta(minutes=30), end=FUTURE)
        second = _res(2, start=FUTURE, end=FUTURE + timedelta(hours=2))
        state = _state(first, second)
        plan = state.plan_guarantee_status([_view("p", reservation_id=1)], NOW)
        assert plan["p"].status == "guaranteed"
        assert plan["p"].guaranteed_until == FUTURE + timedelta(hours=2)


# ---------------------------------------------------------------------------
# get_pod_guarantee_status (pure reader)
# ---------------------------------------------------------------------------


def _pod_with_annotations(annotations: dict | None):
    return SimpleNamespace(metadata=SimpleNamespace(annotations=annotations))


class TestGetPodGuaranteeStatus:
    def test_both_present(self):
        pod = _pod_with_annotations(
            {GUARANTEE_STATUS: "guaranteed", "galends/guaranteed-until": FUTURE_ISO}
        )
        assert get_pod_guarantee_status(pod) == ("guaranteed", FUTURE_ISO)

    def test_absent_returns_none_pair(self):
        assert get_pod_guarantee_status(_pod_with_annotations(None)) == (None, None)
        assert get_pod_guarantee_status(_pod_with_annotations({})) == (None, None)


# ---------------------------------------------------------------------------
# write patches (against a capturing fake _core_v1)
# ---------------------------------------------------------------------------


class _CapturingCore:
    def __init__(self):
        self.patches: list[tuple[str, str, dict]] = []

    def patch_namespaced_pod(self, name, namespace, body):
        self.patches.append((name, namespace, body))
        return SimpleNamespace()


class TestAnnotateGuaranteeStatus:
    def test_runtime_guarantee_also_stamps_guaranteed_status(self, monkeypatch):
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(annotate_runtime_guarantee("pod-x", USERNAME, 7200, FUTURE))
        _name, _ns, body = core.patches[0]
        annotations = body["metadata"]["annotations"]
        assert annotations["galends/pod-runtime-limit-seconds"] == "7200"
        assert annotations["galends/guaranteed-until"] == FUTURE_ISO
        assert annotations[GUARANTEE_STATUS] == "guaranteed"

    def test_guaranteed_writes_status_and_refreshes_until(self, monkeypatch):
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(annotate_guarantee_status("pod-x", USERNAME, "guaranteed", FUTURE))
        _name, _ns, body = core.patches[0]
        annotations = body["metadata"]["annotations"]
        assert annotations == {
            GUARANTEE_STATUS: "guaranteed",
            "galends/guaranteed-until": FUTURE_ISO,
        }

    def test_overstay_writes_status_only(self, monkeypatch):
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(annotate_guarantee_status("pod-x", USERNAME, "overstay", None))
        _name, _ns, body = core.patches[0]
        annotations = body["metadata"]["annotations"]
        # No guaranteed-until key — the pod's now-past value is left frozen.
        assert annotations == {GUARANTEE_STATUS: "overstay"}


# ---------------------------------------------------------------------------
# _apply_guarantee_status (reconcile)
# ---------------------------------------------------------------------------


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://reservation.test")
    monkeypatch.setenv("RESERVATION_API_KEY", "gpures_test")
    import app.main

    return app.main


def _pod(uid, *, guarantee_status=None, guaranteed_until=None) -> ToleratedPodInfo:
    return ToleratedPodInfo(
        namespace=USERNAME,
        name=f"pod-{uid}",
        uid=uid,
        gpu_class=GPU_CLASS_LABEL,
        booking_reference=f"res-{uid}",
        reservation_id=1,
        gpu_count=1,
        phase="Running",
        scheduled_false=False,
        guarantee_status=guarantee_status,
        guaranteed_until=guaranteed_until,
    )


def _patch_status_writer(monkeypatch, m):
    writes: list[tuple] = []

    async def _annotate(name, namespace, status, guaranteed_until):
        writes.append((namespace, name, status, guaranteed_until))

    monkeypatch.setattr(m, "annotate_guarantee_status", _annotate)
    return writes


class TestApplyGuaranteeStatus:
    def test_flips_guaranteed_to_overstay(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_status_writer(monkeypatch, m)
        snapshot = [_pod("1", guarantee_status="guaranteed", guaranteed_until=FUTURE_ISO)]
        plan = {"1": GuaranteeStatus(_view("1", reservation_id=1), "overstay", None)}
        asyncio.run(m._apply_guarantee_status(snapshot, plan))
        assert writes == [(USERNAME, "pod-1", "overstay", None)]

    def test_skips_unchanged_overstay(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_status_writer(monkeypatch, m)
        snapshot = [_pod("1", guarantee_status="overstay")]
        plan = {"1": GuaranteeStatus(_view("1", reservation_id=1), "overstay", None)}
        asyncio.run(m._apply_guarantee_status(snapshot, plan))
        assert writes == []

    def test_skips_unchanged_guaranteed(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_status_writer(monkeypatch, m)
        snapshot = [
            _pod("1", guarantee_status="guaranteed", guaranteed_until=FUTURE_ISO)
        ]
        plan = {"1": GuaranteeStatus(_view("1", reservation_id=1), "guaranteed", FUTURE)}
        asyncio.run(m._apply_guarantee_status(snapshot, plan))
        assert writes == []

    def test_writes_refreshed_end_when_guarantee_grows(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_status_writer(monkeypatch, m)
        grown = FUTURE + timedelta(hours=2)
        snapshot = [
            _pod("1", guarantee_status="guaranteed", guaranteed_until=FUTURE_ISO)
        ]
        plan = {"1": GuaranteeStatus(_view("1", reservation_id=1), "guaranteed", grown)}
        asyncio.run(m._apply_guarantee_status(snapshot, plan))
        assert writes == [(USERNAME, "pod-1", "guaranteed", grown)]

    def test_pod_absent_from_plan_is_untouched(self, monkeypatch):
        m = _main_module(monkeypatch)
        writes = _patch_status_writer(monkeypatch, m)
        snapshot = [_pod("1", guarantee_status="guaranteed", guaranteed_until=FUTURE_ISO)]
        asyncio.run(m._apply_guarantee_status(snapshot, {}))
        assert writes == []
