"""Unit tests for the termination-warning feature.

Covers three layers, all without a real cluster:

- ``ControllerState.plan_termination_warnings`` — the pure planner that
  identifies pods at risk of demand-driven preemption at an upcoming boundary
  (boundary-relative eligibility, full eligible pool, soonest-boundary risk).
- ``get_pod_termination_warning`` — pure annotation reader (SimpleNamespace pods).
- ``annotate_termination_warning`` / ``clear_termination_warning`` — the write /
  retract patches, exercised against a capturing fake ``_core_v1``.

The async sweep wiring (``main._run_preemption_sweep``) is covered in
``test_preemption_sweep.py``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app import k8s_client
from app.controller import PodRuntimeView
from app.k8s_client import (
    TERMINATION_WARNING_AT,
    TERMINATION_WARNING_MESSAGE,
    TERMINATION_WARNING_RISK,
    annotate_termination_warning,
    clear_termination_warning,
    get_pod_termination_warning,
)

from tests.conftest import GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state
from tests.conftest import reservation

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
BOUNDARY = NOW + timedelta(minutes=10)
BOUNDARY_ISO = "2024-01-15T09:10:00Z"


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


def _booking(res_id: int, boundary: datetime, *, gpu_count: int = 1,
             username: str = "bob", user_id: int = 2):
    return reservation(
        res_id,
        start_utc=boundary,
        end_utc=boundary + timedelta(hours=2),
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=username,
        user_id=user_id,
    )


# ---------------------------------------------------------------------------
# plan_termination_warnings
# ---------------------------------------------------------------------------


class TestPlanTerminationWarnings:
    def test_full_pool_warned_even_when_pool_exceeds_shortfall(self):
        # demand 1, free 0, pool of 2 GPUs → shortfall 1 but BOTH pods warned.
        state = _state(_booking(1, BOUNDARY, gpu_count=1))
        pods = [
            _view("v0", reservation_id=100),  # absent reservation → past guarantee
            _view("v1", reservation_id=101),
        ]
        warn = state.plan_termination_warnings([BOUNDARY], {GPU_CLASS_LABEL: 2}, pods, NOW)
        assert set(warn) == {"v0", "v1"}
        assert warn["v0"].terminate_at == BOUNDARY
        assert warn["v0"].risk == 0.5   # shortfall 1 / pool 2
        assert warn["v1"].risk == 0.5

    def test_soonest_boundary_supplies_timestamp_and_risk(self):
        b2 = BOUNDARY + timedelta(minutes=30)
        # B1 demand 2 → risk 1.0; B2 demand 1 → risk 0.5.  The soonest (B1) wins.
        state = _state(
            _booking(1, BOUNDARY, gpu_count=2),
            _booking(2, b2, gpu_count=1),
        )
        pods = [_view("v0", reservation_id=100), _view("v1", reservation_id=101)]
        # Pass boundaries out of order to prove the planner sorts them.
        warn = state.plan_termination_warnings(
            [b2, BOUNDARY], {GPU_CLASS_LABEL: 2}, pods, NOW
        )
        assert warn["v0"].terminate_at == BOUNDARY   # not b2
        assert warn["v0"].risk == 1.0                # B1's value, not B2's 0.5

    def test_boundary_relative_eligibility_flags_soon_to_expire_pod(self):
        # A pod within its guarantee *now* whose guarantee ends AT the boundary:
        # boundary-relative eligibility flags it, though now-relative planning
        # (plan_boundary_candidates) would not.
        holder = reservation(
            2,
            start_utc=NOW - timedelta(minutes=30),
            end_utc=BOUNDARY,  # guarantee ends exactly at the boundary
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
            username=USERNAME,
            user_id=1,
        )
        state = _state(holder, _booking(1, BOUNDARY, gpu_count=1))
        pod = _view("p", reservation_id=2)

        warn = state.plan_termination_warnings([BOUNDARY], {GPU_CLASS_LABEL: 1}, [pod], NOW)
        assert set(warn) == {"p"}
        assert warn["p"].risk == 1.0

        # Contrast: the sweep's own now-relative candidate planning skips it.
        need = state.plan_boundary_candidates(BOUNDARY, {GPU_CLASS_LABEL: 1}, [pod], NOW)
        assert need.candidates_by_class.get(GPU_CLASS_LABEL, []) == []

    def test_no_shortfall_no_warning(self):
        state = _state(_booking(1, BOUNDARY, gpu_count=1))
        pods = [_view("v0", reservation_id=100)]
        warn = state.plan_termination_warnings([BOUNDARY], {GPU_CLASS_LABEL: 4}, pods, NOW)
        assert warn == {}

    def test_shortfall_but_empty_pool_no_warning(self):
        # The only pod is still within guarantee at the boundary → pool empty.
        holder = reservation(
            2,
            start_utc=NOW - timedelta(minutes=30),
            end_utc=BOUNDARY + timedelta(hours=1),  # guarantee outlasts the boundary
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
            username=USERNAME,
            user_id=1,
        )
        state = _state(holder, _booking(1, BOUNDARY, gpu_count=1))
        pod = _view("p", reservation_id=2)
        warn = state.plan_termination_warnings([BOUNDARY], {GPU_CLASS_LABEL: 1}, [pod], NOW)
        assert warn == {}

    def test_risk_clamped_to_one(self):
        # demand 3, pool of a single 1-GPU pod → 3/1 clamps to 1.0.
        state = _state(_booking(1, BOUNDARY, gpu_count=3))
        pods = [_view("v0", reservation_id=100)]
        warn = state.plan_termination_warnings([BOUNDARY], {GPU_CLASS_LABEL: 1}, pods, NOW)
        assert warn["v0"].risk == 1.0


# ---------------------------------------------------------------------------
# get_pod_termination_warning (pure reader)
# ---------------------------------------------------------------------------


def _pod_with_annotations(annotations: dict | None):
    return SimpleNamespace(metadata=SimpleNamespace(annotations=annotations))


class TestGetPodTerminationWarning:
    def test_both_present(self):
        pod = _pod_with_annotations(
            {TERMINATION_WARNING_AT: BOUNDARY_ISO, TERMINATION_WARNING_RISK: "0.50"}
        )
        assert get_pod_termination_warning(pod) == (BOUNDARY_ISO, "0.50")

    def test_absent_returns_none_pair(self):
        assert get_pod_termination_warning(_pod_with_annotations(None)) == (None, None)
        assert get_pod_termination_warning(_pod_with_annotations({})) == (None, None)

    def test_partial(self):
        pod = _pod_with_annotations({TERMINATION_WARNING_AT: BOUNDARY_ISO})
        assert get_pod_termination_warning(pod) == (BOUNDARY_ISO, None)


# ---------------------------------------------------------------------------
# annotate_termination_warning / clear_termination_warning (patch layer)
# ---------------------------------------------------------------------------


class _CapturingCore:
    def __init__(self):
        self.patches: list[tuple[str, str, dict]] = []

    def patch_namespaced_pod(self, name, namespace, body):
        self.patches.append((name, namespace, body))
        return SimpleNamespace()


class TestAnnotateClearTerminationWarning:
    def test_annotate_writes_three_keys(self, monkeypatch):
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(
            annotate_termination_warning("pod-x", "alice", BOUNDARY, "0.50", "heads up")
        )
        assert len(core.patches) == 1
        name, namespace, body = core.patches[0]
        assert (name, namespace) == ("pod-x", "alice")
        annotations = body["metadata"]["annotations"]
        assert annotations[TERMINATION_WARNING_AT] == BOUNDARY_ISO
        assert annotations[TERMINATION_WARNING_RISK] == "0.50"
        assert annotations[TERMINATION_WARNING_MESSAGE] == "heads up"

    def test_clear_nulls_three_keys(self, monkeypatch):
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(clear_termination_warning("pod-x", "alice"))
        assert len(core.patches) == 1
        _name, _ns, body = core.patches[0]
        annotations = body["metadata"]["annotations"]
        assert annotations == {
            TERMINATION_WARNING_AT: None,
            TERMINATION_WARNING_RISK: None,
            TERMINATION_WARNING_MESSAGE: None,
        }
