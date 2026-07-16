"""Tests for the hourly app-side vs physical GPU capacity audit.

Three layers:

- ``controller.reconcile_capacity`` — the pure comparison helper.
- ``main._run_capacity_audit`` — the async loop tick, driven via
  ``asyncio.run`` with ``snapshot_node_gpu_capacity`` monkeypatched at the
  ``app.main`` module level (the convention ``test_preemption_sweep.py`` uses).
- The per-class on-demand pause gate (guard 4) in
  ``main._preflight_ondemand_candidate``, exercised through
  ``main._try_request_lease``.

No real Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

from app.controller import ControllerState, OnDemandCandidate, reconcile_capacity

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, OTHER_CLASS_LABEL, USERNAME


# ---------------------------------------------------------------------------
# reconcile_capacity — pure helper
# ---------------------------------------------------------------------------


class TestReconcileCapacity:
    def test_equal_maps_no_diff_no_overcommit(self):
        diffs, over = reconcile_capacity({"h100": 8}, {"h100": 8})
        assert diffs == []
        assert over == set()

    def test_app_over_physical_is_diff_and_overcommit(self):
        diffs, over = reconcile_capacity({"h100": 16}, {"h100": 8})
        assert len(diffs) == 1
        d = diffs[0]
        assert (d.label, d.app_side, d.physical) == ("h100", 16, 8)
        assert d.overcommitted is True
        assert over == {"h100"}

    def test_app_under_physical_is_diff_but_not_overcommit(self):
        diffs, over = reconcile_capacity({"h100": 4}, {"h100": 8})
        assert len(diffs) == 1
        assert diffs[0].overcommitted is False
        assert over == set()

    def test_class_missing_from_physical_treated_as_zero_overcommit(self):
        # App knows the class but no node carries its taint → physical 0.
        diffs, over = reconcile_capacity({"h100": 8}, {})
        assert diffs == [("h100", 8, 0)]
        assert over == {"h100"}

    def test_class_unknown_app_side_never_overcommit(self):
        # Class present only physically (app-side total_gpus unknown): it can
        # surface as a diff but must never be flagged over-committed.
        diffs, over = reconcile_capacity({}, {"h100": 8})
        assert diffs == [("h100", 0, 8)]
        assert over == set()

    def test_mixed_classes(self):
        app_side = {"h100": 16, "a100": 4, "v100": 2}
        physical = {"h100": 8, "a100": 4, "t4": 5}
        diffs, over = reconcile_capacity(app_side, physical)
        by_label = {d.label: d for d in diffs}
        # a100 matches → no diff; h100 over; v100 over (physical 0); t4 unknown app-side
        assert set(by_label) == {"h100", "v100", "t4"}
        assert over == {"h100", "v100"}
        assert by_label["t4"].overcommitted is False


# ---------------------------------------------------------------------------
# main._run_capacity_audit — async tick
# ---------------------------------------------------------------------------


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-capacity")
    import app.main as main

    return main


def _patch_capacity(monkeypatch, m, capacity, *, fail=False):
    async def _snapshot(_key):
        if fail:
            raise RuntimeError("apiserver down")
        return capacity

    monkeypatch.setattr(m, "snapshot_node_gpu_capacity", _snapshot)


class TestRunCapacityAudit:
    def test_overcommit_sets_pause_and_warns(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 8})
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))

        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}
        assert any(
            "Capacity mismatch" in r.message and r.levelno == logging.WARNING
            for r in caplog.records
        )

    def test_matching_capacity_no_pause_no_warn(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 8})
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 8}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))

        assert state.overcommitted_gpu_classes == set()
        assert not any("Capacity mismatch" in r.message for r in caplog.records)

    def test_under_provisioned_warns_but_does_not_pause(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 8})
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 4}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))

        assert state.overcommitted_gpu_classes == set()
        assert any("Capacity mismatch" in r.message for r in caplog.records)

    def test_recovery_clears_pause(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}

        # First audit: physical short → class paused.
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 8})
        asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))
        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}

        # Nodes added: physical catches up → class clears automatically.
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 16})
        asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))
        assert state.overcommitted_gpu_classes == set()

    def test_snapshot_failure_leaves_pause_set_untouched(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}
        # Pre-existing pause that a transient snapshot failure must not lift.
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        _patch_capacity(monkeypatch, m, {}, fail=True)

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))

        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}
        assert any(
            "failed to snapshot node GPU capacity" in r.message for r in caplog.records
        )

    def test_only_affected_class_paused(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16, OTHER_CLASS_LABEL: 4}
        _patch_capacity(
            monkeypatch, m, {GPU_CLASS_LABEL: 8, OTHER_CLASS_LABEL: 4}
        )
        asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))
        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}


# ---------------------------------------------------------------------------
# Guard 4 — per-class on-demand pause in _preflight_ondemand_candidate
# ---------------------------------------------------------------------------


def _pod(*, uid="uid-1"):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-1", namespace=USERNAME,
            annotations=None, labels={"gpu-class": GPU_CLASS_LABEL},
        ),
        status=SimpleNamespace(
            phase="Pending",
            conditions=[
                SimpleNamespace(
                    type="PodScheduled", status="False", reason="Unschedulable",
                    message="0/10 nodes are available: 5 Insufficient nvidia.com/gpu.",
                )
            ],
        ),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


def _candidate(uid="uid-1"):
    return OnDemandCandidate(
        pod_uid=uid, pod_name="pod-1", pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=1,
        min_runtime_seconds=600,
        pod_created_at=datetime.now(timezone.utc),
        next_attempt_at=datetime.now(timezone.utc),
        group_label=None,
        usage_group="course-a",
    )


class _NoGrantClient:
    """Fails the test if the grant path is ever reached."""

    def __init__(self):
        self.create_requests: list = []

    async def create_ondemand_reservation(self, req):  # pragma: no cover
        self.create_requests.append(req)
        raise AssertionError("grant must not be attempted while class is paused")


def test_overcommitted_class_pauses_ondemand_admission(monkeypatch):
    m = _main_module(monkeypatch)
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}

    candidate = _candidate("uid-1")
    before = candidate.next_attempt_at

    async def fake_read_pod(name, namespace):
        return _pod()

    monkeypatch.setattr(m, "read_pod", fake_read_pod)

    client = _NoGrantClient()
    config = SimpleNamespace(
        ondemand_horizon_minutes=30,
        ondemand_lease_buffer_minutes=10,
        scheduling_gate_name=None,
    )
    result = asyncio.run(
        m._try_request_lease(state, client, config, "uid-1", candidate)
    )

    # Paused: not granted, cooled down for a short retry, no lease requested.
    assert result is False
    assert candidate.next_attempt_at > before
    assert client.create_requests == []
    assert state.reservations == []
