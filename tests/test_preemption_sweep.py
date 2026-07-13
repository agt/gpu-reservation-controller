"""Tests for main._run_preemption_sweep (the async preemption-loop tick).

Drives the sweep directly via ``asyncio.run`` with ``snapshot_tolerated_pods``,
``snapshot_node_gpu_capacity``, ``read_pod``, ``delete_pod``, and
``emit_preempted_event`` monkeypatched at the ``app.main`` module level — the
same convention ``test_admission.py`` uses for Kubernetes-boundary
coroutines.  No real Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from app.config import Config
from app.k8s_client import ToleratedPodInfo

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state
from tests.conftest import reservation

S = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)  # the slot boundary


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-preemption")
    import app.main as main

    return main


def _config(**overrides) -> Config:
    base = dict(
        reservation_api_url="http://reservations.local",
        reservation_api_key="gpures_test",
        reservation_fetch_interval=300,
        reservation_lookahead_days=7,
        kubeconfig_path=None,
        health_port=8000,
        ondemand_placement_enabled=True,
        noshown_timeout_minutes=15,
        noshown_grace_minutes=30,
        pod_list_tick_interval=300,
        scheduling_gate_name=None,
        inbound_api_token=None,
        preemption_lead_minutes=15,
        preemption_check_interval=60,
    )
    base.update(overrides)
    return Config(**base)


def _pod(uid: str, *, booking_reference: str, reservation_id: int, gpu_count: int = 1,
          phase: str = "Running", scheduled_false: bool = False, deletion_timestamp=None,
          namespace: str = USERNAME) -> ToleratedPodInfo:
    return ToleratedPodInfo(
        namespace=namespace,
        name=f"pod-{uid}",
        uid=uid,
        gpu_class=GPU_CLASS_LABEL,
        booking_reference=booking_reference,
        reservation_id=reservation_id,
        gpu_count=gpu_count,
        phase=phase,
        scheduled_false=scheduled_false,
        deletion_timestamp=deletion_timestamp,
    )


def _patch_snapshots(monkeypatch, m, *, pods, capacity, read_pod_ok=True):
    async def _snapshot_pods(_key, _group_label_key=None):
        return pods

    async def _snapshot_capacity(_key):
        return capacity

    deleted: list[tuple[str, str]] = []
    events: list[tuple[str, str, str]] = []

    async def _read_pod(name, namespace):
        if not read_pod_ok:
            raise RuntimeError("apiserver down")
        return object()

    async def _delete_pod(name, namespace):
        deleted.append((namespace, name))

    async def _emit_preempted(pod, name, namespace, message):
        events.append((namespace, name, message))

    monkeypatch.setattr(m, "snapshot_tolerated_pods", _snapshot_pods)
    monkeypatch.setattr(m, "snapshot_node_gpu_capacity", _snapshot_capacity)
    monkeypatch.setattr(m, "read_pod", _read_pod)
    monkeypatch.setattr(m, "delete_pod", _delete_pod)
    monkeypatch.setattr(m, "emit_preempted_event", _emit_preempted)
    return deleted, events


def _boundary_reservation(res_id: int = 1, *, gpu_count: int = 1) -> "reservation":
    return reservation(
        res_id,
        start_utc=S,
        end_utc=S + timedelta(hours=2),
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username="bob",
        user_id=2,
    )


class TestNoBoundariesInScope:
    def test_no_reservations_skips_both_snapshots(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state()
        config = _config()

        called = []

        async def _boom(_key, _group_label_key=None):
            called.append(_key)
            raise AssertionError("should not be called")

        monkeypatch.setattr(m, "snapshot_tolerated_pods", _boom)
        monkeypatch.setattr(m, "snapshot_node_gpu_capacity", _boom)

        asyncio.run(m._run_preemption_sweep(state, config, now=S - timedelta(minutes=10)))
        assert called == []


class TestSnapshotFailureFailsSafe:
    def test_pod_snapshot_failure_kills_nothing(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation())
        config = _config()

        async def _boom(_key, _group_label_key=None):
            raise RuntimeError("apiserver down")

        deleted = []
        monkeypatch.setattr(m, "snapshot_tolerated_pods", _boom)
        monkeypatch.setattr(m, "delete_pod", lambda *a, **k: deleted.append(a))

        now = S - timedelta(minutes=10)
        asyncio.run(m._run_preemption_sweep(state, config, now=now))
        assert deleted == []
        assert state.preemption_fired == {}  # never reached the fired-marking loop

    def test_capacity_snapshot_failure_kills_nothing(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation())
        config = _config()

        async def _ok_pods(_key, _group_label_key=None):
            return []

        async def _boom(_key):
            raise RuntimeError("apiserver down")

        deleted = []
        monkeypatch.setattr(m, "snapshot_tolerated_pods", _ok_pods)
        monkeypatch.setattr(m, "snapshot_node_gpu_capacity", _boom)
        monkeypatch.setattr(m, "delete_pod", lambda *a, **k: deleted.append(a))

        now = S - timedelta(minutes=10)
        asyncio.run(m._run_preemption_sweep(state, config, now=now))
        assert deleted == []
        assert state.preemption_fired == {}


class TestPhaseATwoPhaseKill:
    def test_within_guarantee_protected_in_phase_a_then_killed_in_phase_b(self, monkeypatch):
        """The defining two-phase behavior: a pod whose own window ends exactly
        at the boundary is protected during phase A (its guarantee has not yet
        elapsed) and only becomes an eligible victim once phase B evaluates at
        the boundary itself."""
        m = _main_module(monkeypatch)

        victim_res = reservation(
            2,
            start_utc=S - timedelta(hours=2),
            end_utc=S,
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
            username="alice",
            user_id=1,
        )
        boundary_res = _boundary_reservation(1, gpu_count=1)
        state = _state(victim_res, boundary_res)
        config = _config(preemption_lead_minutes=15)

        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )

        # Phase A: 10 minutes before the boundary, still within lead.
        now_a = S - timedelta(minutes=10)
        asyncio.run(m._run_preemption_sweep(state, config, now=now_a))
        assert deleted == []
        assert state.preemption_fired[S] == {"A"}

        # Phase B: at the boundary itself, the victim's guarantee has elapsed.
        asyncio.run(m._run_preemption_sweep(state, config, now=S))
        assert deleted == [(USERNAME, "pod-v1")]
        assert state.preemption_fired[S] == {"A", "B"}
        assert len(events) == 1
        assert events[0][:2] == (USERNAME, "pod-v1")

    def test_fired_phase_is_not_re_evaluated(self, monkeypatch):
        """A second sweep within the same phase window must not re-plan (and
        potentially re-select) once that phase has already fired."""
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))
        config = _config(preemption_lead_minutes=15)

        overstayer_res = reservation(
            2,
            start_utc=S - timedelta(hours=4),
            end_utc=S - timedelta(hours=2),  # already well past guarantee
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
            username="alice",
            user_id=1,
        )
        state.reservations.append(overstayer_res)
        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )

        now_a = S - timedelta(minutes=10)
        asyncio.run(m._run_preemption_sweep(state, config, now=now_a))
        assert len(deleted) == 1

        # A second sweep in the same phase window (pod already gone from the
        # snapshot, mirroring reality) must not attempt anything further.
        deleted2, _events2 = _patch_snapshots(
            monkeypatch, m, pods=[], capacity={GPU_CLASS_LABEL: 1}
        )
        asyncio.run(m._run_preemption_sweep(state, config, now=now_a + timedelta(seconds=1)))
        assert deleted2 == []


class TestRestartIdempotence:
    def test_already_terminating_victim_not_redeleted(self, monkeypatch):
        """A fresh ControllerState (simulating a restart, marks lost) whose
        only overstayer is already Terminating must not be selected again —
        it is excluded from both usage (already "freed") and eligibility."""
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))  # no marks (fresh)
        config = _config(preemption_lead_minutes=15)

        pod = _pod(
            "v1",
            booking_reference="res-2",
            reservation_id=2,
            gpu_count=1,
            deletion_timestamp=S - timedelta(minutes=1),
        )
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )

        asyncio.run(m._run_preemption_sweep(state, config, now=S))
        assert deleted == []


class TestMultipleBoundariesOneSweep:
    def test_no_double_selection_across_boundaries(self, monkeypatch):
        """Two boundaries evaluated in the same sweep (lead wider than slot
        spacing) must not both select the same victim's GPUs."""
        m = _main_module(monkeypatch)
        s1 = S
        s2 = S + timedelta(minutes=10)
        res1 = reservation(
            1, start_utc=s1, end_utc=s1 + timedelta(hours=1), gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL, username="bob", user_id=2,
        )
        res2 = reservation(
            2, start_utc=s2, end_utc=s2 + timedelta(hours=1), gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL, username="carol", user_id=3,
        )
        overstayer_res = reservation(
            3,
            start_utc=S - timedelta(hours=4),
            end_utc=S - timedelta(hours=2),
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
            username="alice",
            user_id=1,
        )
        state = _state(res1, res2, overstayer_res)
        config = _config(preemption_lead_minutes=15)

        pod = _pod("v1", booking_reference="res-3", reservation_id=3, gpu_count=1)
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )

        asyncio.run(m._run_preemption_sweep(state, config, now=S))
        # Only one victim existed; it can satisfy at most one boundary's
        # shortfall, and must be deleted exactly once.
        assert deleted == [(USERNAME, "pod-v1")]

    def test_release_pod_called_for_victim(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))
        config = _config(preemption_lead_minutes=15)

        overstayer_res = reservation(
            2,
            start_utc=S - timedelta(hours=4),
            end_utc=S - timedelta(hours=2),
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
            username="alice",
            user_id=1,
        )
        state.reservations.append(overstayer_res)
        state.record_placement(2, "v1", 1)
        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        _patch_snapshots(monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1})

        asyncio.run(m._run_preemption_sweep(state, config, now=S))
        assert "v1" not in state.occupancy.get(2, {})
