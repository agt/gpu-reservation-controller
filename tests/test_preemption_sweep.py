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
        http_port=8000,
        ondemand_lease_enabled=True,
        noshow_timeout_minutes=15,
        noshow_grace_minutes=30,
        queue_processor_interval=300,
        scheduling_gate_name=None,
        inbound_api_token=None,
        preemption_lead_minutes=15,
        preemption_check_interval=60,
    )
    base.update(overrides)
    return Config(**base)


def _pod(uid: str, *, booking_reference: str, reservation_id: int, gpu_count: int = 1,
          phase: str = "Running", scheduled_false: bool = False, deletion_timestamp=None,
          namespace: str = USERNAME, termination_warning_at=None,
          termination_warning_risk=None) -> ToleratedPodInfo:
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
        termination_warning_at=termination_warning_at,
        termination_warning_risk=termination_warning_risk,
    )


def _patch_warnings(monkeypatch, m):
    """Capture ``annotate_termination_warning`` / ``clear_termination_warning``."""
    writes: list[tuple] = []
    clears: list[tuple[str, str]] = []

    async def _annotate(name, namespace, terminate_at, risk, message):
        writes.append((namespace, name, terminate_at, risk, message))

    async def _clear(name, namespace):
        clears.append((namespace, name))

    monkeypatch.setattr(m, "annotate_termination_warning", _annotate)
    monkeypatch.setattr(m, "clear_termination_warning", _clear)
    return writes, clears


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


class _FakeSelectClient:
    """Stand-in ReservationClient exposing only ``select_preemption_victims``."""

    def __init__(self, response):
        self._response = response
        self.requests: list = []

    async def select_preemption_victims(self, req):
        self.requests.append(req)
        return self._response


def _overstayer(res_id, username, user_id):
    return reservation(
        res_id,
        start_utc=S - timedelta(hours=4),
        end_utc=S - timedelta(hours=2),  # well past guarantee
        gpu_count=1,
        gpu_class_label=GPU_CLASS_LABEL,
        username=username,
        user_id=user_id,
    )


class TestDelegatedVictimSelection:
    def test_app_selection_is_honoured(self, monkeypatch):
        """The app is asked to choose; the pod it names is the one killed, and
        the request carries the shortfall and the full candidate pool."""
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))
        state.reservations.append(_overstayer(2, "alice", 1))
        config = _config(preemption_lead_minutes=15)

        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        client = _FakeSelectClient(["v1"])

        asyncio.run(m._run_preemption_sweep(state, config, client, now=S))
        assert deleted == [(USERNAME, "pod-v1")]
        assert len(client.requests) == 1
        req = client.requests[0]
        assert req.needed_by_class == {GPU_CLASS_LABEL: 1}
        assert [c.pod_uid for c in req.candidates] == ["v1"]
        assert req.candidates[0].reservation_id == 2

    def test_app_chooses_among_multiple_candidates(self, monkeypatch):
        """With two eligible overstayers and a 1-GPU shortfall, exactly the
        app's named victim is killed — proving selection is the app's, not the
        controller's random pick."""
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))
        state.reservations.append(_overstayer(2, "alice", 1))
        state.reservations.append(_overstayer(3, "dave", 4))
        config = _config(preemption_lead_minutes=15)

        pods = [
            _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1),
            _pod("v2", booking_reference="res-3", reservation_id=3, gpu_count=1),
        ]
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=pods, capacity={GPU_CLASS_LABEL: 2}
        )
        client = _FakeSelectClient(["v2"])

        asyncio.run(m._run_preemption_sweep(state, config, client, now=S))
        assert deleted == [(USERNAME, "pod-v2")]

    def test_empty_selection_is_respected(self, monkeypatch):
        """An empty app response means 'spare everyone' — nothing is killed even
        though an eligible candidate exists (no fallback to local)."""
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))
        state.reservations.append(_overstayer(2, "alice", 1))
        config = _config(preemption_lead_minutes=15)

        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        client = _FakeSelectClient([])

        asyncio.run(m._run_preemption_sweep(state, config, client, now=S))
        assert deleted == []

    def test_none_response_falls_back_to_local(self, monkeypatch):
        """A failed app call (None) falls back to local random selection so
        preemption still happens when the app is unreachable."""
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))
        state.reservations.append(_overstayer(2, "alice", 1))
        config = _config(preemption_lead_minutes=15)

        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        client = _FakeSelectClient(None)

        asyncio.run(m._run_preemption_sweep(state, config, client, now=S))
        assert deleted == [(USERNAME, "pod-v1")]

    def test_unknown_uid_is_ignored(self, monkeypatch):
        """A response naming a pod the controller never offered kills nothing —
        the controller only ever deletes candidates it deemed preemptable."""
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))
        state.reservations.append(_overstayer(2, "alice", 1))
        config = _config(preemption_lead_minutes=15)

        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        client = _FakeSelectClient(["ghost-pod"])

        asyncio.run(m._run_preemption_sweep(state, config, client, now=S))
        assert deleted == []

    def test_delegation_disabled_skips_app(self, monkeypatch):
        """With PREEMPTION_DELEGATE_SELECTION off, the app is never called and
        the local random selection kills the overstayer."""
        m = _main_module(monkeypatch)
        state = _state(_boundary_reservation(1, gpu_count=1))
        state.reservations.append(_overstayer(2, "alice", 1))
        config = _config(preemption_lead_minutes=15, preemption_delegate_selection=False)

        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        client = _FakeSelectClient(["v1"])

        asyncio.run(m._run_preemption_sweep(state, config, client, now=S))
        assert deleted == [(USERNAME, "pod-v1")]
        assert client.requests == []


# ---------------------------------------------------------------------------
# Termination-warning annotations (post-Phase-A reconcile)
# ---------------------------------------------------------------------------

S_ISO = "2024-01-15T10:00:00Z"  # S.strftime("%Y-%m-%dT%H:%M:%SZ")


def _victim_ending_at_boundary():
    """Alice's holder whose runtime guarantee ends exactly at the boundary S."""
    return reservation(
        2,
        start_utc=S - timedelta(hours=2),
        end_utc=S,
        gpu_count=1,
        gpu_class_label=GPU_CLASS_LABEL,
        username="alice",
        user_id=1,
    )


class TestTerminationWarnings:
    def test_at_risk_survivor_warned_in_phase_a(self, monkeypatch):
        """A pod protected in phase A (still within guarantee) but occupying the
        GPU an imminent booking needs is stamped with a warning."""
        m = _main_module(monkeypatch)
        state = _state(_victim_ending_at_boundary(), _boundary_reservation(1, gpu_count=1))
        config = _config(preemption_lead_minutes=15)

        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        writes, clears = _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, config, now=S - timedelta(minutes=10)))

        assert deleted == []          # protected during phase A
        assert clears == []
        assert len(writes) == 1
        namespace, name, at, risk, _msg = writes[0]
        assert (namespace, name) == (USERNAME, "pod-v1")
        assert at == S
        assert risk == "1.00"

    def test_preempted_pod_is_not_warned(self, monkeypatch):
        """At phase B the pod is killed, not warned (it is in the doomed set)."""
        m = _main_module(monkeypatch)
        state = _state(_victim_ending_at_boundary(), _boundary_reservation(1, gpu_count=1))
        config = _config(preemption_lead_minutes=15)

        pod = _pod("v1", booking_reference="res-2", reservation_id=2, gpu_count=1)
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        writes, clears = _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, config, now=S))

        assert deleted == [(USERNAME, "pod-v1")]
        assert writes == []
        assert clears == []

    def test_stale_warning_cleared_when_no_longer_at_risk(self, monkeypatch):
        """A pod carrying a warning that no longer applies (ample capacity) has
        its annotations retracted."""
        m = _main_module(monkeypatch)
        state = _state(_overstayer(2, "alice", 1), _boundary_reservation(1, gpu_count=1))
        config = _config(preemption_lead_minutes=15)

        pod = _pod(
            "v1", booking_reference="res-2", reservation_id=2, gpu_count=1,
            termination_warning_at=S_ISO, termination_warning_risk="1.00",
        )
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 4}  # no shortfall
        )
        writes, clears = _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, config, now=S - timedelta(minutes=10)))

        assert deleted == []
        assert writes == []
        assert clears == [(USERNAME, "pod-v1")]

    def test_unchanged_warning_is_not_repatched(self, monkeypatch):
        """A pod already carrying the exact warning that would be computed is a
        no-op — no write, no clear (avoids per-tick API churn)."""
        m = _main_module(monkeypatch)
        state = _state(_victim_ending_at_boundary(), _boundary_reservation(1, gpu_count=1))
        config = _config(preemption_lead_minutes=15)

        pod = _pod(
            "v1", booking_reference="res-2", reservation_id=2, gpu_count=1,
            termination_warning_at=S_ISO, termination_warning_risk="1.00",
        )
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        writes, clears = _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, config, now=S - timedelta(minutes=10)))

        assert deleted == []
        assert writes == []
        assert clears == []

    def test_disabled_flag_skips_warning_reconcile(self, monkeypatch):
        """With TERMINATION_WARNING_ENABLED off, neither write nor clear runs."""
        m = _main_module(monkeypatch)
        state = _state(_victim_ending_at_boundary(), _boundary_reservation(1, gpu_count=1))
        config = _config(preemption_lead_minutes=15, termination_warning_enabled=False)

        pod = _pod(
            "v1", booking_reference="res-2", reservation_id=2, gpu_count=1,
            termination_warning_at=S_ISO, termination_warning_risk="1.00",
        )
        deleted, _events = _patch_snapshots(
            monkeypatch, m, pods=[pod], capacity={GPU_CLASS_LABEL: 1}
        )
        writes, clears = _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, config, now=S - timedelta(minutes=10)))

        assert writes == []
        assert clears == []
