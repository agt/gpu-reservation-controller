"""Unit tests for the JIT (just-in-time) on-demand lease path.

Covers:
- Occupancy accounting and on-demand candidate bookkeeping (pure state logic)
- ``ControllerState.find_admittable_reservation`` — the budget/horizon-aware
  routing gate between the reserved queue and a JIT lease request
- ``main._try_request_lease`` — the async orchestrator: routing re-check,
  guard 1 / guard 3 gating, lease grant → admit, denial → cooldown, and
  admission-failure → compensating cancel

No Kubernetes or HTTP calls are made; ``main`` is imported after setting the
required env vars, since importing it runs ``create_app()`` at module load.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.controller import ControllerState, OnDemandCandidate, slot_start

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state
from tests.conftest import ondemand_block as _ondemand_block
from tests.conftest import reservation, user_reservation as _user_reservation


# ---------------------------------------------------------------------------
# Occupancy — record, available, release
# ---------------------------------------------------------------------------


def _state_with_block(block):
    """Return a ControllerState pre-loaded with one reservation."""
    state = ControllerState()
    state.reservations = [block]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


class TestOccupancy:
    def test_available_decreases_on_record(self):
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        assert state.available(block) == 4
        state.record_placement(1, "pod-a", 2)
        assert state.available(block) == 2

    def test_available_increases_on_release(self):
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 2)
        state.release_pod("pod-a")
        assert state.available(block) == 4

    def test_available_excludes_named_uid(self):
        # The reserved path passes exclude_uid so a retry doesn't count itself.
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 2)
        state.record_placement(1, "pod-b", 1)
        assert state.available(block) == 1
        assert state.available(block, exclude_uid="pod-a") == 3

    def test_release_returns_block_id(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        result = state.release_pod("pod-a")
        assert result == 1

    def test_release_unknown_pod_returns_none(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        result = state.release_pod("ghost-pod")
        assert result is None

    def test_multi_tenant_fills_to_limit(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.record_placement(1, "pod-b", 1)
        assert state.available(block) == 0

    def test_multi_tenant_partial_release_allows_new(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.record_placement(1, "pod-b", 1)
        state.release_pod("pod-a")
        assert state.available(block) == 1

    def test_occupancy_cleaned_up_when_empty(self):
        block = _ondemand_block(1, gpu_count=1)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.release_pod("pod-a")
        assert 1 not in state.occupancy


# ---------------------------------------------------------------------------
# reconcile_occupancy — rebuild from a live cluster snapshot
# ---------------------------------------------------------------------------


class TestReconcileOccupancy:
    def test_rebuilds_from_snapshot(self):
        state = ControllerState()
        state.reconcile_occupancy(
            [(1, "pod-a", 2), (1, "pod-b", 1), (2, "pod-c", 1)]
        )
        assert state.occupancy == {1: {"pod-a": 2, "pod-b": 1}, 2: {"pod-c": 1}}

    def test_empty_snapshot_clears(self):
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1}}
        state.reconcile_occupancy([])
        assert state.occupancy == {}

    def test_prunes_stale_pod(self):
        # pod-gone is no longer in the snapshot → dropped (self-heal on a missed
        # DELETE event); pod-a survives.
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1, "pod-gone": 1}}
        state.reconcile_occupancy([(1, "pod-a", 1)])
        assert state.occupancy == {1: {"pod-a": 1}}

    def test_overwrites_gpu_count(self):
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1}}
        state.reconcile_occupancy([(1, "pod-a", 3)])
        assert state.occupancy == {1: {"pod-a": 3}}


# ---------------------------------------------------------------------------
# add_ondemand_candidate / remove_ondemand_candidate
# ---------------------------------------------------------------------------


class TestCandidateManagement:
    def test_add_candidate(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert "uid-1" in state.ondemand_candidates
        c = state.ondemand_candidates["uid-1"]
        assert c.gpu_class_label == "h100"
        assert c.min_runtime_seconds == 600
        assert c.group_label is None

    def test_add_candidate_carries_group_label(self):
        state = ControllerState()
        state.add_ondemand_candidate(
            "uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc),
            group_label="CSE151",
        )
        assert state.ondemand_candidates["uid-1"].group_label == "CSE151"

    def test_add_is_idempotent(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert len(state.ondemand_candidates) == 1

    def test_already_placed_pod_not_re_added(self):
        state = ControllerState()
        state.occupancy[42] = {"uid-1": 1}
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert "uid-1" not in state.ondemand_candidates

    def test_remove_candidate(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        state.remove_ondemand_candidate("uid-1")
        assert "uid-1" not in state.ondemand_candidates

    def test_remove_nonexistent_is_noop(self):
        state = ControllerState()
        state.remove_ondemand_candidate("ghost")  # should not raise


# ---------------------------------------------------------------------------
# find_admittable_reservation — the JIT routing gate
# ---------------------------------------------------------------------------


class TestFindAdmittableReservation:
    def test_open_now_with_budget_matches(self):
        res = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        state = _state(res)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)  # mid-window
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is not None and got.id == 1

    def test_budget_full_no_match(self):
        res = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120, gpu_count=1)
        state = _state(res)
        state.record_placement(1, "other-pod", 1)  # fills the single GPU
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is None

    def test_within_horizon_not_yet_open_matches(self):
        res = _user_reservation(1, slot_index=0, start_time="10:00:00", duration_minutes=120)
        state = _state(res)
        now = datetime(2024, 1, 15, 9, 45, tzinfo=timezone.utc)  # opens in 15 min
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is not None and got.id == 1

    def test_beyond_horizon_no_match(self):
        res = _user_reservation(1, slot_index=0, start_time="12:00:00", duration_minutes=120)
        state = _state(res)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)  # opens in 3h
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is None

    def test_no_match_at_all(self):
        state = _state()
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is None


# ---------------------------------------------------------------------------
# main._try_request_lease
# ---------------------------------------------------------------------------


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-jit")
    import app.main as main_module

    return main_module


def _pod(*, uid="uid-1", phase="Pending", tolerations=None, conditions=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-1", namespace=USERNAME,
            annotations=None, labels={"gpu-class": GPU_CLASS_LABEL},
        ),
        status=SimpleNamespace(phase=phase, conditions=conditions),
        spec=SimpleNamespace(
            tolerations=tolerations if tolerations is not None else [],
            containers=[],
            scheduling_gates=None,
        ),
    )


def _gpu_only_condition():
    return SimpleNamespace(
        type="PodScheduled", status="False", reason="Unschedulable",
        message="0/10 nodes are available: 5 Insufficient nvidia.com/gpu.",
    )


def _non_gpu_condition():
    return SimpleNamespace(
        type="PodScheduled", status="False", reason="Unschedulable",
        message="0/10 nodes are available: 5 Insufficient memory.",
    )


def _candidate(uid="uid-1", *, gpu_requested=1, min_runtime_seconds=600, group_label=None):
    return OnDemandCandidate(
        pod_uid=uid, pod_name="pod-1", pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=gpu_requested,
        min_runtime_seconds=min_runtime_seconds,
        pod_created_at=datetime.now(timezone.utc),
        next_attempt_at=datetime.now(timezone.utc),
        group_label=group_label,
    )


def _lease(res_id=500, *, gpu_count=1, duration_seconds=1800):
    now = datetime.now(timezone.utc)
    return reservation(
        res_id,
        start_utc=now,
        end_utc=now + timedelta(seconds=duration_seconds),
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
    )


def _state_ready() -> ControllerState:
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    return state


class _FakeClient:
    def __init__(self, *, lease=None, cancel_result=True):
        self._lease = lease
        self.cancel_result = cancel_result
        self.create_requests: list = []
        self.cancel_calls: list = []

    async def create_ondemand_reservation(self, req):
        self.create_requests.append(req)
        return self._lease

    async def cancel_reservation(self, reservation_id, reason):
        self.cancel_calls.append((reservation_id, reason))
        return self.cancel_result


def _patch_admission(monkeypatch, m, *, apply_error=None):
    """Wire the k8s-facing calls _try_apply_toleration makes, so only the JIT
    orchestration around it is under test."""
    async def fake_apply(*args, **kwargs):
        if apply_error is not None:
            raise apply_error

    async def fake_annotate(*args, **kwargs):
        pass

    async def fake_emit(*args, **kwargs):
        pass

    monkeypatch.setattr(m, "apply_toleration", fake_apply)
    monkeypatch.setattr(m, "annotate_runtime_guarantee", fake_annotate)
    monkeypatch.setattr(m, "emit_runtime_guaranteed_event", fake_emit)


class TestTryRequestLease:
    def test_grant_admits_pod_and_records_idempotency_key(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(500, gpu_count=1)
        client = _FakeClient(lease=lease)
        state = _state_ready()
        candidate = _candidate("uid-1", min_runtime_seconds=600)

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30,
            ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert len(client.create_requests) == 1
        req = client.create_requests[0]
        assert req.idempotency_key == "uid-1"
        assert req.gpu_class_id == GPU_CLASS_ID
        assert req.duration_seconds == 600 + 10 * 60
        assert any(r.id == 500 for r in state.reservations)
        assert state.occupancy.get(500, {}).get("uid-1") == 1
        assert client.cancel_calls == []

    def test_denial_cools_down_without_touching_reservations(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=None)  # denied
        state = _state_ready()
        candidate = _candidate("uid-1")
        before = candidate.next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert candidate.next_attempt_at > before
        assert state.reservations == []

    def test_admission_failure_issues_compensating_cancel(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(501)
        client = _FakeClient(lease=lease)
        state = _state_ready()
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m, apply_error=RuntimeError("transient patch failure"))

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        # Transient failure, pod not terminal -> candidate kept for retry.
        assert result is False
        assert client.cancel_calls == [(501, "controller-revoked")]
        assert all(r.id != 501 for r in state.reservations)

    def test_pod_terminal_after_grant_cancels_and_drops_candidate(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(502)
        client = _FakeClient(lease=lease)
        state = _state_ready()
        candidate = _candidate("uid-1")

        # First read_pod (top-of-function liveness check) sees Pending; the
        # second (inside _try_apply_toleration) sees the pod finished in the
        # meantime.
        calls = {"n": 0}

        async def fake_read_pod(name, namespace):
            calls["n"] += 1
            if calls["n"] == 1:
                return _pod(conditions=[_gpu_only_condition()])
            return _pod(phase="Succeeded")

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True  # terminal pod -> candidate dropped
        assert client.cancel_calls == [(502, "controller-revoked")]
        assert all(r.id != 502 for r in state.reservations)

    def test_gone_pod_drops_candidate_without_requesting_lease(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(600))
        state = _state_ready()
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(phase="Succeeded")

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert client.create_requests == []

    def test_guard1_non_gpu_constraint_drops_candidate(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(601))
        state = _state_ready()
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_non_gpu_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True  # dropped: our toleration cannot help
        assert client.create_requests == []

    def test_guard1_indeterminate_short_retries(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(602))
        state = _state_ready()
        candidate = _candidate("uid-1")
        before = candidate.next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=None)  # no PodScheduled condition yet

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert candidate.next_attempt_at > before
        assert client.create_requests == []

    def test_guard3_stuck_holder_interlock_short_retries(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(603))
        state = _state_ready()
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}
        candidate = _candidate("uid-1")
        before = candidate.next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert candidate.next_attempt_at > before
        assert client.create_requests == []

    def test_unresolved_gpu_class_id_retries_without_requesting(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(604))
        state = ControllerState()  # gpu_class_ids left empty
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert client.create_requests == []

    def test_routing_recheck_prefers_admittable_reservation_over_lease(self, monkeypatch):
        """If a reservation has become admittable since the candidate was
        queued, route to the reserved path instead of requesting a lease."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(605))
        now = datetime.now(timezone.utc)
        open_res = reservation(
            9,
            start_utc=now - timedelta(minutes=10),
            end_utc=now + timedelta(hours=1),
            gpu_count=2,
            gpu_class_label=GPU_CLASS_LABEL,
            username=USERNAME,
        )
        state = _state_ready()
        state.reservations = [open_res]
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert client.create_requests == []  # no lease requested
        assert state.occupancy.get(9, {}).get("uid-1") == 1  # admitted under the reservation
