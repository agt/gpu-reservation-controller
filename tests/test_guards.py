"""Unit tests for on-demand placement guards.

Guard 1 — ``is_gpu_only_pending``: pure function, no k8s I/O needed.
Guard 3 — ``stuck_holder_pod_present`` state field: pure state logic,
          verifying the interlock flag gates placement before any async call.

Mock pods are built with SimpleNamespace following the pattern in
test_k8s_helpers.py.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

import pytest

from app.controller import TOLERATION_KEY
from app.k8s_client import is_gpu_only_pending


# ---------------------------------------------------------------------------
# Mock-pod builder helpers
# ---------------------------------------------------------------------------


def _condition(
    cond_type: str,
    status: str,
    reason: str = "",
    message: str = "",
) -> SimpleNamespace:
    return SimpleNamespace(
        type=cond_type,
        status=status,
        reason=reason,
        message=message,
    )


def _pod(
    phase: Optional[str] = "Pending",
    conditions: Optional[list] = None,
    tolerations: Optional[list] = None,
) -> SimpleNamespace:
    """Build a minimal mock pod for guard tests."""
    if phase is None:
        status = None
    else:
        status = SimpleNamespace(
            phase=phase,
            conditions=conditions if conditions is not None else [],
        )
    spec = SimpleNamespace(tolerations=tolerations or [])
    return SimpleNamespace(status=status, spec=spec)


def _sched_condition(status: str, reason: str = "Unschedulable", message: str = "") -> SimpleNamespace:
    return _condition("PodScheduled", status, reason, message)


# ---------------------------------------------------------------------------
# is_gpu_only_pending — phase / condition availability cases
# ---------------------------------------------------------------------------


def test_not_pending_returns_none():
    pod = _pod(phase="Running")
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


def test_no_status_returns_none():
    pod = _pod(phase=None)
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


def test_no_pod_scheduled_condition_returns_none():
    pod = _pod(phase="Pending", conditions=[])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


def test_pod_scheduled_status_true_returns_none():
    pod = _pod(phase="Pending", conditions=[_sched_condition("True")])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


def test_pod_scheduled_reason_not_unschedulable_returns_none():
    pod = _pod(
        phase="Pending",
        conditions=[_sched_condition("False", reason="SchedulerError", message="some error")],
    )
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


# ---------------------------------------------------------------------------
# is_gpu_only_pending — message content cases
# ---------------------------------------------------------------------------


GPU_ONLY_MSG = (
    "0/10 nodes are available: 5 Insufficient nvidia.com/gpu. "
    "preemption: 0/10 nodes are available: 10 No preemption victims found."
)


def test_gpu_only_message_returns_true():
    pod = _pod(conditions=[_sched_condition("False", message=GPU_ONLY_MSG)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is True


def test_gpu_plus_our_taint_returns_true():
    """Our reservation taint appearing in the message is acceptable."""
    msg = (
        "0/10 nodes are available: 5 Insufficient nvidia.com/gpu, "
        "5 node(s) had untolerated taint {gpu-class-reservation=h100: NoSchedule}."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is True


def test_gpu_plus_memory_returns_false():
    msg = (
        "0/10 nodes are available: 5 Insufficient nvidia.com/gpu, "
        "3 Insufficient memory."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is False


def test_gpu_plus_cpu_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient nvidia.com/gpu, 2 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is False


def test_memory_only_no_gpu_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient memory."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is False


def test_cpu_only_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is False


def test_empty_message_returns_none():
    pod = _pod(conditions=[_sched_condition("False", message="")])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


def test_message_none_returns_none():
    cond = SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable", message=None)
    pod = _pod(conditions=[cond])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


def test_affinity_failure_no_gpu_returns_none():
    msg = "0/5 nodes are available: 5 node(s) didn't match Pod's node affinity/selector."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


def test_other_taint_without_gpu_returns_none():
    msg = "0/5 nodes are available: 5 node(s) had untolerated taint {other-key=val: NoSchedule}."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is None


def test_gpu_message_with_ephemeral_storage_returns_false():
    msg = (
        "0/10 nodes are available: 8 Insufficient nvidia.com/gpu, "
        "2 Insufficient ephemeral-storage."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is False


def test_multiple_non_gpu_insufficient_returns_false():
    msg = "0/10 nodes are available: 5 Insufficient memory, 5 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is False


def test_gpu_only_with_preemption_note_returns_true():
    """Preemption explanation appended by scheduler should not affect result."""
    msg = (
        "0/3 nodes are available: 3 Insufficient nvidia.com/gpu. "
        "preemption: 0/3 nodes are available: 3 No preemption victims found for incoming pod."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod, TOLERATION_KEY) is True


# ---------------------------------------------------------------------------
# Guard 3 — stuck_holder_gpu_classes gates _try_place_ondemand synchronously
# ---------------------------------------------------------------------------


def test_stuck_holder_gpu_classes_default_empty():
    """ControllerState initialises with no stuck classes."""
    from app.controller import ControllerState

    state = ControllerState()
    assert state.stuck_holder_gpu_classes == set()


def test_stuck_holder_gpu_classes_can_be_set():
    from app.controller import ControllerState

    state = ControllerState()
    state.stuck_holder_gpu_classes = {"h100"}
    assert "h100" in state.stuck_holder_gpu_classes
    state.stuck_holder_gpu_classes = set()
    assert state.stuck_holder_gpu_classes == set()


def _make_main_module_and_state(monkeypatch):
    """Import app.main with required env vars and return (main_module, state, block)."""
    import asyncio
    from datetime import date, datetime, timedelta, timezone

    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-guards")
    import app.main as main_module
    from app.controller import ControllerState
    from app.schemas import GpuClassBrief, PolicyBrief, ReservationResponse

    today = date.today()
    midnight = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
    state = ControllerState()
    block = ReservationResponse(
        id=99,
        user_id=None, user=None, group_id=None, group=None,
        gpu_class_id=1,
        gpu_class=GpuClassBrief(id=1, name="H100"),
        policy_id=1,
        slot_index=0,
        policy=PolicyBrief(id=1, name="p", start_time="00:00:00",
                           duration_minutes=1440, repeat_count=1),
        date=today,
        start_utc=midnight,
        end_utc=midnight + timedelta(days=1),
        gpu_count=4, status="active", kind="ondemand",
        created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
    )
    state.reservations = [block]
    state.gpu_class_labels = {1: "h100"}
    return main_module, state


def test_guard3_blocks_matching_gpu_class(monkeypatch):
    """When the stuck set contains the candidate's class, placement is held."""
    import asyncio
    from datetime import datetime, timezone
    from app.controller import OnDemandCandidate

    main_module, state = _make_main_module_and_state(monkeypatch)
    state.stuck_holder_gpu_classes = {"h100"}

    candidate = OnDemandCandidate(
        pod_uid="uid-1", pod_name="pod-1", pod_namespace="ns-1",
        gpu_class_label="h100", gpu_requested=1, min_runtime_seconds=60,
        pod_created_at=datetime.now(timezone.utc), next_attempt_at=datetime.now(timezone.utc),
    )

    called = []

    async def fake_read_pod(name, namespace):
        called.append((name, namespace))
        raise AssertionError("read_pod should not be called when guard 3 is active")

    monkeypatch.setattr(main_module, "read_pod", fake_read_pod)
    result = asyncio.run(main_module._try_place_ondemand(state, "uid-1", candidate))

    assert result is False, "guard 3 should keep candidate when its class is stuck"
    assert called == [], "read_pod must not be called"
    assert state.occupancy == {}


def test_guard3_does_not_block_other_gpu_class(monkeypatch):
    """When the stuck set contains h100, an a100 candidate is not blocked."""
    import asyncio
    from datetime import date, datetime, timedelta, timezone
    from app.controller import OnDemandCandidate
    from app.schemas import GpuClassBrief, PolicyBrief, ReservationResponse

    main_module, state = _make_main_module_and_state(monkeypatch)

    # Add an a100 on-demand block so find_ondemand_block can succeed.
    today = date.today()
    midnight = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
    a100_block = ReservationResponse(
        id=100,
        user_id=None, user=None, group_id=None, group=None,
        gpu_class_id=2,
        gpu_class=GpuClassBrief(id=2, name="A100"),
        policy_id=1, slot_index=0,
        policy=PolicyBrief(id=1, name="p", start_time="00:00:00",
                           duration_minutes=1440, repeat_count=1),
        date=today,
        start_utc=midnight,
        end_utc=midnight + timedelta(days=1),
        gpu_count=4, status="active", kind="ondemand",
        created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
    )
    state.reservations.append(a100_block)
    state.gpu_class_labels[2] = "a100"

    # Only h100 is stuck; a100 should be unaffected.
    state.stuck_holder_gpu_classes = {"h100"}

    candidate = OnDemandCandidate(
        pod_uid="uid-2", pod_name="pod-2", pod_namespace="ns-2",
        gpu_class_label="a100", gpu_requested=1, min_runtime_seconds=60,
        pod_created_at=datetime.now(timezone.utc), next_attempt_at=datetime.now(timezone.utc),
    )

    # Guard 3 should NOT fire; the function will proceed to read_pod.
    # We let read_pod raise so we can verify guard 3 was bypassed.
    guard3_bypassed = []

    async def fake_read_pod(name, namespace):
        guard3_bypassed.append(True)
        raise RuntimeError("stop here — we only need to confirm guard 3 was skipped")

    monkeypatch.setattr(main_module, "read_pod", fake_read_pod)
    # The RuntimeError from fake_read_pod is caught by _try_place_ondemand's
    # outer except, which rolls back and returns False.
    result = asyncio.run(main_module._try_place_ondemand(state, "uid-2", candidate))

    assert guard3_bypassed, "read_pod should have been called (guard 3 did not block a100)"
