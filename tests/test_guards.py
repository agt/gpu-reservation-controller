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
    assert is_gpu_only_pending(pod) is None


def test_no_status_returns_none():
    pod = _pod(phase=None)
    assert is_gpu_only_pending(pod) is None


def test_no_pod_scheduled_condition_returns_none():
    pod = _pod(phase="Pending", conditions=[])
    assert is_gpu_only_pending(pod) is None


def test_pod_scheduled_status_true_returns_none():
    pod = _pod(phase="Pending", conditions=[_sched_condition("True")])
    assert is_gpu_only_pending(pod) is None


def test_pod_scheduled_reason_not_unschedulable_returns_none():
    pod = _pod(
        phase="Pending",
        conditions=[_sched_condition("False", reason="SchedulerError", message="some error")],
    )
    assert is_gpu_only_pending(pod) is None


# ---------------------------------------------------------------------------
# is_gpu_only_pending — message content cases
# ---------------------------------------------------------------------------


GPU_ONLY_MSG = (
    "0/10 nodes are available: 5 Insufficient nvidia.com/gpu. "
    "preemption: 0/10 nodes are available: 10 No preemption victims found."
)


def test_gpu_only_message_returns_true():
    pod = _pod(conditions=[_sched_condition("False", message=GPU_ONLY_MSG)])
    assert is_gpu_only_pending(pod) is True


def test_gpu_plus_our_taint_returns_true():
    """Our reservation taint appearing in the message is acceptable."""
    msg = (
        "0/10 nodes are available: 5 Insufficient nvidia.com/gpu, "
        "5 node(s) had untolerated taint {gpu-class-reservation=h100: NoSchedule}."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is True


def test_gpu_plus_memory_returns_false():
    msg = (
        "0/10 nodes are available: 5 Insufficient nvidia.com/gpu, "
        "3 Insufficient memory."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is False


def test_gpu_plus_cpu_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient nvidia.com/gpu, 2 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is False


def test_memory_only_no_gpu_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient memory."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is False


def test_cpu_only_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is False


def test_empty_message_returns_none():
    pod = _pod(conditions=[_sched_condition("False", message="")])
    assert is_gpu_only_pending(pod) is None


def test_message_none_returns_none():
    cond = SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable", message=None)
    pod = _pod(conditions=[cond])
    assert is_gpu_only_pending(pod) is None


def test_affinity_failure_no_gpu_returns_none():
    msg = "0/5 nodes are available: 5 node(s) didn't match Pod's node affinity/selector."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is None


def test_other_taint_without_gpu_returns_none():
    msg = "0/5 nodes are available: 5 node(s) had untolerated taint {other-key=val: NoSchedule}."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is None


def test_gpu_message_with_ephemeral_storage_returns_false():
    msg = (
        "0/10 nodes are available: 8 Insufficient nvidia.com/gpu, "
        "2 Insufficient ephemeral-storage."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is False


def test_multiple_non_gpu_insufficient_returns_false():
    msg = "0/10 nodes are available: 5 Insufficient memory, 5 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is False


def test_gpu_only_with_preemption_note_returns_true():
    """Preemption explanation appended by scheduler should not affect result."""
    msg = (
        "0/3 nodes are available: 3 Insufficient nvidia.com/gpu. "
        "preemption: 0/3 nodes are available: 3 No preemption victims found for incoming pod."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert is_gpu_only_pending(pod) is True


# ---------------------------------------------------------------------------
# Guard 3 — stuck_holder_gpu_classes state field
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


# Guard 3's synchronous gating of the JIT lease request path
# (``main._try_request_lease``) is covered in test_jit_lease.py.
