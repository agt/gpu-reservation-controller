"""Unit tests for on-demand placement guards.

Guard 1 — ``is_gpu_gated_pending`` + ``node_counts_by_class``: pure, no k8s I/O.
Guard 3 — ``stuck_holder_pod_present`` state field: pure state logic,
          verifying the interlock flag gates placement before any async call.

Mock pods are built with SimpleNamespace following the pattern in
test_k8s_helpers.py.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

from app.k8s_client import is_gpu_gated_pending


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
# is_gpu_gated_pending — phase / condition availability cases
# ---------------------------------------------------------------------------

# The taint key the controller gates on, passed in by every call site.
TAINT_KEY = "gpu-class-reservation"


def _verdict(pod):
    return is_gpu_gated_pending(pod, TAINT_KEY)


def test_not_pending_returns_none():
    pod = _pod(phase="Running")
    assert _verdict(pod) is None


def test_no_status_returns_none():
    pod = _pod(phase=None)
    assert _verdict(pod) is None


def test_no_pod_scheduled_condition_returns_none():
    pod = _pod(phase="Pending", conditions=[])
    assert _verdict(pod) is None


def test_pod_scheduled_status_true_returns_none():
    pod = _pod(phase="Pending", conditions=[_sched_condition("True")])
    assert _verdict(pod) is None


def test_pod_scheduled_reason_not_unschedulable_returns_none():
    pod = _pod(
        phase="Pending",
        conditions=[_sched_condition("False", reason="SchedulerError", message="some error")],
    )
    assert _verdict(pod) is None


# ---------------------------------------------------------------------------
# is_gpu_gated_pending — message content cases
# ---------------------------------------------------------------------------


GPU_ONLY_MSG = (
    "0/10 nodes are available: 5 Insufficient nvidia.com/gpu. "
    "preemption: 0/10 nodes are available: 10 No preemption victims found."
)


def test_gpu_only_message_returns_true():
    pod = _pod(conditions=[_sched_condition("False", message=GPU_ONLY_MSG)])
    assert _verdict(pod) is True


def test_gpu_plus_our_taint_returns_true():
    """Our reservation taint appearing in the message is acceptable."""
    msg = (
        "0/10 nodes are available: 5 Insufficient nvidia.com/gpu, "
        "5 node(s) had untolerated taint {gpu-class-reservation=h100: NoSchedule}."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is True


def test_gpu_plus_memory_returns_false():
    msg = (
        "0/10 nodes are available: 5 Insufficient nvidia.com/gpu, "
        "3 Insufficient memory."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_gpu_plus_cpu_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient nvidia.com/gpu, 2 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_memory_only_no_gpu_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient memory."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_cpu_only_returns_false():
    msg = "0/5 nodes are available: 5 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_empty_message_returns_none():
    pod = _pod(conditions=[_sched_condition("False", message="")])
    assert _verdict(pod) is None


def test_message_none_returns_none():
    cond = SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable", message=None)
    pod = _pod(conditions=[cond])
    assert _verdict(pod) is None


def test_affinity_failure_no_taint_returns_false():
    """No node was rejected on a taint: our toleration cannot help this pod."""
    msg = "0/5 nodes are available: 5 node(s) didn't match Pod's node affinity/selector."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_other_named_taint_without_gpu_returns_false():
    """Taints rejected nodes, but the scheduler names one that is not ours."""
    msg = "0/5 nodes are available: 5 node(s) had untolerated taint {other-key=val: NoSchedule}."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_named_taint_without_value_is_parsed():
    """A valueless taint renders as {key: Effect} — still a name we can check."""
    msg = (
        "0/5 nodes are available: "
        "5 node(s) had untolerated taint {node.kubernetes.io/disk-pressure: NoSchedule}."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_our_named_taint_among_others_returns_true():
    msg = (
        "0/9 nodes are available: "
        "4 node(s) had untolerated taint {other-key=val: NoSchedule}, "
        "5 node(s) had untolerated taint {gpu-class-reservation=xtra: NoSchedule}."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is True


# --- the case that motivated this guard's rewrite -------------------------
#
# Current kube-scheduler reports an anonymous taint count, and TaintToleration
# runs ahead of NodeResourcesFit — so a candidate's own GPU nodes are rejected
# on our taint and never report "Insufficient nvidia.com/gpu" at all.  Verbatim
# from a pod that sat held forever under the old "look for Insufficient
# nvidia.com/gpu" rule.
ANONYMOUS_TAINT_MSG = (
    "0/41 nodes are available: 12 node(s) didn't match Pod's node "
    "affinity/selector, 25 node(s) had untolerated taint(s), 4 node(s) were "
    "unschedulable. no new claims to deallocate, preemption: 0/41 nodes are "
    "available: 41 Preemption is not helpful for scheduling."
)


def test_anonymous_taint_bucket_returns_true():
    pod = _pod(conditions=[_sched_condition("False", message=ANONYMOUS_TAINT_MSG)])
    assert _verdict(pod) is True


def test_anonymous_taint_bucket_with_non_gpu_shortage_still_drops():
    """A named non-GPU shortage outranks the benefit of the doubt."""
    msg = ANONYMOUS_TAINT_MSG.replace(
        "4 node(s) were unschedulable", "4 Insufficient memory"
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_gpu_message_with_ephemeral_storage_returns_false():
    msg = (
        "0/10 nodes are available: 8 Insufficient nvidia.com/gpu, "
        "2 Insufficient ephemeral-storage."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_multiple_non_gpu_insufficient_returns_false():
    msg = "0/10 nodes are available: 5 Insufficient memory, 5 Insufficient cpu."
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is False


def test_gpu_only_with_preemption_note_returns_true():
    """Preemption explanation appended by scheduler should not affect result."""
    msg = (
        "0/3 nodes are available: 3 Insufficient nvidia.com/gpu. "
        "preemption: 0/3 nodes are available: 3 No preemption victims found for incoming pod."
    )
    pod = _pod(conditions=[_sched_condition("False", message=msg)])
    assert _verdict(pod) is True


# ---------------------------------------------------------------------------
# Guard 1b — node_counts_by_class
# ---------------------------------------------------------------------------


def test_node_counts_by_class_counts_nodes_not_gpus():
    from app.controller import node_counts_by_class

    inventory = {"xtra": {"n31": 4}, "h100": {"n1": 8, "n2": 8}, "drained": {}}
    assert node_counts_by_class(inventory) == {"xtra": 1, "h100": 2, "drained": 0}


def test_node_counts_by_class_empty_inventory():
    from app.controller import node_counts_by_class

    assert node_counts_by_class({}) == {}


def test_class_node_counts_default_empty():
    """ControllerState starts with no node data, which must not block anything."""
    from app.controller import ControllerState

    assert ControllerState().class_node_counts == {}


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
