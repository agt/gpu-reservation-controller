"""Kubernetes API wrapper for the GPU reservation controller.

All blocking kubernetes-client calls run in a thread-pool executor so they
never stall the asyncio event loop.

Public surface
--------------
init_k8s(kubeconfig_path)                    — load credentials once at startup
get_pod_gpu_count(pod)                       — sum nvidia.com/gpu requests
get_pod_booking_reference(pod)               — read horae/booking-reference annotation
get_pod_usage_group(pod)                     — read horae/usage-group annotation (JIT lease group)
parse_booking_reference(ref)                 — reservation id from a booking-reference
pod_has_toleration(pod, ...)                 — check for a specific toleration
is_gpu_only_pending(pod)                      — guard 1: GPU-only scheduling failure check
read_pod(name, namespace)                    — fetch current pod object
snapshot_tolerated_pods(tol_key)             — one LIST → occupancy + claims + guard 3
snapshot_node_gpu_inventory(taint_key)       — one LIST → allocatable GPUs per class, per node
snapshot_node_gpu_capacity(taint_key)        — per-class collapse of the inventory (preemption planning)
apply_toleration(...)                        — PATCH a pod to add toleration + booking annotation
PodWatcher                                   — async-generator based pod event stream
acquire_singleton_lease / renew_singleton_lease — coordination Lease duplicate-instance guard
"""

from __future__ import annotations

import asyncio
import functools
import logging
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Callable, Optional, TypeVar

from kubernetes import client as k8s_client, config as k8s_config, watch
from kubernetes.client.rest import ApiException

from .log_fields import kv

log = logging.getLogger(__name__)

# Initialised once by init_k8s(); used by all functions in this module.
_core_v1: Optional[k8s_client.CoreV1Api] = None
_coordination_v1: Optional[k8s_client.CoordinationV1Api] = None


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def init_k8s(kubeconfig_path: Optional[str]) -> None:
    """Load Kubernetes credentials and create the API clients."""
    global _core_v1, _coordination_v1
    if kubeconfig_path:
        k8s_config.load_kube_config(config_file=kubeconfig_path)
        log.info("%s", kv(event="k8s.auth", mode="kubeconfig", path=kubeconfig_path))
    else:
        k8s_config.load_incluster_config()
        log.info("%s", kv(event="k8s.auth", mode="in_cluster"))
    _core_v1 = k8s_client.CoreV1Api()
    _coordination_v1 = k8s_client.CoordinationV1Api()


# ---------------------------------------------------------------------------
# Pure helpers (synchronous, no I/O)
# ---------------------------------------------------------------------------


# Phases from which a pod never returns to Running — its GPU slot is free and
# it should be released from occupancy.  Defined once here (next to
# get_pod_phase) so the watch loop, the reserved path, and the on-demand path
# all agree on what "terminal" means (CODE-REVIEW D1c).  The on-demand *drop*
# additionally treats "Unknown" as gone-for-placement (TERMINAL_PHASES +
# ("Unknown",)); that is a placement decision, not an occupancy release.
TERMINAL_PHASES = ("Succeeded", "Failed")


def get_pod_phase(pod) -> str:
    """Return the pod's phase string (e.g. "Pending", "Running", "Succeeded", "Failed").

    Returns an empty string if the phase is unavailable.
    """
    return (pod.status.phase if pod.status else None) or ""


def is_terminal_phase(pod) -> bool:
    """Return True if *pod* is in a terminal phase (Succeeded/Failed)."""
    return get_pod_phase(pod) in TERMINAL_PHASES


def get_pod_creation_timestamp(pod) -> Optional[datetime]:
    """Return the pod's ``metadata.creationTimestamp``, or ``None`` if unset."""
    return pod.metadata.creation_timestamp


def get_unschedulable_message(pod) -> str:
    """Return the ``PodScheduled`` condition message, truncated to 120 chars.

    Empty string when there is no such condition.  Owning the pod-object dig
    here keeps ``main.py`` out of ``pod.status.conditions`` internals
    (CODE-REVIEW D10).
    """
    conditions = (pod.status.conditions or []) if pod.status else []
    scheduled = next((c for c in conditions if c.type == "PodScheduled"), None)
    if scheduled is None:
        return ""
    return (scheduled.message or "")[:120]


def get_pod_min_runtime_seconds(pod) -> Optional[int]:
    """Read the ``horae/minimum-runtime-seconds`` annotation from *pod*.

    Returns the integer value if the annotation is present and parseable as a
    positive integer, or ``None`` otherwise.
    """
    annotations: dict = pod.metadata.annotations or {}
    raw = annotations.get("horae/minimum-runtime-seconds")
    if raw is None:
        return None
    try:
        value = int(raw)
        return value if value > 0 else None
    except (ValueError, TypeError):
        log.warning("%s", kv(
            event="pod.annotation_invalid",
            ns=pod.metadata.namespace, pod=pod.metadata.name,
            annotation="horae/minimum-runtime-seconds", value=raw,
        ))
        return None


def get_pod_usage_group(pod) -> Optional[str]:
    """Read the ``horae/usage-group`` annotation from *pod*.

    Names the usage group a JIT on-demand lease should be created under —
    ``group_name`` is a required natural key on the app's lease-create endpoint
    (RESERVATION-API.md §"Creating on-demand reservations": the user supplies
    their group via this pod annotation).  Only consulted when
    ``REQUIRED_GROUP_LABEL`` is disabled; when that feature is on, the group
    label is both the match axis and the group source.  Returns ``None`` when
    the annotation is absent or empty.
    """
    annotations: dict = getattr(pod.metadata, "annotations", None) or {}
    return annotations.get("horae/usage-group") or None


def get_pod_gpu_count(pod) -> int:
    """Sum nvidia.com/gpu resource requests across all containers in *pod*."""
    total = 0
    for container in pod.spec.containers or []:
        requests = (
            (container.resources.requests or {}) if container.resources else {}
        )
        try:
            total += int(requests.get("nvidia.com/gpu", 0))
        except (ValueError, TypeError):
            pass
    return total


def get_pod_booking_reference(pod) -> Optional[str]:
    """Return the ``horae/booking-reference`` annotation value, or ``None``."""
    annotations: dict = getattr(pod.metadata, "annotations", None) or {}
    return annotations.get("horae/booking-reference")


# Annotation keys for the informational termination-warning stamped on a pod at
# risk of demand-driven preemption (see main._apply_termination_warnings).  Like
# the runtime-guarantee annotations, these are best-effort and never read back
# to make a decision; the sweep recomputes risk live from reservation state.
TERMINATION_WARNING_AT = "horae/termination-warning-at"
TERMINATION_WARNING_RISK = "horae/termination-warning-risk"
TERMINATION_WARNING_MESSAGE = "horae/termination-warning-message"


def get_pod_termination_warning(pod) -> tuple[Optional[str], Optional[str]]:
    """Return the pod's ``(termination-warning-at, termination-warning-risk)``.

    Either element is ``None`` when its annotation is absent.  Used only to
    diff the desired warning against what the pod already carries, so a
    repeated sweep does not re-patch an unchanged warning.
    """
    annotations: dict = getattr(pod.metadata, "annotations", None) or {}
    return (
        annotations.get(TERMINATION_WARNING_AT),
        annotations.get(TERMINATION_WARNING_RISK),
    )


# Annotation key for the pod's live guarantee status: whether it is still inside
# its runtime guarantee (``"guaranteed"``) or running past it (``"overstay"``).
# It rides alongside the existing ``horae/guaranteed-until`` (the guarantee-end
# instant, kept live) as a general status surface, stamped at admission and
# refreshed as the status changes (guarantee lapsing, adoption/merge).  Like the
# other guarantee/termination annotations it is best-effort and never read back
# to make a decision.
GUARANTEE_STATUS = "horae/guarantee-status"
GUARANTEE_STATUS_GUARANTEED = "guaranteed"
GUARANTEE_STATUS_OVERSTAY = "overstay"


def get_pod_guarantee_status(pod) -> tuple[Optional[str], Optional[str]]:
    """Return the pod's ``(guarantee-status, guaranteed-until)`` annotations.

    Either element is ``None`` when its annotation is absent.  Used only to
    diff the desired status against what the pod already carries, so a repeated
    reconcile does not re-patch an unchanged status.
    """
    annotations: dict = getattr(pod.metadata, "annotations", None) or {}
    return (
        annotations.get(GUARANTEE_STATUS),
        annotations.get("horae/guaranteed-until"),
    )


# Prefix recorded in a horae/booking-reference value.  Every admitted pod is a
# real reservation now (the lease model requests one just-in-time rather than
# placing onto an ad-hoc block), so there is only one kind.  Construction
# (make_booking_reference) and parsing (parse_booking_reference) live together
# so a renamed prefix cannot silently break occupancy reconstruction.
_BOOKING_REFERENCE_PREFIX = "res-"


def make_booking_reference(reservation_id: int) -> str:
    """Build a ``horae/booking-reference`` value for *reservation_id*.

    The result round-trips through ``parse_booking_reference``.
    """
    return f"{_BOOKING_REFERENCE_PREFIX}{reservation_id}"


def parse_booking_reference(reference: Optional[str]) -> Optional[int]:
    """Extract the reservation id embedded in a ``horae/booking-reference`` value.

    ``"res-42"`` returns ``42``.  Returns ``None`` for an unrecognised prefix,
    a non-integer suffix, or ``None``/empty input.  This is the single key
    used to reconstruct occupancy from the cluster.
    """
    if not reference or not reference.startswith(_BOOKING_REFERENCE_PREFIX):
        return None
    try:
        return int(reference[len(_BOOKING_REFERENCE_PREFIX):])
    except ValueError:
        return None


def pod_has_toleration(pod, key: str, value: str, effect: str) -> bool:
    """Return True if *pod* already carries the named toleration."""
    for t in pod.spec.tolerations or []:
        if t.key == key and t.value == value and t.effect == effect:
            return True
    return False


def _pod_has_any_reservation_toleration(pod, toleration_key: str) -> bool:
    """Return True if *pod* has any toleration whose key equals *toleration_key*.

    Key-only match (any value/effect) — used to identify pods that were
    admitted by this controller regardless of the GPU-class value.
    """
    return any(t.key == toleration_key for t in (pod.spec.tolerations or []))


def is_gpu_only_pending(pod) -> Optional[bool]:
    """Guard 1: determine whether *pod* is Pending solely due to GPU shortage.

    Inspects ``pod.status.conditions[type=PodScheduled]`` to classify the
    scheduling failure.

    Returns:
        ``True``  — confirmed GPU-only: message contains ``Insufficient
                    nvidia.com/gpu`` and no other ``Insufficient <resource>``.
                    The controller's own reservation taint appearing in the
                    message is accepted (the pod lacks the toleration yet).
        ``False`` — confirmed non-GPU constraint: message contains
                    ``Insufficient <anything-else>``.  Drop the candidate;
                    our toleration cannot fix its scheduling problem.
        ``None``  — indeterminate: no ``PodScheduled`` condition yet, pod is
                    not Pending, or message carries no ``Insufficient`` signal.
                    Keep the candidate and retry on the next processor tick.
    """
    if get_pod_phase(pod) != "Pending":
        return None

    conditions = (pod.status.conditions or []) if pod.status else []
    scheduled = next((c for c in conditions if c.type == "PodScheduled"), None)
    if scheduled is None or scheduled.status != "False":
        return None
    if scheduled.reason != "Unschedulable":
        return None

    message = scheduled.message or ""

    # Check for any non-GPU resource shortages first.  If any exist, we
    # return False immediately even if GPU is also short — the pod cannot
    # be helped by our toleration alone.  Strip trailing punctuation from
    # the match group because the scheduler appends commas and periods
    # (e.g. "Insufficient nvidia.com/gpu, 3 Insufficient memory.").
    for m in re.finditer(r"Insufficient (\S+)", message):
        resource = m.group(1).rstrip(".,;)")
        if resource != "nvidia.com/gpu":
            return False

    # GPU shortage must be explicitly mentioned.
    if "Insufficient nvidia.com/gpu" not in message:
        return None

    return True


# ---------------------------------------------------------------------------
# Async wrappers around blocking kubernetes calls
# ---------------------------------------------------------------------------

_T = TypeVar("_T")


async def _run(fn: Callable[..., _T], *args, **kwargs) -> _T:
    """Run a blocking kubernetes-client call on the default thread-pool executor.

    Single choke point for every blocking ``_core_v1`` call made from async code
    (CODE-REVIEW D3b) — the natural place to add ApiException mapping or metrics
    later.  The dedicated watch thread calls the client directly and does not use
    this.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))


async def read_pod(name: str, namespace: str) -> "k8s_client.V1Pod":
    """Fetch the current pod object (re-read before patching to get fresh state)."""
    log.debug("%s", kv(event="k8s.read_pod", ns=namespace, pod=name))
    return await _run(_core_v1.read_namespaced_pod, name, namespace)


@dataclass(frozen=True)
class ToleratedPodInfo:
    """A point-in-time view of one pod carrying the controller's toleration.

    Returned by ``snapshot_tolerated_pods``; a single cluster LIST yields
    everything the queue-processor tick needs — occupancy reconstruction, the
    claimed-reservation set, and the guard-3 safety interlock — so no per-attempt
    or per-purpose LIST is required.
    """

    namespace: str
    name: str
    uid: str
    gpu_class: str
    booking_reference: Optional[str]
    reservation_id: Optional[int]
    gpu_count: int
    phase: str
    scheduled_false: bool  # PodScheduled condition present with status == "False"
    # Set when the pod has been marked for deletion (``metadata.deletionTimestamp``).
    # A terminating pod is excluded from residency accounting (preemption
    # planning) and is never selected as a preemption victim (it is already on
    # its way out).
    deletion_timestamp: Optional[datetime] = None
    # Value of the REQUIRED_GROUP_LABEL pod label (when the feature is enabled),
    # carried so the adoption planner can enforce the same group constraint used
    # at admission.  None when the feature is disabled or the label is absent.
    group_label: Optional[str] = None
    # The node this pod is bound to (``spec.nodeName``), or None when it has not
    # been scheduled yet.  Carried so per-node GPU accounting can attribute an
    # admitted pod's GPUs to the node it physically occupies.
    node_name: Optional[str] = None
    # Current values of the termination-warning annotations (raw strings), or
    # None when absent.  Carried so the sweep can diff the warning it wants to
    # write against what the pod already has and skip a no-op re-patch, and can
    # detect a stale warning to clear when the pod leaves the at-risk pool.
    termination_warning_at: Optional[str] = None
    termination_warning_risk: Optional[str] = None
    # Current values of the live guarantee-status annotations (raw strings), or
    # None when absent.  Carried so the per-tick reconcile can diff the status it
    # wants against what the pod already has and skip a no-op re-patch.
    guarantee_status: Optional[str] = None
    guaranteed_until: Optional[str] = None


async def snapshot_tolerated_pods(
    toleration_key: str, group_label_key: Optional[str] = None
) -> list[ToleratedPodInfo]:
    """Return one ``ToleratedPodInfo`` per pod carrying a *toleration_key* toleration.

    Scans all ``gpu-class``-labelled pods (the same selector as ``PodWatcher`` —
    no new RBAC permission) and shapes each admitted pod into the fields the
    controller needs.  One snapshot per queue-processor tick drives occupancy
    reconstruction, the claimed-reservation set, and guard 3, replacing the
    former per-attempt namespaced counts and the separate guard scans.

    *group_label_key* (the configured REQUIRED_GROUP_LABEL, when enabled) names
    an additional pod label whose value is captured into
    ``ToleratedPodInfo.group_label``; unset ⇒ the field stays None.
    """
    log.debug("%s", kv(event="k8s.list_pods", selector="gpu-class", purpose="tolerated_snapshot"))
    pod_list = await _run(
        _core_v1.list_pod_for_all_namespaces, label_selector="gpu-class"
    )
    out: list[ToleratedPodInfo] = []
    for pod in pod_list.items:
        if not _pod_has_any_reservation_toleration(pod, toleration_key):
            continue
        conditions = (pod.status.conditions or []) if pod.status else []
        scheduled = next((c for c in conditions if c.type == "PodScheduled"), None)
        booking = get_pod_booking_reference(pod)
        warning_at, warning_risk = get_pod_termination_warning(pod)
        guarantee_status, guaranteed_until = get_pod_guarantee_status(pod)
        labels = pod.metadata.labels or {}
        out.append(
            ToleratedPodInfo(
                namespace=pod.metadata.namespace,
                name=pod.metadata.name,
                uid=pod.metadata.uid,
                gpu_class=labels.get("gpu-class", ""),
                booking_reference=booking,
                reservation_id=parse_booking_reference(booking),
                gpu_count=get_pod_gpu_count(pod),
                phase=get_pod_phase(pod),
                scheduled_false=(scheduled is not None and scheduled.status == "False"),
                deletion_timestamp=pod.metadata.deletion_timestamp,
                group_label=(
                    labels.get(group_label_key) if group_label_key else None
                ),
                node_name=getattr(pod.spec, "node_name", None) if pod.spec else None,
                termination_warning_at=warning_at,
                termination_warning_risk=warning_risk,
                guarantee_status=guarantee_status,
                guaranteed_until=guaranteed_until,
            )
        )
    log.debug("%s", kv(event="k8s.list_pods_done", purpose="tolerated_snapshot", count=len(out)))
    return out


async def snapshot_node_gpu_inventory(
    taint_key: str, gpu_resource: str = "nvidia.com/gpu"
) -> dict[str, dict[str, int]]:
    """Return allocatable GPUs per GPU-class label, broken down **per node**.

    LISTs all nodes and, for each node carrying a *taint_key* taint, records
    ``status.allocatable[gpu_resource]`` under ``{taint_value: {node_name: gpus}}``
    (the GPU-class label mirrors the toleration the controller applies to pods).
    Nodes that are cordoned (``spec.unschedulable``) or being deleted are
    excluded — their GPUs are not placeable.

    This is the per-node primitive: ``snapshot_node_gpu_capacity`` collapses it
    to per-class totals for consumers that only need the aggregate, while
    per-node accounting (whether any *single* node can host a multi-GPU pod)
    reads the breakdown directly.
    """
    log.debug("%s", kv(event="k8s.list_nodes", purpose="gpu_inventory"))
    node_list = await _run(_core_v1.list_node)
    inventory: dict[str, dict[str, int]] = {}
    for node in node_list.items:
        if node.spec and node.spec.unschedulable:
            continue
        if node.metadata.deletion_timestamp is not None:
            continue
        taints = node.spec.taints if (node.spec and node.spec.taints) else []
        classes = {t.value for t in taints if t.key == taint_key and t.value}
        if not classes:
            continue
        allocatable = (node.status.allocatable or {}) if node.status else {}
        raw = allocatable.get(gpu_resource, "0")
        try:
            gpus = int(raw)
        except (ValueError, TypeError):
            log.warning("%s", kv(
                event="k8s.node_allocatable_invalid", node=node.metadata.name,
                resource=gpu_resource, value=raw,
            ))
            gpus = 0
        for gpu_class in classes:
            inventory.setdefault(gpu_class, {})[node.metadata.name] = gpus
    # The inventory is a nested map, so it is fanned out to one line per class
    # rather than emitted as a dict inside a single field.
    for _cls, _nodes in sorted(inventory.items()):
        log.debug("%s", kv(
            event="k8s.node_inventory", clabel=_cls,
            nodes=len(_nodes), total=sum(_nodes.values()),
        ))
    return inventory


async def snapshot_node_gpu_capacity(
    taint_key: str, gpu_resource: str = "nvidia.com/gpu"
) -> dict[str, int]:
    """Return total allocatable GPUs per GPU-class label, from node taints.

    Thin per-class collapse of ``snapshot_node_gpu_inventory`` (one node LIST,
    summed across the nodes of each class).  Feeds preemption planning's and the
    capacity audit's notion of physical capacity per class; the controller has
    no other source of "how many GPUs actually exist".
    """
    inventory = await snapshot_node_gpu_inventory(taint_key, gpu_resource)
    return {
        gpu_class: sum(per_node.values())
        for gpu_class, per_node in inventory.items()
    }


async def apply_toleration(
    pod_name: str,
    namespace: str,
    pod,
    tol_key: str,
    tol_value: str,
    booking_reference: str,
) -> None:
    """Patch *pod* to add toleration ``tol_key=tol_value:NoSchedule`` and set
    the ``horae/booking-reference`` annotation.

    The booking-reference is the single key from which occupancy is later
    reconstructed (see parse_booking_reference).  The patch preserves all
    existing tolerations; Kubernetes rejects requests that would remove
    tolerations from running pods.
    """
    new_tol = {
        "key": tol_key,
        "operator": "Equal",
        "value": tol_value,
        "effect": "NoSchedule",
    }

    # Serialise existing tolerations into plain dicts for the patch body.
    existing: list[dict] = []
    for t in pod.spec.tolerations or []:
        entry: dict = {
            "key": t.key,
            "operator": t.operator,
            "effect": t.effect,
        }
        if t.value is not None:
            entry["value"] = t.value
        if t.toleration_seconds is not None:
            entry["tolerationSeconds"] = t.toleration_seconds
        existing.append(entry)

    patch = {
        "metadata": {"annotations": {"horae/booking-reference": booking_reference}},
        "spec": {"tolerations": existing + [new_tol]},
    }
    log.debug("%s", kv(
        event="k8s.patch_pod", ns=namespace, pod=pod_name, patch="toleration",
        tol_key=tol_key, tol_value=tol_value, booking_ref=booking_reference,
    ))
    await _run(_core_v1.patch_namespaced_pod, pod_name, namespace, patch)
    log.info("%s", kv(
        event="pod.toleration_applied", ns=namespace, pod=pod_name,
        tol_key=tol_key, tol_value=tol_value, booking_ref=booking_reference,
    ))


async def remove_scheduling_gate(
    pod_name: str, namespace: str, pod, gate_name: str
) -> None:
    """Remove *gate_name* from pod.spec.schedulingGates if present.

    Uses the strategic-merge-patch ``$patch: delete`` directive so only the
    named gate is removed; any other gates on the pod are preserved.
    """
    existing_gates = pod.spec.scheduling_gates or []
    if not any(g.name == gate_name for g in existing_gates):
        log.debug("%s", kv(
            event="pod.gate_absent", ns=namespace, pod=pod_name, gate=gate_name,
        ))
        return

    patch = {"spec": {"schedulingGates": [{"name": gate_name, "$patch": "delete"}]}}
    log.debug("%s", kv(
        event="k8s.patch_pod", ns=namespace, pod=pod_name,
        patch="gate_remove", gate=gate_name,
    ))
    await _run(_core_v1.patch_namespaced_pod, pod_name, namespace, patch)
    log.info("%s", kv(
        event="pod.gate_removed", ns=namespace, pod=pod_name, gate=gate_name,
    ))


async def annotate_runtime_guarantee(
    pod_name: str, namespace: str, seconds: int, guaranteed_until: datetime
) -> None:
    """Annotate *pod_name* with its runtime guarantee.

    Writes ``horae/pod-runtime-limit-seconds`` (the guaranteed duration in
    seconds, consumed by in-pod countdown widgets), ``horae/guaranteed-until``
    (the same instant as an absolute UTC ISO-8601 timestamp), and
    ``horae/guarantee-status`` (``"guaranteed"`` — recording a guarantee always
    means the pod is inside one; the reconcile in ``main`` flips this to
    ``"overstay"`` once the guarantee lapses).  Informational only: this never
    patches ``spec.activeDeadlineSeconds`` — demand-driven preemption enforces
    nothing through a Kubernetes-side deadline, and never reads these
    annotations back to make a decision; it recomputes the guarantee live from
    reservation state (``ControllerState.guarantee_end``).
    """
    until_str = guaranteed_until.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.debug("%s", kv(
        event="k8s.patch_pod", ns=namespace, pod=pod_name, patch="runtime_guarantee",
        guarantee_s=seconds, until=until_str,
    ))
    patch = {
        "metadata": {
            "annotations": {
                "horae/pod-runtime-limit-seconds": str(seconds),
                "horae/guaranteed-until": until_str,
                GUARANTEE_STATUS: GUARANTEE_STATUS_GUARANTEED,
            }
        },
    }
    await _run(_core_v1.patch_namespaced_pod, pod_name, namespace, patch)
    log.info("%s", kv(
        event="pod.guarantee_recorded", ns=namespace, pod=pod_name,
        guarantee_s=seconds, until=until_str,
    ))


async def annotate_guarantee_status(
    pod_name: str,
    namespace: str,
    status: str,
    guaranteed_until: Optional[datetime],
) -> None:
    """Update *pod_name*'s live ``horae/guarantee-status`` (+ ``guaranteed-until``).

    Writes ``horae/guarantee-status`` (``"guaranteed"`` | ``"overstay"``) and,
    **only when** *guaranteed_until* is provided, refreshes
    ``horae/guaranteed-until`` to that absolute UTC ISO-8601 instant.  For an
    overstay pod *guaranteed_until* is ``None`` so the existing (now-past)
    ``guaranteed-until`` value is left frozen — only the status key flips.
    Informational only, mirroring ``annotate_runtime_guarantee``: nothing is
    enforced Kubernetes-side and these are never read back to make a decision.
    """
    annotations: dict = {GUARANTEE_STATUS: status}
    if guaranteed_until is not None:
        annotations["horae/guaranteed-until"] = guaranteed_until.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    log.debug("%s", kv(
        event="k8s.patch_pod", ns=namespace, pod=pod_name,
        patch="guarantee_status", gstatus=status,
    ))
    patch = {"metadata": {"annotations": annotations}}
    await _run(_core_v1.patch_namespaced_pod, pod_name, namespace, patch)
    log.info("%s", kv(event="pod.guarantee_status", ns=namespace, pod=pod_name, gstatus=status))


async def annotate_termination_warning(
    pod_name: str,
    namespace: str,
    terminate_at: datetime,
    risk: str,
    message: str,
) -> None:
    """Stamp *pod_name* with the informational termination-warning annotations.

    Writes ``horae/termination-warning-at`` (the projected preemption instant,
    absolute UTC ISO-8601, matching ``guaranteed-until``), ``-risk`` (a value in
    (0, 1] as a pre-rounded string), and ``-message`` (human-readable).  Purely
    informational — nothing is enforced Kubernetes-side and these are never read
    back to make a decision (the sweep recomputes risk live from reservation
    state); a widget should treat them as a best-effort heads-up.
    """
    at_str = terminate_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.debug("%s", kv(
        event="k8s.patch_pod", ns=namespace, pod=pod_name,
        patch="termination_warning", at=at_str, risk=risk,
    ))
    patch = {
        "metadata": {
            "annotations": {
                TERMINATION_WARNING_AT: at_str,
                TERMINATION_WARNING_RISK: risk,
                TERMINATION_WARNING_MESSAGE: message,
            }
        },
    }
    await _run(_core_v1.patch_namespaced_pod, pod_name, namespace, patch)
    log.info("%s", kv(
        event="pod.termination_warned", ns=namespace, pod=pod_name, at=at_str, risk=risk,
    ))


async def clear_termination_warning(pod_name: str, namespace: str) -> None:
    """Remove the termination-warning annotations from *pod_name*.

    Retraction path: the pod left the at-risk pool (its user re-booked, demand
    evaporated, or it was adopted), so a stale "you may be preempted" stamp
    would mislead.  Setting each annotation value to ``None`` in a strategic
    merge patch deletes the key.
    """
    log.debug("%s", kv(
        event="k8s.patch_pod", ns=namespace, pod=pod_name,
        patch="termination_warning_clear",
    ))
    patch = {
        "metadata": {
            "annotations": {
                TERMINATION_WARNING_AT: None,
                TERMINATION_WARNING_RISK: None,
                TERMINATION_WARNING_MESSAGE: None,
            }
        },
    }
    await _run(_core_v1.patch_namespaced_pod, pod_name, namespace, patch)
    log.info("%s", kv(event="pod.termination_warning_cleared", ns=namespace, pod=pod_name))


async def _emit_pod_event(
    pod,
    pod_name: str,
    namespace: str,
    *,
    name_prefix: str,
    reason: str,
    action: str,
    message: str,
) -> None:
    """Create a ``Normal`` Kubernetes Event linked to *pod*.

    Shared body for the runtime-guarantee, preemption, and cancellation
    emitters (CODE-REVIEW D3c), which differ only in *name_prefix* / *reason*
    / *action* / *message*.  Uses ``generate_name`` so re-emitting for the
    same pod never 409s (B10).
    """
    now = datetime.now(timezone.utc)
    event = k8s_client.CoreV1Event(
        metadata=k8s_client.V1ObjectMeta(
            generate_name=name_prefix,
            namespace=namespace,
        ),
        involved_object=k8s_client.V1ObjectReference(
            api_version="v1",
            kind="Pod",
            name=pod_name,
            namespace=namespace,
            uid=pod.metadata.uid,
        ),
        reason=reason,
        message=message,
        type="Normal",
        first_timestamp=now,
        last_timestamp=now,
        count=1,
        reporting_component="gpu-reservation-controller",
        action=action,
    )
    log.debug("%s", kv(
        event="k8s.create_event", ns=namespace, pod=pod_name, reason=reason,
    ))
    await _run(_core_v1.create_namespaced_event, namespace, event)


async def emit_runtime_guaranteed_event(
    pod,
    pod_name: str,
    namespace: str,
    seconds: int,
    guaranteed_until: datetime,
) -> None:
    """Create a Kubernetes Event linked to *pod* with reason='RuntimeGuaranteed'."""
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    human = (
        f"{hours}h{minutes:02d}m{secs:02d}s"
        if hours
        else f"{minutes}m{secs:02d}s"
    )
    until_str = guaranteed_until.strftime("%Y-%m-%dT%H:%M:%SZ")
    await _emit_pod_event(
        pod,
        pod_name,
        namespace,
        name_prefix="gpu-guarantee-",
        reason="RuntimeGuaranteed",
        action="GuaranteeRuntime",
        message=(
            f"GPU access guaranteed for {human}, until {until_str}. The pod may "
            f"keep running after that, but can be preempted if reserved capacity "
            f"is needed."
        ),
    )
    log.info("%s", kv(
        event="k8s.event_emitted", ns=namespace, pod=pod_name,
        reason="RuntimeGuaranteed", guarantee_s=seconds, until=until_str,
    ))


async def delete_pod(name: str, namespace: str) -> None:
    """Delete a pod by name and namespace.

    A 404 response is silently ignored — the pod may have already been removed
    by the time the controller processes the cancellation.
    """
    log.debug("%s", kv(event="k8s.delete_pod", ns=namespace, pod=name))
    try:
        await _run(_core_v1.delete_namespaced_pod, name, namespace)
        log.info("%s", kv(event="pod.deleted", ns=namespace, pod=name))
    except ApiException as exc:
        if exc.status == 404:
            log.debug("%s", kv(event="pod.already_gone", ns=namespace, pod=name, status=404))
            return
        raise


async def emit_preempted_event(
    pod,
    pod_name: str,
    namespace: str,
    message: str,
) -> None:
    """Create a Kubernetes Event linked to *pod* with reason='Preempted'.

    Emitted when the preemption sweep deletes a pod running past its runtime
    guarantee to recover capacity.  The caller builds *message* (it knows how
    long the pod overstayed).
    """
    await _emit_pod_event(
        pod,
        pod_name,
        namespace,
        name_prefix="gpu-preempt-",
        reason="Preempted",
        action="PreemptPod",
        message=message,
    )
    log.info("%s", kv(
        event="k8s.event_emitted", ns=namespace, pod=pod_name, reason="Preempted",
    ))


async def emit_reservation_cancelled_event(
    pod,
    pod_name: str,
    namespace: str,
    cancelled_by_desc: str,
) -> None:
    """Create a Kubernetes Event linked to *pod* with reason='ReservationCancelled'."""
    await _emit_pod_event(
        pod,
        pod_name,
        namespace,
        name_prefix="gpu-rescancel-",
        reason="ReservationCancelled",
        action="EvictPod",
        message=f"Pod evicted: GPU reservation cancelled {cancelled_by_desc}.",
    )
    log.info("%s", kv(
        event="k8s.event_emitted", ns=namespace, pod=pod_name, reason="ReservationCancelled",
    ))


async def emit_reservation_reassigned_event(
    pod,
    pod_name: str,
    namespace: str,
    new_owner_desc: str,
) -> None:
    """Create a Kubernetes Event linked to *pod* with reason='ReservationReassigned'.

    Emitted when a reservation is reassigned to a new owner ("adoption") and the
    prior owner's admitted pod is evicted so the new owner can claim the window.
    """
    await _emit_pod_event(
        pod,
        pod_name,
        namespace,
        name_prefix="gpu-resadopt-",
        reason="ReservationReassigned",
        action="EvictPod",
        message=f"Pod evicted: GPU reservation reassigned {new_owner_desc}.",
    )
    log.info("%s", kv(
        event="k8s.event_emitted", ns=namespace, pod=pod_name, reason="ReservationReassigned",
    ))


async def emit_overstay_relinked_event(
    pod,
    pod_name: str,
    namespace: str,
    reservation_id: int,
    guaranteed_until: datetime,
) -> None:
    """Create a Kubernetes Event linked to *pod* with reason='OverstayRelinked'.

    Emitted when an overstay pod (running past its runtime guarantee) is re-linked
    to a reservation the same user has since booked, so it is no longer treated as
    overstay.  Distinct from ReservationReassigned (which evicts a pod on an
    owner change) — here the *same* pod keeps running under a new reservation id.
    """
    until_str = guaranteed_until.strftime("%Y-%m-%dT%H:%M:%SZ")
    await _emit_pod_event(
        pod,
        pod_name,
        namespace,
        name_prefix="gpu-relink-",
        reason="OverstayRelinked",
        action="RelinkPod",
        message=(
            f"Pod re-linked to GPU reservation #{reservation_id}; no longer "
            f"overstay. GPU access guaranteed until {until_str}."
        ),
    )
    log.info("%s", kv(
        event="k8s.event_emitted", ns=namespace, pod=pod_name,
        reason="OverstayRelinked", rid=reservation_id, until=until_str,
    ))


# ---------------------------------------------------------------------------
# Pod event stream
# ---------------------------------------------------------------------------


# Watch-loop tuning.  Module constants (not env vars) like the values they
# replace; the PodWatcher constructor takes keyword overrides as test seams.
_WATCH_TIMEOUT_S = 270          # server-side clean close, beats LB idle timeouts
_WATCH_RESYNC_INTERVAL_S = 600  # full re-LIST cadence (routing self-heal)
_WATCH_QUEUE_MAXSIZE = 4096     # backpressure bound; must exceed labeled-pod count
_WATCH_RETRY_DELAY_S = 5        # fixed backoff between failed cycles


class PodWatcher:
    """Async-generator source of pod watch events for all namespaces.

    Uses a background thread running ``kubernetes.watch.Watch`` (which issues
    a long-poll HTTP stream) and forwards events to the asyncio event loop via
    ``loop.call_soon_threadsafe``.  The watch thread automatically reconnects
    after any error.

    Reconnect protocol: the thread tracks the ``resourceVersion`` of every
    event (bookmarks included, requested via ``allow_watch_bookmarks``), so a
    clean stream close — the server honouring ``timeout_seconds`` every ~4.5
    min — *resumes* the watch where it left off instead of re-LISTing and
    replaying the world as ADDED.  A full LIST+replay happens only at start,
    after an error, on HTTP 410 (resourceVersion expired — immediate, no
    backoff), and every ``_WATCH_RESYNC_INTERVAL_S`` as a deliberate resync:
    the periodic replay is the self-heal for a pod whose ADDED was never seen
    (nothing else discovers unrouted pods), and consumers treat replays
    idempotently (see the fast-path cooldown guard, B8).

    The event queue is bounded; if the consumer stalls, the *oldest* event is
    dropped (newest state supersedes it, and the next resync heals the gap)
    rather than growing without limit.

    Usage::

        watcher = PodWatcher(label_selector="gpu-class")
        async for event_type, pod in watcher.events():
            ...  # event_type is "ADDED", "MODIFIED", or "DELETED"
    """

    def __init__(
        self,
        label_selector: str = "gpu-class",
        *,
        resync_interval_s: float = _WATCH_RESYNC_INTERVAL_S,
        queue_maxsize: int = _WATCH_QUEUE_MAXSIZE,
        watch_timeout_s: int = _WATCH_TIMEOUT_S,
        retry_delay_s: float = _WATCH_RETRY_DELAY_S,
    ) -> None:
        self._label_selector = label_selector
        self._resync_interval_s = resync_interval_s
        self._queue_maxsize = queue_maxsize
        self._watch_timeout_s = watch_timeout_s
        self._retry_delay_s = retry_delay_s
        self._watch: Optional[watch.Watch] = None
        self._dropped = 0

    def _log_watch_failure(self, fail_count: int, exc: Exception) -> None:
        """Throttled failure logging: WARNING on the first failure and every
        120th (~10 min at the 5 s retry), DEBUG in between."""
        if fail_count == 1 or fail_count % 120 == 0:
            log.warning("%s", kv(
                event="k8s.watch_error", fails=fail_count, err=exc,
                retry_s=self._retry_delay_s,
            ))
        else:
            log.debug("%s", kv(
                event="k8s.watch_ended", err=exc, retry_s=self._retry_delay_s,
            ))

    async def events(self) -> AsyncIterator[tuple[str, object]]:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue(
            maxsize=self._queue_maxsize
        )
        stop_event = threading.Event()

        def _offer(item: tuple[str, object]) -> None:
            # Runs on the event-loop thread only (always via
            # call_soon_threadsafe), so the full/drop/put triple cannot
            # interleave with the consumer's get.
            if queue.full():
                try:
                    queue.get_nowait()  # drop the OLDEST event
                except asyncio.QueueEmpty:  # pragma: no cover — full ⇒ non-empty
                    pass
                self._dropped += 1
                if self._dropped == 1 or self._dropped % 100 == 0:
                    log.warning("%s", kv(
                        event="k8s.watch_dropped", dropped=self._dropped,
                    ))
            queue.put_nowait(item)

        def _run_watch() -> None:
            """Blocking LIST/WATCH loop; runs in a dedicated daemon thread."""
            fail_count = 0
            rv: Optional[str] = None    # None ⇒ the next cycle must LIST
            relist_reason = "start"
            last_list = 0.0             # time.monotonic() of the last LIST
            while not stop_event.is_set():
                try:
                    resync_due = rv is not None and (
                        time.monotonic() - last_list >= self._resync_interval_s
                    )
                    if rv is None or resync_due:
                        if resync_due:
                            relist_reason = "resync"
                        # LIST — surfaces pods that exist before we started (or
                        # whose events a gap swallowed) by replaying them as ADDED.
                        log.debug("%s", kv(
                            event="k8s.list_pods", selector=self._label_selector,
                            purpose="watch_seed", reason=relist_reason,
                        ))
                        pod_list = _core_v1.list_pod_for_all_namespaces(
                            label_selector=self._label_selector
                        )
                        log.debug("%s", kv(
                            event="k8s.list_pods_done", purpose="watch_seed",
                            count=len(pod_list.items),
                            rv=pod_list.metadata.resource_version,
                        ))
                        for pod in pod_list.items:
                            loop.call_soon_threadsafe(_offer, ("ADDED", pod))
                        rv = pod_list.metadata.resource_version
                        last_list = time.monotonic()
                        mode = "seed"
                    else:
                        # Clean close with a live rv: resume exactly where the
                        # last stream left off — no LIST, no replay.
                        mode = "resume"

                    w = watch.Watch()
                    self._watch = w
                    # WATCH — stream incremental events from rv onward.
                    # timeout_seconds asks the API server to close the stream
                    # cleanly before any proxy/LB idle timeout fires; it also
                    # disables the client's internal silent retry, which is what
                    # guarantees a 410 surfaces here as ApiException rather than
                    # being swallowed (do not remove it).
                    log.debug("%s", kv(
                        event="k8s.watch_open", selector=self._label_selector,
                        rv=rv, timeout_s=self._watch_timeout_s, mode=mode,
                    ))
                    progressed = False
                    for event in w.stream(
                        _core_v1.list_pod_for_all_namespaces,
                        label_selector=self._label_selector,
                        resource_version=rv,
                        timeout_seconds=self._watch_timeout_s,
                        allow_watch_bookmarks=True,
                    ):
                        if stop_event.is_set():
                            w.stop()
                            return
                        if not isinstance(event, dict):
                            # Newer clients can yield None for undecodable lines.
                            continue
                        if not progressed:
                            # The stream demonstrably works — only now reset the
                            # failure counter.  (Resetting right after the LIST,
                            # as before, defeated the throttle whenever the LIST
                            # succeeded but the stream kept failing.)
                            progressed = True
                            fail_count = 0
                        if event.get("type") == "BOOKMARK":
                            # Bookmark objects are raw dicts (never deserialized,
                            # so no model attributes); they exist purely to
                            # advance rv and are not forwarded to the consumer.
                            new_rv = (
                                (event.get("raw_object") or {})
                                .get("metadata", {})
                                .get("resourceVersion")
                            )
                            if new_rv:
                                rv = new_rv
                            log.debug("%s", kv(event="k8s.watch_bookmark", rv=rv))
                            continue
                        obj = event["object"]
                        new_rv = getattr(
                            getattr(obj, "metadata", None), "resource_version", None
                        )
                        if new_rv:
                            rv = new_rv
                        log.debug("%s", kv(
                            event="k8s.watch_event", watch_event=event["type"],
                            ns=obj.metadata.namespace, pod=obj.metadata.name,
                        ))
                        loop.call_soon_threadsafe(_offer, (event["type"], obj))
                    # Clean close: the server honoured timeout_seconds.  A
                    # healthy cycle even if it carried no events.
                    fail_count = 0

                except ApiException as exc:
                    if exc.status == 410:
                        # resourceVersion expired — the standard protocol signal
                        # to re-LIST immediately.  Not a fault: no backoff, no
                        # failure count.
                        log.info("%s", kv(event="k8s.watch_expired", rv=rv))
                        rv = None
                        relist_reason = "expired"
                        continue
                    fail_count += 1
                    rv = None  # rv provenance is not trustworthy after a failure
                    relist_reason = "error"
                    self._log_watch_failure(fail_count, exc)
                    # Interruptible sleep: a set stop_event returns immediately.
                    if stop_event.wait(self._retry_delay_s):
                        return
                except Exception as exc:  # noqa: BLE001
                    fail_count += 1
                    rv = None  # rv provenance is not trustworthy after a failure
                    relist_reason = "error"
                    self._log_watch_failure(fail_count, exc)
                    # Interruptible sleep: a set stop_event returns immediately.
                    if stop_event.wait(self._retry_delay_s):
                        return

        # Run the blocking watch in a dedicated daemon thread rather than the
        # shared default executor: it never returns on its own, so it must not
        # occupy an executor slot every other K8s call contends for, and as a
        # daemon it cannot hang interpreter exit / test teardown.  The stop_event
        # lets us unwind the reconnect loop promptly when the consumer stops
        # iterating (B7).
        thread = threading.Thread(target=_run_watch, name="pod-watch", daemon=True)
        thread.start()

        try:
            while True:
                event_type, pod = await queue.get()
                yield event_type, pod
        finally:
            stop_event.set()
            # Best-effort prompt unblock: on newer kubernetes clients
            # Watch.stop() shuts the socket down and the thread exits at once;
            # on v29 it is a flag write and a quiet stream parks the thread
            # until the server timeout — harmless, it is a daemon thread.
            w = self._watch
            if w is not None:
                try:
                    w.stop()
                except Exception:  # noqa: BLE001 — teardown must never raise
                    pass


# ---------------------------------------------------------------------------
# Singleton lease — duplicate-instance guard (not leader election)
# ---------------------------------------------------------------------------

#: Name of the coordination Lease every controller instance contends for.  One
#: controller per namespace: the chart deploys exactly one release per
#: namespace, so a fixed name needs no configuration.
LEASE_NAME = "gpu-reservation-controller"


@dataclass(frozen=True)
class LeaseOutcome:
    """Result of a singleton-lease acquire/renew attempt.

    Errors are returned, never raised: every caller's response to an API
    failure is the same fail-open "keep running and retry", and a result type
    makes that impossible to forget.
    """

    status: str  # "acquired" | "held_by_other" | "lost" | "error"
    holder: Optional[str] = None  # the other instance's identity, when known
    age_s: Optional[int] = None   # seconds since the holder last renewed
    mode: Optional[str] = None    # "created" | "reacquired" | "takeover" | "renewed"
    err: Optional[str] = None


def _lease_age_seconds(lease, now: datetime) -> Optional[int]:
    """Seconds since *lease* was last renewed, or None if it never was."""
    renewed = getattr(lease.spec, "renew_time", None)
    if renewed is None:
        return None
    if renewed.tzinfo is None:  # defensive: the client returns aware datetimes
        renewed = renewed.replace(tzinfo=timezone.utc)
    return int((now - renewed).total_seconds())


def _lease_body(holder: str, duration_s: int, now: datetime, *, acquire: bool,
                transitions: int = 0):
    """Build a Lease object claiming (or renewing) the singleton lock."""
    spec = k8s_client.V1LeaseSpec(
        holder_identity=holder,
        lease_duration_seconds=duration_s,
        renew_time=now,
        lease_transitions=transitions,
    )
    if acquire:
        spec.acquire_time = now
    return k8s_client.V1Lease(
        metadata=k8s_client.V1ObjectMeta(name=LEASE_NAME), spec=spec
    )


async def acquire_singleton_lease(
    namespace: str, holder: str, duration_s: int
) -> LeaseOutcome:
    """Claim the singleton Lease for *holder*, or report who holds it.

    Returns ``held_by_other`` only when another identity holds an **unexpired**
    lease — the affirmative "a second controller is running" signal.  An
    expired lease is taken over (the previous holder crashed), and our own
    lease is simply refreshed, so a container restart of the same pod recovers
    immediately rather than waiting out the duration.
    """
    now = datetime.now(timezone.utc)
    try:
        try:
            lease = await _run(
                _coordination_v1.read_namespaced_lease, LEASE_NAME, namespace
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
            await _run(
                _coordination_v1.create_namespaced_lease,
                namespace,
                _lease_body(holder, duration_s, now, acquire=True),
            )
            log.debug("%s", kv(event="k8s.lease_write", name=LEASE_NAME, mode="created"))
            return LeaseOutcome(status="acquired", holder=holder, mode="created")

        current = getattr(lease.spec, "holder_identity", None)
        age = _lease_age_seconds(lease, now)
        expiry = getattr(lease.spec, "lease_duration_seconds", None) or duration_s
        transitions = getattr(lease.spec, "lease_transitions", 0) or 0

        if current == holder:
            mode = "reacquired"
        elif age is None or age > expiry:
            mode = "takeover"
            transitions += 1
        else:
            return LeaseOutcome(status="held_by_other", holder=current, age_s=age)

        await _run(
            _coordination_v1.replace_namespaced_lease,
            LEASE_NAME,
            namespace,
            _lease_body(holder, duration_s, now, acquire=True, transitions=transitions),
        )
        log.debug("%s", kv(event="k8s.lease_write", name=LEASE_NAME, mode=mode))
        return LeaseOutcome(
            status="acquired", holder=holder, mode=mode,
            age_s=age if mode == "takeover" else None,
        )
    except Exception as exc:  # noqa: BLE001 — fail open, the caller keeps running
        return LeaseOutcome(status="error", err=str(exc))


async def renew_singleton_lease(
    namespace: str, holder: str, duration_s: int
) -> LeaseOutcome:
    """Refresh our hold on the Lease.

    ``lost`` means another live instance has taken the lease from us — the
    caller must terminate.  A transient API failure is ``error``: the caller
    keeps running and retries, since a renewal blip is not evidence of a
    duplicate.
    """
    now = datetime.now(timezone.utc)
    try:
        lease = await _run(
            _coordination_v1.read_namespaced_lease, LEASE_NAME, namespace
        )
        current = getattr(lease.spec, "holder_identity", None)
        if current is not None and current != holder:
            age = _lease_age_seconds(lease, now)
            expiry = getattr(lease.spec, "lease_duration_seconds", None) or duration_s
            if age is not None and age <= expiry:
                return LeaseOutcome(status="lost", holder=current, age_s=age)
        transitions = getattr(lease.spec, "lease_transitions", 0) or 0
        await _run(
            _coordination_v1.replace_namespaced_lease,
            LEASE_NAME,
            namespace,
            _lease_body(holder, duration_s, now, acquire=False, transitions=transitions),
        )
        log.debug("%s", kv(event="k8s.lease_write", name=LEASE_NAME, mode="renewed"))
        return LeaseOutcome(status="acquired", holder=holder, mode="renewed")
    except Exception as exc:  # noqa: BLE001 — fail open, the caller keeps running
        return LeaseOutcome(status="error", err=str(exc))
