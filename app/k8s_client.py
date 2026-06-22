"""Kubernetes API wrapper for the GPU reservation controller.

All blocking kubernetes-client calls run in a thread-pool executor so they
never stall the asyncio event loop.

Public surface
--------------
init_k8s(kubeconfig_path)                    — load credentials once at startup
get_pod_gpu_count(pod)                       — sum nvidia.com/gpu requests
get_pod_booking_reference(pod)               — read horai/booking-reference annotation
parse_booking_reference(ref)                 — reservation id from a booking-reference
pod_has_toleration(pod, ...)                 — check for a specific toleration
is_gpu_only_pending(pod, toleration_key)     — guard 1: GPU-only scheduling failure check
read_pod(name, namespace)                    — fetch current pod object
snapshot_tolerated_pods(tol_key)             — one LIST → occupancy + claims + guard 3
apply_toleration(...)                        — PATCH a pod to add toleration + booking annotation
PodWatcher                                   — async-generator based pod event stream
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

from kubernetes import client as k8s_client, config as k8s_config, watch

log = logging.getLogger(__name__)

# Initialised once by init_k8s(); used by all functions in this module.
_core_v1: Optional[k8s_client.CoreV1Api] = None


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def init_k8s(kubeconfig_path: Optional[str]) -> None:
    """Load Kubernetes credentials and create the CoreV1Api client."""
    global _core_v1
    if kubeconfig_path:
        k8s_config.load_kube_config(config_file=kubeconfig_path)
        log.info("Kubernetes: loaded kubeconfig from %s", kubeconfig_path)
    else:
        k8s_config.load_incluster_config()
        log.info("Kubernetes: using in-cluster service-account credentials")
    _core_v1 = k8s_client.CoreV1Api()


# ---------------------------------------------------------------------------
# Pure helpers (synchronous, no I/O)
# ---------------------------------------------------------------------------


def get_pod_phase(pod) -> str:
    """Return the pod's phase string (e.g. "Pending", "Running", "Succeeded", "Failed").

    Returns an empty string if the phase is unavailable.
    """
    return (pod.status.phase if pod.status else None) or ""


def get_pod_min_runtime_seconds(pod) -> Optional[int]:
    """Read the ``horai/minimum-runtime-seconds`` annotation from *pod*.

    Returns the integer value if the annotation is present and parseable as a
    positive integer, or ``None`` otherwise.
    """
    annotations: dict = pod.metadata.annotations or {}
    raw = annotations.get("horai/minimum-runtime-seconds")
    if raw is None:
        return None
    try:
        value = int(raw)
        return value if value > 0 else None
    except (ValueError, TypeError):
        log.warning(
            "Pod %s/%s has non-integer horai/minimum-runtime-seconds=%r; ignoring",
            pod.metadata.namespace,
            pod.metadata.name,
            raw,
        )
        return None


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
    """Return the ``horai/booking-reference`` annotation value, or ``None``."""
    annotations: dict = pod.metadata.annotations or {}
    return annotations.get("horai/booking-reference")


# Prefixes used in horai/booking-reference values.  All three embed the
# reservation id that the pod was admitted under; the prefix records which path
# admitted it (reserved / on-demand / no-show) and is otherwise cosmetic.
_BOOKING_REFERENCE_PREFIXES = ("res-", "ondemand-", "noshow-")


def parse_booking_reference(reference: Optional[str]) -> Optional[int]:
    """Extract the reservation id embedded in a ``horai/booking-reference`` value.

    ``"res-42"`` / ``"ondemand-42"`` / ``"noshow-42"`` all return ``42``.
    Returns ``None`` for an unrecognised prefix, a non-integer suffix, or
    ``None``/empty input.  This is the single key used to reconstruct occupancy
    from the cluster, so it must accept every prefix ``apply_toleration`` writes.
    """
    if not reference:
        return None
    for prefix in _BOOKING_REFERENCE_PREFIXES:
        if reference.startswith(prefix):
            try:
                return int(reference[len(prefix):])
            except ValueError:
                return None
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


def is_gpu_only_pending(pod, toleration_key: str) -> Optional[bool]:
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


async def read_pod(name: str, namespace: str):
    """Fetch the current pod object (re-read before patching to get fresh state)."""
    log.debug("k8s: read_namespaced_pod %s/%s", namespace, name)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, lambda: _core_v1.read_namespaced_pod(name, namespace)
    )


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


async def snapshot_tolerated_pods(toleration_key: str) -> list[ToleratedPodInfo]:
    """Return one ``ToleratedPodInfo`` per pod carrying a *toleration_key* toleration.

    Scans all ``gpu-class``-labelled pods (the same selector as ``PodWatcher`` —
    no new RBAC permission) and shapes each admitted pod into the fields the
    controller needs.  One snapshot per queue-processor tick drives occupancy
    reconstruction, the claimed-reservation set, and guard 3, replacing the
    former per-attempt namespaced counts and the separate guard scans.
    """
    loop = asyncio.get_running_loop()
    log.debug(
        "k8s: list_pod_for_all_namespaces selector=gpu-class (tolerated snapshot)"
    )
    pod_list = await loop.run_in_executor(
        None,
        lambda: _core_v1.list_pod_for_all_namespaces(label_selector="gpu-class"),
    )
    out: list[ToleratedPodInfo] = []
    for pod in pod_list.items:
        if not _pod_has_any_reservation_toleration(pod, toleration_key):
            continue
        conditions = (pod.status.conditions or []) if pod.status else []
        scheduled = next((c for c in conditions if c.type == "PodScheduled"), None)
        booking = get_pod_booking_reference(pod)
        out.append(
            ToleratedPodInfo(
                namespace=pod.metadata.namespace,
                name=pod.metadata.name,
                uid=pod.metadata.uid,
                gpu_class=(pod.metadata.labels or {}).get("gpu-class", ""),
                booking_reference=booking,
                reservation_id=parse_booking_reference(booking),
                gpu_count=get_pod_gpu_count(pod),
                phase=get_pod_phase(pod),
                scheduled_false=(scheduled is not None and scheduled.status == "False"),
            )
        )
    log.debug("k8s: tolerated snapshot returned %d pod(s)", len(out))
    return out


async def apply_toleration(
    pod_name: str,
    namespace: str,
    pod,
    tol_key: str,
    tol_value: str,
    booking_reference: str,
) -> None:
    """Patch *pod* to add toleration ``tol_key=tol_value:NoSchedule`` and set
    the ``horai/booking-reference`` annotation.

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
        "metadata": {"annotations": {"horai/booking-reference": booking_reference}},
        "spec": {"tolerations": existing + [new_tol]},
    }
    log.debug(
        "k8s: patch_namespaced_pod %s/%s (add toleration %s=%s:NoSchedule, booking=%s)",
        namespace, pod_name, tol_key, tol_value, booking_reference,
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: _core_v1.patch_namespaced_pod(pod_name, namespace, patch),
    )
    log.info(
        "Applied toleration %s=%s:NoSchedule to pod %s/%s (booking=%s)",
        tol_key,
        tol_value,
        namespace,
        pod_name,
        booking_reference,
    )


async def set_active_deadline(pod_name: str, namespace: str, seconds: int) -> None:
    """Patch pod's spec.activeDeadlineSeconds to *seconds* and record the limit
    in the ``horai/pod-runtime-limit-seconds`` annotation."""
    log.debug(
        "k8s: patch_namespaced_pod %s/%s (activeDeadlineSeconds=%d)",
        namespace, pod_name, seconds,
    )
    loop = asyncio.get_running_loop()
    patch = {
        "metadata": {"annotations": {"horai/pod-runtime-limit-seconds": str(seconds)}},
        "spec": {"activeDeadlineSeconds": seconds},
    }
    await loop.run_in_executor(
        None,
        lambda: _core_v1.patch_namespaced_pod(pod_name, namespace, patch),
    )
    log.info(
        "Set activeDeadlineSeconds=%d on pod %s/%s",
        seconds,
        namespace,
        pod_name,
    )


async def emit_runtime_capped_event(
    pod,
    pod_name: str,
    namespace: str,
    deadline_seconds: int,
) -> None:
    """Create a Kubernetes Event linked to *pod* with reason='RuntimeCapped'."""
    now = datetime.now(timezone.utc)
    minutes, secs = divmod(deadline_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    human = (
        f"{hours}h{minutes:02d}m{secs:02d}s"
        if hours
        else f"{minutes}m{secs:02d}s"
    )
    event = k8s_client.CoreV1Event(
        metadata=k8s_client.V1ObjectMeta(
            name=f"gpu-runcap-{pod.metadata.uid}",
            namespace=namespace,
        ),
        involved_object=k8s_client.V1ObjectReference(
            api_version="v1",
            kind="Pod",
            name=pod_name,
            namespace=namespace,
            uid=pod.metadata.uid,
        ),
        reason="RuntimeCapped",
        message=(
            f"activeDeadlineSeconds set to {deadline_seconds} ({human}) "
            f"to ensure the pod terminates within its GPU reservation window(s)."
        ),
        type="Normal",
        first_timestamp=now,
        last_timestamp=now,
        count=1,
        reporting_component="gpu-reservation-controller",
        action="CapRuntime",
    )
    log.debug(
        "k8s: create_namespaced_event %s (pod=%s, reason=RuntimeCapped)", namespace, pod_name
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: _core_v1.create_namespaced_event(namespace, event),
    )
    log.info(
        "Emitted RuntimeCapped event for pod %s/%s (deadline=%ds)",
        namespace,
        pod_name,
        deadline_seconds,
    )


async def delete_pod(name: str, namespace: str) -> None:
    """Delete a pod by name and namespace.

    A 404 response is silently ignored — the pod may have already been removed
    by the time the controller processes the cancellation.
    """
    log.debug("k8s: delete_namespaced_pod %s/%s", namespace, name)
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: _core_v1.delete_namespaced_pod(name, namespace),
        )
        log.info("Deleted pod %s/%s", namespace, name)
    except Exception as exc:
        if getattr(getattr(exc, "status", None), "__class__", None) is not None:
            status = getattr(exc, "status", None)
            if status == 404:
                log.debug("Pod %s/%s already gone (404)", namespace, name)
                return
        raise


async def emit_reservation_cancelled_event(
    pod,
    pod_name: str,
    namespace: str,
    cancelled_by_desc: str,
) -> None:
    """Create a Kubernetes Event linked to *pod* with reason='ReservationCancelled'."""
    now = datetime.now(timezone.utc)
    event = k8s_client.CoreV1Event(
        metadata=k8s_client.V1ObjectMeta(
            name=f"gpu-rescancel-{pod.metadata.uid}",
            namespace=namespace,
        ),
        involved_object=k8s_client.V1ObjectReference(
            api_version="v1",
            kind="Pod",
            name=pod_name,
            namespace=namespace,
            uid=pod.metadata.uid,
        ),
        reason="ReservationCancelled",
        message=f"Pod evicted: GPU reservation cancelled {cancelled_by_desc}.",
        type="Normal",
        first_timestamp=now,
        last_timestamp=now,
        count=1,
        reporting_component="gpu-reservation-controller",
        action="EvictPod",
    )
    log.debug(
        "k8s: create_namespaced_event %s (pod=%s, reason=ReservationCancelled)",
        namespace,
        pod_name,
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: _core_v1.create_namespaced_event(namespace, event),
    )
    log.info(
        "Emitted ReservationCancelled event for pod %s/%s",
        namespace,
        pod_name,
    )


# ---------------------------------------------------------------------------
# Pod event stream
# ---------------------------------------------------------------------------


class PodWatcher:
    """Async-generator source of pod watch events for all namespaces.

    Uses a background thread running ``kubernetes.watch.Watch`` (which issues
    a long-poll HTTP stream) and forwards events to the asyncio event loop via
    ``loop.call_soon_threadsafe``.  The watch thread automatically reconnects
    after any error.

    Usage::

        watcher = PodWatcher(label_selector="gpu-class")
        async for event_type, pod in watcher.events():
            ...  # event_type is "ADDED", "MODIFIED", or "DELETED"
    """

    def __init__(self, label_selector: str = "gpu-class") -> None:
        self._label_selector = label_selector

    async def events(self) -> AsyncIterator[tuple[str, object]]:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue()

        def _run_watch() -> None:
            """Blocking watch loop; runs in a thread-pool thread."""
            _fail_count = 0
            while True:
                try:
                    w = watch.Watch()

                    # LIST first — surfaces pods that exist before we started.
                    log.debug(
                        "k8s: list_pod_for_all_namespaces selector=%s",
                        self._label_selector,
                    )
                    pod_list = _core_v1.list_pod_for_all_namespaces(
                        label_selector=self._label_selector
                    )
                    log.debug(
                        "k8s: list returned %d pod(s), resourceVersion=%s",
                        len(pod_list.items), pod_list.metadata.resource_version,
                    )
                    for pod in pod_list.items:
                        loop.call_soon_threadsafe(queue.put_nowait, ("ADDED", pod))
                    resource_version = pod_list.metadata.resource_version

                    # WATCH — stream incremental events from where the list left off.
                    # timeout_seconds asks the API server to close the stream cleanly
                    # before any proxy/LB idle timeout fires, avoiding spurious
                    # "Response ended prematurely" errors on reconnect.
                    log.debug(
                        "k8s: watch list_pod_for_all_namespaces selector=%s"
                        " resourceVersion=%s timeout_seconds=270",
                        self._label_selector, resource_version,
                    )
                    _fail_count = 0  # successful LIST+WATCH cycle; reset counter
                    for event in w.stream(
                        _core_v1.list_pod_for_all_namespaces,
                        label_selector=self._label_selector,
                        resource_version=resource_version,
                        timeout_seconds=270,
                    ):
                        obj = event["object"]
                        log.debug(
                            "k8s: watch event %s pod %s/%s",
                            event["type"],
                            obj.metadata.namespace,
                            obj.metadata.name,
                        )
                        loop.call_soon_threadsafe(
                            queue.put_nowait, (event["type"], event["object"])
                        )

                except Exception as exc:  # noqa: BLE001
                    _fail_count += 1
                    if _fail_count == 1 or _fail_count % 120 == 0:
                        log.warning(
                            "Pod watch stream error (failure #%d): %s; "
                            "reconnecting in 5 s",
                            _fail_count,
                            exc,
                        )
                    else:
                        log.debug(
                            "Pod watch stream ended (%s); reconnecting in 5 s", exc
                        )
                    time.sleep(5)

        # Submit the blocking watch to the default thread-pool executor.
        # The thread runs indefinitely (daemon); it will be killed when the
        # process exits.
        loop.run_in_executor(None, _run_watch)

        while True:
            event_type, pod = await queue.get()
            yield event_type, pod
