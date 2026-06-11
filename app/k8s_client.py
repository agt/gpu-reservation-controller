"""Kubernetes API wrapper for the GPU reservation controller.

All blocking kubernetes-client calls run in a thread-pool executor so they
never stall the asyncio event loop.

Public surface
--------------
init_k8s(kubeconfig_path)                    — load credentials once at startup
get_pod_gpu_count(pod)                       — sum nvidia.com/gpu requests
get_pod_booking_reference(pod)               — read dsmlp/booking-reference annotation
get_pod_ondemand_block_id(pod)               — read dsmlp/ondemand-block-id annotation
pod_has_toleration(pod, ...)                 — check for a specific toleration
is_gpu_only_pending(pod, toleration_key)     — guard 1: GPU-only scheduling failure check
read_pod(name, namespace)                    — fetch current pod object
count_tolerated_gpu_usage(...)               — sum GPU usage of eligible sibling pods
apply_toleration(...)                        — PATCH a pod to add toleration + booking annotation
list_stuck_reservation_holder_pods(tol_key) — guard 3: find admitted pods stuck Pending
PodWatcher                                   — async-generator based pod event stream
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
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
    """Read the ``dsmlp/minimum-runtime-seconds`` annotation from *pod*.

    Returns the integer value if the annotation is present and parseable as a
    positive integer, or ``None`` otherwise.
    """
    annotations: dict = pod.metadata.annotations or {}
    raw = annotations.get("dsmlp/minimum-runtime-seconds")
    if raw is None:
        return None
    try:
        value = int(raw)
        return value if value > 0 else None
    except (ValueError, TypeError):
        log.warning(
            "Pod %s/%s has non-integer dsmlp/minimum-runtime-seconds=%r; ignoring",
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
    """Return the ``dsmlp/booking-reference`` annotation value, or ``None``."""
    annotations: dict = pod.metadata.annotations or {}
    return annotations.get("dsmlp/booking-reference")


# Prefixes used in dsmlp/booking-reference values.  All three embed the
# reservation id that the pod was admitted under; the prefix records which path
# admitted it (reserved / on-demand / no-show) and is otherwise cosmetic.
_BOOKING_REFERENCE_PREFIXES = ("res-", "ondemand-", "noshow-")


def parse_booking_reference(reference: Optional[str]) -> Optional[int]:
    """Extract the reservation id embedded in a ``dsmlp/booking-reference`` value.

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


def get_pod_ondemand_block_id(pod) -> Optional[int]:
    """Return the ``dsmlp/ondemand-block-id`` annotation as an int, or ``None``.

    This annotation is stamped at on-demand placement time so the controller
    can reconstruct ``ondemand_occupancy`` after a restart from the pod LIST.
    """
    annotations: dict = pod.metadata.annotations or {}
    raw = annotations.get("dsmlp/ondemand-block-id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        log.warning(
            "Pod %s/%s has non-integer dsmlp/ondemand-block-id=%r; ignoring",
            pod.metadata.namespace,
            pod.metadata.name,
            raw,
        )
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


async def count_tolerated_gpu_usage(
    namespace: str,
    label_selector: str,
    tol_key: str,
    tol_value: str,
    exclude_uid: str,
    booking_reference: str,
) -> int:
    """Count nvidia.com/gpu already consumed by sibling pods in *namespace*.

    A pod is counted only if it:
    - matches *label_selector*
    - is in Running or Pending phase
    - already carries the toleration ``tol_key=tol_value:NoSchedule``
    - has a ``dsmlp/booking-reference`` annotation equal to *booking_reference*
    - is not the pod identified by *exclude_uid* (the one we're evaluating)
    """
    log.debug(
        "k8s: list_namespaced_pod namespace=%s selector=%s", namespace, label_selector
    )
    loop = asyncio.get_running_loop()
    pod_list = await loop.run_in_executor(
        None,
        lambda: _core_v1.list_namespaced_pod(
            namespace=namespace, label_selector=label_selector
        ),
    )
    total = 0
    for pod in pod_list.items:
        if pod.metadata.uid == exclude_uid:
            continue
        phase = (pod.status.phase if pod.status else None) or ""
        if phase not in ("Running", "Pending"):
            continue
        if not pod_has_toleration(pod, tol_key, tol_value, "NoSchedule"):
            continue
        if get_pod_booking_reference(pod) != booking_reference:
            continue
        total += get_pod_gpu_count(pod)
    log.debug(
        "k8s: counted %d tolerated GPU(s) in %s for booking %s (excluding uid=%s)",
        total, namespace, booking_reference, exclude_uid,
    )
    return total


async def list_stuck_reservation_holder_pods(
    toleration_key: str,
) -> list[tuple[str, str, str]]:
    """Guard 3: find reservation-holder pods that are stuck in Pending.

    A pod is a "stuck reservation holder" when all of the following hold:
    - Has a toleration with key == *toleration_key* (was admitted by this
      controller for a user or on-demand reservation)
    - Phase is "Pending"
    - Has a ``PodScheduled`` condition with ``status == "False"`` (the
      scheduler has determined it cannot be placed)

    Returns a list of ``(namespace, name, gpu_class_label)`` tuples — empty
    if none found.  Pods whose ``gpu-class`` label value is absent or empty
    are skipped; we cannot determine which class they affect.

    Uses ``list_pod_for_all_namespaces`` with ``label_selector="gpu-class"``,
    the same selector as ``PodWatcher`` — no new RBAC permission required.
    """
    loop = asyncio.get_running_loop()
    log.debug("k8s: list_pod_for_all_namespaces selector=gpu-class (guard-3 check)")
    pod_list = await loop.run_in_executor(
        None,
        lambda: _core_v1.list_pod_for_all_namespaces(label_selector="gpu-class"),
    )
    stuck: list[tuple[str, str, str]] = []
    for pod in pod_list.items:
        if get_pod_phase(pod) != "Pending":
            continue
        if not _pod_has_any_reservation_toleration(pod, toleration_key):
            continue
        conditions = (pod.status.conditions or []) if pod.status else []
        scheduled = next((c for c in conditions if c.type == "PodScheduled"), None)
        if scheduled is not None and scheduled.status == "False":
            gpu_class_label = (pod.metadata.labels or {}).get("gpu-class", "")
            if not gpu_class_label:
                log.warning(
                    "Stuck reservation-holder pod %s/%s has no gpu-class label; "
                    "cannot scope interlock — skipping",
                    pod.metadata.namespace,
                    pod.metadata.name,
                )
                continue
            stuck.append((pod.metadata.namespace, pod.metadata.name, gpu_class_label))
    log.debug("k8s: guard-3 found %d stuck reservation-holder pod(s)", len(stuck))
    return stuck


async def list_reservation_holder_pods(toleration_key: str) -> list[int]:
    """Return the booking reservation ids of live reserved-path holder pods.

    A holder pod (a) carries a toleration with key == *toleration_key*, (b) is in
    Running or Pending phase, and (c) has a ``dsmlp/booking-reference`` with the
    ``res-`` prefix.  The ids feed ``refresh_claimed_reservations`` so that every
    window a holder occupies — including back-to-back chained windows it never
    booked a pod directly under — is protected from no-show conversion.

    Uses ``list_pod_for_all_namespaces`` with ``label_selector="gpu-class"``, the
    same selector as ``PodWatcher`` — no new RBAC permission required.  Returns
    raw ids (duplicates possible when a holder runs several pods); the caller
    de-duplicates via set union.
    """
    loop = asyncio.get_running_loop()
    log.debug("k8s: list_pod_for_all_namespaces selector=gpu-class (holder scan)")
    pod_list = await loop.run_in_executor(
        None,
        lambda: _core_v1.list_pod_for_all_namespaces(label_selector="gpu-class"),
    )
    ids: list[int] = []
    for pod in pod_list.items:
        if get_pod_phase(pod) not in ("Running", "Pending"):
            continue
        if not _pod_has_any_reservation_toleration(pod, toleration_key):
            continue
        ref = get_pod_booking_reference(pod)
        if not ref or not ref.startswith("res-"):
            continue
        rid = parse_booking_reference(ref)
        if rid is not None:
            ids.append(rid)
    log.debug("k8s: holder scan found %d reserved-path holder pod(s)", len(ids))
    return ids


async def apply_toleration(
    pod_name: str,
    namespace: str,
    pod,
    tol_key: str,
    tol_value: str,
    booking_reference: str,
    extra_annotations: Optional[dict[str, str]] = None,
) -> None:
    """Patch *pod* to add toleration ``tol_key=tol_value:NoSchedule`` and set
    the ``dsmlp/booking-reference`` annotation.

    *extra_annotations* are merged into the metadata annotations patch.
    The patch preserves all existing tolerations; Kubernetes rejects requests
    that would remove tolerations from running pods.
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

    annotations = {"dsmlp/booking-reference": booking_reference}
    if extra_annotations:
        annotations.update(extra_annotations)
    patch = {
        "metadata": {"annotations": annotations},
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
    in the ``dsmlp/pod-runtime-limit-seconds`` annotation."""
    log.debug(
        "k8s: patch_namespaced_pod %s/%s (activeDeadlineSeconds=%d)",
        namespace, pod_name, seconds,
    )
    loop = asyncio.get_running_loop()
    patch = {
        "metadata": {"annotations": {"dsmlp/pod-runtime-limit-seconds": str(seconds)}},
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
