"""Kubernetes API wrapper for the GPU reservation controller.

All blocking kubernetes-client calls run in a thread-pool executor so they
never stall the asyncio event loop.

Public surface
--------------
init_k8s(kubeconfig_path)          — load credentials once at startup
get_pod_gpu_count(pod)             — sum nvidia.com/gpu requests
pod_has_toleration(pod, ...)       — check for a specific toleration
read_pod(name, namespace)          — fetch current pod object
count_tolerated_gpu_usage(...)     — sum GPU usage of eligible sibling pods
apply_toleration(...)              — PATCH a pod to add one toleration
PodWatcher                         — async-generator based pod event stream
"""

from __future__ import annotations

import asyncio
import logging
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


def pod_has_toleration(pod, key: str, value: str, effect: str) -> bool:
    """Return True if *pod* already carries the named toleration."""
    for t in pod.spec.tolerations or []:
        if t.key == key and t.value == value and t.effect == effect:
            return True
    return False


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
) -> int:
    """Count nvidia.com/gpu already consumed by sibling pods in *namespace*.

    A pod is counted only if it:
    - matches *label_selector*
    - is in Running or Pending phase
    - already carries the toleration ``tol_key=tol_value:NoSchedule``
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
        total += get_pod_gpu_count(pod)
    log.debug(
        "k8s: counted %d tolerated GPU(s) in %s (excluding uid=%s)",
        total, namespace, exclude_uid,
    )
    return total


async def apply_toleration(
    pod_name: str,
    namespace: str,
    pod,
    tol_key: str,
    tol_value: str,
) -> None:
    """Patch *pod* to add toleration ``tol_key=tol_value:NoSchedule``.

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

    patch = {"spec": {"tolerations": existing + [new_tol]}}
    log.debug(
        "k8s: patch_namespaced_pod %s/%s (add toleration %s=%s:NoSchedule)",
        namespace, pod_name, tol_key, tol_value,
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: _core_v1.patch_namespaced_pod(pod_name, namespace, patch),
    )
    log.info(
        "Applied toleration %s=%s:NoSchedule to pod %s/%s",
        tol_key,
        tol_value,
        namespace,
        pod_name,
    )


async def set_active_deadline(pod_name: str, namespace: str, seconds: int) -> None:
    """Patch pod's spec.activeDeadlineSeconds to *seconds*."""
    log.debug(
        "k8s: patch_namespaced_pod %s/%s (activeDeadlineSeconds=%d)",
        namespace, pod_name, seconds,
    )
    loop = asyncio.get_running_loop()
    patch = {"spec": {"activeDeadlineSeconds": seconds}}
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
