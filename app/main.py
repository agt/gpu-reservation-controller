"""GPU Reservation Kubernetes Controller — entry point.

Starts three background asyncio tasks inside a FastAPI lifespan:

1. reservation_fetch_loop  — periodically refreshes the reservation list
2. pod_watch_loop          — streams pod events and updates the work queue
3. queue_processor_loop    — applies tolerations when reservation windows open

Additionally, when a pod is detected arriving *inside* an already-open
reservation window (e.g. a JupyterHub notebook pod), the pod-watch loop
bypasses the 30-second queue-processor polling interval and attempts to
apply the toleration immediately, minimising scheduler delay for the user.

A minimal GET /health endpoint allows Kubernetes liveness probes to verify
the process is alive.
"""

from __future__ import annotations

import asyncio
import logging
import random
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncIterator

from fastapi import FastAPI

from .config import Config
from .controller import (
    TOLERATION_KEY,
    ControllerState,
    QueueEntry,
    slot_end,
    slot_start,
)
from .k8s_client import (
    PodWatcher,
    apply_toleration,
    count_tolerated_gpu_usage,
    get_pod_gpu_count,
    init_k8s,
    pod_has_toleration,
    read_pod,
)
from .reservation_client import ReservationClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _refresh_reservations(
    state: ControllerState, client: ReservationClient
) -> None:
    """Fetch the current reservation list and update shared state.

    Also resolves gpu_class_id → label_value for every GPU class referenced
    in the fetched reservations (results are cached within this cycle; the
    cache is rebuilt from scratch each cycle so stale entries don't linger).
    After updating the reservations, reconciles the task queue.
    """
    reservations = await client.fetch_reservations()

    # Resolve label_value for each unique GPU class in this batch.
    class_ids = {r.gpu_class_id for r in reservations}
    new_labels: dict[int, str] = {}
    for cid in class_ids:
        # Re-use cached value if we already know it.
        cached = state.gpu_class_labels.get(cid)
        if cached is not None:
            new_labels[cid] = cached
            continue
        gpu_class = await client.fetch_gpu_class(cid)
        if gpu_class and gpu_class.label_value:
            new_labels[cid] = gpu_class.label_value
            log.info("GPU class %d (%s) → label_value=%r", cid, gpu_class.name, gpu_class.label_value)
        else:
            log.warning(
                "GPU class %d has no label_value; pods for this class "
                "cannot be matched to reservations",
                cid,
            )

    state.reservations = reservations
    state.gpu_class_labels = new_labels

    # Drop / re-match queue entries whose reservation was cancelled.
    state.reconcile_queue()


# ---------------------------------------------------------------------------
# Toleration applicator (shared by the queue processor and the fast path)
# ---------------------------------------------------------------------------


async def _try_apply_toleration(
    state: ControllerState, uid: str, entry: QueueEntry
) -> bool:
    """Check GPU budget and patch the toleration onto the pod if eligible.

    Returns ``True``  — entry should be removed from the queue (toleration
                        applied, or the pod already carried it).
    Returns ``False`` — entry should remain; ``entry.next_attempt_at`` has
                        been pushed forward (budget full or transient error).

    **Does not** evaluate timing (window open/closed, retry cooldown); callers
    are responsible for those guards before invoking this function.
    """
    try:
        other_gpus = await count_tolerated_gpu_usage(
            namespace=entry.pod_namespace,
            label_selector=f"gpu-class={entry.gpu_class_label}",
            tol_key=TOLERATION_KEY,
            tol_value=entry.gpu_class_label,
            exclude_uid=uid,
        )

        total_requested = entry.gpu_requested + other_gpus
        if total_requested <= entry.reservation.gpu_count:
            # Re-fetch the pod immediately before patching so we include any
            # tolerations that arrived since we last saw it.
            fresh_pod = await read_pod(entry.pod_name, entry.pod_namespace)

            if pod_has_toleration(
                fresh_pod, TOLERATION_KEY, entry.gpu_class_label, "NoSchedule"
            ):
                log.info(
                    "Pod %s/%s already has toleration; dequeuing",
                    entry.pod_namespace,
                    entry.pod_name,
                )
            else:
                await apply_toleration(
                    entry.pod_name,
                    entry.pod_namespace,
                    fresh_pod,
                    TOLERATION_KEY,
                    entry.gpu_class_label,
                )
            return True

        else:
            delay = random.randint(120, 300)
            log.debug(
                "Pod %s/%s: GPU budget full "
                "(%d requested + %d in use > %d reserved); retry in %d s",
                entry.pod_namespace,
                entry.pod_name,
                entry.gpu_requested,
                other_gpus,
                entry.reservation.gpu_count,
                delay,
            )
            entry.next_attempt_at = datetime.now() + timedelta(seconds=delay)
            return False

    except Exception as exc:  # noqa: BLE001
        delay = random.randint(120, 300)
        log.warning(
            "Error processing pod %s/%s: %s; retry in %d s",
            entry.pod_namespace,
            entry.pod_name,
            exc,
            delay,
        )
        entry.next_attempt_at = datetime.now() + timedelta(seconds=delay)
        return False


# ---------------------------------------------------------------------------
# Background loop 1: reservation refresh
# ---------------------------------------------------------------------------


async def reservation_fetch_loop(
    state: ControllerState, client: ReservationClient, interval: int
) -> None:
    """Re-fetch reservations every *interval* seconds.

    The initial fetch is done synchronously in the lifespan before this loop
    starts, so we sleep first and then enter the refresh–sleep cycle.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            await _refresh_reservations(state, client)
        except Exception as exc:  # noqa: BLE001
            log.error("Reservation refresh failed: %s", exc)


# ---------------------------------------------------------------------------
# Background loop 2: pod watch
# ---------------------------------------------------------------------------


async def pod_watch_loop(state: ControllerState) -> None:
    """Stream pod events and update the task queue accordingly.

    - ADDED / MODIFIED with gpu-class label and no toleration → enqueue
    - ADDED / MODIFIED with gpu-class label and toleration already present
      → dequeue (toleration applied externally or by a previous controller run)
    - DELETED → dequeue

    **Fast path:** when a pod ADDED event arrives while its reservation window
    is already open (e.g. a JupyterHub pod launched mid-window), the toleration
    is attempted immediately rather than waiting up to 30 s for the next
    queue-processor tick.  MODIFIED events respect the normal retry cadence so
    a rapid burst of updates doesn't hammer the Kubernetes API.
    """
    watcher = PodWatcher(label_selector="gpu-class")
    async for event_type, pod in watcher.events():
        uid: str = pod.metadata.uid
        name: str = pod.metadata.name
        namespace: str = pod.metadata.namespace
        labels: dict = pod.metadata.labels or {}
        gpu_class_label: str | None = labels.get("gpu-class")

        if not gpu_class_label:
            # Label key present but value is empty string — skip.
            continue

        if event_type == "DELETED":
            state.dequeue_pod(uid)

        elif event_type in ("ADDED", "MODIFIED"):
            if pod_has_toleration(pod, TOLERATION_KEY, gpu_class_label, "NoSchedule"):
                # Pod already has the toleration — remove from queue if present.
                state.dequeue_pod(uid)
            else:
                gpu_count = get_pod_gpu_count(pod)
                state.enqueue_pod(uid, name, namespace, gpu_class_label, gpu_count)

                # Fast path: ADDED pod inside an open window — don't wait for
                # the queue processor's 30-second polling interval.
                if event_type == "ADDED":
                    entry = state.task_queue.get(uid)
                    if entry is not None:
                        now = datetime.now()
                        if slot_start(entry.reservation) <= now < slot_end(entry.reservation):
                            log.info(
                                "Pod %s/%s arrived inside reservation window; "
                                "attempting immediate toleration",
                                namespace,
                                name,
                            )
                            if await _try_apply_toleration(state, uid, entry):
                                state.task_queue.pop(uid, None)


# ---------------------------------------------------------------------------
# Background loop 3: queue processor
# ---------------------------------------------------------------------------


async def queue_processor_loop(state: ControllerState) -> None:
    """Every 30 s, scan the work queue and apply tolerations where eligible.

    Per-entry logic:
    1. If the reservation window has expired → remove from queue.
    2. If the window hasn't opened yet, or the entry is in retry cooldown → skip.
    3. Delegate budget check + patch to ``_try_apply_toleration``.

    Note: pods that arrive *inside* an open window are handled immediately by
    the pod-watch loop fast path and typically won't reach this loop at all.
    This loop covers pods that were queued before the window opened, and retries
    for pods that were ineligible (budget full) on a previous attempt.
    """
    while True:
        await asyncio.sleep(30)
        now = datetime.now()
        to_remove: list[str] = []

        for uid, entry in list(state.task_queue.items()):
            start = slot_start(entry.reservation)
            end = slot_end(entry.reservation)

            # --- window expired ---
            if now > end:
                log.info(
                    "Reservation #%d window expired; removing pod %s/%s from queue",
                    entry.reservation.id,
                    entry.pod_namespace,
                    entry.pod_name,
                )
                to_remove.append(uid)
                continue

            # --- window not yet open, or still in retry cooldown ---
            if now < start or now < entry.next_attempt_at:
                continue

            # --- window is active: attempt to apply the toleration ---
            if await _try_apply_toleration(state, uid, entry):
                to_remove.append(uid)

        for uid in to_remove:
            state.task_queue.pop(uid, None)


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    config: Config = app.state.config  # injected in create_app()
    client = ReservationClient(config)
    state = ControllerState()

    # Initialise Kubernetes client.
    init_k8s(config.kubeconfig_path)

    # Perform the first reservation fetch synchronously so that the pod-watch
    # loop has data to match against from the moment it starts.
    log.info("Performing initial reservation fetch…")
    try:
        await _refresh_reservations(state, client)
        log.info(
            "Initial fetch complete: %d reservation(s), %d GPU class(es) resolved",
            len(state.reservations),
            len(state.gpu_class_labels),
        )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Initial reservation fetch failed (%s); controller will retry "
            "in %d s, pod matching may be delayed",
            exc,
            config.reservation_fetch_interval,
        )

    # Launch the three background loops as asyncio tasks.
    tasks = [
        asyncio.create_task(
            reservation_fetch_loop(state, client, config.reservation_fetch_interval),
            name="reservation-fetch",
        ),
        asyncio.create_task(pod_watch_loop(state), name="pod-watch"),
        asyncio.create_task(queue_processor_loop(state), name="queue-processor"),
    ]
    log.info("GPU reservation controller started")

    try:
        yield
    finally:
        log.info("Shutting down GPU reservation controller…")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("Controller stopped")


def create_app() -> FastAPI:
    config = Config.from_env()
    app = FastAPI(
        title="GPU Reservation Controller",
        description="Applies GPU reservation tolerations to Kubernetes pods",
        lifespan=lifespan,
    )
    app.state.config = config
    return app


app = create_app()


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


@app.get("/health", tags=["ops"])
async def health() -> dict:
    """Liveness probe — returns 200 OK when the process is running."""
    return {"status": "ok"}
