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
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncIterator

from fastapi import FastAPI

from .config import Config
from .controller import (
    TOLERATION_KEY,
    ControllerState,
    OnDemandCandidate,
    QueueEntry,
    slot_end,
    slot_start,
)
from .k8s_client import (
    PodWatcher,
    apply_toleration,
    count_tolerated_gpu_usage,
    emit_runtime_capped_event,
    get_pod_booking_reference,
    get_pod_gpu_count,
    get_pod_min_runtime_seconds,
    get_pod_phase,
    init_k8s,
    is_gpu_only_pending,
    list_stuck_reservation_holder_pods,
    pod_has_toleration,
    read_pod,
    set_active_deadline,
)
from .reservation_client import ReservationClient

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
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
    # Prune occupancy for on-demand blocks that are no longer active.
    state.reconcile_ondemand()


# ---------------------------------------------------------------------------
# Toleration applicator (shared by the queue processor and the fast path)
# ---------------------------------------------------------------------------


async def _enforce_deadline(
    state: ControllerState, fresh_pod, entry: QueueEntry
) -> None:
    """Cap the pod's activeDeadlineSeconds to its reservation window(s).

    Best-effort: logs a warning on failure but does not raise, so a deadline
    enforcement failure never rolls back an already-applied toleration.
    """
    try:
        now = datetime.now()
        max_secs = state.compute_max_deadline_seconds(now, entry.reservation)
        current = fresh_pod.spec.active_deadline_seconds
        if current is None or current > max_secs:
            await set_active_deadline(entry.pod_name, entry.pod_namespace, max_secs)
            await emit_runtime_capped_event(
                fresh_pod, entry.pod_name, entry.pod_namespace, max_secs
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Failed to enforce activeDeadlineSeconds on pod %s/%s: %s",
            entry.pod_namespace,
            entry.pod_name,
            exc,
        )


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
    booking_reference = f"res-{entry.reservation.id}"
    try:
        other_gpus = await count_tolerated_gpu_usage(
            namespace=entry.pod_namespace,
            label_selector=f"gpu-class={entry.gpu_class_label}",
            tol_key=TOLERATION_KEY,
            tol_value=entry.gpu_class_label,
            exclude_uid=uid,
            booking_reference=booking_reference,
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
                    booking_reference,
                )
                await _enforce_deadline(state, fresh_pod, entry)
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
# On-demand placement coroutine
# ---------------------------------------------------------------------------


async def _try_place_ondemand(
    state: ControllerState, uid: str, candidate: OnDemandCandidate
) -> bool:
    """Attempt to place an on-demand candidate onto a suitable block.

    Returns ``True``  — candidate should be removed (placed, or pod gone/terminal).
    Returns ``False`` — candidate should remain; ``candidate.next_attempt_at``
                        has been pushed forward.

    Placement steps:
    1. Find a suitable block (class + window + capacity + min-runtime).
    2. Reserve capacity optimistically (before any await) to prevent races
       within the single-threaded event loop.
    3. Re-read the pod; if it is gone or terminal, roll back and drop.
    4. Apply toleration.
    5. Cap runtime to the block's window end and emit a RuntimeCapped event.
    """
    now = datetime.now()
    block = state.find_ondemand_block(
        candidate.gpu_class_label,
        now,
        candidate.gpu_requested,
        candidate.min_runtime_seconds,
    )
    if block is None:
        delay = random.randint(120, 300)
        log.debug(
            "On-demand candidate %s/%s: no suitable block available; retry in %d s",
            candidate.pod_namespace,
            candidate.pod_name,
            delay,
        )
        candidate.next_attempt_at = datetime.now() + timedelta(seconds=delay)
        return False

    # Guard 3: safety interlock — hold on-demand placement for any GPU class
    # that has a stuck reservation-holder pod.  Other classes are unaffected.
    if candidate.gpu_class_label in state.stuck_holder_gpu_classes:
        log.debug(
            "On-demand candidate %s/%s: safety interlock active for gpu-class=%s; "
            "retry in 30 s",
            candidate.pod_namespace,
            candidate.pod_name,
            candidate.gpu_class_label,
        )
        candidate.next_attempt_at = datetime.now() + timedelta(seconds=30)
        return False

    if block.id in state.noshow_reservation_ids:
        booking_reference = f"noshow-{block.id}"
    else:
        booking_reference = f"ondemand-{block.id}"
    # --- optimistic reservation (before any await) ---
    state.record_ondemand_placement(block.id, uid, candidate.gpu_requested)

    try:
        fresh_pod = await read_pod(candidate.pod_name, candidate.pod_namespace)

        # Drop gone or terminal pods.
        phase = get_pod_phase(fresh_pod)
        if phase in ("Succeeded", "Failed", "Unknown"):
            log.info(
                "On-demand candidate %s/%s is %s; dropping",
                candidate.pod_namespace,
                candidate.pod_name,
                phase,
            )
            state.release_ondemand_pod(uid)
            return True

        # Guard 1: GPU-only-pending check.
        gpu_only = is_gpu_only_pending(fresh_pod, TOLERATION_KEY)
        if gpu_only is False:
            # Pod has non-GPU resource constraints; our toleration cannot help.
            msg = ""
            if fresh_pod.status and fresh_pod.status.conditions:
                sched = next(
                    (c for c in fresh_pod.status.conditions if c.type == "PodScheduled"),
                    None,
                )
                if sched:
                    msg = (sched.message or "")[:120]
            log.info(
                "On-demand candidate %s/%s: not GPU-only-pending (%r); dropping",
                candidate.pod_namespace,
                candidate.pod_name,
                msg,
            )
            state.release_ondemand_pod(uid)
            return True
        if gpu_only is None:
            # Scheduling conditions not yet populated; keep candidate, retry next tick.
            log.debug(
                "On-demand candidate %s/%s: scheduling conditions not yet set; retry in 30 s",
                candidate.pod_namespace,
                candidate.pod_name,
            )
            state.release_ondemand_pod(uid)
            candidate.next_attempt_at = datetime.now() + timedelta(seconds=30)
            return False

        if pod_has_toleration(fresh_pod, TOLERATION_KEY, candidate.gpu_class_label, "NoSchedule"):
            # Toleration was applied externally between the event and now.
            log.info(
                "On-demand pod %s/%s already has toleration; "
                "recording as placed on block #%d",
                candidate.pod_namespace,
                candidate.pod_name,
                block.id,
            )
            # Keep the occupancy record — it was placed somehow.
            return True

        await apply_toleration(
            candidate.pod_name,
            candidate.pod_namespace,
            fresh_pod,
            TOLERATION_KEY,
            candidate.gpu_class_label,
            booking_reference,
        )
        log.info(
            "Placed on-demand pod %s/%s onto block #%d "
            "(gpu-class=%s, gpus=%d, block has %d/%d free after placement)",
            candidate.pod_namespace,
            candidate.pod_name,
            block.id,
            candidate.gpu_class_label,
            candidate.gpu_requested,
            state.ondemand_available(block),
            block.gpu_count,
        )

        # Cap runtime to the on-demand block's window end (no back-to-back chaining).
        remaining = int((slot_end(block) - datetime.now()).total_seconds())
        remaining = max(remaining, 1)
        try:
            current_deadline = fresh_pod.spec.active_deadline_seconds
            if current_deadline is None or current_deadline > remaining:
                await set_active_deadline(
                    candidate.pod_name, candidate.pod_namespace, remaining
                )
                await emit_runtime_capped_event(
                    fresh_pod, candidate.pod_name, candidate.pod_namespace, remaining
                )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Failed to enforce activeDeadlineSeconds on on-demand pod %s/%s: %s",
                candidate.pod_namespace,
                candidate.pod_name,
                exc,
            )

        return True

    except Exception as exc:  # noqa: BLE001
        # Roll back the optimistic occupancy record so capacity is not leaked.
        state.release_ondemand_pod(uid)
        delay = random.randint(120, 300)
        log.warning(
            "Error placing on-demand pod %s/%s: %s; retry in %d s",
            candidate.pod_namespace,
            candidate.pod_name,
            exc,
            delay,
        )
        candidate.next_attempt_at = datetime.now() + timedelta(seconds=delay)
        return False


# ---------------------------------------------------------------------------
# Background loop 1: reservation refresh
# ---------------------------------------------------------------------------


async def reservation_fetch_loop(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Re-fetch reservations every ``config.reservation_fetch_interval`` seconds.

    The initial fetch is done synchronously in the lifespan before this loop
    starts, so we sleep first and then enter the refresh–sleep cycle.
    """
    while True:
        await asyncio.sleep(config.reservation_fetch_interval)
        try:
            await _refresh_reservations(state, client)
            now = datetime.now()
            state.reconcile_noshow()
            state.update_noshow_tracking(
                now,
                config.noshown_timeout_minutes,
                config.noshown_grace_minutes,
            )
        except Exception as exc:  # noqa: BLE001
            log.error("Reservation refresh failed: %s", exc)


# ---------------------------------------------------------------------------
# Background loop 2: pod watch
# ---------------------------------------------------------------------------


async def pod_watch_loop(state: ControllerState, config: Config) -> None:
    """Stream pod events and update the task queue / on-demand candidates.

    Reserved path (kind="user"):
    - ADDED / MODIFIED, no toleration → enqueue for reservation matching
    - ADDED / MODIFIED, toleration present → dequeue (already admitted)
    - DELETED → dequeue
    - ADDED inside open window → fast-path immediate toleration attempt

    On-demand path (kind="ondemand", when ``config.ondemand_placement_enabled``):
    - ADDED, Pending, has ``dsmlp/minimum-runtime-seconds`` annotation,
      no matching user reservation → add as on-demand candidate
    - DELETED or terminal (Succeeded/Failed) → release any held on-demand slot
      and attempt immediate placement of a waiting candidate of the same class
    - MODIFIED with toleration present → dequeue from candidates (already placed)
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
            # --- reserved path cleanup ---
            state.dequeue_pod(uid)
            # --- on-demand path cleanup ---
            state.remove_ondemand_candidate(uid)
            if config.ondemand_placement_enabled:
                block_id = state.release_ondemand_pod(uid)
                if block_id is not None:
                    await _recycle_ondemand_block(state, gpu_class_label)

        elif event_type in ("ADDED", "MODIFIED"):
            phase = get_pod_phase(pod)
            has_tol = pod_has_toleration(pod, TOLERATION_KEY, gpu_class_label, "NoSchedule")

            # --- terminal on-demand pod: free its slot ---
            if config.ondemand_placement_enabled and phase in ("Succeeded", "Failed"):
                state.remove_ondemand_candidate(uid)
                block_id = state.release_ondemand_pod(uid)
                if block_id is not None:
                    await _recycle_ondemand_block(state, gpu_class_label)
                continue

            if has_tol:
                # Pod already admitted — remove from whichever queue it may be in.
                state.dequeue_pod(uid)
                state.remove_ondemand_candidate(uid)
                state.mark_pod_seen_for_noshow(namespace, gpu_class_label)
            else:
                gpu_count = get_pod_gpu_count(pod)
                reservation = state.find_best_reservation(namespace, gpu_class_label)

                if reservation is not None:
                    # ---- reserved path ----
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

                elif config.ondemand_placement_enabled and event_type == "ADDED":
                    # ---- on-demand path ----
                    # Only ADDED events enqueue candidates; MODIFIED events for a
                    # pod we're already tracking are handled by the processor loop.
                    if phase == "Pending":
                        min_rt = get_pod_min_runtime_seconds(pod)
                        if min_rt is not None:
                            state.add_ondemand_candidate(
                                uid, name, namespace, gpu_class_label, gpu_count, min_rt
                            )


# ---------------------------------------------------------------------------
# On-demand recycling helper
# ---------------------------------------------------------------------------


async def _recycle_ondemand_block(
    state: ControllerState, gpu_class_label: str
) -> None:
    """After a pod vacates an on-demand block, try to place the next candidate.

    Scans ``state.ondemand_candidates`` for a candidate of the same GPU class
    whose ``next_attempt_at`` has passed and immediately tries to place it.
    Only the first successful placement is made per call; the queue processor
    will handle any remaining candidates on its next tick.
    """
    now = datetime.now()
    for uid, candidate in list(state.ondemand_candidates.items()):
        if candidate.gpu_class_label != gpu_class_label:
            continue
        if now < candidate.next_attempt_at:
            continue
        if await _try_place_ondemand(state, uid, candidate):
            state.ondemand_candidates.pop(uid, None)
            return  # one placement per recycle event is enough


# ---------------------------------------------------------------------------
# Background loop 3: queue processor
# ---------------------------------------------------------------------------


async def queue_processor_loop(state: ControllerState, config: Config) -> None:
    """Every 30 s, scan the work queue and apply tolerations where eligible.

    Reserved-path logic per entry:
    1. If the reservation window has expired → remove from queue.
    2. If the window hasn't opened yet, or the entry is in retry cooldown → skip.
    3. Delegate budget check + patch to ``_try_apply_toleration``.

    On-demand-path logic (when ``config.ondemand_placement_enabled``):
    4. For each candidate whose ``next_attempt_at`` has passed, attempt placement.
    5. Drop candidates whose pod no longer exists or is terminal.

    Note: reserved pods that arrive inside an open window are handled immediately
    by the pod-watch loop fast path and typically won't reach this loop at all.
    This loop covers pods queued before their window opened and retries for pods
    that were ineligible on a previous attempt.
    """
    while True:
        await asyncio.sleep(30)
        now = datetime.now()
        state.check_noshow_deadlines(now)

        # Guard 3: refresh safety interlock once per tick.
        if config.ondemand_placement_enabled:
            stuck = await list_stuck_reservation_holder_pods(TOLERATION_KEY)
            new_classes = {gpu_class for _, _, gpu_class in stuck}
            old_classes = state.stuck_holder_gpu_classes
            state.stuck_holder_gpu_classes = new_classes
            for gpu_class in new_classes - old_classes:
                affected = [(ns, name) for ns, name, gc in stuck if gc == gpu_class]
                log.warning(
                    "Safety interlock activated for gpu-class=%s: %d reservation-holder "
                    "pod(s) stuck Pending (%s); on-demand placement for this class held",
                    gpu_class,
                    len(affected),
                    ", ".join(f"{ns}/{name}" for ns, name in affected),
                )
            for gpu_class in old_classes - new_classes:
                log.info(
                    "Safety interlock cleared for gpu-class=%s: on-demand placement resumed",
                    gpu_class,
                )

        to_remove: list[str] = []

        # --- reserved path ---
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

        # --- on-demand path ---
        if config.ondemand_placement_enabled:
            od_to_remove: list[str] = []
            for uid, candidate in list(state.ondemand_candidates.items()):
                if now < candidate.next_attempt_at:
                    continue
                if await _try_place_ondemand(state, uid, candidate):
                    od_to_remove.append(uid)
            for uid in od_to_remove:
                state.ondemand_candidates.pop(uid, None)


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
        now = datetime.now()
        state.initialize_noshow_tracking(
            now,
            config.noshown_timeout_minutes,
            config.noshown_grace_minutes,
        )
        log.info(
            "No-show tracking initialised: %d reservation(s) watched",
            len(state.noshow_deadlines),
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
            reservation_fetch_loop(state, client, config),
            name="reservation-fetch",
        ),
        asyncio.create_task(pod_watch_loop(state, config), name="pod-watch"),
        asyncio.create_task(queue_processor_loop(state, config), name="queue-processor"),
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
