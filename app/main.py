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
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from fastapi import FastAPI

from .config import Config
from .controller import (
    TOLERATION_KEY,
    ControllerState,
    OnDemandCandidate,
    QueueEntry,
    canceller_description,
    slot_end,
    slot_start,
)
from .k8s_client import (
    PodWatcher,
    apply_toleration,
    delete_pod,
    emit_reservation_cancelled_event,
    emit_runtime_capped_event,
    get_pod_booking_reference,
    get_pod_gpu_count,
    get_pod_min_runtime_seconds,
    get_pod_phase,
    init_k8s,
    is_gpu_only_pending,
    parse_booking_reference,
    pod_has_toleration,
    read_pod,
    remove_scheduling_gate,
    set_active_deadline,
    snapshot_tolerated_pods,
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
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Fetch the current reservation list and update shared state.

    Also resolves gpu_class_id → label_value for every GPU class referenced
    in the fetched reservations (results are cached within this cycle; the
    cache is rebuilt from scratch each cycle so stale entries don't linger).
    After updating the reservations, reconciles the task queue.
    """
    all_reservations = await client.fetch_reservations()
    active_reservations = [r for r in all_reservations if r.status == "active"]

    # Refresh the reclaim-preempt guard from app settings; keep the previous
    # value on a failed fetch so merging is not disrupted by a transient error.
    settings = await client.fetch_settings()
    if settings is not None:
        state.reclaim_preempt_guard_minutes = settings.reclaim_preempt_guard_minutes

    # Detect reservations cancelled mid-window before overwriting the state.
    now = datetime.now(timezone.utc)
    cancelled_in_window = state.detect_cancelled_in_window(all_reservations, now)

    # Resolve label_value for each unique GPU class in active reservations.
    class_ids = {r.gpu_class_id for r in active_reservations}
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

    state.reservations = active_reservations
    state.gpu_class_labels = new_labels
    # Stamp the freshness of this data; the reclaim-merge commitment test anchors
    # its guard horizon here, not on the between-fetch tick clock.
    state.last_reservation_fetch_at = now

    # Drop / re-match queue entries whose reservation was cancelled.
    state.reconcile_queue()
    # Occupancy is rebuilt from a live cluster snapshot each queue-processor
    # tick (reconcile_occupancy), so no reservation-driven prune is needed here.

    # Handle mid-window cancellations: evict pods and reclaim capacity.
    if cancelled_in_window:
        await _handle_cancelled_reservations(state, cancelled_in_window, now)

    # Re-apply persistent reclaim-block merges to the freshly loaded reservation
    # objects (and discover new ones) so a reload never re-exposes an absorbed
    # block.  Only meaningful when on-demand placement is enabled.
    if config.ondemand_placement_enabled:
        state.reconcile_reclaim_merges(datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Cancellation handler
# ---------------------------------------------------------------------------


async def _handle_cancelled_reservations(
    state: ControllerState,
    cancelled_in_window: list,
    now: datetime,
) -> None:
    """Evict pods admitted under cancelled reservations and reclaim capacity.

    For each in-window cancelled reservation (already carrying ``cancelled_by``
    info from the ``status=all`` fetch):
    1. Snapshot live tolerated pods and filter those admitted under this reservation.
    2. Emit a ReservationCancelled event on each pod, then delete it.
    3. Record the reservation in ``state.cancelled_reservations`` so its freed
       GPU capacity is immediately available for on-demand placement.
    """
    # One pod snapshot serves the whole batch.
    pod_snapshot = []
    try:
        pod_snapshot = await snapshot_tolerated_pods(TOLERATION_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not snapshot pods for cancellation eviction: %s", exc)

    for cancelled_res in cancelled_in_window:
        cancelled_by_desc = canceller_description(cancelled_res)

        pods_for_res = [p for p in pod_snapshot if p.reservation_id == cancelled_res.id]
        if pods_for_res:
            log.info(
                "Evicting %d pod(s) for cancelled reservation #%d (%s)",
                len(pods_for_res),
                cancelled_res.id,
                cancelled_by_desc,
            )
        for pod_info in pods_for_res:
            # Emit event before deletion so the event record survives.
            try:
                pod_obj = await read_pod(pod_info.name, pod_info.namespace)
                await emit_reservation_cancelled_event(
                    pod_obj, pod_info.name, pod_info.namespace, cancelled_by_desc
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "Could not emit ReservationCancelled event for pod %s/%s: %s",
                    pod_info.namespace,
                    pod_info.name,
                    exc,
                )
            try:
                await delete_pod(pod_info.name, pod_info.namespace)
                state.release_pod(pod_info.uid)
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "Could not delete pod %s/%s: %s",
                    pod_info.namespace,
                    pod_info.name,
                    exc,
                )

        # Record immediately so on-demand candidates can use the freed capacity.
        state.record_cancelled_reservation(cancelled_res)


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
        now = datetime.now(timezone.utc)
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


async def _enforce_scheduling_gate_removal(
    pod_name: str, namespace: str, fresh_pod, gate_name: Optional[str]
) -> None:
    """Remove the configured scheduling gate from *fresh_pod* if present.

    Best-effort: logs a warning on failure; never revokes an applied toleration.
    """
    if not gate_name:
        return
    try:
        await remove_scheduling_gate(pod_name, namespace, fresh_pod, gate_name)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Failed to remove scheduling gate %r from pod %s/%s: %s",
            gate_name, namespace, pod_name, exc,
        )


async def _try_apply_toleration(
    state: ControllerState, uid: str, entry: QueueEntry,
    scheduling_gate_name: Optional[str] = None,
) -> bool:
    """Check GPU budget and patch the toleration onto the pod if eligible.

    Returns ``True``  — entry should be removed from the queue (toleration
                        applied, or the pod already carried it).
    Returns ``False`` — entry should remain; ``entry.next_attempt_at`` has
                        been pushed forward (budget full or transient error).

    The budget check reads the in-memory occupancy map (no API round-trip) and
    optimistically records the placement before any ``await`` so two concurrent
    attempts on the single event loop cannot both claim the same slot; the record
    is rolled back on failure.

    **Does not** evaluate timing (window open/closed, retry cooldown); callers
    are responsible for those guards before invoking this function.
    """
    booking_reference = f"res-{entry.reservation.id}"

    available = state.available(entry.reservation, exclude_uid=uid)
    if entry.gpu_requested > available:
        delay = random.randint(120, 300)
        log.debug(
            "Pod %s/%s: GPU budget full "
            "(%d requested > %d available of %d reserved); retry in %d s",
            entry.pod_namespace,
            entry.pod_name,
            entry.gpu_requested,
            available,
            entry.reservation.gpu_count,
            delay,
        )
        entry.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
        return False

    # Optimistically reserve capacity before any await (single-threaded loop).
    state.record_placement(entry.reservation.id, uid, entry.gpu_requested)
    try:
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
            await _enforce_scheduling_gate_removal(
                entry.pod_name, entry.pod_namespace, fresh_pod, scheduling_gate_name
            )
        return True

    except Exception as exc:  # noqa: BLE001
        # Roll back the optimistic reservation so capacity is not leaked.
        state.release_pod(uid)
        delay = random.randint(120, 300)
        log.warning(
            "Error processing pod %s/%s: %s; retry in %d s",
            entry.pod_namespace,
            entry.pod_name,
            exc,
            delay,
        )
        entry.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
        return False


# ---------------------------------------------------------------------------
# On-demand placement coroutine
# ---------------------------------------------------------------------------


async def _try_place_ondemand(
    state: ControllerState, uid: str, candidate: OnDemandCandidate,
    scheduling_gate_name: Optional[str] = None,
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
    now = datetime.now(timezone.utc)
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
        candidate.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
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
        candidate.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=30)
        return False

    if block.id in state.noshow_reservation_ids:
        booking_reference = f"noshow-{block.id}"
    else:
        booking_reference = f"ondemand-{block.id}"
    # --- optimistic reservation (before any await) ---
    state.record_placement(block.id, uid, candidate.gpu_requested)

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
            state.release_pod(uid)
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
            state.release_pod(uid)
            return True
        if gpu_only is None:
            # Scheduling conditions not yet populated; keep candidate, retry next tick.
            log.debug(
                "On-demand candidate %s/%s: scheduling conditions not yet set; retry in 30 s",
                candidate.pod_namespace,
                candidate.pod_name,
            )
            state.release_pod(uid)
            candidate.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=30)
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
        await _enforce_scheduling_gate_removal(
            candidate.pod_name, candidate.pod_namespace, fresh_pod, scheduling_gate_name
        )
        log.info(
            "Placed on-demand pod %s/%s onto block #%d "
            "(gpu-class=%s, gpus=%d, block has %d/%d free after placement)",
            candidate.pod_namespace,
            candidate.pod_name,
            block.id,
            candidate.gpu_class_label,
            candidate.gpu_requested,
            state.available(block),
            block.gpu_count,
        )

        # Cap runtime to the on-demand block's window end (no back-to-back chaining).
        remaining = int((slot_end(block) - datetime.now(timezone.utc)).total_seconds())
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
        state.release_pod(uid)
        delay = random.randint(120, 300)
        log.warning(
            "Error placing on-demand pod %s/%s: %s; retry in %d s",
            candidate.pod_namespace,
            candidate.pod_name,
            exc,
            delay,
        )
        candidate.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
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
        log.debug("Reservation refresh cycle starting")
        try:
            await _refresh_reservations(state, client, config)
            now = datetime.now(timezone.utc)
            state.reconcile_noshow()
            state.update_noshow_tracking(
                now,
                config.noshown_timeout_minutes,
                config.noshown_grace_minutes,
            )
            log.info(
                "Reservation refresh complete: %d active reservation(s), %d GPU class(es) resolved",
                len(state.reservations),
                len(state.gpu_class_labels),
            )
        except Exception as exc:  # noqa: BLE001
            log.error("Reservation refresh failed: %s", exc)


# ---------------------------------------------------------------------------
# Background loop 2: pod watch
# ---------------------------------------------------------------------------


async def pod_watch_loop(state: ControllerState, config: Config) -> None:
    """Stream pod events and update the task queue / on-demand candidates.

    Reserved path (kind="booking"):
    - ADDED / MODIFIED, no toleration → enqueue for reservation matching
    - ADDED / MODIFIED, toleration present → dequeue (already admitted)
    - DELETED → dequeue
    - ADDED inside open window → fast-path immediate toleration attempt

    On-demand path (places onto kind="reclaim" blocks, when ``config.ondemand_placement_enabled``):
    - ADDED, Pending, has ``horae/minimum-runtime-seconds`` annotation,
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
            unplaced = state.ondemand_candidates.get(uid)
            if unplaced is not None:
                deletion_time = datetime.now(timezone.utc)
                waited = int((deletion_time - unplaced.pod_created_at).total_seconds())
                log.info(
                    "On-demand candidate %s/%s deleted before placement "
                    "(gpu-class=%s, gpus=%d, min-runtime=%ds, "
                    "submitted=%s, deleted=%s, waited=%ds)",
                    unplaced.pod_namespace,
                    unplaced.pod_name,
                    unplaced.gpu_class_label,
                    unplaced.gpu_requested,
                    unplaced.min_runtime_seconds,
                    unplaced.pod_created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    deletion_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    waited,
                )
            state.remove_ondemand_candidate(uid)
            if config.ondemand_placement_enabled:
                block_id = state.release_pod(uid)
                if block_id is not None:
                    await _recycle_ondemand_block(state, gpu_class_label, config.scheduling_gate_name)

        elif event_type in ("ADDED", "MODIFIED"):
            phase = get_pod_phase(pod)
            has_tol = pod_has_toleration(pod, TOLERATION_KEY, gpu_class_label, "NoSchedule")

            # --- terminal on-demand pod: free its slot ---
            if config.ondemand_placement_enabled and phase in ("Succeeded", "Failed"):
                state.remove_ondemand_candidate(uid)
                block_id = state.release_pod(uid)
                if block_id is not None:
                    await _recycle_ondemand_block(state, gpu_class_label, config.scheduling_gate_name)
                continue

            if has_tol:
                # Pod already admitted — remove from whichever queue it may be in.
                state.dequeue_pod(uid)
                state.remove_ondemand_candidate(uid)
                # A reserved-path holder vouches for every window its chained
                # session spans; pass its booking id so all are cleared at once.
                booking_id = parse_booking_reference(get_pod_booking_reference(pod))
                state.mark_pod_seen_for_noshow(namespace, gpu_class_label, booking_id)
                # Keep occupancy warm between ticks: record this admitted pod under
                # its booking-reference id (covers reserved / on-demand / no-show
                # alike), so capacity accounting survives a restart.
                if booking_id is not None:
                    state.record_placement(booking_id, uid, get_pod_gpu_count(pod))
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
                            now = datetime.now(timezone.utc)
                            if slot_start(entry.reservation) <= now < slot_end(entry.reservation):
                                log.info(
                                    "Pod %s/%s arrived inside reservation window; "
                                    "attempting immediate toleration",
                                    namespace,
                                    name,
                                )
                                if await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name):
                                    state.task_queue.pop(uid, None)

                elif config.ondemand_placement_enabled and event_type == "ADDED":
                    # ---- on-demand path ----
                    # Only ADDED events enqueue candidates; MODIFIED events for a
                    # pod we're already tracking are handled by the processor loop.
                    if phase == "Pending":
                        min_rt = get_pod_min_runtime_seconds(pod)
                        if min_rt is not None:
                            ts = pod.metadata.creation_timestamp
                            pod_created_at = ts if ts is not None else datetime.now(timezone.utc)
                            log.debug(
                                "Pod %s/%s ADDED: no open reservation window (gpu-class=%s); "
                                "routing to on-demand queue",
                                namespace,
                                name,
                                gpu_class_label,
                            )
                            state.add_ondemand_candidate(
                                uid, name, namespace, gpu_class_label, gpu_count, min_rt,
                                pod_created_at,
                            )


# ---------------------------------------------------------------------------
# On-demand recycling helper
# ---------------------------------------------------------------------------


async def _recycle_ondemand_block(
    state: ControllerState, gpu_class_label: str,
    scheduling_gate_name: Optional[str] = None,
) -> None:
    """After a pod vacates an on-demand block, try to place the next candidate.

    Scans ``state.ondemand_candidates`` for a candidate of the same GPU class
    whose ``next_attempt_at`` has passed and immediately tries to place it.
    Only the first successful placement is made per call; the queue processor
    will handle any remaining candidates on its next tick.
    """
    now = datetime.now(timezone.utc)
    ordered = sorted(state.ondemand_candidates.items(), key=lambda kv: kv[1].pod_created_at)
    eligible = [
        (uid, c) for uid, c in ordered
        if c.gpu_class_label == gpu_class_label and now >= c.next_attempt_at
    ]
    if eligible:
        log.debug(
            "Recycling on-demand block for gpu-class=%s: %d candidate(s) eligible",
            gpu_class_label,
            len(eligible),
        )
    for uid, candidate in eligible:
        if await _try_place_ondemand(state, uid, candidate, scheduling_gate_name):
            state.ondemand_candidates.pop(uid, None)
            return  # one placement per recycle event is enough


# ---------------------------------------------------------------------------
# Background loop 3: queue processor
# ---------------------------------------------------------------------------


async def queue_processor_loop(state: ControllerState, config: Config) -> None:
    """Every ``config.pod_list_tick_interval`` s, scan the work queue and apply tolerations where eligible.

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
        await asyncio.sleep(config.pod_list_tick_interval)
        now = datetime.now(timezone.utc)

        # One cluster snapshot of tolerated pods drives occupancy, the claimed
        # set, and guard 3 — replacing the per-attempt namespaced counts and the
        # separate guard scans.  On failure, keep the previous state rather than
        # dropping budget / no-show protection.
        snapshot = None
        try:
            snapshot = await snapshot_tolerated_pods(TOLERATION_KEY)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to snapshot tolerated pods: %s", exc)

        if snapshot is not None:
            live = [p for p in snapshot if p.phase in ("Running", "Pending")]
            # Rebuild occupancy from live tolerated pods (self-healing).
            state.reconcile_occupancy(
                [
                    (p.reservation_id, p.uid, p.gpu_count)
                    for p in live
                    if p.reservation_id is not None
                ]
            )
            # Claim every window a live reserved-path holder occupies (chain-aware)
            # before declaring no-shows.
            holder_ids = [
                p.reservation_id
                for p in live
                if p.reservation_id is not None
                and (p.booking_reference or "").startswith("res-")
            ]
            state.refresh_claimed_reservations(holder_ids, now)

        state.check_noshow_deadlines(now)
        state.cleanup_cancelled_reservations(now)

        # Re-apply / extend reclaim-block merges now that the claimed, no-show
        # and cancelled sets are current — picks up future blocks that have
        # entered the preempt guard since the last reservation reload.
        if config.ondemand_placement_enabled:
            state.reconcile_reclaim_merges(now)

        # Guard 3: refresh safety interlock from the same snapshot.
        if config.ondemand_placement_enabled and snapshot is not None:
            stuck = [
                (p.namespace, p.name, p.gpu_class)
                for p in snapshot
                if p.phase == "Pending" and p.scheduled_false and p.gpu_class
            ]
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
            if await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name):
                to_remove.append(uid)

        for uid in to_remove:
            state.task_queue.pop(uid, None)

        # --- on-demand path ---
        if config.ondemand_placement_enabled:
            od_to_remove: list[str] = []
            ordered = sorted(state.ondemand_candidates.items(), key=lambda kv: kv[1].pod_created_at)
            for uid, candidate in ordered:
                if now < candidate.next_attempt_at:
                    continue
                if await _try_place_ondemand(state, uid, candidate, config.scheduling_gate_name):
                    od_to_remove.append(uid)
            for uid in od_to_remove:
                state.ondemand_candidates.pop(uid, None)

        log.debug(
            "Queue processor tick: %d reserved queue entr(ies), %d on-demand candidate(s)",
            len(state.task_queue),
            len(state.ondemand_candidates),
        )


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
        await _refresh_reservations(state, client, config)
        log.info(
            "Initial fetch complete: %d reservation(s), %d GPU class(es) resolved",
            len(state.reservations),
            len(state.gpu_class_labels),
        )
        now = datetime.now(timezone.utc)
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
        await client.aclose()
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
