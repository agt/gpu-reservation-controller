"""GPU Reservation Kubernetes Controller — entry point.

Starts three background asyncio tasks inside a FastAPI lifespan:

1. reservation_fetch_loop  — periodically refreshes the reservation list
2. pod_watch_loop          — streams pod events and updates the work queue
3. queue_processor_loop    — applies tolerations when reservation windows open

Additionally, when a pod is detected arriving *inside* an already-open
reservation window (e.g. a JupyterHub notebook pod), the pod-watch loop
bypasses the queue-processor polling interval (POD_LIST_TICK_INTERVAL,
default 300 s) and attempts to apply the toleration immediately, minimising
scheduler delay for the user.

A minimal GET /health endpoint allows Kubernetes liveness probes to verify
the process is alive.
"""

from __future__ import annotations

import asyncio
import logging
import random
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status

from .config import Config
from .controller import (
    TOLERATION_KEY,
    ControllerState,
    OnDemandCandidate,
    QueueEntry,
    apply_push_to_active,
    canceller_description,
    slot_end,
    slot_start,
)
from .k8s_client import (
    BOOKING_KIND_NOSHOW,
    BOOKING_KIND_ONDEMAND,
    BOOKING_KIND_RESERVED,
    TERMINAL_PHASES,
    PodWatcher,
    apply_toleration,
    delete_pod,
    emit_reservation_cancelled_event,
    emit_reservation_reassigned_event,
    emit_runtime_capped_event,
    get_pod_active_deadline,
    get_pod_booking_reference,
    get_pod_creation_timestamp,
    get_pod_gpu_count,
    get_pod_min_runtime_seconds,
    get_pod_phase,
    get_unschedulable_message,
    init_k8s,
    is_gpu_only_pending,
    is_reserved_path,
    is_terminal_phase,
    make_booking_reference,
    parse_booking_reference,
    pod_has_toleration,
    read_pod,
    remove_scheduling_gate,
    set_active_deadline,
    snapshot_tolerated_pods,
)
from .reservation_client import ReservationClient
from .schemas import (
    ReclaimTakeBackRequest,
    ReclaimTakeBackResponse,
    ReservationPushRequest,
    ReservationPushResponse,
    ReservationResponse,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def _configure_logging(config: Config) -> None:
    """Apply the configured root log level (LOG_LEVEL via Config, CODE-REVIEW H1).

    Keeps all environment parsing in ``config.py`` — ``main.py`` no longer reads
    ``os.environ`` directly.
    """
    logging.getLogger().setLevel(config.log_level.upper())


# Retry backoff shared by both admission paths (CODE-REVIEW D1e).  The jittered
# range is used when a placement attempt fails for budget/transient reasons; the
# short retry is used when a pod's scheduling state is not yet knowable and we
# want to look again promptly (well within one POD_LIST_TICK_INTERVAL tick).
RETRY_JITTER_RANGE = (120, 300)
SHORT_RETRY_SECONDS = 30


def _jittered_retry_at(now: datetime) -> datetime:
    """Return *now* pushed forward by a random 2–5 min backoff."""
    return now + timedelta(seconds=random.randint(*RETRY_JITTER_RANGE))


def _short_retry_at(now: datetime) -> datetime:
    """Return *now* pushed forward by the short (30 s) retry interval."""
    return now + timedelta(seconds=SHORT_RETRY_SECONDS)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _reconcile_after_reservation_change(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
    active_reservations: list[ReservationResponse],
    cancelled_in_window: list[ReservationResponse],
    owner_changes: list[tuple[ReservationResponse, str]],
    now: datetime,
    *,
    update_fetch_stamp: bool,
) -> None:
    """Apply a new active reservation set and run the reconciliation tail.

    Shared by the periodic fetch loop (which supplies a full snapshot) and the
    inbound push endpoint (which supplies a partial delta merged into the current
    set).  Resolves gpu_class_id → label_value for every referenced GPU class
    (rebuilt from scratch, reusing cached values), assigns the new reservation
    list and label map, reconciles the task queue, evicts pods for any in-window
    cancellations or owner changes (adoption), and re-applies reclaim-block merges.

    The caller must already hold ``state.reservation_lock`` and must have computed
    *cancelled_in_window* and *owner_changes* against the OLD reservation set (both
    detectors compare the incoming entries with ``state.reservations`` before it is
    replaced here).  *update_fetch_stamp* is ``True`` only for a full fetch;
    a partial push passes ``False`` so it does not advance
    ``last_reservation_fetch_at`` (the reclaim-merge commitment guard anchors on
    that stamp — advancing it on partial data could race an unseen booking).
    """
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

    if update_fetch_stamp:
        # A bulk fetch may predate a take-back grant (the HTTP GET runs outside
        # the lock), and the app's DB keeps returning a relinquished block until
        # its replacement booking commits — drop tombstoned ids so neither can
        # resurrect ceded capacity.  Pushes are deliberate updates and instead
        # clear the tombstone before applying (see push_reservations).
        active_reservations = state.filter_taken_back(active_reservations, now)
    state.reservations = active_reservations
    state.gpu_class_labels = new_labels
    if update_fetch_stamp:
        # Stamp the freshness of this data; the reclaim-merge commitment test
        # anchors its guard horizon here, not on the between-fetch tick clock.
        # A partial push must not advance this stamp (see docstring).
        state.last_reservation_fetch_at = now

    # Drop / re-match queue entries whose reservation was cancelled.
    state.reconcile_queue()
    # Occupancy is rebuilt from a live cluster snapshot each queue-processor
    # tick (reconcile_occupancy), so no reservation-driven prune is needed here.

    # Handle mid-window cancellations: evict pods and reclaim capacity.
    if cancelled_in_window:
        await _handle_cancelled_reservations(state, cancelled_in_window)

    # Handle owner changes (adoption): evict the prior owner's admitted pod so
    # the new owner can claim the still-active reservation.
    if owner_changes:
        await _handle_owner_changes(state, owner_changes)

    # Re-apply persistent reclaim-block merges to the freshly loaded reservation
    # objects (and discover new ones) so a reload never re-exposes an absorbed
    # block.  Only meaningful when on-demand placement is enabled.
    if config.ondemand_placement_enabled:
        state.reconcile_reclaim_merges(datetime.now(timezone.utc))


async def _refresh_reservations(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Fetch the current reservation list and update shared state.

    Pulls the full ``status=all`` reservation set, refreshes the reclaim-preempt
    guard from app settings, then hands the active subset to
    ``_reconcile_after_reservation_change`` (under ``reservation_lock``) to apply
    it and run the reconciliation tail.
    """
    all_reservations = await client.fetch_reservations()
    active_reservations = [r for r in all_reservations if r.status == "active"]

    # Refresh the reclaim-preempt guard from app settings; keep the previous
    # value on a failed fetch so merging is not disrupted by a transient error.
    settings = await client.fetch_settings()
    if settings is not None:
        state.reclaim_preempt_guard_minutes = settings.reclaim_preempt_guard_minutes

    now = datetime.now(timezone.utc)
    async with state.reservation_lock:
        # Detect reservations cancelled mid-window or reassigned to a new owner
        # before overwriting the state (both compare against the old owner set).
        cancelled_in_window = state.detect_cancelled_in_window(all_reservations, now)
        owner_changes = state.detect_owner_changed_in_window(all_reservations, now)
        await _reconcile_after_reservation_change(
            state,
            client,
            config,
            active_reservations,
            cancelled_in_window,
            owner_changes,
            now,
            update_fetch_stamp=True,
        )


# ---------------------------------------------------------------------------
# Cancellation handler
# ---------------------------------------------------------------------------


async def _handle_cancelled_reservations(
    state: ControllerState,
    cancelled_in_window: list[ReservationResponse],
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


async def _handle_owner_changes(
    state: ControllerState,
    owner_changes: list[tuple[ReservationResponse, str]],
) -> None:
    """Evict the prior owner's admitted pod for each reassigned reservation.

    For each in-progress reservation whose owner changed (adoption), the pod
    already admitted under it lives in the *prior* owner's namespace and can no
    longer be legitimately matched to the reservation.  For each such change:
    1. Snapshot live tolerated pods and filter those admitted under this
       reservation id **in the prior owner's namespace** (the ``namespace ==
       prior_username`` guard ensures a pod the new owner may already have had
       admitted is never touched).
    2. Emit a ReservationReassigned event on each pod, then delete it.
    3. Release its capacity so the new owner's pod can be admitted under the same
       still-active reservation on a subsequent tick / watch event.

    Unlike cancellation, the reservation stays active — it is not recorded in
    ``cancelled_reservations``; it simply changes hands.
    """
    # One pod snapshot serves the whole batch.
    pod_snapshot = []
    try:
        pod_snapshot = await snapshot_tolerated_pods(TOLERATION_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not snapshot pods for owner-change eviction: %s", exc)

    for res, prior_username in owner_changes:
        new_owner = res.user.username if res.user else "another user"
        new_owner_desc = f"to {new_owner}"

        pods_for_res = [
            p
            for p in pod_snapshot
            if p.reservation_id == res.id and p.namespace == prior_username
        ]
        if pods_for_res:
            log.info(
                "Reservation #%d reassigned from %s %s; evicting %d prior-owner pod(s)",
                res.id,
                prior_username,
                new_owner_desc,
                len(pods_for_res),
            )
        for pod_info in pods_for_res:
            # Emit event before deletion so the event record survives.
            try:
                pod_obj = await read_pod(pod_info.name, pod_info.namespace)
                await emit_reservation_reassigned_event(
                    pod_obj, pod_info.name, pod_info.namespace, new_owner_desc
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "Could not emit ReservationReassigned event for pod %s/%s: %s",
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


# ---------------------------------------------------------------------------
# Toleration applicator (shared by the queue processor and the fast path)
# ---------------------------------------------------------------------------


async def _enforce_deadline(
    pod_name: str, namespace: str, fresh_pod, max_seconds: int
) -> None:
    """Cap the pod's activeDeadlineSeconds to *max_seconds* and emit an Event.

    Shared by both admission paths (CODE-REVIEW D1a); callers compute
    *max_seconds* (the reserved path chains back-to-back windows, the on-demand
    path uses the single block's remaining time).  Only patches when the current
    deadline is unset or looser than *max_seconds*.

    Best-effort: logs a warning on failure but does not raise, so a deadline
    enforcement failure never rolls back an already-applied toleration.
    """
    try:
        current = get_pod_active_deadline(fresh_pod)
        if current is None or current > max_seconds:
            await set_active_deadline(pod_name, namespace, max_seconds)
            await emit_runtime_capped_event(
                fresh_pod, pod_name, namespace, max_seconds
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Failed to enforce activeDeadlineSeconds on pod %s/%s: %s",
            namespace,
            pod_name,
            exc,
        )


async def _enforce_scheduling_gate_removal(
    pod_name: str, namespace: str, fresh_pod, gate_name: str | None
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
    scheduling_gate_name: str | None = None,
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
    booking_reference = make_booking_reference(BOOKING_KIND_RESERVED, entry.reservation.id)

    available = state.available(entry.reservation, exclude_uid=uid)
    if entry.gpu_requested > available:
        now = datetime.now(timezone.utc)
        log.debug(
            "Pod %s/%s: GPU budget full "
            "(%d requested > %d available of %d reserved); retry later",
            entry.pod_namespace,
            entry.pod_name,
            entry.gpu_requested,
            available,
            entry.reservation.gpu_count,
        )
        entry.next_attempt_at = _jittered_retry_at(now)
        return False

    # Optimistically reserve capacity before any await (single-threaded loop).
    state.record_placement(entry.reservation.id, uid, entry.gpu_requested)
    try:
        # Re-fetch the pod immediately before patching so we include any
        # tolerations that arrived since we last saw it.
        fresh_pod = await read_pod(entry.pod_name, entry.pod_namespace)

        # Drop a pod that completed while queued — mirrors the on-demand path's
        # terminal-phase drop, so a finished pod is never tolerated / capped
        # (which would only fail into the deadline warning path) (CODE-REVIEW D1c).
        if is_terminal_phase(fresh_pod):
            log.info(
                "Pod %s/%s is %s; dropping from queue",
                entry.pod_namespace,
                entry.pod_name,
                get_pod_phase(fresh_pod),
            )
            state.release_pod(uid)
            return True

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
            now = datetime.now(timezone.utc)
            max_secs = state.compute_max_deadline_seconds(now, entry.reservation)
            await _enforce_deadline(
                entry.pod_name, entry.pod_namespace, fresh_pod, max_secs
            )
            await _enforce_scheduling_gate_removal(
                entry.pod_name, entry.pod_namespace, fresh_pod, scheduling_gate_name
            )
            log.info(
                "Admitted pod %s/%s under reservation #%d "
                "(gpu-class=%s, gpus=%d, %d/%d free after placement, cap=%ds)",
                entry.pod_namespace,
                entry.pod_name,
                entry.reservation.id,
                entry.gpu_class_label,
                entry.gpu_requested,
                state.available(entry.reservation),
                entry.reservation.gpu_count,
                max_secs,
            )
        return True

    except Exception as exc:  # noqa: BLE001
        # Roll back the optimistic reservation so capacity is not leaked.
        state.release_pod(uid)
        log.warning(
            "Error processing pod %s/%s: %s; will retry",
            entry.pod_namespace,
            entry.pod_name,
            exc,
        )
        entry.next_attempt_at = _jittered_retry_at(datetime.now(timezone.utc))
        return False


# ---------------------------------------------------------------------------
# On-demand placement coroutine
# ---------------------------------------------------------------------------


async def _try_place_ondemand(
    state: ControllerState, uid: str, candidate: OnDemandCandidate,
    scheduling_gate_name: str | None = None,
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
        log.debug(
            "On-demand candidate %s/%s: no suitable block available; retry later",
            candidate.pod_namespace,
            candidate.pod_name,
        )
        candidate.next_attempt_at = _jittered_retry_at(datetime.now(timezone.utc))
        return False

    # Guard 3: safety interlock — hold on-demand placement for any GPU class
    # that has a stuck reservation-holder pod.  Other classes are unaffected.
    if candidate.gpu_class_label in state.stuck_holder_gpu_classes:
        log.debug(
            "On-demand candidate %s/%s: safety interlock active for gpu-class=%s; "
            "retry shortly",
            candidate.pod_namespace,
            candidate.pod_name,
            candidate.gpu_class_label,
        )
        candidate.next_attempt_at = _short_retry_at(datetime.now(timezone.utc))
        return False

    if block.id in state.noshow_reservation_ids:
        booking_reference = make_booking_reference(BOOKING_KIND_NOSHOW, block.id)
    else:
        booking_reference = make_booking_reference(BOOKING_KIND_ONDEMAND, block.id)
    # --- optimistic reservation (before any await) ---
    state.record_placement(block.id, uid, candidate.gpu_requested)

    try:
        fresh_pod = await read_pod(candidate.pod_name, candidate.pod_namespace)

        # Drop gone or terminal pods.  Placement additionally treats "Unknown"
        # (node unreachable) as gone — there is nothing to schedule onto — unlike
        # occupancy release, which only frees confirmed-terminal slots (D1c).
        phase = get_pod_phase(fresh_pod)
        if phase in TERMINAL_PHASES or phase == "Unknown":
            log.info(
                "On-demand candidate %s/%s is %s; dropping",
                candidate.pod_namespace,
                candidate.pod_name,
                phase,
            )
            state.release_pod(uid)
            return True

        # Guard 1: GPU-only-pending check.
        gpu_only = is_gpu_only_pending(fresh_pod)
        if gpu_only is False:
            # Pod has non-GPU resource constraints; our toleration cannot help.
            log.info(
                "On-demand candidate %s/%s: not GPU-only-pending (%r); dropping",
                candidate.pod_namespace,
                candidate.pod_name,
                get_unschedulable_message(fresh_pod),
            )
            state.release_pod(uid)
            return True
        if gpu_only is None:
            # Scheduling conditions not yet populated; keep candidate, retry shortly.
            log.debug(
                "On-demand candidate %s/%s: scheduling conditions not yet set; retry shortly",
                candidate.pod_namespace,
                candidate.pod_name,
            )
            state.release_pod(uid)
            candidate.next_attempt_at = _short_retry_at(datetime.now(timezone.utc))
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

        # Cap runtime to the on-demand block's window end (no back-to-back
        # chaining) BEFORE lifting the scheduling gate: on a block whose whole
        # premise is "free only until slot_end", the pod must not be allowed to
        # start running with no deadline if the cap patch fails (CODE-REVIEW D1b).
        remaining = max(int((state.effective_end(block) - datetime.now(timezone.utc)).total_seconds()), 1)
        await _enforce_deadline(
            candidate.pod_name, candidate.pod_namespace, fresh_pod, remaining
        )
        await _enforce_scheduling_gate_removal(
            candidate.pod_name, candidate.pod_namespace, fresh_pod, scheduling_gate_name
        )
        log.info(
            "Placed on-demand pod %s/%s onto block #%d "
            "(gpu-class=%s, gpus=%d, block has %d/%d free after placement, cap=%ds)",
            candidate.pod_namespace,
            candidate.pod_name,
            block.id,
            candidate.gpu_class_label,
            candidate.gpu_requested,
            state.available(block),
            block.gpu_count,
            remaining,
        )

        return True

    except Exception as exc:  # noqa: BLE001
        # Roll back the optimistic occupancy record so capacity is not leaked.
        state.release_pod(uid)
        log.warning(
            "Error placing on-demand pod %s/%s: %s; will retry",
            candidate.pod_namespace,
            candidate.pod_name,
            exc,
        )
        candidate.next_attempt_at = _jittered_retry_at(datetime.now(timezone.utc))
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
            # exc_info so an unexpected bug (e.g. a TypeError in merge arithmetic)
            # is distinguishable from a transient API error in the logs (H2).
            log.error("Reservation refresh failed: %s", exc, exc_info=True)


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
        labels: dict[str, str] = pod.metadata.labels or {}
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
            # Occupancy is the unified budget map for every admission path, so a
            # deleted pod must always be released — not only when on-demand
            # placement is enabled (otherwise reserved-path budget leaks until the
            # next reconcile).  Only the on-demand recycle is gated on the flag.
            block_id = state.release_pod(uid)
            if config.ondemand_placement_enabled and block_id is not None:
                await _place_ondemand_candidates(state, config, gpu_class=gpu_class_label, max_placements=1)

        elif event_type in ("ADDED", "MODIFIED"):
            phase = get_pod_phase(pod)
            has_tol = pod_has_toleration(pod, TOLERATION_KEY, gpu_class_label, "NoSchedule")

            # --- terminal pod: free its slot ---
            # Unconditional (not gated on the on-demand flag) for the same reason
            # as the DELETED branch: occupancy covers all paths.  The continue also
            # keeps a terminal pod out of the has_tol keep-warm below, which would
            # otherwise re-add it to occupancy on every MODIFIED event.
            if phase in TERMINAL_PHASES:
                state.remove_ondemand_candidate(uid)
                block_id = state.release_pod(uid)
                if config.ondemand_placement_enabled and block_id is not None:
                    await _place_ondemand_candidates(state, config, gpu_class=gpu_class_label, max_placements=1)
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
                    # the queue processor's POD_LIST_TICK_INTERVAL tick (default 300 s).
                    if event_type == "ADDED":
                        entry = state.task_queue.get(uid)
                        if entry is not None:
                            now = datetime.now(timezone.utc)
                            # Honor the retry cooldown: on a watch reconnect every
                            # pod is replayed as ADDED, and enqueue_pod is
                            # idempotent, so without this guard the fast path would
                            # retry an entry still in budget-full/error backoff,
                            # ignoring next_attempt_at as the queue processor does (B8).
                            if (
                                slot_start(entry.reservation) <= now < slot_end(entry.reservation)
                                and now >= entry.next_attempt_at
                            ):
                                log.info(
                                    "Pod %s/%s arrived inside reservation window; "
                                    "attempting immediate toleration",
                                    namespace,
                                    name,
                                )
                                if await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name):
                                    state.dequeue_pod(uid)

                elif config.ondemand_placement_enabled and event_type == "ADDED":
                    # ---- on-demand path ----
                    # Only ADDED events enqueue candidates; MODIFIED events for a
                    # pod we're already tracking are handled by the processor loop.
                    if phase == "Pending":
                        min_rt = get_pod_min_runtime_seconds(pod)
                        if min_rt is not None:
                            ts = get_pod_creation_timestamp(pod)
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


async def _place_ondemand_candidates(
    state: ControllerState,
    config: Config,
    *,
    gpu_class: str | None = None,
    max_placements: int | None = None,
) -> int:
    """Attempt on-demand placement for eligible candidates in FIFO order.

    Single scan shared by the queue processor (all classes, no cap) and the
    post-vacate recycle path (one class, ``max_placements=1``) — replacing the
    two copies that sorted by ``pod_created_at``, filtered on ``next_attempt_at``,
    called ``_try_place_ondemand``, and popped on success (CODE-REVIEW D5).

    *gpu_class* restricts to one GPU class; *max_placements* stops after that many
    successful placements.  Returns the number placed.
    """
    now = datetime.now(timezone.utc)
    placed = 0
    ordered = sorted(state.ondemand_candidates.items(), key=lambda kv: kv[1].pod_created_at)
    for uid, candidate in ordered:
        if now < candidate.next_attempt_at:
            continue
        if gpu_class is not None and candidate.gpu_class_label != gpu_class:
            continue
        if await _try_place_ondemand(state, uid, candidate, config.scheduling_gate_name):
            state.remove_ondemand_candidate(uid)
            placed += 1
            if max_placements is not None and placed >= max_placements:
                break
    return placed


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
            log.warning("Failed to snapshot tolerated pods: %s", exc, exc_info=True)

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
                and is_reserved_path(p.booking_reference)
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

        # Route removals through the logging helper so admissions produce a
        # "Dequeued" line, not just deletions (CODE-REVIEW D5).
        for uid in to_remove:
            state.dequeue_pod(uid)

        # --- on-demand path ---
        if config.ondemand_placement_enabled:
            await _place_ondemand_candidates(state, config)

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

    # Expose the shared state and client so request handlers (e.g. the inbound
    # push endpoint) can reach them; the background loops receive them as task
    # arguments.  Only ``config`` is available on ``app.state`` before this.
    app.state.controller_state = state
    app.state.reservation_client = client

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
        state.update_noshow_tracking(
            now,
            config.noshown_timeout_minutes,
            config.noshown_grace_minutes,
            reason="init",
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
    _configure_logging(config)
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
async def health() -> dict[str, str]:
    """Liveness probe — returns 200 OK when the process is running."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Inbound reservation-push API
# ---------------------------------------------------------------------------


def _require_push_auth(
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    """Authenticate an inbound API call (push / take-back) via a static bearer token.

    - 503 if ``INBOUND_API_TOKEN`` is unset — the inbound API is opt-in and
      disabled by default, so existing deployments are unaffected.
    - 401 if the ``Authorization: Bearer <token>`` header is missing or does not
      match (constant-time compare).
    """
    config: Config = request.app.state.config
    expected = config.inbound_api_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Inbound API is disabled (INBOUND_API_TOKEN not set)",
        )
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not secrets.compare_digest(token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


@app.post(
    "/api/reservations/push",
    tags=["sync"],
    response_model=ReservationPushResponse,
    dependencies=[Depends(_require_push_auth)],
)
async def push_reservations(
    body: ReservationPushRequest, request: Request
) -> ReservationPushResponse:
    """Apply one or more pushed reservation entries into controller state.

    A fast, partial delta from the reservation app (bulk sync remains a
    controller-initiated pull).  Entries are upserted by id; an entry whose
    ``status`` is not ``"active"`` (e.g. a cancellation) drops the reservation
    from the active set, and any in-window cancellation evicts its admitted pod
    and reclaims the freed capacity — the same path a mid-window cancellation
    takes on a normal fetch.  An entry that keeps the same id but changes owner
    (adoption) evicts the prior owner's admitted pod from its namespace so the
    new owner can claim the still-active reservation.  The next full pull remains
    the source of truth.
    """
    state: ControllerState = request.app.state.controller_state
    client: ReservationClient = request.app.state.reservation_client
    config: Config = request.app.state.config

    pushed = body.reservations
    now = datetime.now(timezone.utc)

    async with state.reservation_lock:
        # An explicit active push of a taken-back id restores it — the app
        # deliberately handing back relinquished capacity (e.g. its tentative
        # booking fell through) — so lift tombstones before the upsert.
        state.clear_taken_back({r.id for r in pushed if r.status == "active"})
        # Evictable in-window cancellations carried by this push (idempotent:
        # detect_cancelled_in_window skips ids already recorded / declared no-show).
        cancelled_in_window = state.detect_cancelled_in_window(pushed, now)
        # Owner changes (adoption) must be detected before apply_push_to_active
        # upserts the new owner over the old one in state.reservations.
        owner_changes = state.detect_owner_changed_in_window(pushed, now)
        merged_active = apply_push_to_active(state.reservations, pushed)

        await _reconcile_after_reservation_change(
            state,
            client,
            config,
            merged_active,
            cancelled_in_window,
            owner_changes,
            now,
            update_fetch_stamp=False,
        )

        # Re-arm / prune no-show tracking for the new set, mirroring what the
        # fetch loop does after _refresh_reservations.
        state.reconcile_noshow()
        state.update_noshow_tracking(
            now,
            config.noshown_timeout_minutes,
            config.noshown_grace_minutes,
            reason="push",
        )

    applied = sum(1 for r in pushed if r.status == "active")
    log.info(
        "Push applied: %d active upsert(s), %d in-window cancellation(s), "
        "%d owner change(s); %d active reservation(s) now tracked",
        applied,
        len(cancelled_in_window),
        len(owner_changes),
        len(state.reservations),
    )
    return ReservationPushResponse(
        applied=applied,
        cancelled=len(cancelled_in_window),
        adopted=len(owner_changes),
        total_active=len(state.reservations),
    )


# ---------------------------------------------------------------------------
# Inbound reclaim-block take-back API
# ---------------------------------------------------------------------------


@app.post(
    "/api/reservations/take-back",
    tags=["sync"],
    response_model=ReclaimTakeBackResponse,
    dependencies=[Depends(_require_push_auth)],
)
async def take_back_reclaim_blocks(
    body: ReclaimTakeBackRequest, request: Request
) -> ReclaimTakeBackResponse:
    """Relinquish specific idle reclaim blocks so the reservation app can re-book them.

    The app calls this before committing a booking built from capacity the
    controller is holding (e.g. a tentative offer inside the preempt guard).
    All-or-nothing: if any requested block is in use — a pod admitted on it, or
    a merged extension a running job's deadline reaches into — the whole request
    fails with 409 and nothing changes.  On success the blocks leave the
    controller's scheduling universe immediately (absorbed blocks are detached
    from their merge subject) and are tombstoned so a stale fetch cannot
    resurrect them; pushing the id back (``POST /api/reservations/push``) is the
    restore path if the booking falls through.  503 when idleness cannot be
    verified (pod snapshot failed) — fail closed, nothing granted.
    """
    state: ControllerState = request.app.state.controller_state
    config: Config = request.app.state.config

    ids = set(body.reclaim_ids)
    now = datetime.now(timezone.utc)

    async with state.reservation_lock:
        # Fail closed: idleness must be verified against live pods.
        try:
            snapshot = await snapshot_tolerated_pods(TOLERATION_KEY)
        except Exception as exc:  # noqa: BLE001
            log.warning("Take-back rejected: could not snapshot pods: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Cannot verify block idleness (pod snapshot failed); retry later",
            )

        # Projected end per live pod, keyed by the reservation id it occupies.
        # None = unknown/unbounded (fail safe).  In-memory occupancy is merged
        # in to cover pods whose admission patch is still in flight (recorded
        # synchronously before any await) and thus not yet visible to the LIST.
        # No await may occur between here and take_back_blocks: that keeps the
        # check-and-mutate atomic against the placement coroutines.
        pod_ends: dict[int, list[datetime | None]] = {}
        seen_uids: set[str] = set()
        for p in snapshot:
            if p.phase not in ("Running", "Pending") or p.reservation_id is None:
                continue
            seen_uids.add(p.uid)
            if p.active_deadline_seconds is not None and p.start_time is not None:
                projected = p.start_time + timedelta(seconds=p.active_deadline_seconds)
            else:
                projected = None
            pod_ends.setdefault(p.reservation_id, []).append(projected)
        for rid, occupants in state.occupancy.items():
            for uid in occupants:
                if uid not in seen_uids:
                    pod_ends.setdefault(rid, []).append(None)

        result = state.take_back_blocks(
            ids,
            pod_ends,
            now,
            unknown_expiry=now
            + timedelta(seconds=2 * config.reservation_fetch_interval),
        )

    if result.invalid:
        log.info(
            "Take-back rejected (400): invalid target(s) %s",
            [(c.reservation_id, c.reason) for c in result.invalid],
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "message": "One or more ids are not reclaim blocks; nothing was taken back",
                "invalid": [
                    {"id": c.reservation_id, "reason": c.reason}
                    for c in result.invalid
                ],
            },
        )
    if result.conflicts:
        log.info(
            "Take-back rejected (409): in-use block(s) %s",
            [(c.reservation_id, c.reason) for c in result.conflicts],
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "One or more blocks are in use; nothing was taken back",
                "in_use": [
                    {"id": c.reservation_id, "reason": c.reason}
                    for c in result.conflicts
                ],
            },
        )

    return ReclaimTakeBackResponse(
        taken_back=result.taken_back,
        already_taken_back=result.already_taken_back,
        unknown=result.unknown,
        detached=result.detached,
        total_active=len(state.reservations),
    )


def main() -> None:
    """Run the controller, binding uvicorn to the configured HEALTH_PORT.

    Launching programmatically (rather than via a hardcoded ``uvicorn`` CLI port)
    is what makes ``HEALTH_PORT`` actually take effect, so Helm's ``healthPort``
    and both probes stay consistent with the listening port (CODE-REVIEW P2).
    """
    import uvicorn

    config: Config = app.state.config
    uvicorn.run(app, host="0.0.0.0", port=config.health_port)


if __name__ == "__main__":
    main()
