"""GPU Reservation Kubernetes Controller — entry point.

Starts four background asyncio tasks inside a FastAPI lifespan:

1. reservation_fetch_loop  — periodically refreshes the reservation list
2. pod_watch_loop          — streams pod events and updates the work queue
3. queue_processor_loop    — applies tolerations when reservation windows open
4. preemption_loop         — recovers capacity from overstaying pods near a
                              reservation boundary (demand-driven preemption)

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
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status

from .config import Config
from .controller import (
    TOLERATION_KEY,
    BoundaryPreemptionNeed,
    ControllerState,
    OnDemandCandidate,
    PodRuntimeView,
    PreemptionForecast,
    QueueEntry,
    apply_push_to_active,
    build_preemption_plan,
    canceller_description,
    select_victims_locally,
    slot_end,
    slot_start,
)
from .k8s_client import (
    TERMINAL_PHASES,
    PodWatcher,
    annotate_runtime_guarantee,
    apply_toleration,
    delete_pod,
    emit_overstay_relinked_event,
    emit_preempted_event,
    emit_reservation_cancelled_event,
    emit_reservation_reassigned_event,
    emit_runtime_guaranteed_event,
    get_pod_booking_reference,
    get_pod_creation_timestamp,
    get_pod_gpu_count,
    get_pod_min_runtime_seconds,
    get_pod_phase,
    get_unschedulable_message,
    init_k8s,
    is_gpu_only_pending,
    is_terminal_phase,
    make_booking_reference,
    parse_booking_reference,
    pod_has_toleration,
    read_pod,
    remove_scheduling_gate,
    snapshot_node_gpu_capacity,
    snapshot_tolerated_pods,
)
from .reservation_client import ReservationClient
from .schemas import (
    ForecastBucket,
    ForecastClassSummary,
    ForecastPod,
    ForecastPodBucket,
    OnDemandReservationRequest,
    PreemptionCandidate,
    PreemptionRiskForecastResponse,
    PreemptionSelectionRequest,
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
    active_reservations: list[ReservationResponse],
    cancelled_in_window: list[ReservationResponse],
    owner_changes: list[tuple[ReservationResponse, str]],
    now: datetime,
) -> None:
    """Apply a new active reservation set and run the reconciliation tail.

    Shared by the periodic fetch loop (which supplies a full snapshot) and the
    inbound push endpoint (which supplies a partial delta merged into the current
    set).  Refreshes the gpu_class_id ↔ label_value maps, assigns the new
    reservation list, reconciles the task queue, and evicts pods for any
    in-window cancellations or owner changes (adoption).

    The caller must already hold ``state.reservation_lock`` and must have computed
    *cancelled_in_window* and *owner_changes* against the OLD reservation set (both
    detectors compare the incoming entries with ``state.reservations`` before it is
    replaced here).
    """
    # Refresh the full GPU class list (both label and JIT id-lookup maps).  A
    # failed bulk fetch keeps the previous cycle's maps rather than losing all
    # label resolution.
    gpu_classes = await client.fetch_gpu_classes()
    if gpu_classes is not None:
        new_labels: dict[int, str] = {}
        new_ids: dict[str, int] = {}
        for gc in gpu_classes:
            if gc.label_value:
                new_labels[gc.id] = gc.label_value
                new_ids[gc.label_value] = gc.id
    else:
        new_labels = dict(state.gpu_class_labels)
        new_ids = dict(state.gpu_class_ids)

    # Fallback: resolve any class referenced by active reservations that the
    # bulk list didn't cover (e.g. a class created since the last successful
    # fetch, or a pushed reservation referencing an id not yet seen).
    class_ids = {r.gpu_class_id for r in active_reservations}
    for cid in class_ids:
        if cid in new_labels:
            continue
        gpu_class = await client.fetch_gpu_class(cid)
        if gpu_class and gpu_class.label_value:
            new_labels[cid] = gpu_class.label_value
            new_ids[gpu_class.label_value] = cid
            log.info("GPU class %d (%s) → label_value=%r", cid, gpu_class.name, gpu_class.label_value)
        else:
            log.warning(
                "GPU class %d has no label_value; pods for this class "
                "cannot be matched to reservations",
                cid,
            )

    state.reservations = active_reservations
    state.gpu_class_labels = new_labels
    state.gpu_class_ids = new_ids

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


async def _refresh_reservations(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Fetch the current reservation list and update shared state.

    Pulls the full ``status=all`` reservation set, then hands the active subset
    to ``_reconcile_after_reservation_change`` (under ``reservation_lock``) to
    apply it and run the reconciliation tail.
    """
    all_reservations = await client.fetch_reservations()
    active_reservations = [r for r in all_reservations if r.status == "active"]

    now = datetime.now(timezone.utc)
    async with state.reservation_lock:
        # Detect reservations cancelled mid-window or reassigned to a new owner
        # before overwriting the state (both compare against the old owner set).
        cancelled_in_window = state.detect_cancelled_in_window(all_reservations, now)
        owner_changes = state.detect_owner_changed_in_window(all_reservations, now)
        # Bridge the grant-vs-snapshot race: keep our own recently-granted
        # on-demand leases the app has not surfaced in this snapshot yet, so a
        # live lease's pod is not dropped to guarantee_end=None and wrongly
        # preempted (see ControllerState.preserve_local_ondemand_leases).  These
        # ids are absent from the fetch, so they never collide with the active
        # subset, the cancellation detector, or the owner-change detector.
        preserved = state.preserve_local_ondemand_leases(all_reservations, now)
        if preserved:
            log.info(
                "Preserving %d locally-granted on-demand lease(s) absent from "
                "this fetch snapshot: %s",
                len(preserved),
                ", ".join(f"#{r.id}" for r in preserved),
            )
        await _reconcile_after_reservation_change(
            state,
            client,
            active_reservations + preserved,
            cancelled_in_window,
            owner_changes,
            now,
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

    Unlike cancellation, the reservation stays active; it simply changes hands.
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


async def _record_guarantee(
    pod_name: str, namespace: str, fresh_pod, guaranteed_until: datetime, now: datetime
) -> None:
    """Annotate the pod with its runtime guarantee and emit a RuntimeGuaranteed Event.

    Callers compute *guaranteed_until* by chaining back-to-back windows
    (``compute_guaranteed_until``).  Unlike the retired hard deadline this sets
    no Kubernetes enforcement — no
    ``spec.activeDeadlineSeconds`` is patched, so a pod may run past its
    guarantee freely.  Demand-driven preemption recovers capacity from an
    overstaying pod only when needed (see ``preemption_loop``), deciding by
    recomputing the guarantee live from reservation state
    (``ControllerState.guarantee_end``) — never by reading these annotations
    back.

    Best-effort: logs a warning on failure but does not raise, so a failure to
    record the guarantee never rolls back an already-applied toleration.
    """
    try:
        seconds = max(1, int((guaranteed_until - now).total_seconds()))
        await annotate_runtime_guarantee(pod_name, namespace, seconds, guaranteed_until)
        await emit_runtime_guaranteed_event(
            fresh_pod, pod_name, namespace, seconds, guaranteed_until
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Failed to record runtime guarantee on pod %s/%s: %s",
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
    booking_reference = make_booking_reference(entry.reservation.id)

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
        # terminal-phase drop, so a finished pod is never tolerated / stamped with
        # a guarantee (which would only fail into the warning path) (CODE-REVIEW D1c).
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
            guaranteed_until = state.compute_guaranteed_until(now, entry.reservation)
            await _record_guarantee(
                entry.pod_name, entry.pod_namespace, fresh_pod, guaranteed_until, now
            )
            await _enforce_scheduling_gate_removal(
                entry.pod_name, entry.pod_namespace, fresh_pod, scheduling_gate_name
            )
            log.info(
                "Admitted pod %s/%s under reservation #%d "
                "(gpu-class=%s, gpus=%d, %d/%d free after placement, "
                "guaranteed until %s)",
                entry.pod_namespace,
                entry.pod_name,
                entry.reservation.id,
                entry.gpu_class_label,
                entry.gpu_requested,
                state.available(entry.reservation),
                entry.reservation.gpu_count,
                guaranteed_until.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
# JIT on-demand lease request coroutine
# ---------------------------------------------------------------------------


async def _try_request_lease(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
    uid: str,
    candidate: OnDemandCandidate,
) -> bool:
    """Attempt to secure GPU access for a JIT on-demand candidate.

    Returns ``True``  — candidate should be removed (routed to the reserved
                        queue instead, dropped, or admitted under a granted lease).
    Returns ``False`` — candidate should remain; ``candidate.next_attempt_at``
                        has been pushed forward.

    Steps:
    1. Re-read the pod; drop if gone/terminal/Unknown.
    2. Re-run the reserved-path routing check: a matching reservation may have
       appeared since the candidate was queued (a new booking, or simply time
       passing into the horizon) — route there instead of requesting a lease.
    3. Guard 1 (GPU-only-pending).
    4. Guard 3 (stuck reservation-holder safety interlock).
    5. Resolve the pod's gpu-class label to a numeric id.
    6. Request a JIT lease (``create_ondemand_reservation``); a denial cools
       the candidate down for a retry.
    7. On grant, admit the pod under the new lease; if admission does not
       succeed, issue a compensating cancel (``reason="controller-revoked"``)
       so the lease is not left dangling.
    """
    now = datetime.now(timezone.utc)
    try:
        fresh_pod = await read_pod(candidate.pod_name, candidate.pod_namespace)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Error reading on-demand candidate %s/%s: %s; will retry",
            candidate.pod_namespace,
            candidate.pod_name,
            exc,
        )
        candidate.next_attempt_at = _jittered_retry_at(now)
        return False

    phase = get_pod_phase(fresh_pod)
    if phase in TERMINAL_PHASES or phase == "Unknown":
        log.info(
            "On-demand candidate %s/%s is %s; dropping",
            candidate.pod_namespace,
            candidate.pod_name,
            phase,
        )
        return True

    # Step 2: a matching reservation may have appeared since this candidate
    # was queued (or since its last attempt) — prefer it over requesting a
    # fresh lease.
    horizon = timedelta(minutes=config.ondemand_horizon_minutes)
    admittable = state.find_admittable_reservation(
        candidate.pod_namespace,
        candidate.gpu_class_label,
        candidate.gpu_requested,
        now,
        horizon,
        candidate.group_label,
    )
    if admittable is not None:
        state.enqueue_pod(
            uid,
            candidate.pod_name,
            candidate.pod_namespace,
            candidate.gpu_class_label,
            candidate.gpu_requested,
            candidate.group_label,
        )
        entry = state.task_queue.get(uid)
        if entry is not None:
            # Re-fetch now: enqueue_pod just stamped next_attempt_at with its
            # own datetime.now(), which can be a hair later than the *now*
            # captured at the top of this function.
            fast_path_now = datetime.now(timezone.utc)
            if (
                slot_start(entry.reservation) <= fast_path_now < slot_end(entry.reservation)
                and fast_path_now >= entry.next_attempt_at
            ):
                if await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name):
                    state.dequeue_pod(uid)
        return True

    # Guard 1: GPU-only-pending check.
    gpu_only = is_gpu_only_pending(fresh_pod)
    if gpu_only is False:
        log.info(
            "On-demand candidate %s/%s: not GPU-only-pending (%r); dropping",
            candidate.pod_namespace,
            candidate.pod_name,
            get_unschedulable_message(fresh_pod),
        )
        return True
    if gpu_only is None:
        log.debug(
            "On-demand candidate %s/%s: scheduling conditions not yet set; retry shortly",
            candidate.pod_namespace,
            candidate.pod_name,
        )
        candidate.next_attempt_at = _short_retry_at(now)
        return False

    # Guard 3: safety interlock — hold JIT requests for any GPU class that has
    # a stuck reservation-holder pod.  Other classes are unaffected.
    if candidate.gpu_class_label in state.stuck_holder_gpu_classes:
        log.debug(
            "On-demand candidate %s/%s: safety interlock active for gpu-class=%s; "
            "retry shortly",
            candidate.pod_namespace,
            candidate.pod_name,
            candidate.gpu_class_label,
        )
        candidate.next_attempt_at = _short_retry_at(now)
        return False

    gpu_class_id = state.gpu_class_ids.get(candidate.gpu_class_label)
    if gpu_class_id is None:
        log.warning(
            "On-demand candidate %s/%s: gpu-class=%s has no known id; retry later",
            candidate.pod_namespace,
            candidate.pod_name,
            candidate.gpu_class_label,
        )
        candidate.next_attempt_at = _jittered_retry_at(now)
        return False

    duration_seconds = candidate.min_runtime_seconds + config.ondemand_lease_buffer_minutes * 60
    request = OnDemandReservationRequest(
        username=candidate.pod_namespace,
        group_name=candidate.group_label,
        gpu_class_id=gpu_class_id,
        gpu_count=candidate.gpu_requested,
        duration_seconds=duration_seconds,
        idempotency_key=uid,
    )
    lease = await client.create_ondemand_reservation(request)
    if lease is None:
        log.info(
            "On-demand lease request denied for pod %s/%s (gpu-class=%s, gpus=%d); "
            "retrying later",
            candidate.pod_namespace,
            candidate.pod_name,
            candidate.gpu_class_label,
            candidate.gpu_requested,
        )
        candidate.next_attempt_at = _jittered_retry_at(now)
        return False

    log.info(
        "On-demand lease #%d granted for pod %s/%s (gpu-class=%s, gpus=%d, "
        "duration=%ds)",
        lease.id,
        candidate.pod_namespace,
        candidate.pod_name,
        candidate.gpu_class_label,
        candidate.gpu_requested,
        duration_seconds,
    )

    async with state.reservation_lock:
        state.reservations = apply_push_to_active(state.reservations, [lease])
        entry = QueueEntry(
            pod_uid=uid,
            pod_name=candidate.pod_name,
            pod_namespace=candidate.pod_namespace,
            gpu_class_label=candidate.gpu_class_label,
            gpu_requested=candidate.gpu_requested,
            reservation=lease,
            next_attempt_at=now,
            group_label=candidate.group_label,
        )
        admitted_queue = await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name)
        admitted = uid in state.occupancy.get(lease.id, {})
        if not admitted:
            log.warning(
                "Admission failed after granting lease #%d for pod %s/%s; "
                "issuing compensating cancel",
                lease.id,
                candidate.pod_namespace,
                candidate.pod_name,
            )
            await client.cancel_reservation(lease.id, "controller-revoked")
            state.reservations = [r for r in state.reservations if r.id != lease.id]

    if admitted_queue:
        # Either admitted successfully, or the pod went terminal while we were
        # granting the lease (in which case the compensating cancel above
        # already released it) — either way the candidate is done.
        return True
    # Budget-full or a transient patch error: keep the candidate, which will
    # request a fresh lease on its next attempt.
    candidate.next_attempt_at = _jittered_retry_at(now)
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


async def _teardown_ondemand_lease(
    state: ControllerState, client: ReservationClient, pod
) -> None:
    """Cancel the JIT on-demand lease backing *pod* when the pod has gone away.

    A JIT lease exists solely to cover one pod (its ``idempotency_key`` is the
    pod's UID), so once that pod terminates (clean exit / crash), is deleted, or
    is preempted, the lease is no longer needed and should be released back to
    the app — otherwise it keeps holding capacity and accruing SU until it
    naturally expires.

    The on-demand-vs-booking distinction is read live off the reservation's
    ``kind`` field (the app returns leases as ``kind="on_demand"`` and the pull
    keeps them in ``state.reservations``), so nothing about which reservations
    are leases is tracked in memory — the pod's ``horae/booking-reference``
    annotation resolves to the lease id, and its ``kind`` is looked up there.

    Best-effort: only ``kind == "on_demand"`` rows are ever touched (a user
    booking's pod ending never cancels anything), the cancel is idempotent
    (already-cancelled / gone ids are a harmless no-op), and a failure just logs
    — the next app poll reconciles.  Occupancy is released separately by the
    caller, independent of this cancel succeeding.
    """
    booking_id = parse_booking_reference(get_pod_booking_reference(pod))
    if booking_id is None:
        return
    # Hold the lock across the cancel + list edit, mirroring the compensating
    # cancel in _try_request_lease, so a concurrent fetch can't replace
    # state.reservations mid-operation.
    async with state.reservation_lock:
        res = next((r for r in state.reservations if r.id == booking_id), None)
        if res is None or res.status != "active" or res.kind != "on_demand":
            return
        log.info(
            "On-demand pod gone; cancelling lease #%d (%s)",
            booking_id,
            res.gpu_class.name,
        )
        if await client.cancel_reservation(booking_id, "pod-terminated"):
            state.reservations = [r for r in state.reservations if r.id != booking_id]


async def pod_watch_loop(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Stream pod events and update the task queue / on-demand candidates.

    Reserved path (kind="booking"):
    - ADDED / MODIFIED, no toleration → enqueue for reservation matching
    - ADDED / MODIFIED, toleration present → dequeue (already admitted)
    - DELETED → dequeue
    - ADDED inside open window → fast-path immediate toleration attempt

    JIT on-demand path (when ``config.ondemand_placement_enabled``): a pod with
    no reservation admittable now or within ``ONDEMAND_HORIZON_MINUTES`` is
    routed here instead of waiting — see ``_try_request_lease``.
    - ADDED, Pending, has ``horae/minimum-runtime-seconds`` annotation, group
      label present when REQUIRED_GROUP_LABEL is set → add as a candidate and
      attempt a lease request immediately
    - MODIFIED for a tracked candidate (respecting its retry cooldown) →
      attempt again, so a guard-1 short retry resolves quickly
    - DELETED or terminal (Succeeded/Failed) → release any held slot, and if the
      pod was admitted under a JIT on-demand lease, cancel that lease too
      (``_teardown_ondemand_lease`` — a lease covers only its one pod)
    - MODIFIED with toleration present → dequeue from candidates (already placed)

    A pod matching neither path (no admittable/future reservation, and not
    JIT-eligible) is left Pending.
    """
    watcher = PodWatcher(label_selector="gpu-class")
    horizon = timedelta(minutes=config.ondemand_horizon_minutes)
    async for event_type, pod in watcher.events():
        uid: str = pod.metadata.uid
        name: str = pod.metadata.name
        namespace: str = pod.metadata.namespace
        labels: dict[str, str] = pod.metadata.labels or {}
        gpu_class_label: str | None = labels.get("gpu-class")

        if not gpu_class_label:
            # Label key present but value is empty string — skip.
            continue

        # Optional usage-group constraint (REQUIRED_GROUP_LABEL).  None both when
        # the feature is disabled and when the pod lacks a (non-empty) value; a
        # labelless pod (feature on) matches no booking and is never JIT-eligible
        # either — it is left Pending for future "born overstay" handling.
        group_label: str | None = (
            labels.get(config.required_group_label) or None
            if config.required_group_label
            else None
        )

        if event_type == "DELETED":
            # --- reserved path cleanup ---
            state.dequeue_pod(uid)
            # --- JIT candidate cleanup ---
            unplaced = state.ondemand_candidates.get(uid)
            if unplaced is not None:
                deletion_time = datetime.now(timezone.utc)
                waited = int((deletion_time - unplaced.pod_created_at).total_seconds())
                log.info(
                    "On-demand candidate %s/%s deleted before a lease was granted "
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
            # deleted pod must always be released, regardless of on-demand
            # placement being enabled (otherwise reserved-path budget leaks until
            # the next reconcile).
            state.release_pod(uid)
            # If this pod was admitted under a JIT on-demand lease, release the
            # lease too — it exists only to cover this pod (no-op for bookings).
            await _teardown_ondemand_lease(state, client, pod)

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
                state.release_pod(uid)
                # A pod that finished on its own no longer needs its JIT lease;
                # cancel it if that's what admitted this pod (no-op otherwise).
                await _teardown_ondemand_lease(state, client, pod)
                continue

            if has_tol:
                # Pod already admitted — remove from whichever queue it may be in.
                state.dequeue_pod(uid)
                state.remove_ondemand_candidate(uid)
                # A reserved-path holder vouches for every window its chained
                # session spans; pass its booking id so all are cleared at once.
                booking_id = parse_booking_reference(get_pod_booking_reference(pod))
                state.mark_pod_seen_for_noshow(
                    namespace, gpu_class_label, booking_id, group_label
                )
                # Keep occupancy warm between ticks: record this admitted pod under
                # its booking-reference id, so capacity accounting survives a restart.
                if booking_id is not None:
                    state.record_placement(booking_id, uid, get_pod_gpu_count(pod))
                continue

            gpu_count = get_pod_gpu_count(pod)
            now = datetime.now(timezone.utc)
            admittable = state.find_admittable_reservation(
                namespace, gpu_class_label, gpu_count, now, horizon, group_label
            )

            if admittable is not None:
                # ---- reserved path: a match is open now, or opens soon ----
                state.remove_ondemand_candidate(uid)
                state.enqueue_pod(
                    uid, name, namespace, gpu_class_label, gpu_count, group_label
                )

                # Fast path: ADDED pod inside an open window — don't wait for
                # the queue processor's POD_LIST_TICK_INTERVAL tick (default 300 s).
                if event_type == "ADDED":
                    entry = state.task_queue.get(uid)
                    if entry is not None:
                        # Re-fetch now: enqueue_pod just stamped next_attempt_at
                        # with its own datetime.now(), which can be a hair later
                        # than the *now* captured above for the admittable check.
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
                continue

            min_rt = get_pod_min_runtime_seconds(pod)
            jit_eligible = (
                config.ondemand_placement_enabled
                and phase == "Pending"
                and min_rt is not None
                and (group_label is not None or config.required_group_label is None)
            )

            if jit_eligible:
                # ---- JIT on-demand path ----
                if event_type == "ADDED":
                    ts = get_pod_creation_timestamp(pod)
                    pod_created_at = ts if ts is not None else now
                    log.debug(
                        "Pod %s/%s ADDED: no admittable reservation (gpu-class=%s); "
                        "routing to JIT on-demand queue",
                        namespace,
                        name,
                        gpu_class_label,
                    )
                    state.add_ondemand_candidate(
                        uid, name, namespace, gpu_class_label, gpu_count, min_rt,
                        pod_created_at, group_label,
                    )
                # Attempt immediately (ADDED) and on every subsequent MODIFIED
                # re-check (respecting the retry cooldown), so a guard-1 "not
                # yet scheduled" short retry resolves quickly rather than
                # waiting a full queue-processor tick.
                candidate = state.ondemand_candidates.get(uid)
                if candidate is not None and now >= candidate.next_attempt_at:
                    if await _try_request_lease(state, client, config, uid, candidate):
                        state.remove_ondemand_candidate(uid)
                continue

            # Not JIT-eligible (missing the min-runtime annotation or the
            # required group label): preserve the existing wait-for-window
            # behaviour if some future reservation matches, however far off
            # or over budget; otherwise leave the pod Pending.
            any_match = state.find_best_reservation(namespace, gpu_class_label, group_label)
            if any_match is not None:
                state.enqueue_pod(
                    uid, name, namespace, gpu_class_label, gpu_count, group_label
                )
            elif event_type == "ADDED":
                log.debug(
                    "Pod %s/%s: no matching reservation and not JIT-eligible; left Pending",
                    namespace,
                    name,
                )


async def _cancel_pending_noshows(
    state: ControllerState, client: ReservationClient, snapshot: list
) -> None:
    """Durably cancel each declared no-show still awaiting one (POST
    ``/api/reservations/{id}/cancel``, ``reason="no-show"``), so the app can
    re-book the window immediately and a restart never needs to re-arm it.

    Re-verified against *snapshot* (this tick's fresh pod snapshot) first — an
    id with a pod now admitted under it (a last-second arrival racing the
    declaration) is skipped rather than cancelled out from under it.  On
    success the id is dropped from ``state.reservations`` and
    ``state.pending_noshow_cancels``; on failure it is left pending and
    retried next tick.
    """
    if not state.pending_noshow_cancels:
        return
    occupied_ids = {
        p.reservation_id
        for p in snapshot
        if p.phase in ("Running", "Pending") and p.reservation_id is not None
    }
    async with state.reservation_lock:
        for rid in sorted(state.pending_noshow_cancels):
            if rid in occupied_ids:
                log.info(
                    "No-show cancel for reservation #%d skipped this tick: "
                    "a pod is now admitted under it",
                    rid,
                )
                continue
            if await client.cancel_reservation(rid, "no-show"):
                state.reservations = [r for r in state.reservations if r.id != rid]
                state.pending_noshow_cancels.discard(rid)
                log.info("Reservation #%d cancelled (no-show)", rid)
            else:
                log.warning(
                    "Failed to cancel no-show reservation #%d; will retry next tick",
                    rid,
                )


# ---------------------------------------------------------------------------
# Background loop 3: queue processor
# ---------------------------------------------------------------------------


async def queue_processor_loop(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Every ``config.pod_list_tick_interval`` s, scan the work queue and apply tolerations where eligible.

    Reserved-path logic per entry:
    1. If the reservation window has expired → remove from queue.
    2. If the window hasn't opened yet, or the entry is in retry cooldown → skip.
    3. Delegate budget check + patch to ``_try_apply_toleration``.

    JIT on-demand path (when ``config.ondemand_placement_enabled``):
    4. For each candidate whose ``next_attempt_at`` has passed, attempt a lease
       request (``_try_request_lease``) in FIFO order.

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
            snapshot = await snapshot_tolerated_pods(
                TOLERATION_KEY, config.required_group_label
            )
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
            # Claim every window a live holder occupies (chain-aware) before
            # declaring no-shows.
            holder_ids = [
                p.reservation_id for p in live if p.reservation_id is not None
            ]
            state.refresh_claimed_reservations(holder_ids, now)

        state.check_noshow_deadlines(now)

        # No-show → cancel: durably free the window app-side.  Skipped
        # entirely when the snapshot failed this tick; pending ids simply
        # retry next tick.
        if snapshot is not None:
            await _cancel_pending_noshows(state, client, snapshot)

        # Adopt overstay pods whose user has re-booked capacity: re-link them to
        # the new reservation so they stop surfacing as overstay even when no
        # boundary is near for the preemption sweep to act on.  Held under the
        # reservation lock (unlike the rest of this tick) so a concurrent
        # fetch/push cannot swap the reservation set across the patch awaits.
        if config.pod_adoption_enabled and snapshot is not None:
            async with state.reservation_lock:
                await _adopt_pods(
                    state, config, [_pod_view(p) for p in snapshot], now
                )

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

        # --- JIT on-demand path: request leases for due candidates, FIFO ---
        if config.ondemand_placement_enabled:
            ordered = sorted(
                state.ondemand_candidates.items(), key=lambda kv: kv[1].pod_created_at
            )
            for cand_uid, candidate in ordered:
                if now < candidate.next_attempt_at:
                    continue
                if await _try_request_lease(state, client, config, cand_uid, candidate):
                    state.remove_ondemand_candidate(cand_uid)

        log.debug(
            "Queue processor tick: %d reserved queue entr(ies), %d on-demand candidate(s)",
            len(state.task_queue),
            len(state.ondemand_candidates),
        )


# ---------------------------------------------------------------------------
# Background loop 4: preemption sweep
# ---------------------------------------------------------------------------


def _pod_view(p) -> PodRuntimeView:
    """Digest a ``k8s_client.ToleratedPodInfo`` into the plain view the pure
    preemption-planning functions in controller.py operate on."""
    return PodRuntimeView(
        uid=p.uid,
        namespace=p.namespace,
        name=p.name,
        gpu_class=p.gpu_class,
        gpu_count=p.gpu_count,
        reservation_id=p.reservation_id,
        node_resident=(p.phase == "Running" or (p.phase == "Pending" and not p.scheduled_false)),
        terminating=p.deletion_timestamp is not None,
        group_label=p.group_label,
    )


def _preemption_message(
    state: ControllerState, view: PodRuntimeView, boundary: datetime, now: datetime
) -> str:
    """Build the human-readable ``Preempted`` event message for *view*."""
    end = state.guarantee_end(view.reservation_id, now=now)
    if end is not None:
        overstay = max(0, int((now - end).total_seconds()))
        minutes, secs = divmod(overstay, 60)
        hours, minutes = divmod(minutes, 60)
        human = f"{hours}h{minutes:02d}m{secs:02d}s" if hours else f"{minutes}m{secs:02d}s"
        guarantee_desc = f"overstayed its runtime guarantee by {human}"
    else:
        guarantee_desc = "its runtime guarantee could no longer be resolved (reservation no longer active)"
    return (
        f"Pod preempted to free capacity for reservation(s) starting "
        f"{boundary.strftime('%Y-%m-%dT%H:%M:%SZ')}: {guarantee_desc}."
    )


async def _preempt_pod(
    state: ControllerState, namespace: str, name: str, uid: str, message: str
) -> None:
    """Delete an overstaying pod to recover capacity.

    Mirrors ``_handle_cancelled_reservations``'s shape exactly: emit the event before
    deleting (best-effort — a failed emit does not block the delete), then
    delete and release occupancy together (best-effort — if the delete fails,
    occupancy is left as-is since the pod may still be there).
    """
    try:
        pod_obj = await read_pod(name, namespace)
        await emit_preempted_event(pod_obj, name, namespace, message)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Could not emit Preempted event for pod %s/%s: %s", namespace, name, exc
        )
    try:
        await delete_pod(name, namespace)
        state.release_pod(uid)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not delete pod %s/%s: %s", namespace, name, exc)


async def _adopt_pods(
    state: ControllerState,
    config: Config,
    pods: list[PodRuntimeView],
    now: datetime,
) -> None:
    """Re-link overstay pods to a reservation their user has since booked.

    Caller must hold ``state.reservation_lock`` and pass the ``pods`` list it
    derived from a fresh ``snapshot_tolerated_pods``.  For each rescue planned
    by ``plan_pod_adoptions`` (a pod already past its runtime guarantee whose
    user has since booked a non-abutting or differently-sized follow-on
    window), re-annotate the pod's booking-reference to the new reservation
    (the toleration is already present, so this patch just rewrites the
    annotation) and, **only on patch success**, move its occupancy and update
    the in-memory view so subsequent planning in the same tick sees the new
    binding.  Each pod is independently best-effort: a failure logs a warning
    and never deletes the pod.  *pods* is mutated in place — an adopted entry
    is replaced with a view carrying the new reservation id.
    """
    if not config.pod_adoption_enabled:
        return
    for view, res_new in state.plan_pod_adoptions(pods, now):
        booking_reference = make_booking_reference(res_new.id)
        try:
            fresh_pod = await read_pod(view.name, view.namespace)
            if is_terminal_phase(fresh_pod):
                continue
            await apply_toleration(
                view.name,
                view.namespace,
                fresh_pod,
                TOLERATION_KEY,
                view.gpu_class,
                booking_reference,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Failed to re-link overstay pod %s/%s to reservation #%d: %s",
                view.namespace,
                view.name,
                res_new.id,
                exc,
            )
            continue

        # Patch landed: re-home occupancy and refresh the in-memory view so the
        # adopted pod contributes zero demand and is no longer past-guarantee.
        state.relink_occupancy(view.uid, res_new.id, view.gpu_count)
        idx = pods.index(view)
        pods[idx] = replace(view, reservation_id=res_new.id)

        guaranteed_until = state.compute_guaranteed_until(now, res_new)
        log.info(
            "Re-linked overstay pod %s/%s to reservation #%d (guaranteed until %s)",
            view.namespace,
            view.name,
            res_new.id,
            guaranteed_until.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        await _record_guarantee(
            view.name, view.namespace, fresh_pod, guaranteed_until, now
        )
        try:
            await emit_overstay_relinked_event(
                fresh_pod, view.name, view.namespace, res_new.id, guaranteed_until
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Could not emit OverstayRelinked event for pod %s/%s: %s",
                view.namespace,
                view.name,
                exc,
            )


def _build_selection_request(need: BoundaryPreemptionNeed) -> PreemptionSelectionRequest:
    """Flatten a boundary's per-class candidate pool into an app request body."""
    candidates = [
        PreemptionCandidate(
            pod_uid=p.uid,
            namespace=p.namespace,
            pod_name=p.name,
            gpu_class=gpu_class,
            gpu_count=p.gpu_count,
            reservation_id=p.reservation_id,
        )
        for gpu_class, cands in need.candidates_by_class.items()
        for p in cands
    ]
    return PreemptionSelectionRequest(
        needed_by_class=dict(need.kills_needed_by_class),
        candidates=candidates,
    )


def _map_selected_victims(
    need: BoundaryPreemptionNeed, uids: list[str]
) -> dict[str, list[PodRuntimeView]]:
    """Resolve the app's chosen ``pod_uid``s back to candidate views, by class.

    Only pods the controller actually *offered* can be selected: an unknown or
    duplicate uid is dropped (the app can choose among the candidates but can
    never introduce a new victim), so a buggy or malicious response can never
    make the controller kill a pod it did not independently deem preemptable.
    """
    by_uid = {
        p.uid: (gpu_class, p)
        for gpu_class, cands in need.candidates_by_class.items()
        for p in cands
    }
    selected: dict[str, list[PodRuntimeView]] = {}
    seen: set[str] = set()
    for uid in uids:
        entry = by_uid.get(uid)
        if entry is None:
            log.warning(
                "Preemption victim selection returned unknown pod uid=%s; ignoring",
                uid,
            )
            continue
        if uid in seen:
            continue
        seen.add(uid)
        gpu_class, view = entry
        selected.setdefault(gpu_class, []).append(view)
    return selected


async def _select_boundary_victims(
    config: Config,
    client: Optional[ReservationClient],
    need: BoundaryPreemptionNeed,
) -> dict[str, list[PodRuntimeView]]:
    """Choose victims for one boundary, delegating to the app when possible.

    When delegation is enabled and a client is available, the eligible pool is
    sent to the app (``POST /api/reservations/preemption-victims``) so it can
    prioritise; the returned uids are mapped back to candidate views.  A
    ``None`` return (endpoint absent / network / parse failure) falls back to
    local uniform-random selection so preemption still works when the app is
    unreachable — an empty list, by contrast, is a deliberate app decision and
    is respected as-is.
    """
    if config.preemption_delegate_selection and client is not None:
        uids = await client.select_preemption_victims(_build_selection_request(need))
        if uids is not None:
            return _map_selected_victims(need, uids)
        log.warning(
            "Preemption victim selection unavailable; using local random fallback"
        )
    return select_victims_locally(need)


async def _run_preemption_sweep(
    state: ControllerState,
    config: Config,
    client: Optional[ReservationClient] = None,
    now: Optional[datetime] = None,
) -> None:
    """One preemption-sweep evaluation: clear demand at any in-scope boundary.

    For each slot boundary within ``PREEMPTION_LEAD_MINUTES`` of *now* whose
    phase ("A" = lead-time, "B" = at-boundary) has not already been evaluated,
    plan the kills needed to cover its demand and execute them.  The two
    snapshots (pods, node capacity) are taken outside the lock; either
    failing skips the whole sweep with a WARNING — the controller never kills
    a pod based on unknown physical state.  Planning and the resulting
    deletions run under ``reservation_lock`` (mirrors the
    cancellation/owner-change eviction paths); boundaries are
    processed in ascending order with a running ``doomed`` set so one sweep
    never double-selects a pod's GPUs across two boundaries.
    """
    now = now or datetime.now(timezone.utc)
    lead = timedelta(minutes=config.preemption_lead_minutes)
    state.prune_preemption_marks(now, lead)
    boundaries = state.upcoming_boundaries(now, lead)
    if not boundaries:
        return

    try:
        snapshot = await snapshot_tolerated_pods(
            TOLERATION_KEY, config.required_group_label
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Preemption sweep: failed to snapshot pods: %s", exc, exc_info=True)
        return
    try:
        capacity = await snapshot_node_gpu_capacity(TOLERATION_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Preemption sweep: failed to snapshot node GPU capacity: %s", exc, exc_info=True
        )
        return

    async with state.reservation_lock:
        pods = [_pod_view(p) for p in snapshot]
        # Rescue overstay pods whose user has re-booked capacity before planning
        # any kills: an adopted pod's occupancy re-homes to its new reservation
        # (zeroing that boundary's demand) and its refreshed view is no longer
        # past-guarantee, so it can never be selected as a victim.
        await _adopt_pods(state, config, pods, now)
        doomed: set[str] = set()
        to_kill: list[tuple[PodRuntimeView, str]] = []
        for boundary in boundaries:
            phase = "B" if boundary <= now else "A"
            fired = state.preemption_fired.setdefault(boundary, set())
            if phase in fired:
                continue
            fired.add(phase)
            available_pods = [p for p in pods if p.uid not in doomed]
            need = state.plan_boundary_candidates(boundary, capacity, available_pods, now)
            # Delegate the *choice* of victims to the app (it can prioritise);
            # the controller still owns which pods are eligible at all.  Skip the
            # round-trip entirely when nothing needs reclaiming here.
            if need.kills_needed_by_class:
                selected = await _select_boundary_victims(config, client, need)
            else:
                selected = {}
            plan = build_preemption_plan(need, selected)
            if plan.demand_by_class:
                log.info(
                    "Preemption sweep boundary=%s phase=%s: demand=%s free=%s kills=%d",
                    boundary.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    phase,
                    plan.demand_by_class,
                    plan.free_by_class,
                    len(plan.victims),
                )
            for gpu_class, shortfall in plan.unmet_by_class.items():
                log.warning(
                    "Preemption sweep boundary=%s phase=%s: %d GPU(s) of gpu-class=%s "
                    "still short after preempting all eligible overstayers",
                    boundary.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    phase,
                    shortfall,
                    gpu_class,
                )
            for victim in plan.victims:
                doomed.add(victim.uid)
                to_kill.append((victim, _preemption_message(state, victim, boundary, now)))

        for victim, message in to_kill:
            log.info(
                "Preempting pod %s/%s (gpu-class=%s, gpus=%d): %s",
                victim.namespace,
                victim.name,
                victim.gpu_class,
                victim.gpu_count,
                message,
            )
            await _preempt_pod(state, victim.namespace, victim.name, victim.uid, message)


async def preemption_loop(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Every ``config.preemption_check_interval`` s, run a preemption sweep.

    Passes the reservation *client* through so the sweep can delegate victim
    selection to the app (``PREEMPTION_DELEGATE_SELECTION``); the sweep falls
    back to local random selection if that call fails.
    """
    while True:
        await asyncio.sleep(config.preemption_check_interval)
        try:
            await _run_preemption_sweep(state, config, client)
        except Exception as exc:  # noqa: BLE001
            log.error("Preemption sweep failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    config: Config = app.state.config  # injected in create_app()
    client = ReservationClient(config)
    state = ControllerState()
    # Enable the optional usage-group match constraint (REQUIRED_GROUP_LABEL).
    # None keeps the group gate off, preserving prior behaviour.
    state.required_group_label = config.required_group_label

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

    # Launch the four background loops as asyncio tasks.
    tasks = [
        asyncio.create_task(
            reservation_fetch_loop(state, client, config),
            name="reservation-fetch",
        ),
        asyncio.create_task(pod_watch_loop(state, client, config), name="pod-watch"),
        asyncio.create_task(
            queue_processor_loop(state, client, config), name="queue-processor"
        ),
        asyncio.create_task(preemption_loop(state, client, config), name="preemption"),
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


def _require_inbound_auth(
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    """Authenticate an inbound API call (push, forecast) via a static bearer token.

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
    dependencies=[Depends(_require_inbound_auth)],
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
            merged_active,
            cancelled_in_window,
            owner_changes,
            now,
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
# Preemption-risk forecast API
# ---------------------------------------------------------------------------


def _forecast_response(
    forecast: PreemptionForecast,
    config: Config,
    namespace: Optional[str],
) -> PreemptionRiskForecastResponse:
    """Convert a pure ``PreemptionForecast`` into the API response shape.

    *namespace* filters ``pods`` only — the bucket/class summaries stay
    cluster-global, because every displayed risk's denominator
    (``eligible_pool_gpus``) and driver (demand from *other* users' bookings)
    are global; a scoped summary could not explain the pod numbers beside it.
    Pure, so it is unit-testable without a TestClient.
    """
    buckets = [
        ForecastBucket(
            start=b.start,
            end=b.end,
            classes={
                c: ForecastClassSummary(
                    capacity=b.capacity_by_class.get(c, 0),
                    free=b.free_by_class.get(c, 0),
                    demand=b.demand_by_class.get(c, 0),
                    shortfall=b.shortfall_by_class.get(c, 0),
                    eligible_pool_gpus=b.eligible_pool_gpus_by_class.get(c, 0),
                    pending_jit_gpus=b.pending_jit_gpus_by_class.get(c, 0),
                )
                # All six summary maps share one key set (see
                # ForecastBucketSummary), so iterating any one of them is safe.
                for c in b.capacity_by_class
            },
        )
        for b in forecast.buckets
    ]
    pods = [
        ForecastPod(
            namespace=pf.view.namespace,
            name=pf.view.name,
            uid=pf.view.uid,
            gpu_class=pf.view.gpu_class,
            gpu_count=pf.view.gpu_count,
            reservation_id=pf.view.reservation_id,
            guarantee_end=pf.guarantee_end,
            buckets=[
                ForecastPodBucket(risk=pb.risk, state=pb.state) for pb in pf.buckets
            ],
        )
        for pf in forecast.pods
        if namespace is None or pf.view.namespace == namespace
    ]
    return PreemptionRiskForecastResponse(
        generated_at=forecast.generated_at,
        lead_minutes=forecast.lead_minutes,
        selection_delegated=config.preemption_delegate_selection,
        buckets=buckets,
        pods=pods,
    )


@app.get(
    "/api/forecast/preemption-risk",
    tags=["forecast"],
    response_model=PreemptionRiskForecastResponse,
    dependencies=[Depends(_require_inbound_auth)],
)
async def preemption_risk_forecast(
    request: Request, namespace: Optional[str] = None
) -> PreemptionRiskForecastResponse:
    """Per-pod preemption-risk forecast for the current + next two hours.

    Read-only: projects the same demand/free/eligibility arithmetic the
    preemption sweep runs, across every booking boundary in the horizon.  A
    pod inside its runtime guarantee has zero risk; an overstayer's risk is
    the projected GPU shortfall over the eligible overstay pool for each
    boundary whose kill window (``[boundary − lead, boundary]``) touches the
    bucket.  ``?namespace=`` filters the ``pods`` list only (unknown
    namespace ⇒ empty list, 200); summaries stay cluster-global.

    503 when either cluster snapshot fails — the forecast never reports risk
    based on unknown physical state (same fail-safe rule as the sweep).
    """
    state: ControllerState = request.app.state.controller_state
    config: Config = request.app.state.config
    now = datetime.now(timezone.utc)

    # Snapshots are awaited OUTSIDE the lock, mirroring the preemption sweep.
    try:
        snapshot = await snapshot_tolerated_pods(
            TOLERATION_KEY, config.required_group_label
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Forecast: failed to snapshot pods: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cluster pod snapshot unavailable; forecast cannot be computed",
        )
    try:
        capacity = await snapshot_node_gpu_capacity(TOLERATION_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Forecast: failed to snapshot node GPU capacity: %s", exc, exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Node capacity snapshot unavailable; forecast cannot be computed",
        )

    async with state.reservation_lock:
        pods = [_pod_view(p) for p in snapshot]
        pending = list(state.ondemand_candidates.values())
        forecast = state.forecast_preemption_risk(
            capacity,
            pods,
            pending,
            now,
            lead=timedelta(minutes=config.preemption_lead_minutes),
        )

    return _forecast_response(forecast, config, namespace)


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
