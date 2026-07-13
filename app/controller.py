"""Core controller state machine.

This module owns the in-memory state shared by the three background loops:

``ControllerState``    — reservations list, GPU-class label map, task queue,
                         on-demand candidates, on-demand occupancy map
``QueueEntry``         — one pod waiting for its reservation window (reserved path)
``OnDemandCandidate``  — one pod searching for an on-demand block (on-demand path)
``slot_start / slot_end`` — UTC time-window accessors
``TOLERATION_KEY``     — the taint/toleration key used by the reservation system

No Kubernetes or HTTP I/O is done here; those live in k8s_client.py and
reservation_client.py respectively.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from .schemas import ReservationResponse

log = logging.getLogger(__name__)


def canceller_description(r: ReservationResponse) -> str:
    """Return the 'by X' fragment for a ReservationCancelled event message.

    Priority:
    1. ``cancelled_by`` UserBrief present and is a different user → "by <username>"
    2. ``cancelled_by_id`` set, differs from owner, no name available → "by another user"
    3. Fallback → "by user" (self-cancellation or unknown canceller)
    """
    if r.cancelled_by is not None:
        if r.user_id is None or r.cancelled_by.id != r.user_id:
            return f"by {r.cancelled_by.username}"
    elif r.cancelled_by_id is not None:
        if r.user_id is None or r.cancelled_by_id != r.user_id:
            return "by another user"
    return "by user"

# Toleration key applied by the controller.  The full toleration is:
#   gpu-class-reservation=<gpu-class-label>:NoSchedule
TOLERATION_KEY = "gpu-class-reservation"


# ---------------------------------------------------------------------------
# Time-window helpers
# ---------------------------------------------------------------------------


def slot_start(r: ReservationResponse) -> datetime:
    """Return the UTC start of *r*'s reserved window."""
    return r.start_utc


def slot_end(r: ReservationResponse) -> datetime:
    """Return the UTC end of *r*'s reserved window."""
    return r.end_utc


def apply_push_to_active(
    current: list[ReservationResponse],
    pushed: list[ReservationResponse],
) -> list[ReservationResponse]:
    """Upsert *pushed* entries into the *current* active reservation list by id.

    Used by the inbound push API to apply a partial delta:
    - a pushed entry with ``status == "active"`` replaces any existing entry of
      the same id, or is inserted if new (upsert);
    - a pushed entry with any other ``status`` (e.g. ``"cancelled"``) removes
      that id from the active set.

    Pure and I/O-free so it is unit-testable in isolation; the eviction of any
    already-admitted pod for a cancelled entry is handled separately by the
    caller via ``detect_cancelled_in_window`` + the cancellation handler.  When
    the same id appears more than once in *pushed*, the last occurrence wins.
    """
    by_id: dict[int, ReservationResponse] = {r.id: r for r in current}
    for r in pushed:
        if r.status == "active":
            by_id[r.id] = r
        else:
            by_id.pop(r.id, None)
    return list(by_id.values())


def free_capacity_by_class(
    capacity_by_class: dict[str, int],
    pods: "list[PodRuntimeView]",
) -> dict[str, int]:
    """Return free GPUs per class: physical capacity minus node-resident use.

    Only pods that are ``node_resident`` and not ``terminating`` count against
    capacity — a terminating pod's GPUs are already being recovered by
    Kubernetes and are treated as free for planning purposes.  A class absent
    from *capacity_by_class* (unknown physical capacity) is not included in
    the result.  A returned value may be negative, signalling an
    over-committed class (more GPUs in use than the snapshot says exist).
    """
    used: dict[str, int] = {}
    for p in pods:
        if not p.node_resident or p.terminating:
            continue
        if p.gpu_class not in capacity_by_class:
            continue
        used[p.gpu_class] = used.get(p.gpu_class, 0) + p.gpu_count
    return {
        gpu_class: capacity - used.get(gpu_class, 0)
        for gpu_class, capacity in capacity_by_class.items()
    }


# ---------------------------------------------------------------------------
# Task queue entry
# ---------------------------------------------------------------------------


@dataclass
class QueueEntry:
    """A pod that has been matched to a reservation and is waiting its turn."""

    pod_uid: str
    pod_name: str
    pod_namespace: str
    gpu_class_label: str   # value of pod label "gpu-class" (e.g. "h100")
    gpu_requested: int     # nvidia.com/gpu units requested by this pod
    reservation: ReservationResponse
    next_attempt_at: datetime  # earliest time to try applying the toleration
    group_label: Optional[str] = None  # value of REQUIRED_GROUP_LABEL pod label; None when disabled


@dataclass
class OnDemandCandidate:
    """A pod eligible for a just-in-time (JIT) on-demand reservation lease.

    Distinct from QueueEntry: a QueueEntry is bound to a specific reservation;
    an OnDemandCandidate has none yet and requests one from the reservation
    app (``main._try_request_lease``).
    """

    pod_uid: str
    pod_name: str
    pod_namespace: str
    gpu_class_label: str   # value of pod label "gpu-class" (e.g. "h100")
    gpu_requested: int     # nvidia.com/gpu units requested by this pod
    min_runtime_seconds: int  # horae/minimum-runtime-seconds annotation value
    pod_created_at: datetime  # metadata.creationTimestamp; used for FIFO ordering
    next_attempt_at: datetime  # earliest time to try requesting a lease
    group_label: Optional[str] = None  # value of REQUIRED_GROUP_LABEL pod label; None when disabled


@dataclass(frozen=True)
class PodRuntimeView:
    """Preemption-planning view of one live tolerated pod.

    Built by main.py from a ``k8s_client.ToleratedPodInfo`` (digesting the
    Kubernetes-shaped snapshot into the plain fields the pure planning
    functions below need) — keeping this module free of Kubernetes imports.
    """

    uid: str
    namespace: str
    name: str
    gpu_class: str
    gpu_count: int
    reservation_id: Optional[int]  # parsed booking-reference id; None = not ours
    node_resident: bool            # Running, or (Pending and not scheduled_false)
    terminating: bool              # metadata.deletionTimestamp is set
    group_label: Optional[str] = None  # value of REQUIRED_GROUP_LABEL pod label; None when disabled


@dataclass
class PreemptionPlan:
    """Outcome of ``ControllerState.plan_boundary_preemption`` for one boundary/phase."""

    boundary: datetime
    phase: str                       # "A" (lead-time) or "B" (at-boundary)
    demand_by_class: dict[str, int]
    free_by_class: dict[str, int]
    victims: list[PodRuntimeView]
    unmet_by_class: dict[str, int]    # shortfall remaining after all eligible kills


# ---------------------------------------------------------------------------
# Shared controller state
# ---------------------------------------------------------------------------


class ControllerState:
    """In-memory state accessed by all background loops.

    All mutations happen inside the asyncio event loop, so no locking is
    required for the background loops (asyncio is single-threaded).  The one
    exception is ``reservation_lock``, which serialises reservation-state
    reconciliation between the fetch loop and the inbound push endpoint — two
    coroutines that both mutate ``reservations`` across ``await`` points.
    """

    def __init__(self) -> None:
        # Current snapshot of active reservations from the API.
        self.reservations: list[ReservationResponse] = []

        # Mapping from gpu_class_id → Kubernetes label value (e.g. "h100").
        # Refreshed each reconcile from GET /api/gpu-classes (full list), with a
        # per-id GET /api/gpu-classes/{id} fallback for a class referenced by a
        # reservation but missing from the bulk list.
        self.gpu_class_labels: dict[int, str] = {}

        # Reverse of gpu_class_labels (label → id).  A pod carries only its
        # gpu-class label, but requesting a JIT lease needs the numeric
        # gpu_class_id, so this map is what main._try_request_lease resolves
        # against.  Refreshed alongside gpu_class_labels.
        self.gpu_class_ids: dict[str, int] = {}

        # Name of the pod label naming the usage group to match (REQUIRED_GROUP_LABEL),
        # or None when the feature is disabled.  Set once from config at startup.
        # When set, the reserved-path matchers additionally require a pod's group
        # label value to equal the reservation's group.name (see _group_ok).
        self.required_group_label: Optional[str] = None

        # Active work queue keyed by pod UID (reserved path).
        self.task_queue: dict[str, QueueEntry] = {}

        # Pods eligible for on-demand placement, keyed by pod UID.
        # These have the horae/minimum-runtime-seconds annotation and are
        # Pending, but do not match any user reservation.
        self.ondemand_candidates: dict[str, OnDemandCandidate] = {}

        # Unified occupancy map for ALL admitted pods (reserved, on-demand, and
        # no-show alike): reservation id → { pod_uid: gpu_count }.  Available
        # capacity on any reservation = reservation.gpu_count - sum(values).
        #
        # Kept warm incrementally (record_placement on admission, release_pod on
        # vacate) and rebuilt from a live cluster snapshot each queue-processor
        # tick (reconcile_occupancy), so a missed watch event self-heals within
        # one tick.  Each pod is bucketed by the reservation id parsed from its
        # horae/booking-reference, so no separate annotation is needed.
        self.occupancy: dict[int, dict[str, int]] = {}

        # Safety interlock (guard 3): set of GPU class labels that currently
        # have at least one reservation-holder pod stuck Pending.  Updated each
        # queue_processor_loop tick.  On-demand placement is held for any class
        # in this set; other classes are unaffected.  Empty = no interlock.
        self.stuck_holder_gpu_classes: set[str] = set()

        # No-show tracking:
        # Maps reservation_id → deadline by which a matching pod must appear.
        # Cleared when a pod is matched; moved to noshow_reservation_ids on expiry.
        self.noshow_deadlines: dict[int, datetime] = {}

        # Reservations permanently declared no-show for this controller lifetime.
        # Excluded from reserved-path matching and chaining from the moment of
        # declaration (see find_best_reservation / _chain_for).
        self.noshow_reservation_ids: set[int] = set()

        # Declared no-shows still awaiting a durable controller-issued cancel
        # (POST /api/reservations/{id}/cancel, reason="no-show").  Populated by
        # check_noshow_deadlines; drained by queue_processor_loop once the
        # cancel succeeds.  Retried every tick until it does, since a durable
        # app-side cancel is what actually frees the window for re-booking.
        self.pending_noshow_cancels: set[int] = set()

        # Reservation ids currently "claimed" by a live reserved-path holder pod
        # — each holder's booking reservation plus the back-to-back chain its
        # runtime cap extends across (see reservations_claimed_by).  A reservation
        # in this set must not be declared a no-show or lent out as on-demand
        # capacity, because a holder is actively occupying its window via a
        # chained deadline even though no pod is booked directly under it.
        # Recomputed from the live pod snapshot each queue-processor tick.
        self.claimed_reservation_ids: set[int] = set()

        # Preemption sweep bookkeeping: for each upcoming slot boundary,
        # which phase(s) ("A" = lead-time, "B" = at-boundary) have already
        # been evaluated.  Prevents a flapping snapshot from re-planning (and
        # potentially re-selecting fresh victims) within the same phase
        # window; a restart simply loses the marks and re-evaluates once,
        # which is safe because recomputation is idempotent (a killed pod
        # shows as Terminating or gone).  Pruned by ``prune_preemption_marks``.
        self.preemption_fired: dict[datetime, set[str]] = {}

        # Serialises reservation-state reconciliation between the fetch loop and
        # the inbound push endpoint.  Both mutate ``reservations`` and evict pods
        # across ``await`` points; without this lock a push landing mid-fetch
        # could be clobbered by the fetch's wholesale replace, or double-evict.
        # (This is the one place the "no locking" note above is relaxed, now that
        # an inbound HTTP handler runs concurrently with the background loops.)
        self.reservation_lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # No-show tracking
    # ------------------------------------------------------------------

    def update_noshow_tracking(
        self,
        now: datetime,
        timeout_minutes: int,
        grace_minutes: int,
        reason: str = "new",
    ) -> None:
        """Arm no-show deadlines for active user reservations not yet tracked.

        Called once after the initial reservation fetch (``reason="init"``) and
        after each subsequent refresh (``reason="new"``, the default).  For each
        active user reservation not already tracked, not already declared a
        no-show, and not claimed by a live chained holder:
        - Window not yet open: deadline = slot_start + timeout_minutes
        - Window already open (mid-window startup): deadline = now + grace_minutes
        - Window already expired: skipped

        Does not overwrite existing deadlines or resurrect declared no-shows.  At
        startup the ``noshow_reservation_ids`` / ``claimed_reservation_ids``
        guards are provably empty, so this is a superset of the old
        ``initialize_noshow_tracking`` — which was deleted so a guard added here
        can never be forgotten in a parallel copy (CODE-REVIEW D7).
        """
        for r in self.reservations:
            if r.kind != "booking" or r.user is None:
                continue
            if r.id in self.noshow_deadlines:
                continue
            if r.id in self.noshow_reservation_ids:
                continue
            if r.id in self.claimed_reservation_ids:
                continue  # a live chained holder is occupying this window
            end = slot_end(r)
            start = slot_start(r)
            if end <= now:
                continue
            if start > now:
                deadline = start + timedelta(minutes=timeout_minutes)
            else:
                deadline = now + timedelta(minutes=grace_minutes)
            self.noshow_deadlines[r.id] = deadline
            log.debug(
                "No-show tracking (%s): reservation #%d deadline=%s",
                reason,
                r.id,
                deadline.strftime("%Y-%m-%d %H:%M"),
            )

    def reconcile_noshow(self) -> None:
        """Prune no-show state for reservations no longer in the active list.

        Called after each reservation refresh alongside reconcile_queue.  An id
        leaves the active list once its controller-issued cancel has actually
        landed (or the app removed it some other way), so ``pending_noshow_cancels``
        is pruned the same way as ``noshow_reservation_ids`` — no further retry
        is needed for an id the next fetch no longer reports as active.
        """
        active_ids = {r.id for r in self.reservations}
        stale = [rid for rid in self.noshow_deadlines if rid not in active_ids]
        for rid in stale:
            self.noshow_deadlines.pop(rid)
            log.debug("No-show deadline pruned: reservation #%d left active list", rid)
        stale_noshow = [rid for rid in self.noshow_reservation_ids if rid not in active_ids]
        for rid in stale_noshow:
            self.noshow_reservation_ids.discard(rid)
            log.info("No-show reservation #%d removed: left active list", rid)
        stale_pending = [rid for rid in self.pending_noshow_cancels if rid not in active_ids]
        for rid in stale_pending:
            self.pending_noshow_cancels.discard(rid)
            log.debug("Pending no-show cancel #%d pruned: left active list", rid)

    def check_noshow_deadlines(self, now: datetime) -> None:
        """Declare no-shows for any reservation whose deadline has passed.

        Called at the start of each queue_processor_loop tick.  Moves expired
        entries from noshow_deadlines into noshow_reservation_ids and queues
        them in pending_noshow_cancels for a durable controller-issued cancel
        (``queue_processor_loop`` sends ``POST /api/reservations/{id}/cancel``,
        reason="no-show", later in the same tick) — that cancel is what
        actually frees the window app-side for re-booking.  A reservation
        currently claimed by a live chained holder is never declared a no-show —
        ``refresh_claimed_reservations`` clears its deadline, and this guard is a
        belt-and-suspenders check against tick ordering.
        """
        expired = [
            rid
            for rid, deadline in self.noshow_deadlines.items()
            if now >= deadline and rid not in self.claimed_reservation_ids
        ]
        for rid in expired:
            del self.noshow_deadlines[rid]
            res = next((r for r in self.reservations if r.id == rid), None)
            # Don't declare a no-show for a window that has already ended: it can
            # never be re-booked anyway, so "capacity opened" would be a
            # misleading log line (CODE-REVIEW D9).  Just drop the deadline.
            if res is not None and slot_end(res) <= now:
                log.debug(
                    "No-show deadline for reservation #%d passed but window already "
                    "ended; not declaring no-show",
                    rid,
                )
                continue
            self.noshow_reservation_ids.add(rid)
            self.pending_noshow_cancels.add(rid)
            user = res.user.username if (res and res.user) else "unknown"
            gpu_class = self.gpu_class_labels.get(res.gpu_class_id, "unknown") if res else "unknown"
            log.info(
                "Reservation #%d declared no-show (user=%s, gpu-class=%s): "
                "no matching pod appeared before deadline; queued for cancellation",
                rid,
                user,
                gpu_class,
            )

    def mark_pod_seen_for_noshow(
        self,
        namespace: str,
        gpu_class_label: str,
        booking_reservation_id: Optional[int] = None,
        group_label: Optional[str] = None,
    ) -> None:
        """Clear the no-show deadline(s) vouched for by an already-admitted pod.

        Called when a pod carrying the toleration is detected.  Only a
        reserved-path holder (booking ``res-<id>``) vouches for a reservation;
        when *booking_reservation_id* is that holder's id we clear the deadline
        for **every** window the holder's chained session spans
        (``reservations_claimed_by``), closing the booked-id-vs-occupied-id gap.
        On-demand / no-show squatters (other prefixes) vouch for nothing.

        Falls back — when no booking id is available (e.g. a pod tolerated by
        something other than this controller) — to clearing the soonest matching
        user reservation by ``slot_start``, the historical behaviour.
        """
        if booking_reservation_id is not None:
            res = next(
                (r for r in self.reservations if r.id == booking_reservation_id),
                None,
            )
            if res is None or res.kind != "booking" or res.user is None:
                return  # on-demand / no-show booking — not a holder
            cleared = [
                rid
                for rid in self.reservations_claimed_by(booking_reservation_id)
                if self.noshow_deadlines.pop(rid, None) is not None
            ]
            if cleared:
                log.debug(
                    "No-show deadline(s) cleared for reservation(s) %s: holder pod "
                    "admitted (namespace=%s, gpu-class=%s)",
                    sorted(cleared),
                    namespace,
                    gpu_class_label,
                )
            return

        candidates = [
            r
            for r in self.reservations
            if r.kind == "booking"
            and r.user is not None
            and r.user.username == namespace
            and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
            and self._group_ok(r, group_label)
            and r.id in self.noshow_deadlines
        ]
        if not candidates:
            return
        best = min(candidates, key=slot_start)
        self.noshow_deadlines.pop(best.id, None)
        log.debug(
            "No-show deadline cleared for reservation #%d: "
            "matching pod already admitted (namespace=%s, gpu-class=%s)",
            best.id,
            namespace,
            gpu_class_label,
        )

    def refresh_claimed_reservations(
        self,
        holder_reservation_ids: list[int],
        now: Optional[datetime] = None,
    ) -> None:
        """Recompute ``claimed_reservation_ids`` from live reserved-path holders.

        *holder_reservation_ids* are the booking ids (``res-<id>``) of the
        Running/Pending holder pods currently in the cluster.  Each expands to
        its back-to-back chain, so every window a holder occupies — directly or
        via a chained deadline — is marked claimed.  Claimed reservations also
        have their no-show deadline cleared: a chained holder is actively using
        the window, so it must not count down toward no-show.  When the holder
        later vacates, the reservation drops out of the claimed set and the next
        ``update_noshow_tracking`` re-arms it with the grace timeout (the same
        path that recycles any vacated window).

        Called each queue-processor tick, before ``check_noshow_deadlines``.
        """
        now = now or datetime.now(timezone.utc)
        claimed: set[int] = set()
        for rid in holder_reservation_ids:
            claimed |= self.reservations_claimed_by(rid, now)
        self.claimed_reservation_ids = claimed
        for rid in claimed:
            self.noshow_deadlines.pop(rid, None)

    # ------------------------------------------------------------------
    # Reservation matching
    # ------------------------------------------------------------------

    def find_best_reservation(
        self,
        namespace: str,
        gpu_class_label: str,
        group_label: Optional[str] = None,
    ) -> Optional[ReservationResponse]:
        """Find the nearest upcoming (or active) reservation for *namespace* / *gpu_class_label*.

        Matching rules:
        - reservation.user.username == namespace
        - gpu_class_labels[reservation.gpu_class_id] == gpu_class_label
        - usage group satisfies REQUIRED_GROUP_LABEL (see _group_ok); *group_label*
          is the pod's group label value, ignored when the feature is disabled
        - reservation window has not yet expired

        When multiple matches exist, return the one whose window starts soonest.
        """
        now = datetime.now(timezone.utc)
        candidates = [
            r
            for r in self.reservations
            if r.kind == "booking"
            and r.user is not None
            and r.user.username == namespace
            and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
            and self._group_ok(r, group_label)
            and slot_end(r) > now  # still has time left
            and r.id not in self.noshow_reservation_ids
        ]
        if not candidates:
            return None
        return min(candidates, key=slot_start)

    def find_admittable_reservation(
        self,
        namespace: str,
        gpu_class_label: str,
        gpu_requested: int,
        now: datetime,
        horizon: timedelta,
        group_label: Optional[str] = None,
    ) -> Optional[ReservationResponse]:
        """Find the nearest reservation admittable soon, with room, for a pod.

        The JIT-routing-aware sibling of ``find_best_reservation``: that method
        answers "does any future reservation match at all" (used to preserve
        the existing wait-for-window behaviour for a pod that is not
        JIT-eligible); this one answers "is one admittable soon enough, with
        budget" — the trigger for routing a pod to the reserved queue instead
        of requesting a JIT on-demand lease. Matching rules are the same as
        ``find_best_reservation`` plus two additional requirements:
        - the window opens now or within *horizon* (``slot_start(r) <= now +
          horizon``)
        - it has spare budget for *gpu_requested* (``available(r) >=
          gpu_requested``)

        When multiple match, the one whose window starts soonest wins.
        """
        candidates = [
            r
            for r in self.reservations
            if r.kind == "booking"
            and r.user is not None
            and r.user.username == namespace
            and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
            and self._group_ok(r, group_label)
            and slot_end(r) > now
            and r.id not in self.noshow_reservation_ids
            and slot_start(r) <= now + horizon
            and self.available(r) >= gpu_requested
        ]
        if not candidates:
            return None
        return min(candidates, key=slot_start)

    def find_open_booking_for(
        self,
        namespace: str,
        gpu_class_label: str,
        now: datetime,
        gpu_requested: int,
        extra_used: Optional[dict[int, int]] = None,
        group_label: Optional[str] = None,
    ) -> Optional[ReservationResponse]:
        """Find a *currently-open* booking *namespace* holds that can adopt a pod.

        Used to re-link an overstay pod (one running past its runtime guarantee)
        to a reservation the same user has since booked, so it is no longer a
        preemption victim.  Unlike ``find_best_reservation`` this requires the
        window to be **open right now** (``slot_start <= now < slot_end``) and to
        have spare budget for *gpu_requested*.  *extra_used* lets a caller
        planning several adoptions in one pass reserve budget it has already
        assigned but not yet recorded (mirrors the ``available``-minus-running-tally
        shape ``plan_boundary_preemption`` uses).  Among candidates the
        latest-ending window wins (longest guarantee), ties broken by most free
        capacity.
        """
        extra_used = extra_used or {}
        candidates = [
            r
            for r in self.reservations
            if r.kind == "booking"
            and r.user is not None
            and r.user.username == namespace
            and self._effective_label(r) == gpu_class_label
            and self._group_ok(r, group_label)
            and slot_start(r) <= now < slot_end(r)
            and r.id not in self.noshow_reservation_ids
            and self.available(r) - extra_used.get(r.id, 0) >= gpu_requested
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda r: (slot_end(r), self.available(r)))

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------

    def enqueue_pod(
        self,
        pod_uid: str,
        pod_name: str,
        pod_namespace: str,
        gpu_class_label: str,
        gpu_requested: int,
        group_label: Optional[str] = None,
    ) -> None:
        """Match pod to a reservation and add to the work queue if eligible.

        If the pod is already queued for the same reservation, this is a no-op
        (idempotent).  If the reservation changes (e.g. old one cancelled), the
        entry is replaced.  *group_label* is the pod's REQUIRED_GROUP_LABEL value
        (ignored when the feature is disabled); it is carried on the QueueEntry so
        reconcile_queue can re-match with the same group constraint.
        """
        reservation = self.find_best_reservation(
            pod_namespace, gpu_class_label, group_label
        )
        if reservation is None:
            return  # no matching reservation; nothing to do

        # A pod appeared for this reservation — clear its no-show deadline.
        self.noshow_deadlines.pop(reservation.id, None)

        existing = self.task_queue.get(pod_uid)
        if existing and existing.reservation.id == reservation.id:
            return  # already queued for the same reservation

        entry = QueueEntry(
            pod_uid=pod_uid,
            pod_name=pod_name,
            pod_namespace=pod_namespace,
            gpu_class_label=gpu_class_label,
            gpu_requested=gpu_requested,
            reservation=reservation,
            next_attempt_at=datetime.now(timezone.utc),
            group_label=group_label,
        )
        self.task_queue[pod_uid] = entry
        log.info(
            "Enqueued pod %s/%s for reservation #%d "
            "(window %s–%s, %d GPU(s) reserved, pod requests %d)",
            pod_namespace,
            pod_name,
            reservation.id,
            slot_start(reservation).strftime("%Y-%m-%d %H:%M"),
            slot_end(reservation).strftime("%H:%M"),
            reservation.gpu_count,
            gpu_requested,
        )

    def dequeue_pod(self, pod_uid: str) -> None:
        """Remove a pod from the work queue (e.g. pod deleted, or toleration applied)."""
        if pod_uid in self.task_queue:
            entry = self.task_queue.pop(pod_uid)
            log.debug(
                "Dequeued pod %s/%s (uid=%s)",
                entry.pod_namespace,
                entry.pod_name,
                pod_uid,
            )

    def _chain_for(
        self,
        reservation: ReservationResponse,
        now: datetime,
    ) -> list[ReservationResponse]:
        """Return the back-to-back chain *following* *reservation*, in window order.

        A reservation extends the chain when it shares the same namespace, GPU
        class, and gpu_count, has not yet expired, is not a no-show, and starts
        exactly when the previous window ends (``slot_start(next) ==
        slot_end(previous)``).  When REQUIRED_GROUP_LABEL is enabled it must also
        share the same usage group (``group_id``), so a guarantee never chains
        across a different group's window.  *reservation* itself is **not**
        included.

        Shared by ``compute_guaranteed_until`` (runtime-guarantee arithmetic) and
        ``reservations_claimed_by`` (no-show protection) so both agree on what a
        single holder pod's session spans.
        """
        if reservation.user is None:
            return []
        namespace = reservation.user.username
        gpu_class_label = self.gpu_class_labels.get(reservation.gpu_class_id)
        gpu_count = reservation.gpu_count

        candidates = sorted(
            [
                r
                for r in self.reservations
                if r.id != reservation.id
                and r.kind == "booking"
                and r.user is not None
                and r.user.username == namespace
                and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
                and r.gpu_count == gpu_count
                and (
                    self.required_group_label is None
                    or r.group_id == reservation.group_id
                )
                and slot_end(r) > now
                and r.id not in self.noshow_reservation_ids
            ],
            key=slot_start,
        )

        chain: list[ReservationResponse] = []
        prev_end = slot_end(reservation)
        visited: set[int] = set()
        while True:
            # If several reservations abut the same instant (shouldn't happen in
            # this data model, but the two window-extension mechanisms must answer
            # it identically), take the longest — the same max(key=slot_end) rule
            # reclaim-merge discovery uses (CODE-REVIEW D4b).
            abutting = [
                r
                for r in candidates
                if r.id not in visited and slot_start(r) == prev_end
            ]
            if not abutting:
                break
            next_res = max(abutting, key=slot_end)
            chain.append(next_res)
            prev_end = slot_end(next_res)
            visited.add(next_res.id)
        return chain

    def compute_guaranteed_until(
        self,
        now: datetime,
        current_reservation: ReservationResponse,
    ) -> datetime:
        """Return the absolute UTC end of the runtime guarantee for a reserved-path
        admission under *current_reservation*.

        The guarantee is the end of *current_reservation*'s window, extended
        across any back-to-back future reservations that share the same
        namespace, GPU class, and gpu_count (see ``_chain_for``).  *now* is
        used only to select which future reservations still qualify as
        "future" — the result is an absolute instant, which callers write to
        the pod as an informational annotation.  No flooring: an absolute end
        may equal or even (in a stale-data race) precede *now*; callers
        convert to a duration in seconds where a positive value is required.
        """
        chain = self._chain_for(current_reservation, now)
        return slot_end(chain[-1]) if chain else slot_end(current_reservation)

    def guarantee_end(
        self,
        reservation_id: Optional[int],
        *,
        now: datetime,
    ) -> Optional[datetime]:
        """Return the absolute UTC end of the runtime guarantee for a pod admitted
        under *reservation_id*, or ``None`` if no live guarantee can be resolved.

        This is the live, restart-safe replacement for the frozen
        ``activeDeadlineSeconds`` a pod used to be capped at: it is recomputed
        from current window arithmetic on every call rather than read back from
        the pod, so a pod's guarantee can grow (an abutting follow-on booking)
        the way a Kubernetes deadline never could.

        Looks up *reservation_id* in ``self.reservations`` and returns the end
        of its back-to-back chain (``compute_guaranteed_until``).  ``None`` if
        the reservation is no longer in the active list — its window is over
        and the pod is unconditionally past guarantee — or if
        *reservation_id* is ``None`` (a pod not admitted by this controller, or
        whose booking-reference does not parse): it is never "ours" to
        guarantee, and it is never a preemption victim.
        """
        if reservation_id is None:
            return None
        res = next((r for r in self.reservations if r.id == reservation_id), None)
        if res is None:
            return None
        return self.compute_guaranteed_until(now, res)

    def reservations_claimed_by(
        self,
        reservation_id: int,
        now: Optional[datetime] = None,
    ) -> set[int]:
        """Return the set of reservation ids a holder booked under *reservation_id*
        occupies: the reservation itself plus its back-to-back chain.

        Used to protect every window a single chained holder pod spans from being
        declared a no-show or lent out as on-demand capacity, closing the
        booked-id-vs-occupied-id gap.
        """
        now = now or datetime.now(timezone.utc)
        res = next((r for r in self.reservations if r.id == reservation_id), None)
        claimed = {reservation_id}
        if res is not None and res.kind == "booking" and res.user is not None:
            claimed |= {r.id for r in self._chain_for(res, now)}
        return claimed

    def reconcile_queue(self) -> None:
        """Re-validate queue entries against the current reservation list.

        Called after each reservation refresh.  Entries whose reservation was
        cancelled are removed and, if a new matching reservation exists, the pod
        is re-queued for it.  Surviving entries have their captured reservation
        object swapped for the freshly loaded one of the same id: ``reservations``
        is replaced wholesale each cycle, and this is the only place a
        wholesale-replaced object would otherwise be retained stale (CODE-REVIEW
        D9).
        """
        by_id = {r.id: r for r in self.reservations}
        active_ids = set(by_id)
        stale_uids = [
            uid
            for uid, entry in self.task_queue.items()
            if entry.reservation.id not in active_ids
        ]
        # Re-point surviving entries at the fresh reservation object.
        for entry in self.task_queue.values():
            fresh = by_id.get(entry.reservation.id)
            if fresh is not None:
                entry.reservation = fresh
        for uid in stale_uids:
            entry = self.task_queue.pop(uid)
            new_res = self.find_best_reservation(
                entry.pod_namespace, entry.gpu_class_label, entry.group_label
            )
            if new_res:
                self.task_queue[uid] = QueueEntry(
                    pod_uid=uid,
                    pod_name=entry.pod_name,
                    pod_namespace=entry.pod_namespace,
                    gpu_class_label=entry.gpu_class_label,
                    gpu_requested=entry.gpu_requested,
                    reservation=new_res,
                    next_attempt_at=datetime.now(timezone.utc),
                    group_label=entry.group_label,
                )
                log.info(
                    "Pod %s/%s re-queued: reservation #%d cancelled, "
                    "now targeting reservation #%d",
                    entry.pod_namespace,
                    entry.pod_name,
                    entry.reservation.id,
                    new_res.id,
                )
            else:
                log.info(
                    "Pod %s/%s removed from queue: reservation #%d cancelled "
                    "and no replacement found",
                    entry.pod_namespace,
                    entry.pod_name,
                    entry.reservation.id,
                )

    # ------------------------------------------------------------------
    # On-demand candidate management
    # ------------------------------------------------------------------

    def add_ondemand_candidate(
        self,
        pod_uid: str,
        pod_name: str,
        pod_namespace: str,
        gpu_class_label: str,
        gpu_requested: int,
        min_runtime_seconds: int,
        pod_created_at: datetime,
        group_label: Optional[str] = None,
    ) -> None:
        """Register a pod as a JIT on-demand lease candidate (idempotent).

        If the pod is already registered with the same parameters this is a
        no-op.  A pod already tracked in ``occupancy`` (already placed) is also
        left untouched.
        """
        # Already placed — don't re-add as a candidate.
        for occupants in self.occupancy.values():
            if pod_uid in occupants:
                return

        if pod_uid in self.ondemand_candidates:
            return  # already registered

        candidate = OnDemandCandidate(
            pod_uid=pod_uid,
            pod_name=pod_name,
            pod_namespace=pod_namespace,
            gpu_class_label=gpu_class_label,
            gpu_requested=gpu_requested,
            min_runtime_seconds=min_runtime_seconds,
            pod_created_at=pod_created_at,
            next_attempt_at=datetime.now(timezone.utc),
            group_label=group_label,
        )
        self.ondemand_candidates[pod_uid] = candidate
        log.info(
            "On-demand candidate: pod %s/%s (uid=%s, gpu-class=%s, "
            "gpus=%d, min-runtime=%ds)",
            pod_namespace,
            pod_name,
            pod_uid,
            gpu_class_label,
            gpu_requested,
            min_runtime_seconds,
        )

    def remove_ondemand_candidate(self, pod_uid: str) -> None:
        """Remove a pod from the on-demand candidate list (e.g. pod deleted)."""
        if pod_uid in self.ondemand_candidates:
            c = self.ondemand_candidates.pop(pod_uid)
            log.debug(
                "Removed on-demand candidate %s/%s (uid=%s)",
                c.pod_namespace,
                c.pod_name,
                pod_uid,
            )

    # ------------------------------------------------------------------
    # Occupancy: availability, placement, release, reconciliation
    # ------------------------------------------------------------------

    def available(
        self, reservation: ReservationResponse, exclude_uid: Optional[str] = None
    ) -> int:
        """Return the GPUs still free on *reservation*, across all kinds.

        Counts every pod recorded against *reservation* in the unified occupancy
        map.  *exclude_uid* omits one pod (the one being evaluated) so a retry
        does not count itself.
        """
        used = sum(
            g
            for uid, g in self.occupancy.get(reservation.id, {}).items()
            if uid != exclude_uid
        )
        return max(0, reservation.gpu_count - used)

    def _effective_label(self, r: ReservationResponse) -> Optional[str]:
        """Return *r*'s GPU-class label: the cached value, else the label_value
        embedded in the reservation's ``gpu_class`` (populated for cancelled
        blocks whose class dropped out of the active-reservation cache)."""
        cached = self.gpu_class_labels.get(r.gpu_class_id)
        if cached is not None:
            return cached
        return r.gpu_class.label_value if r.gpu_class else None

    def _group_ok(
        self, r: ReservationResponse, group_label: Optional[str]
    ) -> bool:
        """Whether *r*'s usage group satisfies the REQUIRED_GROUP_LABEL gate.

        When the feature is disabled (``required_group_label is None``) this is
        always True — no constraint.  When enabled, *group_label* is the value
        of the pod's group label; it must equal ``r.group.name``.  A pod with no
        such label (``group_label is None`` while enabled) matches no grouped
        reservation, so it is never admitted on the reserved path (it falls
        through to the group-agnostic on-demand path)."""
        if self.required_group_label is None:
            return True
        return r.group is not None and r.group.name == group_label

    def record_placement(
        self, reservation_id: int, pod_uid: str, gpu_count: int
    ) -> None:
        """Record that *pod_uid* occupies *gpu_count* GPUs on *reservation_id*.

        Idempotent by pod uid.  Used by every admission path (reserved,
        on-demand, no-show) and by occupancy reconstruction.
        """
        if reservation_id not in self.occupancy:
            self.occupancy[reservation_id] = {}
        self.occupancy[reservation_id][pod_uid] = gpu_count
        log.debug(
            "Recorded placement: reservation #%d ← pod uid=%s (%d GPU(s)); %d/%d free",
            reservation_id,
            pod_uid,
            gpu_count,
            self.available_by_id(reservation_id),
            self._reservation_gpu_count(reservation_id),
        )

    def release_pod(self, pod_uid: str) -> Optional[int]:
        """Remove *pod_uid* from whatever reservation it occupies.

        Returns the reservation id it was released from, or ``None`` if the pod
        was not tracked (e.g. a candidate that was never placed).
        """
        for reservation_id, occupants in self.occupancy.items():
            if pod_uid in occupants:
                gpu_count = occupants.pop(pod_uid)
                log.info(
                    "Released capacity: reservation #%d ← pod uid=%s freed %d GPU(s)",
                    reservation_id,
                    pod_uid,
                    gpu_count,
                )
                # Clean up empty dicts to keep the map tidy.
                if not occupants:
                    del self.occupancy[reservation_id]
                return reservation_id
        return None

    def relink_occupancy(
        self, pod_uid: str, new_reservation_id: int, gpu_count: int
    ) -> None:
        """Move *pod_uid*'s occupancy to *new_reservation_id* (release + record).

        Used when an overstay pod is adopted into a reservation the same user
        has since booked: the pod is unlinked from whatever id it was recorded
        under and recorded against the new one, so budget accounting and
        ``guarantee_end`` immediately reflect the new binding.
        """
        self.release_pod(pod_uid)
        self.record_placement(new_reservation_id, pod_uid, gpu_count)

    def available_by_id(self, reservation_id: int) -> int:
        """GPU capacity remaining for *reservation_id* (0 if unknown)."""
        gpu_count = self._reservation_gpu_count(reservation_id)
        if gpu_count == 0:
            return 0
        used = sum(self.occupancy.get(reservation_id, {}).values())
        return max(0, gpu_count - used)

    def _reservation_gpu_count(self, reservation_id: int) -> int:
        """Return gpu_count for a reservation by id, or 0 if not found."""
        for r in self.reservations:
            if r.id == reservation_id:
                return r.gpu_count
        return 0

    # ------------------------------------------------------------------
    # Cancellation and owner-change detection
    # ------------------------------------------------------------------

    def detect_cancelled_in_window(
        self,
        all_reservations: list[ReservationResponse],
        now: datetime,
    ) -> list[ReservationResponse]:
        """Identify user reservations that were cancelled while their window was open.

        Scans *all_reservations* (the full ``status=all`` API response) for records
        with ``status == "cancelled"`` whose window has not yet closed.  A cancelled
        reservation is returned when:
        - its ``status`` is ``"cancelled"``, AND
        - its window has not yet ended (``slot_end > now``), AND
        - it was not already declared a no-show, AND
        - it was still in ``self.reservations`` (the active set as of the last
          reconcile).  Must be called *before* ``state.reservations`` is replaced
          with the new snapshot (mirrors ``detect_owner_changed_in_window``): this
          is what makes eviction idempotent across cycles — a handled cancellation
          drops out of the active set and is never re-added, so it fails this
          check on every subsequent fetch without needing a separate "already
          handled" record.

        Returns the list of ``ReservationResponse`` objects (with ``cancelled_by``
        already populated from the API) for every reservation that meets all criteria.
        """
        prior_active_ids = {r.id for r in self.reservations}
        return [
            r for r in all_reservations
            if r.status == "cancelled"
            and slot_end(r) > now
            and r.id not in self.noshow_reservation_ids
            and r.id in prior_active_ids
        ]

    def detect_owner_changed_in_window(
        self,
        incoming: list[ReservationResponse],
        now: datetime,
    ) -> list[tuple[ReservationResponse, str]]:
        """Identify in-progress reservations whose owner changed ("adoption").

        The reservation app can reassign a booking to a new owner (a teammate)
        while it stays ``status == "active"``.  Because ownership is coupled to
        the Kubernetes namespace (``pod.namespace == reservation.user.username``),
        the pod already admitted under the reservation lives in the *prior*
        owner's namespace and can no longer be legitimately matched to it.

        Compares each incoming active booking against the reservation of the same
        id **currently stored** in ``self.reservations`` (this must be called
        *before* the wholesale replace, while the old owner is still visible).  A
        reservation is returned as ``(new_reservation, prior_username)`` when:
        - incoming ``status == "active"``, ``kind == "booking"``, ``user`` set, AND
        - a prior reservation of the same id exists with ``user``/``user_id`` set, AND
        - the owner changed (``prior.user_id != r.user_id``), AND
        - it is in progress: ``slot_start(r) <= now < slot_end(r)``.

        *prior_username* (the old ``user.username``, i.e. the old namespace) is
        what lets the caller locate the pod "under the prior name/namespace".
        """
        prior_by_id = {r.id: r for r in self.reservations}
        changes: list[tuple[ReservationResponse, str]] = []
        for r in incoming:
            if r.status != "active" or r.kind != "booking" or r.user is None:
                continue
            prior = prior_by_id.get(r.id)
            if prior is None or prior.user is None or prior.user_id is None:
                continue
            if prior.user_id == r.user_id:
                continue
            if not (slot_start(r) <= now < slot_end(r)):
                continue
            changes.append((r, prior.user.username))
        return changes

    def reconcile_occupancy(self, placements: list[tuple[int, str, int]]) -> None:
        """Rebuild the occupancy map from a live cluster snapshot.

        *placements* is ``(reservation_id, pod_uid, gpu_count)`` for every live
        (Running/Pending) tolerated pod, bucketed by the reservation id parsed
        from its booking-reference.  Rebuilding wholesale is self-healing: a pod
        deleted during a watch disconnect is dropped here even if its DELETE
        event was missed.

        Called each queue-processor tick.  An optimistic record made between
        ticks whose patch is not yet visible in the snapshot may be briefly
        dropped; the placing coroutine has already committed and the next tick
        re-captures it, so the window is bounded by the tick interval.
        """
        old_total = sum(g for pods in self.occupancy.values() for g in pods.values())
        rebuilt: dict[int, dict[str, int]] = {}
        for reservation_id, pod_uid, gpu_count in placements:
            rebuilt.setdefault(reservation_id, {})[pod_uid] = gpu_count
        self.occupancy = rebuilt
        new_total = sum(g for pods in rebuilt.values() for g in pods.values())
        n_res = len(rebuilt)
        if new_total != old_total:
            log.info(
                "Occupancy reconciled: %d reservation(s), %d GPU(s) in use (was %d)",
                n_res,
                new_total,
                old_total,
            )
        else:
            log.debug(
                "Occupancy reconciled: %d reservation(s), %d GPU(s) in use",
                n_res,
                new_total,
            )

    # ------------------------------------------------------------------
    # Preemption planning (demand-driven capacity recovery)
    # ------------------------------------------------------------------

    def upcoming_boundaries(self, now: datetime, lead: timedelta) -> list[datetime]:
        """Return distinct booking start times within *lead* of *now*, ascending.

        A boundary ``S`` is in scope while ``now - lead < S <= now + lead``: it
        enters phase A (proactive) as soon as it is within the lead window
        before it opens, and stays in scope through phase B (at-boundary,
        ``S <= now``) for one more *lead*-sized window after it opens — long
        enough for a delayed sweep or a post-restart re-evaluation to still
        catch it.  Data-driven (distinct ``slot_start`` values of active
        bookings): hour alignment and DST are the reservation app's concern,
        not this arithmetic's.
        """
        boundaries = {
            slot_start(r)
            for r in self.reservations
            if r.kind == "booking" and now - lead < slot_start(r) <= now + lead
        }
        return sorted(boundaries)

    def _past_guarantee(self, view: PodRuntimeView, now: datetime) -> bool:
        """Return True if *view*'s runtime guarantee is over (or unresolvable).

        Shared by ``plan_boundary_preemption`` (victim eligibility) and
        ``plan_pod_adoptions`` (rescue eligibility) so both agree on what
        "overstay" means: ``guarantee_end`` is ``None`` (not ours / no longer
        active) or ``<= now``.
        """
        end = self.guarantee_end(view.reservation_id, now=now)
        return end is None or end <= now

    def plan_pod_adoptions(
        self,
        pods: list[PodRuntimeView],
        now: datetime,
    ) -> list[tuple[PodRuntimeView, ReservationResponse]]:
        """Plan (but do not execute) re-links of overstay pods to a user's open booking.

        A pod is rescued once **past its runtime guarantee** (``_past_guarantee``)
        — there is no reason to touch a pod already correctly tied to its own
        live booking. This rescues a pod whose user re-booked capacity the
        existing back-to-back chaining cannot reach (a non-abutting follow-on
        window or a different ``gpu_count``) before the preemption sweep would
        reclaim it.

        Requires the pod to be controller-admitted (``reservation_id`` set),
        physically present (``node_resident``, not ``terminating``,
        ``gpu_count > 0``), past its runtime guarantee, and a currently-open
        booking the same user holds with spare budget (``find_open_booking_for``)
        that differs from the pod's current reservation.

        Budget is respected across the pass: a running per-reservation tally is
        subtracted from ``available`` so several adopted pods cannot over-book
        one reservation. Pure — no I/O, no state mutation; the caller performs
        the annotation patch and occupancy re-home.
        """
        assignments: list[tuple[PodRuntimeView, ReservationResponse]] = []
        extra_used: dict[int, int] = {}
        for p in pods:
            if (
                p.reservation_id is None
                or not p.node_resident
                or p.terminating
                or p.gpu_count <= 0
            ):
                continue
            if not self._past_guarantee(p, now):
                continue
            res = self.find_open_booking_for(
                p.namespace,
                p.gpu_class,
                now,
                p.gpu_count,
                extra_used=extra_used,
                group_label=p.group_label,
            )
            if res is None or res.id == p.reservation_id:
                continue
            assignments.append((p, res))
            extra_used[res.id] = extra_used.get(res.id, 0) + p.gpu_count
        return assignments

    def boundary_demand(
        self,
        boundary: datetime,
        pods: list[PodRuntimeView],
        now: datetime,
    ) -> dict[str, int]:
        """Return GPUs still needed per GPU-class label for bookings starting at *boundary*.

        For each active ``kind == "booking"`` reservation whose window starts
        exactly at *boundary*, the demand is its remaining budget
        (``available``) minus the GPUs already supplied by a live reserved-path
        holder whose back-to-back chain covers it (``reservations_claimed_by``)
        — a chained holder needs no new capacity for the window it already
        physically occupies.  Subtracting per-reservation rather than excluding
        a whole claimed reservation from demand avoids under-counting a
        partially-chained multi-GPU reservation (a 1-GPU holder chaining into a
        4-GPU booking still leaves 3 GPUs of genuine demand).  A no-show
        reservation contributes no demand — its capacity is already on-demand
        pool territory. Reservations whose GPU-class label cannot be resolved
        are skipped (logged at debug).
        """
        chained_gpus: dict[int, int] = {}
        for p in pods:
            if (
                not p.node_resident
                or p.terminating
                or p.reservation_id is None
            ):
                continue
            for rid in self.reservations_claimed_by(p.reservation_id, now):
                chained_gpus[rid] = chained_gpus.get(rid, 0) + p.gpu_count

        demand: dict[str, int] = {}
        for r in self.reservations:
            if r.kind != "booking" or r.user is None:
                continue
            if slot_start(r) != boundary:
                continue
            if r.id in self.noshow_reservation_ids:
                continue
            label = self._effective_label(r)
            if label is None:
                log.debug(
                    "Boundary demand: reservation #%d has no resolvable "
                    "gpu-class label; skipping",
                    r.id,
                )
                continue
            need = max(0, self.available(r) - chained_gpus.get(r.id, 0))
            if need:
                demand[label] = demand.get(label, 0) + need
        return demand

    def plan_boundary_preemption(
        self,
        boundary: datetime,
        capacity_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        now: datetime,
        rng: Optional[random.Random] = None,
    ) -> PreemptionPlan:
        """Plan (but do not execute) the kills needed to clear *boundary*'s demand.

        Per GPU class: ``kills_needed = max(0, demand - free)``.  Eligible
        victims are pods of that class admitted by this controller
        (``reservation_id`` set), physically running or about to
        (``node_resident``), not already ``terminating``, requesting at least
        one GPU, and past their runtime guarantee (``guarantee_end`` is
        ``None`` — unresolvable, treated as already over — or ``<= now``).  A
        pod within its guarantee is never selected, regardless of shortfall.
        Selection is uniform-random among eligible victims (priority ranking
        is future work) and greedy — it stops once enough GPUs are covered,
        which may overshoot when victims aren't exactly the shortfall size.
        Classes are planned independently; a victim from one class's plan
        cannot be re-selected for another (they don't share a GPU class, so
        this only matters for bookkeeping clarity).  Pure — no I/O, no state
        mutation; the caller executes the deletions and marks bookkeeping.
        """
        rng = rng or random.Random()
        demand = self.boundary_demand(boundary, pods, now)
        free = free_capacity_by_class(capacity_by_class, pods)

        victims: list[PodRuntimeView] = []
        unmet: dict[str, int] = {}
        for gpu_class, need in demand.items():
            kills_needed = max(0, need - free.get(gpu_class, 0))
            if kills_needed == 0:
                continue
            eligible = [
                p
                for p in pods
                if p.gpu_class == gpu_class
                and p.reservation_id is not None
                and p.node_resident
                and not p.terminating
                and p.gpu_count > 0
                and self._past_guarantee(p, now)
            ]
            rng.shuffle(eligible)
            killed_gpus = 0
            for p in eligible:
                if killed_gpus >= kills_needed:
                    break
                victims.append(p)
                killed_gpus += p.gpu_count
            if killed_gpus < kills_needed:
                unmet[gpu_class] = kills_needed - killed_gpus

        return PreemptionPlan(
            boundary=boundary,
            phase="B" if boundary <= now else "A",
            demand_by_class=demand,
            free_by_class=free,
            victims=victims,
            unmet_by_class=unmet,
        )

    def prune_preemption_marks(self, now: datetime, lead: timedelta) -> None:
        """Drop boundary bookkeeping once phase B's grace window has closed.

        Called at the start of every preemption sweep, mirroring
        ``reconcile_noshow``'s prune-then-act shape.
        """
        stale = [b for b in self.preemption_fired if b <= now - lead]
        for b in stale:
            del self.preemption_fired[b]
