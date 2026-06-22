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

import logging
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


@dataclass
class OnDemandCandidate:
    """A pod that is eligible for on-demand placement and is searching for a block.

    Distinct from QueueEntry: a QueueEntry is bound to a specific reservation;
    an OnDemandCandidate is still searching for any on-demand block that can
    accommodate it.
    """

    pod_uid: str
    pod_name: str
    pod_namespace: str
    gpu_class_label: str   # value of pod label "gpu-class" (e.g. "h100")
    gpu_requested: int     # nvidia.com/gpu units requested by this pod
    min_runtime_seconds: int  # horai/minimum-runtime-seconds annotation value
    pod_created_at: datetime  # metadata.creationTimestamp; used for FIFO ordering
    next_attempt_at: datetime  # earliest time to try placement


@dataclass
class ReclaimMerge:
    """A record of one subject block that has absorbed future reclaim block(s).

    The subject's window is extended to ``extended_end`` so an on-demand job
    beginning in it can run through the whole merged span; each absorbed reclaim
    block becomes a stub (excluded from independent placement).  Persisted across
    reservation reloads and re-applied by ``reconcile_reclaim_merges``.
    """

    subject_id: int            # block that absorbs (becomes the long block)
    absorbed_ids: list[int]    # future reclaim ids folded in, in window order
    extended_end: datetime     # slot_end of the last absorbed block


# ---------------------------------------------------------------------------
# Shared controller state
# ---------------------------------------------------------------------------


class ControllerState:
    """In-memory state accessed by all background loops.

    All mutations happen inside the asyncio event loop, so no locking is
    required (asyncio is single-threaded).
    """

    def __init__(self) -> None:
        # Current snapshot of active reservations from the API.
        self.reservations: list[ReservationResponse] = []

        # Mapping from gpu_class_id → Kubernetes label value (e.g. "h100").
        # Populated by the reservation-fetch loop using GET /api/gpu-classes/{id}.
        self.gpu_class_labels: dict[int, str] = {}

        # Active work queue keyed by pod UID (reserved path).
        self.task_queue: dict[str, QueueEntry] = {}

        # Pods eligible for on-demand placement, keyed by pod UID.
        # These have the horai/minimum-runtime-seconds annotation and are
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
        # horai/booking-reference, so no separate annotation is needed.
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
        # These are treated as on-demand capacity for placement purposes.
        self.noshow_reservation_ids: set[int] = set()

        # Reservation ids currently "claimed" by a live reserved-path holder pod
        # — each holder's booking reservation plus the back-to-back chain its
        # runtime cap extends across (see reservations_claimed_by).  A reservation
        # in this set must not be declared a no-show or lent out as on-demand
        # capacity, because a holder is actively occupying its window via a
        # chained deadline even though no pod is booked directly under it.
        # Recomputed from the live pod snapshot each queue-processor tick.
        self.claimed_reservation_ids: set[int] = set()

        # Cancelled in-window user reservations retained until their window ends
        # so their freed GPU capacity can be offered for on-demand placement.
        # Keyed by reservation id; values are the last-known ReservationResponse
        # (from the cycle before cancellation was detected).
        self.cancelled_reservations: dict[int, ReservationResponse] = {}

        # ``reclaim_preempt_guard_minutes`` from GET /api/settings: lead time
        # before a reclaim hold's start within which the reservation app treats
        # it as committed (non-preemptible) capacity.  None until first fetched;
        # reclaim-block merging is skipped while unknown.
        self.reclaim_preempt_guard_minutes: Optional[int] = None

        # Wall-clock time of the most recent successful reservation fetch.  The
        # commitment ("within guard") test for a merge candidate is judged against
        # THIS instant, not the current tick clock: a reclaim block is only safe to
        # merge if it was already inside the guard in the data we actually hold.
        # Judging against an advancing between-fetch clock would let a block that
        # was still preemptible at fetch time drift into the guard and be merged,
        # racing a last-minute front-end booking the controller has not yet seen.
        self.last_reservation_fetch_at: Optional[datetime] = None

        # Persistent reclaim-block merges, keyed by subject reservation id.  Each
        # records the future reclaim block(s) folded into the subject and the
        # extended end the subject window is stretched to.  Re-applied to freshly
        # loaded reservations every cycle so a reload never re-exposes an absorbed
        # block while a deadline-extended job is still running on it.
        self.reclaim_merges: dict[int, ReclaimMerge] = {}

        # Reclaim block ids absorbed into a subject (stubs).  Excluded from
        # on-demand placement so they are never independently double-booked.
        self.merged_stub_ids: set[int] = set()

    # ------------------------------------------------------------------
    # No-show tracking
    # ------------------------------------------------------------------

    def initialize_noshow_tracking(
        self,
        now: datetime,
        timeout_minutes: int,
        grace_minutes: int,
    ) -> None:
        """Set up no-show deadlines for all active user reservations.

        Called once after the initial reservation fetch in the lifespan.
        For each active user reservation:
        - Window not yet open: deadline = slot_start + timeout_minutes
        - Window already open (mid-window startup): deadline = now + grace_minutes
        - Window already expired: skipped

        The initial pod LIST events processed shortly after by pod_watch_loop
        will clear deadlines for reservations that already have matching pods.
        """
        for r in self.reservations:
            if r.kind != "booking" or r.user is None:
                continue
            if r.id in self.noshow_deadlines:
                continue  # already tracked (e.g. called twice)
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
                "No-show tracking: reservation #%d deadline=%s",
                r.id,
                deadline.strftime("%Y-%m-%d %H:%M"),
            )

    def update_noshow_tracking(
        self,
        now: datetime,
        timeout_minutes: int,
        grace_minutes: int,
    ) -> None:
        """Add newly-fetched reservations to no-show tracking.

        Called after each subsequent reservation refresh.  Does not overwrite
        existing deadlines and does not resurrect already-declared no-shows.
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
                "No-show tracking (new): reservation #%d deadline=%s",
                r.id,
                deadline.strftime("%Y-%m-%d %H:%M"),
            )

    def reconcile_noshow(self) -> None:
        """Prune no-show state for reservations no longer in the active list.

        Called after each reservation refresh alongside reconcile_queue.
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

    def check_noshow_deadlines(self, now: datetime) -> None:
        """Declare no-shows for any reservation whose deadline has passed.

        Called at the start of each queue_processor_loop tick.  Moves expired
        entries from noshow_deadlines into noshow_reservation_ids.  A reservation
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
            self.noshow_reservation_ids.add(rid)
            res = next((r for r in self.reservations if r.id == rid), None)
            user = res.user.username if (res and res.user) else "unknown"
            gpu_class = self.gpu_class_labels.get(res.gpu_class_id, "unknown") if res else "unknown"
            log.info(
                "Reservation #%d declared no-show (user=%s, gpu-class=%s): "
                "no matching pod appeared before deadline; "
                "capacity opened for on-demand placement",
                rid,
                user,
                gpu_class,
            )

    def mark_pod_seen_for_noshow(
        self,
        namespace: str,
        gpu_class_label: str,
        booking_reservation_id: Optional[int] = None,
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
        self, namespace: str, gpu_class_label: str
    ) -> Optional[ReservationResponse]:
        """Find the nearest upcoming (or active) reservation for *namespace* / *gpu_class_label*.

        Matching rules:
        - reservation.user.username == namespace
        - gpu_class_labels[reservation.gpu_class_id] == gpu_class_label
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
            and slot_end(r) > now  # still has time left
            and r.id not in self.noshow_reservation_ids
        ]
        if not candidates:
            return None
        return min(candidates, key=slot_start)

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
    ) -> None:
        """Match pod to a reservation and add to the work queue if eligible.

        If the pod is already queued for the same reservation, this is a no-op
        (idempotent).  If the reservation changes (e.g. old one cancelled), the
        entry is replaced.
        """
        reservation = self.find_best_reservation(pod_namespace, gpu_class_label)
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
        slot_end(previous)``).  *reservation* itself is **not** included.

        Shared by ``compute_max_deadline_seconds`` (runtime-cap arithmetic) and
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
                and slot_end(r) > now
                and r.id not in self.noshow_reservation_ids
            ],
            key=slot_start,
        )

        chain: list[ReservationResponse] = []
        prev_end = slot_end(reservation)
        visited: set[int] = set()
        while True:
            next_res = next(
                (
                    r
                    for r in candidates
                    if r.id not in visited and slot_start(r) == prev_end
                ),
                None,
            )
            if next_res is None:
                break
            chain.append(next_res)
            prev_end = slot_end(next_res)
            visited.add(next_res.id)
        return chain

    def compute_max_deadline_seconds(
        self,
        now: datetime,
        current_reservation: ReservationResponse,
    ) -> int:
        """Return the maximum permitted pod lifetime in seconds, anchored to *now*.

        Sums the remaining time in *current_reservation* plus the full duration of
        any back-to-back future reservations that share the same namespace, GPU class,
        and gpu_count — with no gap between consecutive windows.

        "Back-to-back" means slot_start(next) == slot_end(previous) exactly.
        """
        prev_end = slot_end(current_reservation)
        total = max(0.0, (prev_end - now).total_seconds())
        for res in self._chain_for(current_reservation, now):
            total += (slot_end(res) - slot_start(res)).total_seconds()
        return int(total)

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
        is re-queued for it.
        """
        active_ids = {r.id for r in self.reservations}
        stale_uids = [
            uid
            for uid, entry in self.task_queue.items()
            if entry.reservation.id not in active_ids
        ]
        for uid in stale_uids:
            entry = self.task_queue.pop(uid)
            new_res = self.find_best_reservation(
                entry.pod_namespace, entry.gpu_class_label
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
    ) -> None:
        """Register a pod as an on-demand placement candidate (idempotent).

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

    def reconcile_reclaim_merges(self, now: datetime) -> None:
        """Re-apply persistent reclaim merges and discover new ones.

        A "subject block" is any currently-open on-demand window — a reclaim
        hold, a declared no-show, or a cancelled-in-window reservation.  When a
        subject abuts a future ``kind == "reclaim"`` block of the same GPU class
        and equal ``gpu_count`` that was already inside
        ``reclaim_preempt_guard_minutes`` **at the last reservation fetch** (the
        app has committed it and will not preempt it), the two are merged: the
        subject's window is stretched to the future block's end and that block is
        recorded as a stub (excluded from independent on-demand placement).  This
        maximises the runtime of an on-demand job beginning in the subject block.

        The commitment test uses ``last_reservation_fetch_at + guard`` as the
        horizon, **not** ``now``: a block must have been within the guard in the
        data we actually hold.  Judging against the advancing between-fetch tick
        clock would let a block that was still preemptible at fetch time drift
        into the guard and be merged, racing a last-minute front-end booking the
        controller has not yet fetched.  Because the guard is sized to exceed the
        poll interval, a block legitimately entering the guard is always seen by a
        fresh fetch (still present, or gone if preempted) before it is merged.

        Re-applying surviving records first — before the freshly loaded
        reservation objects are consulted by the placement logic — is what makes
        merges survive a wholesale reservation reload, so a reload never
        re-exposes an absorbed block while a deadline-extended job still depends
        on it.  Idempotent; safe to call every refresh and every queue tick.
        Does nothing while the guard is unknown.
        """
        guard = self.reclaim_preempt_guard_minutes
        self.merged_stub_ids = set()
        if guard is None:
            self.reclaim_merges = {}
            return

        # Index reservations a merge might reference: live reservations plus
        # retained cancelled-in-window blocks (which can be subjects).
        by_id: dict[int, ReservationResponse] = {r.id: r for r in self.reservations}
        for rid, r in self.cancelled_reservations.items():
            by_id.setdefault(rid, r)

        def _label(r: ReservationResponse) -> Optional[str]:
            cached = self.gpu_class_labels.get(r.gpu_class_id)
            if cached is not None:
                return cached
            return r.gpu_class.label_value if r.gpu_class else None

        # --- 1. Re-apply surviving records (persistence across reloads) ---
        surviving: dict[int, ReclaimMerge] = {}
        for subject_id, merge in self.reclaim_merges.items():
            subject = by_id.get(subject_id)
            if subject is None or now >= merge.extended_end:
                log.info(
                    "Reclaim merge for subject #%d dropped: reservation no longer active or window ended",
                    subject_id,
                )
                continue  # subject gone, or whole merged span has ended
            if subject_id in self.claimed_reservation_ids:
                continue  # a reserved holder now occupies it — not on-demand
            absorbed = [by_id.get(aid) for aid in merge.absorbed_ids]
            if any(a is None or a.kind != "reclaim" for a in absorbed):
                missing = [
                    aid
                    for aid, a in zip(merge.absorbed_ids, absorbed)
                    if a is None or a.kind != "reclaim"
                ]
                log.info(
                    "Reclaim merge for subject #%d: absorbed block(s) %s no longer active; merge dropped",
                    subject_id,
                    missing,
                )
                continue  # an absorbed block vanished (e.g. preempted) — drop
            subject.end_utc = merge.extended_end
            self.merged_stub_ids.update(merge.absorbed_ids)
            surviving[subject_id] = merge
            log.debug(
                "Re-applied reclaim merge: subject #%d extended to %s (absorbed: %s)",
                subject_id,
                merge.extended_end.isoformat(),
                merge.absorbed_ids,
            )
        self.reclaim_merges = surviving

        # --- 2. Discover new merges (and extend existing ones transitively) ---
        # Commitment is judged against the data we hold: a candidate is eligible
        # only if its start was within the guard at the last reservation fetch.
        # Without a fetch timestamp we cannot make that judgement safely, so we
        # re-apply surviving merges but discover none.
        if self.last_reservation_fetch_at is None:
            return
        horizon = self.last_reservation_fetch_at + timedelta(minutes=guard)
        reclaim_blocks = [r for r in self.reservations if r.kind == "reclaim"]

        subjects: list[ReservationResponse] = [
            r
            for r in self.reservations
            if (r.kind == "reclaim" or r.id in self.noshow_reservation_ids)
            and r.id not in self.claimed_reservation_ids
            and slot_start(r) <= now < slot_end(r)
        ]
        subjects += [
            r
            for r in self.cancelled_reservations.values()
            if r.id not in self.claimed_reservation_ids
            and slot_start(r) <= now < slot_end(r)
        ]

        seen: set[int] = set()
        for subject in subjects:
            if subject.id in seen or subject.id in self.merged_stub_ids:
                continue
            seen.add(subject.id)
            label = _label(subject)
            if label is None:
                continue
            existing = self.reclaim_merges.get(subject.id)
            absorbed_ids: list[int] = list(existing.absorbed_ids) if existing else []
            cur_end = slot_end(subject)  # already extended if a record survived
            grew = False
            while True:
                targets = [
                    r
                    for r in reclaim_blocks
                    if r.id != subject.id
                    and r.id not in self.merged_stub_ids
                    and r.id not in absorbed_ids
                    and r.gpu_count == subject.gpu_count
                    and _label(r) == label
                    and slot_start(r) == cur_end
                    and slot_start(r) <= horizon
                ]
                if not targets:
                    break
                target = max(targets, key=slot_end)
                absorbed_ids.append(target.id)
                self.merged_stub_ids.add(target.id)
                cur_end = slot_end(target)
                subject.end_utc = cur_end
                grew = True
            if absorbed_ids:
                self.reclaim_merges[subject.id] = ReclaimMerge(
                    subject_id=subject.id,
                    absorbed_ids=absorbed_ids,
                    extended_end=cur_end,
                )
                if grew:
                    log.info(
                        "Merged reclaim block(s) %s into subject reservation #%d "
                        "(gpu-class=%s, gpu_count=%d); window extended to %s",
                        absorbed_ids,
                        subject.id,
                        label,
                        subject.gpu_count,
                        cur_end.isoformat(),
                    )

    def find_ondemand_block(
        self,
        gpu_class_label: str,
        now: datetime,
        gpu_requested: int,
        min_runtime_seconds: int,
    ) -> Optional[ReservationResponse]:
        """Find the best on-demand block for a given pod.

        Criteria (all must hold):
        - ``kind == "reclaim"`` or the reservation has been declared a no-show
        - not currently claimed by a live chained holder
        - GPU class label matches *gpu_class_label*
        - Window is currently open: ``slot_start(r) <= now < slot_end(r)``
        - Sufficient capacity: ``available(r) >= gpu_requested``
        - Enough time remains: ``(slot_end(r) - now).total_seconds() >= min_runtime_seconds``

        Selection: prefer the block with the **latest** ``slot_end`` (maximises the
        pod's effective runtime); break ties by most available capacity.
        """
        def _label_matches(r: ReservationResponse) -> bool:
            cached = self.gpu_class_labels.get(r.gpu_class_id)
            if cached is not None:
                return cached == gpu_class_label
            # Fall back to the label_value embedded in the reservation response
            # (populated from GpuClassBrief) for cancelled reservations whose
            # GPU class may have dropped out of the active-reservation cache.
            return (r.gpu_class.label_value if r.gpu_class else None) == gpu_class_label

        # Active on-demand / no-show blocks from the live reservation list.
        from_active = [
            r
            for r in self.reservations
            if (r.kind == "reclaim" or r.id in self.noshow_reservation_ids)
            and r.id not in self.claimed_reservation_ids
            and r.id not in self.merged_stub_ids
            and _label_matches(r)
            and slot_start(r) <= now < slot_end(r)
            and self.available(r) >= gpu_requested
            and (slot_end(r) - now).total_seconds() >= min_runtime_seconds
        ]
        # Freed capacity from cancelled user reservations (not in self.reservations).
        from_cancelled = [
            r
            for r in self.cancelled_reservations.values()
            if r.id not in self.claimed_reservation_ids
            and r.id not in self.merged_stub_ids
            and _label_matches(r)
            and slot_start(r) <= now < slot_end(r)
            and self.available(r) >= gpu_requested
            and (slot_end(r) - now).total_seconds() >= min_runtime_seconds
        ]
        candidates = from_active + from_cancelled
        if not candidates:
            return None
        # Latest slot_end first; break ties by most available capacity (desc).
        block = max(
            candidates,
            key=lambda r: (slot_end(r), self.available(r)),
        )
        log.debug(
            "Selected on-demand block #%d for gpu-class=%s (window %s–%s, %d/%d free)",
            block.id,
            gpu_class_label,
            slot_start(block).strftime("%Y-%m-%d %H:%M"),
            slot_end(block).strftime("%H:%M"),
            self.available(block),
            block.gpu_count,
        )
        return block

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
                    "Released slot: reservation #%d ← pod uid=%s freed %d GPU(s)",
                    reservation_id,
                    pod_uid,
                    gpu_count,
                )
                # Clean up empty dicts to keep the map tidy.
                if not occupants:
                    del self.occupancy[reservation_id]
                return reservation_id
        return None

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
    # Cancellation detection and freed-capacity tracking
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
        - it was not already declared a no-show (freed capacity already lent to
          on-demand use), AND
        - it was not already recorded in ``cancelled_reservations`` (idempotent).

        Returns the list of ``ReservationResponse`` objects (with ``cancelled_by``
        already populated from the API) for every reservation that meets all criteria.
        """
        return [
            r for r in all_reservations
            if r.status == "cancelled"
            and slot_end(r) > now
            and r.id not in self.noshow_reservation_ids
            and r.id not in self.cancelled_reservations
        ]

    def record_cancelled_reservation(self, r: ReservationResponse) -> None:
        """Register *r* as a cancelled in-window reservation for on-demand use.

        The freed GPU capacity becomes available to on-demand candidates via
        ``find_ondemand_block`` until the reservation's window ends.
        """
        self.cancelled_reservations[r.id] = r
        gpu_class_label = (
            self.gpu_class_labels.get(r.gpu_class_id)
            or (r.gpu_class.label_value if r.gpu_class else None)
            or "unknown"
        )
        user = r.user.username if r.user else "?"
        log.info(
            "Reservation #%d (user=%s, gpu-class=%s, %d GPU(s)) cancelled mid-window; "
            "freed capacity available for on-demand placement until %s",
            r.id,
            user,
            gpu_class_label,
            r.gpu_count,
            slot_end(r).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    def cleanup_cancelled_reservations(self, now: datetime) -> None:
        """Remove expired entries from ``cancelled_reservations``.

        Called each queue-processor tick so stale entries don't accumulate.
        """
        expired = [rid for rid, r in self.cancelled_reservations.items() if slot_end(r) <= now]
        for rid in expired:
            del self.cancelled_reservations[rid]
            log.debug("Cancelled reservation #%d window ended; removed from on-demand pool", rid)

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
