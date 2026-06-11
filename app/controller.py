"""Core controller state machine.

This module owns the in-memory state shared by the three background loops:

``ControllerState``    — reservations list, GPU-class label map, task queue,
                         on-demand candidates, on-demand occupancy map
``QueueEntry``         — one pod waiting for its reservation window (reserved path)
``OnDemandCandidate``  — one pod searching for an on-demand block (on-demand path)
``slot_start / slot_end`` — reservation time-window arithmetic
``TOLERATION_KEY``     — the taint/toleration key used by the reservation system

No Kubernetes or HTTP I/O is done here; those live in k8s_client.py and
reservation_client.py respectively.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from .schemas import ReservationResponse

log = logging.getLogger(__name__)

# Toleration key applied by the controller.  The full toleration is:
#   gpu-class-reservation=<gpu-class-label>:NoSchedule
TOLERATION_KEY = "gpu-class-reservation"


# ---------------------------------------------------------------------------
# Time-window helpers
# ---------------------------------------------------------------------------


def _time_to_minutes(time_str: str) -> int:
    """Convert "HH:MM:SS" to total minutes from midnight."""
    parts = time_str.split(":")
    return int(parts[0]) * 60 + int(parts[1])


def slot_start(r: ReservationResponse) -> datetime:
    """Compute the (naive, local-time) start of *r*'s reserved window.

    Formula from RESERVATION-API.md §4:
        slot_start = policy.start_time + slot_index × policy.duration_minutes
    """
    midnight = datetime.combine(r.date, datetime.min.time())
    offset = timedelta(
        minutes=(
            _time_to_minutes(r.policy.start_time)
            + r.slot_index * r.policy.duration_minutes
        )
    )
    return midnight + offset


def slot_end(r: ReservationResponse) -> datetime:
    """Compute the (naive, local-time) end of *r*'s reserved window."""
    return slot_start(r) + timedelta(minutes=r.policy.duration_minutes)


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
    min_runtime_seconds: int  # dsmlp/minimum-runtime-seconds annotation value
    next_attempt_at: datetime  # earliest time to try placement


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
        # These have the dsmlp/minimum-runtime-seconds annotation and are
        # Pending, but do not match any user reservation.
        self.ondemand_candidates: dict[str, OnDemandCandidate] = {}

        # Occupancy map for on-demand blocks: block reservation id →
        # { pod_uid: gpu_count }.  This is the authoritative record of what
        # the controller has placed onto each on-demand block.  Available
        # capacity = block.gpu_count - sum(values).
        #
        # Reconstructed at startup from the dsmlp/ondemand-block-id annotation
        # that is stamped on each pod at placement time (see pod_watch_loop).
        # Pods placed by a controller version predating that annotation will not
        # be counted, so the first restart after upgrading may briefly allow one
        # extra placement per affected block.
        self.ondemand_occupancy: dict[int, dict[str, int]] = {}

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
            if r.kind != "user" or r.user is None:
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
            if r.kind != "user" or r.user is None:
                continue
            if r.id in self.noshow_deadlines:
                continue
            if r.id in self.noshow_reservation_ids:
                continue
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
        entries from noshow_deadlines into noshow_reservation_ids.
        """
        expired = [
            rid for rid, deadline in self.noshow_deadlines.items() if now >= deadline
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
        self, namespace: str, gpu_class_label: str
    ) -> None:
        """Clear the no-show deadline for the soonest matching reservation.

        Called when a pod with an existing toleration is detected (already
        admitted), so we have no booking-reference to look up the exact
        reservation.  Clears the deadline for the soonest active user
        reservation matching namespace + gpu_class_label that still has a
        pending deadline, mirroring find_best_reservation's selection logic.
        """
        candidates = [
            r
            for r in self.reservations
            if r.kind == "user"
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
        now = datetime.now()
        candidates = [
            r
            for r in self.reservations
            if r.kind == "user"
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
            next_attempt_at=datetime.now(),
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
                and r.kind == "user"
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
            total += res.policy.duration_minutes * 60.0
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
        now = now or datetime.now()
        res = next((r for r in self.reservations if r.id == reservation_id), None)
        claimed = {reservation_id}
        if res is not None and res.kind == "user" and res.user is not None:
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
                    next_attempt_at=datetime.now(),
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
    ) -> None:
        """Register a pod as an on-demand placement candidate (idempotent).

        If the pod is already registered with the same parameters this is a
        no-op.  A pod already tracked in ``ondemand_occupancy`` (already
        placed) is also left untouched.
        """
        # Already placed — don't re-add as a candidate.
        for occupants in self.ondemand_occupancy.values():
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
            next_attempt_at=datetime.now(),
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
    # On-demand block matching and occupancy
    # ------------------------------------------------------------------

    def ondemand_available(self, block: ReservationResponse) -> int:
        """Return the number of GPUs still available on *block*."""
        used = sum(self.ondemand_occupancy.get(block.id, {}).values())
        return max(0, block.gpu_count - used)

    def find_ondemand_block(
        self,
        gpu_class_label: str,
        now: datetime,
        gpu_requested: int,
        min_runtime_seconds: int,
    ) -> Optional[ReservationResponse]:
        """Find the best on-demand block for a given pod.

        Criteria (all must hold):
        - ``kind == "ondemand"``
        - GPU class label matches *gpu_class_label*
        - Window is currently open: ``slot_start(r) <= now < slot_end(r)``
        - Sufficient capacity: ``ondemand_available(r) >= gpu_requested``
        - Enough time remains: ``(slot_end(r) - now).total_seconds() >= min_runtime_seconds``

        Selection: prefer the block with the **latest** ``slot_end`` (maximises the
        pod's effective runtime); break ties by most available capacity.
        """
        candidates = [
            r
            for r in self.reservations
            if (r.kind == "ondemand" or r.id in self.noshow_reservation_ids)
            and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
            and slot_start(r) <= now < slot_end(r)
            and self.ondemand_available(r) >= gpu_requested
            and (slot_end(r) - now).total_seconds() >= min_runtime_seconds
        ]
        if not candidates:
            return None
        # Latest slot_end first; break ties by most available capacity (desc).
        return max(
            candidates,
            key=lambda r: (slot_end(r), self.ondemand_available(r)),
        )

    def record_ondemand_placement(
        self, block_id: int, pod_uid: str, gpu_count: int
    ) -> None:
        """Record that *pod_uid* has been placed onto *block_id*, consuming *gpu_count* GPUs."""
        if block_id not in self.ondemand_occupancy:
            self.ondemand_occupancy[block_id] = {}
        self.ondemand_occupancy[block_id][pod_uid] = gpu_count
        log.debug(
            "Recorded on-demand placement: block #%d ← pod uid=%s (%d GPU(s)); "
            "block now has %d/%d free",
            block_id,
            pod_uid,
            gpu_count,
            self.ondemand_available_by_id(block_id),
            self._block_gpu_count(block_id),
        )

    def release_ondemand_pod(self, pod_uid: str) -> Optional[int]:
        """Remove *pod_uid* from whatever on-demand block it occupies.

        Returns the block id it was released from, or ``None`` if the pod was
        not tracked in any block (e.g. it was a candidate that was never placed,
        or a reserved-path pod).
        """
        for block_id, occupants in self.ondemand_occupancy.items():
            if pod_uid in occupants:
                gpu_count = occupants.pop(pod_uid)
                log.info(
                    "Released on-demand slot: block #%d ← pod uid=%s freed %d GPU(s)",
                    block_id,
                    pod_uid,
                    gpu_count,
                )
                # Clean up empty dicts to keep the map tidy.
                if not occupants:
                    del self.ondemand_occupancy[block_id]
                return block_id
        return None

    def ondemand_available_by_id(self, block_id: int) -> int:
        """GPU capacity remaining for *block_id* (0 if block not in occupancy map)."""
        # Look up the block's gpu_count from the reservations list.
        gpu_count = self._block_gpu_count(block_id)
        if gpu_count == 0:
            return 0
        used = sum(self.ondemand_occupancy.get(block_id, {}).values())
        return max(0, gpu_count - used)

    def _block_gpu_count(self, block_id: int) -> int:
        """Return gpu_count for an on-demand block by id, or 0 if not found."""
        for r in self.reservations:
            if r.id == block_id:
                return r.gpu_count
        return 0

    # ------------------------------------------------------------------
    # On-demand reconciliation
    # ------------------------------------------------------------------

    def reconcile_ondemand(self) -> None:
        """Prune occupancy entries for on-demand blocks that are no longer active.

        Called after each reservation refresh alongside ``reconcile_queue``.
        Removes occupancy entries for blocks that have been cancelled or whose
        window has ended.  The pods that were placed on those blocks will have
        already been killed by ``activeDeadlineSeconds`` or will be reaped
        by the next pod-watch event.
        """
        now = datetime.now()
        active_ondemand_ids = {
            r.id
            for r in self.reservations
            if (r.kind == "ondemand" or r.id in self.noshow_reservation_ids)
            and slot_end(r) > now
        }
        stale_block_ids = [
            bid
            for bid in list(self.ondemand_occupancy.keys())
            if bid not in active_ondemand_ids
        ]
        for bid in stale_block_ids:
            occupants = self.ondemand_occupancy.pop(bid)
            log.info(
                "On-demand block #%d is no longer active; "
                "pruned %d occupant(s) from tracking",
                bid,
                len(occupants),
            )
