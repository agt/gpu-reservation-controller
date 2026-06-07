"""Core controller state machine.

This module owns the in-memory state shared by the three background loops:

``ControllerState``  — reservations list, GPU-class label map, task queue
``QueueEntry``       — one pod waiting for its reservation window
``slot_start / slot_end`` — reservation time-window arithmetic
``TOLERATION_KEY``   — the taint/toleration key used by the reservation system

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

        # Active work queue keyed by pod UID.
        self.task_queue: dict[str, QueueEntry] = {}

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
            if r.user.username == namespace
            and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
            and slot_end(r) > now  # still has time left
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
        namespace = current_reservation.user.username
        gpu_class_label = self.gpu_class_labels.get(current_reservation.gpu_class_id)
        gpu_count = current_reservation.gpu_count

        prev_end = slot_end(current_reservation)
        remaining = max(0.0, (prev_end - now).total_seconds())

        # Collect future reservations that could extend the chain.
        candidates = sorted(
            [
                r
                for r in self.reservations
                if r.id != current_reservation.id
                and r.user.username == namespace
                and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
                and r.gpu_count == gpu_count
                and slot_end(r) > now
            ],
            key=slot_start,
        )

        total = remaining
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
            total += next_res.policy.duration_minutes * 60.0
            prev_end = slot_end(next_res)
            visited.add(next_res.id)

        return int(total)

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
