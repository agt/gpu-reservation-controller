"""Pydantic models for the GPU Reservation API response types.

Mirrors the shapes documented in RESERVATION-API.md §6.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class UserBrief(BaseModel):
    id: int
    username: str


class GroupBrief(BaseModel):
    id: int
    name: str


class GpuClassBrief(BaseModel):
    id: int
    name: str
    label_value: Optional[str] = None


class ReservationResponse(BaseModel):
    id: int
    user_id: Optional[int] = None   # null for kind="reclaim"
    user: Optional[UserBrief] = None  # null for kind="reclaim"
    group_id: Optional[int] = None
    group: Optional[GroupBrief] = None
    gpu_class_id: int
    gpu_class: GpuClassBrief
    start_dt: Optional[datetime] = None  # site-local wall-clock (naive); display only
    end_dt: Optional[datetime] = None    # site-local wall-clock (naive); display only
    date: date               # calendar date of the reservation (local time, for display)
    start_utc: datetime      # UTC start; use this for all time comparisons
    end_utc: datetime        # UTC end; use this for activeDeadlineSeconds
    gpu_count: int           # number of GPUs reserved
    su_cost: Optional[float] = None  # total Service Units; not consumed by the controller
    status: str              # "active" | "cancelled"
    kind: str                # "booking" | "reclaim"
    notes: Optional[str] = None
    submitted_by_id: Optional[int] = None
    submitted_by: Optional[UserBrief] = None
    created_at: datetime
    updated_at: datetime
    cancelled_at: Optional[datetime] = None
    cancelled_by_id: Optional[int] = None
    cancelled_by: Optional[UserBrief] = None


class AppSettings(BaseModel):
    """Subset of GET /api/settings the controller consumes.

    The endpoint returns additional UI-oriented fields which Pydantic ignores.
    ``reclaim_preempt_guard_minutes`` is the lead time before a reclaim hold's
    start within which the reservation app treats it as committed (non-preemptible)
    capacity — safe for the controller to merge/schedule onto.
    """

    # Nothing consumes reclaim_window_minutes; give it a default so a renamed or
    # omitted field doesn't fail validation and silently disable reclaim merging
    # (fetch_settings would return None) (CODE-REVIEW H7).
    reclaim_window_minutes: Optional[int] = None
    reclaim_preempt_guard_minutes: int


class GpuClassDetail(GpuClassBrief):
    """Returned by GET /api/gpu-classes/{id}.

    Identical shape to ``GpuClassBrief`` (id, name, optional ``label_value`` —
    the Kubernetes node-label value used to match pod gpu-class labels, e.g.
    "h100"); subclassed rather than redeclared so the two cannot drift
    (CODE-REVIEW H7).
    """


# ---------------------------------------------------------------------------
# Inbound push API (POST /api/reservations/push)
# ---------------------------------------------------------------------------


class ReservationPushRequest(BaseModel):
    """Body of a push from the reservation app.

    Carries one or more updated reservation entries (a partial delta, not a full
    snapshot — bulk synchronisation remains a controller-initiated pull).  Each
    entry is a full ``ReservationResponse`` so the same reconciliation code path
    that handles a fetched reservation applies unchanged.  The envelope object
    leaves room for future push kinds (e.g. standby assignments) without an API
    break.
    """

    reservations: list[ReservationResponse]


class ReservationPushResponse(BaseModel):
    """Summary returned after a push has been reconciled into controller state."""

    applied: int        # entries upserted into the active set
    cancelled: int      # in-window cancellations evicted / reclaimed
    total_active: int   # size of the active reservation set after the push
