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

    reclaim_window_minutes: int
    reclaim_preempt_guard_minutes: int


class GpuClassDetail(BaseModel):
    """Returned by GET /api/gpu-classes/{id}.

    ``label_value`` is the Kubernetes node-label value used when matching
    pod gpu-class labels (e.g. "h100", "a100-80gb").  It is optional in the
    API; if absent, pods for this GPU class cannot be matched.
    """

    id: int
    name: str
    label_value: Optional[str] = None
