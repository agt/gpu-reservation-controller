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


class PolicyBrief(BaseModel):
    id: int
    name: str
    start_time: str          # "HH:MM:SS" — minutes from midnight on reservation date
    duration_minutes: int
    repeat_count: int


class ReservationResponse(BaseModel):
    id: int
    user_id: Optional[int] = None   # null for kind="ondemand"
    user: Optional[UserBrief] = None  # null for kind="ondemand"
    group_id: Optional[int] = None
    group: Optional[GroupBrief] = None
    gpu_class_id: int
    gpu_class: GpuClassBrief
    policy_id: int
    slot_index: int          # 0-based; see time-window formula in RESERVATION-API.md §4
    policy: PolicyBrief
    date: date               # calendar date of the reservation
    gpu_count: int           # number of GPUs reserved
    status: str              # "active" | "cancelled"
    kind: str                # "user" | "ondemand"
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    cancelled_at: Optional[datetime] = None
    cancelled_by_id: Optional[int] = None
    created_by_id: Optional[int] = None  # admin who created an ondemand block; null if auto-filled


class GpuClassDetail(BaseModel):
    """Returned by GET /api/gpu-classes/{id}.

    ``label_value`` is the Kubernetes node-label value used when matching
    pod gpu-class labels (e.g. "h100", "a100-80gb").  It is optional in the
    API; if absent, pods for this GPU class cannot be matched.
    """

    id: int
    name: str
    label_value: Optional[str] = None
