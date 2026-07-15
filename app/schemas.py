"""Pydantic models for the GPU Reservation API response types.

Mirrors the shapes documented in RESERVATION-API.md §6.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

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
    user_id: Optional[int] = None   # always set in practice (nullability is legacy)
    user: Optional[UserBrief] = None  # always set in practice (nullability is legacy)
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
    kind: str                # "booking" | "on_demand"  (JIT lease)
    notes: Optional[str] = None
    submitted_by_id: Optional[int] = None
    submitted_by: Optional[UserBrief] = None
    created_at: datetime
    updated_at: datetime
    cancelled_at: Optional[datetime] = None
    cancelled_by_id: Optional[int] = None
    cancelled_by: Optional[UserBrief] = None


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
    adopted: int = 0    # in-window owner changes whose prior-owner pod was evicted
    total_active: int   # size of the active reservation set after the push


# ---------------------------------------------------------------------------
# JIT on-demand reservation request (POST /api/reservations)
# ---------------------------------------------------------------------------


class OnDemandReservationRequest(BaseModel):
    """Body of a JIT on-demand booking request, sent on behalf of a pending pod.

    The app anchors ``start_utc`` at its own "now" (avoids controller/app clock
    skew) and sets ``end_utc = start_utc + duration_seconds``.
    ``on_demand=True`` relaxes policy limits (SU/caps/min-duration) only —
    never physical calendar capacity.  ``idempotency_key`` is the admitting
    pod's UID: a retry with the same key returns the original reservation
    rather than creating a duplicate.
    """

    username: str
    group_name: Optional[str] = None
    gpu_class_id: int
    gpu_count: int
    duration_seconds: int
    on_demand: bool = True
    idempotency_key: str


# ---------------------------------------------------------------------------
# Preemption victim selection (POST /api/reservations/preemption-victims)
# ---------------------------------------------------------------------------


class PreemptionCandidate(BaseModel):
    """One eligible overstay pod offered to the app for victim selection.

    The controller has already decided this pod is *preemptable* — live,
    past its runtime guarantee, admitted by this controller, of the class in
    question.  ``reservation_id`` is the pod's booking-reference id: the app's
    handle for looking the reservation (and thus its owner/group/kind) up when
    it prioritises.  ``pod_uid`` is opaque to the app — it is echoed back in the
    response so the controller can map the choice to a pod to delete.
    """

    pod_uid: str
    namespace: str
    pod_name: str
    gpu_class: str          # Kubernetes label value (e.g. "h100")
    gpu_count: int
    reservation_id: int     # booking-reference id (always set — eligibility requires it)


class PreemptionSelectionRequest(BaseModel):
    """Body of ``POST /api/reservations/preemption-victims``.

    ``needed_by_class`` is how many GPUs must be reclaimed per gpu-class label
    at one boundary; ``candidates`` is the full eligible pool the app chooses
    from.  The app returns the victims it selects; the controller kills only
    those (and only ones it offered).
    """

    needed_by_class: dict[str, int]
    candidates: list[PreemptionCandidate]


class PreemptionSelectionResponse(BaseModel):
    """Victims the app chose to preempt, as the ``pod_uid``s it was offered."""

    victim_pod_uids: list[str]


# ---------------------------------------------------------------------------
# Preemption-risk forecast (GET /api/forecast/preemption-risk)
# ---------------------------------------------------------------------------


class ForecastClassSummary(BaseModel):
    """One GPU class's capacity/demand outlook within one forecast bucket."""

    capacity: int            # physical GPUs (node snapshot); 0 = class unknown to nodes
    free: int                # capacity − node-resident use; may be NEGATIVE (overcommit)
    demand: int              # booking demand attributed to this bucket
    shortfall: int           # GPUs the sweep would have to reclaim (demand − free, ≥ 0)
    eligible_pool_gpus: int  # Σ gpu_count of past-guarantee pods the shortfall draws from
    pending_jit_gpus: int = 0  # pods awaiting a JIT lease; informational pressure only —
                               # a granted lease cannot trigger boundary preemption today,
                               # so this is never folded into ``shortfall``


class ForecastBucket(BaseModel):
    """One forecast bucket: a half-open ``[start, end)`` wall-clock interval."""

    start: datetime
    end: datetime
    classes: dict[str, ForecastClassSummary]  # keyed by gpu-class label (e.g. "h100")


class ForecastPodBucket(BaseModel):
    """One admitted pod's outlook within one bucket (index-aligned with buckets)."""

    risk: float  # 0.0–1.0 preemption likelihood; exactly 0 while guaranteed
    state: Literal["guaranteed", "overstay", "mixed"]


class ForecastPod(BaseModel):
    """Per-pod forecast entry.  Only controller-admitted pods appear — a pod
    without a booking-reference can never be preempted by this controller."""

    namespace: str
    name: str
    uid: str
    gpu_class: str
    gpu_count: int
    reservation_id: int
    guarantee_end: Optional[datetime]  # live chain-aware instant; None = already unresolvable
    buckets: list[ForecastPodBucket]


class PreemptionRiskForecastResponse(BaseModel):
    """Response of ``GET /api/forecast/preemption-risk``.

    Covers the remainder of the current wall-clock hour plus the next two
    full hours.  ``selection_delegated`` is the honesty flag for the numeric
    risk: when true (``PREEMPTION_DELEGATE_SELECTION``, the default), the
    reservation app chooses victims by its own policy and the number models
    the controller's uniform-random local fallback — pool *membership*
    (at-risk vs safe) is exact either way.
    """

    generated_at: datetime
    lead_minutes: int          # PREEMPTION_LEAD_MINUTES — how early a kill can land
    selection_delegated: bool
    buckets: list[ForecastBucket]
    pods: list[ForecastPod]
