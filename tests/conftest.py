"""Shared test factories and fixtures (CODE-REVIEW T1).

Before this file, ``_compute_window`` was byte-identical in five test modules and
the reservation factory existed in six drifted copies.  Everything here builds on
one canonical ``reservation()`` factory (explicit UTC windows) so the
``ReservationResponse`` shape has a single source of truth; the ``user_reservation``
/ ``ondemand_block`` / ``block`` wrappers exist only to keep call sites readable.

Import from test modules with ``from tests.conftest import ...``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from app.controller import ControllerState
from app.schemas import GpuClassBrief, GroupBrief, ReservationResponse, UserBrief

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

GPU_CLASS_ID = 10
GPU_CLASS_LABEL = "h100"
OTHER_CLASS_ID = 20
OTHER_CLASS_LABEL = "a100"
USERNAME = "alice"
ADMIN_USERNAME = "sysadmin"
FIXED_DATE = date(2024, 1, 15)    # past; window timing controlled via explicit `now`
FUTURE_DATE = date(2099, 6, 15)   # far future; slot_end always > datetime.now(utc)

_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------


def window(
    day: date,
    start_time: str = "08:00:00",
    slot_index: int = 0,
    duration_minutes: int = 120,
) -> tuple[datetime, datetime]:
    """Return ``(start_utc, end_utc)`` from a date + ``HH:MM[:SS]`` + slot offset."""
    parts = start_time.split(":")
    minutes = int(parts[0]) * 60 + int(parts[1]) + slot_index * duration_minutes
    return window_from_minutes(minutes, duration_minutes, day)


def window_from_minutes(
    minutes_from_midnight: int,
    duration_minutes: int,
    day: date = FIXED_DATE,
) -> tuple[datetime, datetime]:
    """Return ``(start_utc, end_utc)`` from a minutes-past-midnight offset (UTC)."""
    midnight = datetime.combine(day, datetime.min.time()).replace(tzinfo=timezone.utc)
    start = midnight + timedelta(minutes=minutes_from_midnight)
    return start, start + timedelta(minutes=duration_minutes)


# ---------------------------------------------------------------------------
# Canonical reservation factory + readable wrappers
# ---------------------------------------------------------------------------


def reservation(
    res_id: int,
    *,
    start_utc: datetime,
    end_utc: datetime,
    kind: str = "booking",
    username: str = USERNAME,
    user_id: int = 1,
    with_user: bool | None = None,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
    gpu_count: int = 2,
    status: str = "active",
    cancelled_by_id: int | None = None,
    cancelled_by: UserBrief | None = None,
    group: str | None = None,
    group_id: int | None = None,
    day: date | None = None,
) -> ReservationResponse:
    """Build a ``ReservationResponse`` with an explicit UTC window.

    A ``kind="booking"`` reservation carries a user by default; ``kind="reclaim"``
    carries none.  Pass ``with_user`` to override that coupling (a cancelled
    booking that keeps its user, or a reclaim owned by a user in a test).

    Pass ``group`` (the usage-group name) to attach a ``GroupBrief`` for
    REQUIRED_GROUP_LABEL tests; ``group_id`` defaults to ``res_id`` when a group
    name is given so distinct groups get distinct ids for chaining tests.
    """
    if with_user is None:
        with_user = kind == "booking"
    user = UserBrief(id=user_id, username=username) if with_user else None
    group_brief = None
    if group is not None:
        gid = group_id if group_id is not None else res_id
        group_brief = GroupBrief(id=gid, name=group)
    return ReservationResponse(
        id=res_id,
        user_id=user_id if with_user else None,
        user=user,
        group_id=group_brief.id if group_brief else group_id,
        group=group_brief,
        gpu_class_id=gpu_class_id,
        gpu_class=GpuClassBrief(id=gpu_class_id, name="H100", label_value=gpu_class_label),
        date=day or start_utc.date(),
        start_utc=start_utc,
        end_utc=end_utc,
        gpu_count=gpu_count,
        status=status,
        kind=kind,
        created_at=_EPOCH,
        updated_at=_EPOCH,
        cancelled_by_id=cancelled_by_id,
        cancelled_by=cancelled_by,
    )


def user_reservation(
    res_id: int,
    *,
    username: str = USERNAME,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
    gpu_count: int = 2,
    slot_index: int = 0,
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
    user_id: int = 1,
    status: str = "active",
    cancelled_by_id: int | None = None,
    cancelled_by: UserBrief | None = None,
) -> ReservationResponse:
    """A ``kind="booking"`` reservation whose window comes from date + slot fields."""
    start_utc, end_utc = window(reservation_date, start_time, slot_index, duration_minutes)
    return reservation(
        res_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind="booking",
        username=username,
        user_id=user_id,
        gpu_class_id=gpu_class_id,
        gpu_class_label=gpu_class_label,
        gpu_count=gpu_count,
        status=status,
        cancelled_by_id=cancelled_by_id,
        cancelled_by=cancelled_by,
        day=reservation_date,
    )


def reclaim_reservation(
    res_id: int,
    *,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
    gpu_count: int = 2,
    slot_index: int = 0,
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
) -> ReservationResponse:
    """A ``kind="reclaim"`` reservation whose window comes from date + slot fields."""
    start_utc, end_utc = window(reservation_date, start_time, slot_index, duration_minutes)
    return reservation(
        res_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind="reclaim",
        gpu_class_id=gpu_class_id,
        gpu_class_label=gpu_class_label,
        gpu_count=gpu_count,
        day=reservation_date,
    )


def ondemand_block(
    block_id: int,
    *,
    gpu_count: int = 2,
    slot_index: int = 0,
    duration_minutes: int = 120,
    date_offset_days: int = 0,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
) -> ReservationResponse:
    """A ``kind="reclaim"`` block whose window is *today* (plus offset) at 08:00 UTC."""
    day = (datetime.now(timezone.utc) + timedelta(days=date_offset_days)).date()
    start_utc, end_utc = window(day, "08:00:00", slot_index, duration_minutes)
    return reservation(
        block_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind="reclaim",
        gpu_class_id=gpu_class_id,
        gpu_class_label=gpu_class_label,
        gpu_count=gpu_count,
        day=day,
    )


def block(
    block_id: int,
    start_utc: datetime,
    end_utc: datetime,
    *,
    gpu_count: int = 2,
    kind: str = "reclaim",
    status: str = "active",
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
    user: bool = False,
) -> ReservationResponse:
    """A reservation with an explicit UTC window (the reclaim-merge test style)."""
    return reservation(
        block_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind=kind,
        with_user=user,
        gpu_class_id=gpu_class_id,
        gpu_class_label=gpu_class_label,
        gpu_count=gpu_count,
        status=status,
    )


# ---------------------------------------------------------------------------
# State helper
# ---------------------------------------------------------------------------


def make_state(
    *reservations: ReservationResponse,
    labels: dict[int, str] | None = None,
    required_group_label: str | None = None,
) -> ControllerState:
    """A ``ControllerState`` seeded with *reservations* and a gpu-class label map.

    Defaults the label map to ``{GPU_CLASS_ID: GPU_CLASS_LABEL}``; pass
    ``labels={}`` to exercise the unresolved-class path.  Pass
    ``required_group_label`` to enable the usage-group match constraint.
    """
    state = ControllerState()
    state.reservations = list(reservations)
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL} if labels is None else labels
    state.required_group_label = required_group_label
    return state
