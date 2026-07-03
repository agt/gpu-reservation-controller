"""Focused regressions for CODE-REVIEW-2026-07 Part I bug fixes (pure logic).

B3 — compute_max_deadline_seconds floors at 1 (Kubernetes rejects 0).
B11 — _reservation_gpu_count / available_by_id consult cancelled-in-window blocks.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.controller import ControllerState
from app.schemas import GpuClassBrief, ReservationResponse


def _res(res_id: int, start: datetime, end: datetime, *, gpu_count: int = 2,
         kind: str = "reclaim") -> ReservationResponse:
    return ReservationResponse(
        id=res_id, user_id=None, user=None, group_id=None, group=None,
        gpu_class_id=10, gpu_class=GpuClassBrief(id=10, name="H100"),
        date=start.date(), start_utc=start, end_utc=end,
        gpu_count=gpu_count, status="active", kind=kind,
        created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
    )


def test_compute_max_deadline_floors_at_one_for_expired_window():
    """B3: a window that expired between the now-check and enforcement must not
    produce activeDeadlineSeconds: 0 (rejected by the API → pod runs uncapped)."""
    now = datetime.now(timezone.utc)
    expired = _res(1, now - timedelta(hours=2), now - timedelta(seconds=5), kind="booking")
    state = ControllerState()
    state.reservations = [expired]

    assert state.compute_max_deadline_seconds(now, expired) == 1


def test_reservation_gpu_count_consults_cancelled_blocks():
    """B11: on-demand placement onto freed cancelled capacity should report real
    free counts, not 0/0."""
    now = datetime.now(timezone.utc)
    cancelled = _res(5, now - timedelta(minutes=10), now + timedelta(hours=1), gpu_count=4)
    state = ControllerState()
    state.cancelled_reservations = {5: cancelled}

    # Nothing placed yet → all 4 free.
    assert state.available_by_id(5) == 4
    state.record_placement(5, "uid-a", 1)
    assert state.available_by_id(5) == 3
