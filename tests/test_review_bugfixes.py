"""Focused regressions for CODE-REVIEW-2026-07 Part I bug fixes (pure logic).

B3 — the runtime-guarantee seconds annotation floors at 1, never 0 or negative
     (originally a compute_max_deadline_seconds guard; the floor now lives in
     main._record_guarantee's seconds conversion since guarantee_end/
     compute_guaranteed_until deliberately return unfloored absolute instants —
     see test_guarantees.py).
B11 — _reservation_gpu_count / available_by_id consult cancelled-in-window blocks.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

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


def test_record_guarantee_floors_at_one_second_for_past_guarantee(monkeypatch):
    """B3: a guarantee whose absolute end already passed between the caller's
    now-check and enforcement (a stale-data race) must never produce a 0 or
    negative seconds annotation."""
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-bugfix")
    import app.main as main

    calls = {}

    async def fake_annotate(pod_name, namespace, seconds, guaranteed_until):
        calls["seconds"] = seconds

    async def fake_emit(*args, **kwargs):
        pass

    monkeypatch.setattr(main, "annotate_runtime_guarantee", fake_annotate)
    monkeypatch.setattr(main, "emit_runtime_guaranteed_event", fake_emit)

    now = datetime.now(timezone.utc)
    guaranteed_until = now - timedelta(seconds=5)
    fresh_pod = SimpleNamespace(metadata=SimpleNamespace(uid="uid-1"))
    asyncio.run(main._record_guarantee("pod-1", "ns", fresh_pod, guaranteed_until, now))

    assert calls["seconds"] == 1


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
