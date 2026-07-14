"""Focused regressions for CODE-REVIEW-2026-07 Part I bug fixes (pure logic).

B3 — the runtime-guarantee seconds annotation floors at 1, never 0 or negative
     (originally a compute_max_deadline_seconds guard; the floor now lives in
     main._record_guarantee's seconds conversion since guarantee_end/
     compute_guaranteed_until deliberately return unfloored absolute instants —
     see test_guarantees.py).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace


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
