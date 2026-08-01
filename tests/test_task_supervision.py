"""Background-task supervision and the health endpoint that reflects it.

``_on_task_done`` makes a crashed loop loud (CRITICAL ``task.crashed`` +
``_task_health``), and ``GET /health`` — liveness, readiness, and container
healthcheck — turns 503 so Kubernetes restarts the pod.  Uses the
``_patched_main`` lifespan harness shape from ``tests/test_forecast_api.py``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging

from fastapi.testclient import TestClient


def _main(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-supervision")
    import app.main as main

    main._task_health.reset()  # module-level state; isolate from other tests
    return main


def test_crashed_task_is_marked_and_logged(monkeypatch, caplog):
    main = _main(monkeypatch)

    async def _scenario():
        async def boom():
            raise RuntimeError("loop exploded")

        task = asyncio.create_task(boom(), name="pod-watch")
        await asyncio.gather(task, return_exceptions=True)
        main._on_task_done(task)

    with caplog.at_level(logging.CRITICAL, logger="app.main"):
        asyncio.run(_scenario())

    assert main._task_health.dead == {"pod-watch": "RuntimeError: loop exploded"}
    assert not main._task_health.ok
    crashed = [r for r in caplog.records if "task.crashed" in r.getMessage()]
    assert len(crashed) == 1
    assert "task=pod-watch" in crashed[0].getMessage()
    assert crashed[0].exc_info is not None  # traceback attached via exc_info=exc
    main._task_health.reset()


def test_cancelled_and_clean_tasks_are_not_marked(monkeypatch):
    main = _main(monkeypatch)

    async def _scenario():
        async def forever():
            await asyncio.sleep(3600)

        async def clean():
            return None

        cancelled = asyncio.create_task(forever(), name="preemption")
        cancelled.cancel()
        finished = asyncio.create_task(clean(), name="queue-processor")
        await asyncio.gather(cancelled, finished, return_exceptions=True)
        main._on_task_done(cancelled)
        main._on_task_done(finished)

    asyncio.run(_scenario())
    # Cancellation is the normal shutdown path; a clean return is what the
    # test harnesses' no-op loop stubs do.  Neither may trip /health.
    assert main._task_health.ok


def test_health_endpoint_reflects_task_health(monkeypatch):
    main = _main(monkeypatch)

    ok = asyncio.run(main.health())
    assert ok.status_code == 200
    assert json.loads(ok.body) == {"status": "ok"}

    main._task_health.mark_dead("pod-watch", RuntimeError("loop exploded"))
    degraded = asyncio.run(main.health())
    assert degraded.status_code == 503
    body = json.loads(degraded.body)
    assert body["status"] == "degraded"
    assert body["dead_tasks"] == ["pod-watch"]
    assert body["detail"]["pod-watch"] == "RuntimeError: loop exploded"
    main._task_health.reset()


def test_lifespan_resets_health_and_serves_ok(monkeypatch):
    """Full app: healthy on start, 503 after a marked death, reset on restart."""
    main = _main(monkeypatch)

    async def _noop(*a, **k):
        return None

    # Neutralise everything the lifespan touches so startup does no I/O
    # (same stubbing as tests/test_forecast_api.py).
    monkeypatch.setattr(main, "init_k8s", lambda *a, **k: None)
    monkeypatch.setattr(main, "_refresh_reservations", _noop)
    monkeypatch.setattr(main, "_run_capacity_audit", _noop)
    monkeypatch.setattr(main, "reservation_fetch_loop", _noop)
    monkeypatch.setattr(main, "pod_watch_loop", _noop)
    monkeypatch.setattr(main, "queue_processor_loop", _noop)
    monkeypatch.setattr(main, "preemption_loop", _noop)
    monkeypatch.setattr(main, "capacity_audit_loop", _noop)
    orig = main.app.state.config
    monkeypatch.setattr(
        main.app.state, "config", dataclasses.replace(orig, inbound_api_token=None)
    )

    with TestClient(main.app) as client:
        assert client.get("/health").status_code == 200
        main._task_health.mark_dead("pod-watch", RuntimeError("boom"))
        resp = client.get("/health")
        assert resp.status_code == 503
        assert "pod-watch" in resp.json()["dead_tasks"]

    # A fresh lifespan (pod restart) starts with a clean supervision slate.
    with TestClient(main.app) as client:
        assert client.get("/health").status_code == 200
