"""Singleton lease guard — the duplicate-instance interlock.

Two layers: the ``k8s_client`` primitives that talk to coordination.k8s.io
(acquire/renew, returning outcomes rather than raising), and the ``main``
policy built on them (terminate on loss, fail open on API errors, abort
startup when another live instance holds the lease).
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from kubernetes.client.rest import ApiException

import app.k8s_client as k8s_client
from app.k8s_client import (
    LEASE_NAME,
    LeaseOutcome,
    acquire_singleton_lease,
    renew_singleton_lease,
)

NS = "gpu-system"
ME = "controller-abc"
OTHER = "controller-xyz"
DURATION = 60


RV = "48213"


def _lease(holder, *, age_s=0, duration=DURATION, transitions=3, rv=RV):
    renew = datetime.now(timezone.utc) - timedelta(seconds=age_s)
    return SimpleNamespace(
        metadata=SimpleNamespace(name=LEASE_NAME, resource_version=rv),
        spec=SimpleNamespace(
            holder_identity=holder,
            lease_duration_seconds=duration,
            renew_time=renew,
            lease_transitions=transitions,
        ),
    )


class _FakeCoordination:
    def __init__(self, *, read=None):
        self._read = read
        self.created = []
        self.replaced = []

    def read_namespaced_lease(self, name, namespace):
        assert name == LEASE_NAME and namespace == NS
        if isinstance(self._read, Exception):
            raise self._read
        return self._read

    def create_namespaced_lease(self, namespace, body):
        self.created.append(body)
        return body

    def replace_namespaced_lease(self, name, namespace, body):
        # The apiserver rejects an update whose metadata.resourceVersion is
        # unset ("must be specified for an update"), which is exactly what a
        # body built from scratch rather than from the read object carries.
        if not getattr(body.metadata, "resource_version", None):
            raise ApiException(status=422, reason="Unprocessable Entity")
        self.replaced.append(body)
        return body


def _install(monkeypatch, fake):
    monkeypatch.setattr(k8s_client, "_coordination_v1", fake)
    return fake


# --- k8s_client primitives ------------------------------------------------


def test_acquire_creates_lease_when_absent(monkeypatch):
    fake = _install(monkeypatch, _FakeCoordination(read=ApiException(status=404)))
    out = asyncio.run(acquire_singleton_lease(NS, ME, DURATION))

    assert out.status == "acquired" and out.mode == "created"
    assert len(fake.created) == 1
    spec = fake.created[0].spec
    assert spec.holder_identity == ME
    assert spec.lease_duration_seconds == DURATION
    assert spec.acquire_time is not None


def test_acquire_reports_live_other_holder(monkeypatch):
    _install(monkeypatch, _FakeCoordination(read=_lease(OTHER, age_s=5)))
    out = asyncio.run(acquire_singleton_lease(NS, ME, DURATION))

    assert out.status == "held_by_other"
    assert out.holder == OTHER and out.age_s == 5


def test_acquire_takes_over_expired_lease(monkeypatch):
    fake = _install(monkeypatch, _FakeCoordination(read=_lease(OTHER, age_s=120)))
    out = asyncio.run(acquire_singleton_lease(NS, ME, DURATION))

    assert out.status == "acquired" and out.mode == "takeover"
    assert out.age_s == 120
    # lease_transitions is bumped on a genuine handover.
    assert fake.replaced[0].spec.lease_transitions == 4
    assert fake.replaced[0].spec.holder_identity == ME
    assert fake.replaced[0].metadata.resource_version == RV


def test_acquire_reacquires_own_lease_immediately(monkeypatch):
    """A container restart of the same pod must not wait out the duration."""
    fake = _install(monkeypatch, _FakeCoordination(read=_lease(ME, age_s=1)))
    out = asyncio.run(acquire_singleton_lease(NS, ME, DURATION))

    assert out.status == "acquired" and out.mode == "reacquired"
    assert fake.replaced[0].spec.lease_transitions == 3  # not a handover


def test_acquire_errors_are_returned_not_raised(monkeypatch):
    _install(monkeypatch, _FakeCoordination(read=ApiException(status=403)))
    out = asyncio.run(acquire_singleton_lease(NS, ME, DURATION))

    assert out.status == "error" and "403" in out.err


def test_renew_refreshes_our_lease(monkeypatch):
    fake = _install(monkeypatch, _FakeCoordination(read=_lease(ME, age_s=20)))
    out = asyncio.run(renew_singleton_lease(NS, ME, DURATION))

    assert out.status == "acquired" and out.mode == "renewed"
    # A renewal must not restamp acquire_time.
    assert getattr(fake.replaced[0].spec, "acquire_time", None) is None
    # ...and must carry the read object's resourceVersion, or the apiserver
    # 422s every renewal and the lease silently goes stale.
    assert fake.replaced[0].metadata.resource_version == RV


def test_renew_fails_open_when_the_server_rejects_the_update(monkeypatch):
    """The 422 this regression produced is returned, never raised."""
    _install(monkeypatch, _FakeCoordination(read=_lease(ME, age_s=20, rv=None)))
    out = asyncio.run(renew_singleton_lease(NS, ME, DURATION))

    assert out.status == "error" and "422" in out.err


def test_renew_detects_live_takeover(monkeypatch):
    _install(monkeypatch, _FakeCoordination(read=_lease(OTHER, age_s=3)))
    out = asyncio.run(renew_singleton_lease(NS, ME, DURATION))

    assert out.status == "lost" and out.holder == OTHER


def test_renew_reclaims_expired_other_holder(monkeypatch):
    """A stale foreign lease is not a live duplicate — reclaim, don't die."""
    _install(monkeypatch, _FakeCoordination(read=_lease(OTHER, age_s=999)))
    out = asyncio.run(renew_singleton_lease(NS, ME, DURATION))

    assert out.status == "acquired"


def test_renew_errors_are_returned_not_raised(monkeypatch):
    _install(monkeypatch, _FakeCoordination(read=RuntimeError("connection reset")))
    out = asyncio.run(renew_singleton_lease(NS, ME, DURATION))

    assert out.status == "error" and "connection reset" in out.err


# --- main policy ----------------------------------------------------------


def _main(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-lease")
    import app.main as main

    main._task_health.reset()
    return main


def _config(main, **overrides):
    return dataclasses.replace(main.app.state.config, **overrides)


def _drive(main, monkeypatch, outcomes, *, held, max_ticks=10):
    """Run lease_guard_loop over a scripted outcome list; record termination."""
    terminated = []
    monkeypatch.setattr(main, "_terminate_process", lambda: terminated.append(True))

    script = list(outcomes)
    ticks = 0

    async def _next(*a, **k):
        return script.pop(0) if script else LeaseOutcome(status="acquired", mode="renewed")

    async def _sleep(_seconds):
        nonlocal ticks
        ticks += 1
        if ticks > max_ticks:
            raise _Done()

    monkeypatch.setattr(main, "renew_singleton_lease", _next)
    monkeypatch.setattr(main, "acquire_singleton_lease", _next)
    monkeypatch.setattr(asyncio, "sleep", _sleep)

    async def _run():
        try:
            await main.lease_guard_loop(_config(main), held=held)
        except _Done:
            pass

    asyncio.run(_run())
    return terminated


class _Done(Exception):
    """Stops the otherwise-infinite guard loop in tests."""


def test_guard_terminates_when_lease_is_lost(monkeypatch, caplog):
    main = _main(monkeypatch)
    with caplog.at_level(logging.CRITICAL, logger="app.main"):
        terminated = _drive(
            main, monkeypatch,
            [LeaseOutcome(status="lost", holder=OTHER)],
            held=True,
        )

    assert terminated == [True]
    assert any("singleton.lost" in r.getMessage() for r in caplog.records)


def test_guard_survives_transient_renew_errors(monkeypatch, caplog):
    """A coordination blip is not evidence of a duplicate — keep running."""
    main = _main(monkeypatch)
    with caplog.at_level(logging.WARNING, logger="app.main"):
        terminated = _drive(
            main, monkeypatch,
            [
                LeaseOutcome(status="error", err="timeout"),
                LeaseOutcome(status="error", err="timeout"),
                LeaseOutcome(status="acquired", mode="renewed"),
            ],
            held=True,
            max_ticks=4,
        )

    assert terminated == []
    warns = [r for r in caplog.records if "singleton.renew_failed" in r.getMessage()]
    assert len(warns) == 1  # throttled: first failure only
    assert "fails=1" in warns[0].getMessage()


def test_unguarded_start_terminates_on_affirmative_duplicate(monkeypatch, caplog):
    """Started fail-open; a later held_by_other is the duplicate signal."""
    main = _main(monkeypatch)
    with caplog.at_level(logging.CRITICAL, logger="app.main"):
        terminated = _drive(
            main, monkeypatch,
            [
                LeaseOutcome(status="error", err="403"),
                LeaseOutcome(status="held_by_other", holder=OTHER, age_s=2),
            ],
            held=False,
            max_ticks=4,
        )

    assert terminated == [True]


def test_unguarded_start_acquires_later(monkeypatch, caplog):
    main = _main(monkeypatch)
    with caplog.at_level(logging.INFO, logger="app.main"):
        terminated = _drive(
            main, monkeypatch,
            [
                LeaseOutcome(status="error", err="403"),
                LeaseOutcome(status="acquired", mode="takeover", age_s=90),
            ],
            held=False,
            max_ticks=3,
        )

    assert terminated == []
    assert any("singleton.acquired" in r.getMessage() for r in caplog.records)


def test_startup_claim_raises_when_another_instance_holds(monkeypatch, caplog):
    main = _main(monkeypatch)

    async def _held(*a, **k):
        return LeaseOutcome(status="held_by_other", holder=OTHER, age_s=4)

    monkeypatch.setattr(main, "acquire_singleton_lease", _held)
    with caplog.at_level(logging.CRITICAL, logger="app.main"):
        with pytest.raises(RuntimeError, match="another controller instance"):
            asyncio.run(main._claim_singleton_lease(_config(main)))

    assert any("singleton.held_by_other" in r.getMessage() for r in caplog.records)


def test_startup_claim_fails_open_on_api_error(monkeypatch, caplog):
    main = _main(monkeypatch)

    async def _error(*a, **k):
        return LeaseOutcome(status="error", err="leases is forbidden")

    monkeypatch.setattr(main, "acquire_singleton_lease", _error)
    with caplog.at_level(logging.WARNING, logger="app.main"):
        held = asyncio.run(main._claim_singleton_lease(_config(main)))

    # Runs unguarded rather than refusing to start.
    assert held is False
    assert any("singleton.acquire_failed" in r.getMessage() for r in caplog.records)


def test_lease_namespace_and_holder_fallbacks(monkeypatch):
    main = _main(monkeypatch)
    explicit = _config(main, pod_namespace="team-ns", pod_name="pod-1")
    assert main._lease_namespace(explicit) == "team-ns"
    assert main._lease_holder(explicit) == "pod-1"

    # No downward API: namespace falls back (no SA file in this environment),
    # holder falls back to the hostname.
    bare = _config(main, pod_namespace=None, pod_name=None)
    assert main._lease_namespace(bare) == "default"
    assert main._lease_holder(bare)  # non-empty hostname


# --- lifespan wiring ------------------------------------------------------


def _patched_lifespan_main(monkeypatch, **config_overrides):
    main = _main(monkeypatch)

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(main, "init_k8s", lambda *a, **k: None)
    monkeypatch.setattr(main, "_refresh_reservations", _noop)
    monkeypatch.setattr(main, "_run_capacity_audit", _noop)
    monkeypatch.setattr(main, "reservation_fetch_loop", _noop)
    monkeypatch.setattr(main, "pod_watch_loop", _noop)
    monkeypatch.setattr(main, "queue_processor_loop", _noop)
    monkeypatch.setattr(main, "preemption_loop", _noop)
    monkeypatch.setattr(main, "capacity_audit_loop", _noop)
    monkeypatch.setattr(main, "lease_guard_loop", _noop)
    monkeypatch.setattr(
        main.app.state, "config", _config(main, **config_overrides)
    )
    return main


def test_lifespan_aborts_when_duplicate_detected(monkeypatch):
    main = _patched_lifespan_main(monkeypatch, singleton_lease_enabled=True)

    async def _held(*a, **k):
        return LeaseOutcome(status="held_by_other", holder=OTHER, age_s=1)

    monkeypatch.setattr(main, "acquire_singleton_lease", _held)

    # A lifespan startup exception propagates out of TestClient's context
    # entry — the process would exit non-zero under uvicorn.
    with pytest.raises(RuntimeError, match="another controller instance"):
        with TestClient(main.app):
            pass


def test_lifespan_skips_lease_when_disabled(monkeypatch, caplog):
    main = _patched_lifespan_main(monkeypatch, singleton_lease_enabled=False)

    called = []

    async def _acquire(*a, **k):
        called.append(True)
        return LeaseOutcome(status="acquired")

    monkeypatch.setattr(main, "acquire_singleton_lease", _acquire)

    with caplog.at_level(logging.INFO, logger="app.main"):
        with TestClient(main.app) as client:
            assert client.get("/health").status_code == 200

    assert called == []
    assert any("singleton.disabled" in r.getMessage() for r in caplog.records)
