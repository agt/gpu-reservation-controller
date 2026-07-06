"""Inbound reservation-push API — POST /api/reservations/push.

Two layers of coverage:

* ``apply_push_to_active`` — the pure upsert-by-id merge helper.
* The HTTP endpoint — auth (503 when the token is unset, 401 on a bad bearer),
  an active upsert, and an in-window cancellation that evicts its admitted pod.

This is the first HTTP-surface test in the suite: it drives the real FastAPI app
through ``TestClient`` (so the auth dependency and status codes are exercised),
with the lifespan's network / Kubernetes touch-points stubbed to no-ops so
startup does no I/O.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.controller import apply_push_to_active
from app.k8s_client import ToleratedPodInfo
from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME, block


def _res(res_id: int, *, status: str = "active", **kw):
    """An open (now-1h … now+1h) reservation for merge / endpoint tests."""
    now = datetime.now(timezone.utc)
    return block(res_id, now - timedelta(hours=1), now + timedelta(hours=1),
                 status=status, **kw)


def _json(*reservations):
    return {"reservations": [r.model_dump(mode="json") for r in reservations]}


# ---------------------------------------------------------------------------
# apply_push_to_active (pure)
# ---------------------------------------------------------------------------


class TestApplyPushToActive:
    def test_inserts_new_active_by_id(self):
        out = apply_push_to_active([_res(1)], [_res(2)])
        assert {r.id for r in out} == {1, 2}

    def test_replaces_existing_by_id(self):
        out = apply_push_to_active([_res(1, gpu_count=2)], [_res(1, gpu_count=8)])
        assert len(out) == 1 and out[0].gpu_count == 8

    def test_drops_non_active(self):
        out = apply_push_to_active([_res(1), _res(2)], [_res(1, status="cancelled")])
        assert {r.id for r in out} == {2}

    def test_last_occurrence_of_an_id_wins(self):
        out = apply_push_to_active([], [_res(1, gpu_count=2), _res(1, gpu_count=4)])
        assert len(out) == 1 and out[0].gpu_count == 4

    def test_does_not_mutate_inputs(self):
        current = [_res(1)]
        apply_push_to_active(current, [_res(1, status="cancelled"), _res(2)])
        assert [r.id for r in current] == [1]


# ---------------------------------------------------------------------------
# HTTP endpoint
# ---------------------------------------------------------------------------


def _patched_main(monkeypatch, token):
    """Import ``app.main`` with startup I/O stubbed and *token* installed."""
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-push")
    import app.main as main

    async def _noop(*a, **k):
        return None

    # Neutralise everything the lifespan touches so startup does no I/O.
    monkeypatch.setattr(main, "init_k8s", lambda *a, **k: None)
    monkeypatch.setattr(main, "_refresh_reservations", _noop)
    monkeypatch.setattr(main, "reservation_fetch_loop", _noop)
    monkeypatch.setattr(main, "pod_watch_loop", _noop)
    monkeypatch.setattr(main, "queue_processor_loop", _noop)

    orig = main.app.state.config
    monkeypatch.setattr(
        main.app.state, "config", dataclasses.replace(orig, inbound_api_token=token)
    )
    return main


def test_push_disabled_returns_503_when_token_unset(monkeypatch):
    main = _patched_main(monkeypatch, token=None)
    with TestClient(main.app) as client:
        resp = client.post("/api/reservations/push", json=_json(_res(1)))
    assert resp.status_code == 503


@pytest.mark.parametrize("headers", [
    {},                                   # missing
    {"Authorization": "Bearer wrong"},    # mismatched
    {"Authorization": "secret"},          # no scheme
])
def test_push_bad_or_missing_bearer_returns_401(monkeypatch, headers):
    main = _patched_main(monkeypatch, token="secret")
    with TestClient(main.app) as client:
        resp = client.post("/api/reservations/push", json=_json(_res(1)), headers=headers)
    assert resp.status_code == 401


def test_push_active_upsert_updates_state(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_res(1, gpu_count=2)]
        pushed = _res(1, gpu_count=8)
        new = _res(2, gpu_count=4)

        resp = client.post(
            "/api/reservations/push",
            json=_json(pushed, new),
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body == {"applied": 2, "cancelled": 0, "total_active": 2}

        by_id = {r.id: r for r in state.reservations}
        assert by_id.keys() == {1, 2}
        assert by_id[1].gpu_count == 8   # replaced, not duplicated


def test_push_cancellation_evicts_admitted_pod(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")

    pod = ToleratedPodInfo(
        namespace=USERNAME, name="pod-1", uid="uid-1", gpu_class=GPU_CLASS_LABEL,
        booking_reference="res-1", reservation_id=1, gpu_count=2,
        phase="Running", scheduled_false=False,
    )
    deleted: list[tuple[str, str]] = []

    async def _snapshot(_key):
        return [pod]

    async def _delete(name, ns):
        deleted.append((name, ns))

    async def _read(name, ns):
        return SimpleNamespace()

    async def _emit(*a, **k):
        return None

    monkeypatch.setattr(main, "snapshot_tolerated_pods", _snapshot)
    monkeypatch.setattr(main, "delete_pod", _delete)
    monkeypatch.setattr(main, "read_pod", _read)
    monkeypatch.setattr(main, "emit_reservation_cancelled_event", _emit)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_res(1, gpu_count=2)]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        state.record_placement(1, "uid-1", 2)  # pod occupies the reservation

        resp = client.post(
            "/api/reservations/push",
            json=_json(_res(1, status="cancelled")),
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.status_code == 200
        assert resp.json() == {"applied": 0, "cancelled": 1, "total_active": 0}

        assert deleted == [("pod-1", USERNAME)]         # pod evicted
        assert state.occupancy.get(1, {}) == {}         # budget freed
        assert 1 in state.cancelled_reservations        # capacity offered on-demand
        assert [r.id for r in state.reservations] == [] # dropped from active set
