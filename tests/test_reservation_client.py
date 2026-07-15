"""Tests for the reservation HTTP client boundary.

Uses httpx.MockTransport (built in — no pytest-httpx dependency) to drive the
async client without a live server.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx

from app.config import Config
from app.reservation_client import ReservationClient
from app.schemas import (
    OnDemandAdmissionCandidate,
    OnDemandAdmissionRequest,
    OnDemandReservationRequest,
)

from tests.conftest import reservation as _reservation


def _config(**overrides) -> Config:
    base = dict(
        reservation_api_url="http://reservations.local",
        reservation_api_key="gpures_test",
        reservation_fetch_interval=300,
        reservation_lookahead_days=7,
        kubeconfig_path=None,
        health_port=8000,
        ondemand_placement_enabled=True,
        noshown_timeout_minutes=15,
        noshown_grace_minutes=30,
        pod_list_tick_interval=300,
        scheduling_gate_name=None,
        inbound_api_token=None,
        preemption_lead_minutes=15,
        preemption_check_interval=60,
    )
    base.update(overrides)
    return Config(**base)


def _client_with_handler(config: Config, handler) -> ReservationClient:
    client = ReservationClient(config)
    # Swap the live AsyncClient for one backed by a MockTransport.
    client._client = httpx.AsyncClient(
        base_url=config.reservation_api_url,
        transport=httpx.MockTransport(handler),
    )
    return client


# ---------------------------------------------------------------------------
# B2 — fetch date bounds are UTC-derived and widened by one day
# ---------------------------------------------------------------------------


def test_fetch_reservations_date_start_is_utc_and_widened():
    """date_start must be (UTC today - 1 day); date_end must be UTC today + lookahead.

    Regression for CODE-REVIEW-2026-07 B2: date.today() used the process TZ and
    could drop reservations whose window is open right now.
    """
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(dict(request.url.params))
        return httpx.Response(200, json=[])

    config = _config(reservation_lookahead_days=7)
    client = _client_with_handler(config, handler)

    before = datetime.now(timezone.utc).date()
    asyncio.run(client.fetch_reservations())
    after = datetime.now(timezone.utc).date()

    # Allow for a midnight rollover between the two now() samples.
    expected_starts = {(before - timedelta(days=1)).isoformat(),
                       (after - timedelta(days=1)).isoformat()}
    expected_ends = {(before + timedelta(days=7)).isoformat(),
                    (after + timedelta(days=7)).isoformat()}
    assert captured["date_start"] in expected_starts
    assert captured["date_end"] in expected_ends
    assert captured["status"] == "all"

    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# B9 — malformed payloads honor the "None on error" contract
# ---------------------------------------------------------------------------


def test_fetch_gpu_class_returns_none_on_invalid_json():
    """Non-JSON body (ValueError/JSONDecodeError) must also return None (B9)."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.fetch_gpu_class(1)) is None
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# fetch_gpu_classes — full class list for the JIT label → id reverse map
# ---------------------------------------------------------------------------


def test_fetch_gpu_classes_returns_list():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/gpu-classes"
        return httpx.Response(
            200,
            json=[
                {"id": 10, "name": "H100", "label_value": "h100"},
                {"id": 20, "name": "A100", "label_value": "a100"},
            ],
        )

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.fetch_gpu_classes())
    assert result is not None
    assert [(c.id, c.label_value) for c in result] == [(10, "h100"), (20, "a100")]
    asyncio.run(client.aclose())


def test_fetch_gpu_classes_returns_none_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.fetch_gpu_classes()) is None
    asyncio.run(client.aclose())


def test_fetch_gpu_classes_returns_none_on_invalid_json():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.fetch_gpu_classes()) is None
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# create_ondemand_reservation — JIT lease request
# ---------------------------------------------------------------------------


def _ondemand_request(**overrides) -> OnDemandReservationRequest:
    base = dict(
        username="alice",
        group_name="cse151b",
        gpu_class_id=10,
        gpu_count=1,
        duration_seconds=1200,
        on_demand=True,
        idempotency_key="pod-uid-1",
        notes="on-demand lease for pod alice/train-1",
    )
    base.update(overrides)
    return OnDemandReservationRequest(**base)


def test_create_ondemand_reservation_201_returns_reservation():
    now = datetime.now(timezone.utc)
    res = _reservation(7, start_utc=now, end_utc=now + timedelta(minutes=20))

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/reservations"
        body = json.loads(request.content)
        assert body["idempotency_key"] == "pod-uid-1"
        assert body["on_demand"] is True
        # group_name is a required natural key on the app side; notes carry
        # which pod the lease covers for admin traceability.
        assert body["group_name"] == "cse151b"
        assert body["notes"] == "on-demand lease for pod alice/train-1"
        return httpx.Response(201, json=res.model_dump(mode="json"))

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert result is not None and result.id == 7
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_409_returns_none():
    """A capacity/policy denial must degrade to None, not raise."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "no capacity"})

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.create_ondemand_reservation(_ondemand_request())) is None
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_timeout_returns_none():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.create_ondemand_reservation(_ondemand_request())) is None
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# select_ondemand_admissions — app-delegated JIT admission selection
# ---------------------------------------------------------------------------


def _admission_request(**overrides) -> OnDemandAdmissionRequest:
    base = dict(
        pod_uid="pod-uid-1",
        username="alice",
        group_name=None,
        gpu_class_id=10,
        gpu_count=1,
        duration_seconds=1200,
    )
    base.update(overrides)
    return OnDemandAdmissionRequest(
        candidates=[
            OnDemandAdmissionCandidate(**base),
            OnDemandAdmissionCandidate(
                pod_uid="pod-uid-2", username="bob", gpu_class_id=10,
                gpu_count=2, duration_seconds=600,
            ),
        ]
    )


def test_select_ondemand_admissions_200_returns_granted_uids():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/reservations/ondemand-admission"
        body = json.loads(request.content)
        assert [c["pod_uid"] for c in body["candidates"]] == ["pod-uid-1", "pod-uid-2"]
        return httpx.Response(200, json={"granted_pod_uids": ["pod-uid-2"]})

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.select_ondemand_admissions(_admission_request()))
    assert result == ["pod-uid-2"]
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_empty_list_is_respected():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"granted_pod_uids": []})

    client = _client_with_handler(_config(), handler)
    # Empty (grant none) is a valid decision — distinct from None (fall back).
    assert asyncio.run(client.select_ondemand_admissions(_admission_request())) == []
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_404_returns_none():
    """A missing endpoint (older app) degrades to None → caller grants all."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "not found"})

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.select_ondemand_admissions(_admission_request())) is None
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_timeout_returns_none():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.select_ondemand_admissions(_admission_request())) is None
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_bad_body_returns_none():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.select_ondemand_admissions(_admission_request())) is None
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# cancel_reservation — no-show / controller-revoked
# ---------------------------------------------------------------------------


def test_cancel_reservation_200_returns_true():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/reservations/42/cancel"
        assert json.loads(request.content) == {"reason": "no-show"}
        return httpx.Response(200, json={"status": "cancelled"})

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.cancel_reservation(42, "no-show")) is True
    asyncio.run(client.aclose())


def test_cancel_reservation_already_gone_404_treated_as_success():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.cancel_reservation(42, "controller-revoked")) is True
    asyncio.run(client.aclose())


def test_cancel_reservation_error_returns_false():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.cancel_reservation(42, "no-show")) is False
    asyncio.run(client.aclose())
