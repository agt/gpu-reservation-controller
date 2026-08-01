"""Tests for the reservation HTTP client boundary.

Uses httpx.MockTransport (built in — no pytest-httpx dependency) to drive the
async client without a live server.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx

from app import reservation_client
from app.config import Config
from app.reservation_client import LeaseOutcome, ReservationClient
from app.schemas import (
    OnDemandAdmissionCandidate,
    OnDemandAdmissionRequest,
    OnDemandReservationRequest,
    OverstayReportRequest,
)

from tests.conftest import reservation as _reservation


def _config(**overrides) -> Config:
    base = dict(
        reservation_api_url="http://reservations.local",
        reservation_api_key="gpures_test",
        reservation_fetch_interval=300,
        reservation_lookahead_days=7,
        kubeconfig_path=None,
        http_port=8000,
        ondemand_lease_enabled=True,
        noshow_timeout_minutes=15,
        noshow_grace_minutes=30,
        queue_processor_interval=300,
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
# fetch_reservations — bounded transient retry per page
#
# Without it, one blip mid-pagination aborts the whole refresh cycle and leaves
# the controller on stale reservation state for a full RESERVATION_FETCH_INTERVAL.
# ---------------------------------------------------------------------------


def _no_backoff(monkeypatch) -> None:
    """Keep the retry path instant so these tests don't sleep for real."""
    monkeypatch.setattr(reservation_client, "_FETCH_RETRY_BACKOFF_SECONDS", (0, 0))


def test_fetch_reservations_retries_a_transient_page_failure(monkeypatch):
    _no_backoff(monkeypatch)
    now = datetime.now(timezone.utc)
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ConnectTimeout("blip", request=request)
        return httpx.Response(
            200,
            json=[_reservation(1, start_utc=now, end_utc=now + timedelta(hours=1))
                  .model_dump(mode="json")],
        )

    client = _client_with_handler(_config(), handler)
    results = asyncio.run(client.fetch_reservations())
    assert len(calls) == 2
    assert [r.id for r in results] == [1]
    asyncio.run(client.aclose())


def test_fetch_reservations_retries_a_5xx_page(monkeypatch):
    _no_backoff(monkeypatch)
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(503, json={"detail": "unavailable"})
        return httpx.Response(200, json=[])

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.fetch_reservations()) == []
    assert len(calls) == 2
    asyncio.run(client.aclose())


def test_fetch_reservations_retry_preserves_earlier_pages(monkeypatch):
    """The headline case: a blip on page 2 must not lose page 1.

    Page 1 returns a full ``limit`` (200) so pagination continues; page 2 fails
    once, is retried, and the accumulated results span both pages.
    """
    _no_backoff(monkeypatch)
    now = datetime.now(timezone.utc)
    page1 = [
        _reservation(i, start_utc=now, end_utc=now + timedelta(hours=1)).model_dump(mode="json")
        for i in range(1, 201)
    ]
    page2 = [
        _reservation(201, start_utc=now, end_utc=now + timedelta(hours=1)).model_dump(mode="json")
    ]
    seen_offsets: list[str] = []
    failed_once = {"done": False}

    def handler(request: httpx.Request) -> httpx.Response:
        offset = request.url.params["offset"]
        seen_offsets.append(offset)
        if offset == "0":
            return httpx.Response(200, json=page1)
        if not failed_once["done"]:
            failed_once["done"] = True
            raise httpx.ReadTimeout("blip on page 2", request=request)
        return httpx.Response(200, json=page2)

    client = _client_with_handler(_config(), handler)
    results = asyncio.run(client.fetch_reservations())
    assert [r.id for r in results] == list(range(1, 202))
    # Page 2 was re-requested at the same offset — the retry resumes, it does
    # not restart pagination from zero.
    assert seen_offsets == ["0", "200", "200"]
    asyncio.run(client.aclose())


def test_fetch_reservations_does_not_retry_a_4xx(monkeypatch):
    """A bad key or bad filter will not fix itself — fail fast, don't retry."""
    _no_backoff(monkeypatch)
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(403, json={"detail": "forbidden"})

    client = _client_with_handler(_config(), handler)
    try:
        asyncio.run(client.fetch_reservations())
        raise AssertionError("expected HTTPStatusError")
    except httpx.HTTPStatusError as exc:
        assert exc.response.status_code == 403
    assert len(calls) == 1
    asyncio.run(client.aclose())


def test_fetch_reservations_raises_after_exhausting_attempts(monkeypatch):
    """The fail-safe is intact: a persistent failure still aborts the cycle.

    Acting on a partial page set would wholesale-replace state.reservations with
    an under-count, so raising remains the correct outcome.
    """
    _no_backoff(monkeypatch)
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        raise httpx.ConnectError("down", request=request)

    client = _client_with_handler(_config(), handler)
    try:
        asyncio.run(client.fetch_reservations())
        raise AssertionError("expected RequestError")
    except httpx.RequestError:
        pass
    assert len(calls) == reservation_client._FETCH_MAX_ATTEMPTS == 3
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
                # First class carries total_gpus; second omits it entirely.
                {"id": 10, "name": "H100", "label_value": "h100", "total_gpus": 8},
                {"id": 20, "name": "A100", "label_value": "a100"},
            ],
        )

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.fetch_gpu_classes())
    assert result is not None
    assert [(c.id, c.label_value) for c in result] == [(10, "h100"), (20, "a100")]
    # total_gpus is parsed when present, and defaults to None when absent.
    assert result[0].total_gpus == 8
    assert result[1].total_gpus is None
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
    assert result.granted and result.lease is not None and result.lease.id == 7
    assert result.transient is False
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_409_is_denied_not_transient():
    """A capacity/policy denial is a real answer: no lease, and not transient."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "no capacity"})

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert result.outcome is LeaseOutcome.DENIED
    assert result.granted is False and result.lease is None
    # The distinction that matters at the call site: a denial keeps the slow
    # jittered cooldown rather than the 30 s short retry.
    assert result.transient is False
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_timeout_is_transient():
    """An unreachable app is transient — the caller retries sooner than a denial."""
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert result.outcome is LeaseOutcome.UNAVAILABLE
    assert result.granted is False and result.lease is None
    assert result.transient is True
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_5xx_is_transient():
    """A 5xx is the app failing, not the app deciding."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "unavailable"})

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert result.outcome is LeaseOutcome.UNAVAILABLE
    assert result.transient is True
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_malformed_body_is_not_transient():
    """A 2xx we cannot parse is likely deterministic — keep the slow cooldown."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, content=b"not json")

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert result.outcome is LeaseOutcome.MALFORMED
    assert result.granted is False and result.transient is False
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


# ---------------------------------------------------------------------------
# report_overstay — analysis-only, best-effort
# ---------------------------------------------------------------------------


def _overstay_req() -> OverstayReportRequest:
    start = datetime(2026, 7, 19, 17, 0, 0, tzinfo=timezone.utc)
    return OverstayReportRequest(
        pod_uid="pod-abc",
        gpu_count=1,
        start_utc=start,
        end_utc=start + timedelta(minutes=30),
        end_reason="preempted",
    )


def test_report_overstay_posts_to_reservation_path():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": 1})

    client = _client_with_handler(_config(), handler)
    ok = asyncio.run(client.report_overstay(4412, _overstay_req()))
    assert ok is True
    assert seen["path"] == "/api/reservations/4412/overstay"
    assert seen["body"]["pod_uid"] == "pod-abc"
    assert seen["body"]["end_reason"] == "preempted"
    asyncio.run(client.aclose())


def test_report_overstay_swallows_errors():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = _client_with_handler(_config(), handler)
    # Best-effort: a non-2xx returns False and never raises.
    assert asyncio.run(client.report_overstay(42, _overstay_req())) is False
    asyncio.run(client.aclose())
