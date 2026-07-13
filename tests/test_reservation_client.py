"""Tests for the reservation HTTP client boundary.

Uses httpx.MockTransport (built in — no pytest-httpx dependency) to drive the
async client without a live server.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from app.config import Config
from app.reservation_client import ReservationClient


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
