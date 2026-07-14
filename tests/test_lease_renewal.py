"""Tests for on-demand lease renewal (chaining).

Two layers:

- ``ReservationClient.renew_reservation`` against ``httpx.MockTransport`` —
  mapping HTTP status codes to ``RenewalOutcome`` (mirrors
  ``test_reservation_client.py``).
- ``main._run_renewal_sweep`` — candidate selection and per-outcome handling,
  with ``snapshot_tolerated_pods`` / ``read_pod`` / ``_record_guarantee`` and the
  event emitters monkeypatched at the ``app.main`` module level (mirrors
  ``test_preemption_sweep.py``).  No live cluster or API.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from app.config import Config
from app.k8s_client import ToleratedPodInfo
from app.reservation_client import RenewalOutcome, RenewalResult, ReservationClient

from tests.conftest import GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state
from tests.conftest import reservation


# ---------------------------------------------------------------------------
# Config helper
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Client-boundary tests (httpx.MockTransport)
# ---------------------------------------------------------------------------


def _client_with_handler(config: Config, handler) -> ReservationClient:
    client = ReservationClient(config)
    client._client = httpx.AsyncClient(
        base_url=config.reservation_api_url,
        transport=httpx.MockTransport(handler),
    )
    return client


def _lease_json(res_id: int, end_utc: datetime) -> dict:
    r = reservation(
        res_id,
        start_utc=end_utc - timedelta(hours=1),
        end_utc=end_utc,
        kind="on_demand",
        with_user=True,
        gpu_class_label=GPU_CLASS_LABEL,
    )
    return r.model_dump(mode="json")


class TestRenewClient:
    def test_200_returns_renewed_with_parsed_response(self):
        captured = {}
        end = datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc)

        def handler(request: httpx.Request) -> httpx.Response:
            captured["method"] = request.method
            captured["path"] = request.url.path
            captured["body"] = request.content
            return httpx.Response(200, json=_lease_json(7, end))

        client = _client_with_handler(_config(), handler)
        result = asyncio.run(client.renew_reservation(7))
        asyncio.run(client.aclose())

        assert captured["method"] == "POST"
        assert captured["path"] == "/api/reservations/7/renew"
        assert captured["body"] == b""  # no request body
        assert result.outcome is RenewalOutcome.RENEWED
        assert result.reservation is not None
        assert result.reservation.id == 7
        assert result.reservation.end_utc == end

    def test_409_is_denied(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(409, json={"detail": "no capacity"})

        client = _client_with_handler(_config(), handler)
        result = asyncio.run(client.renew_reservation(7))
        asyncio.run(client.aclose())
        assert result.outcome is RenewalOutcome.DENIED
        assert result.reservation is None

    def test_400_is_not_renewable(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"detail": "already ended"})

        client = _client_with_handler(_config(), handler)
        result = asyncio.run(client.renew_reservation(7))
        asyncio.run(client.aclose())
        assert result.outcome is RenewalOutcome.NOT_RENEWABLE

    def test_404_is_not_renewable(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, json={"detail": "not found"})

        client = _client_with_handler(_config(), handler)
        result = asyncio.run(client.renew_reservation(7))
        asyncio.run(client.aclose())
        assert result.outcome is RenewalOutcome.NOT_RENEWABLE

    def test_500_is_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        client = _client_with_handler(_config(), handler)
        result = asyncio.run(client.renew_reservation(7))
        asyncio.run(client.aclose())
        assert result.outcome is RenewalOutcome.ERROR

    def test_network_error_is_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        client = _client_with_handler(_config(), handler)
        result = asyncio.run(client.renew_reservation(7))
        asyncio.run(client.aclose())
        assert result.outcome is RenewalOutcome.ERROR


# ---------------------------------------------------------------------------
# Sweep tests (main._run_renewal_sweep)
# ---------------------------------------------------------------------------

NOW = datetime(2024, 1, 15, 14, 22, tzinfo=timezone.utc)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-renewal")
    import app.main as main

    return main


def _pod(uid: str, reservation_id: int, *, phase: str = "Running",
         deletion_timestamp=None, namespace: str = USERNAME) -> ToleratedPodInfo:
    return ToleratedPodInfo(
        namespace=namespace,
        name=f"pod-{uid}",
        uid=uid,
        gpu_class=GPU_CLASS_LABEL,
        booking_reference=f"res-{reservation_id}",
        reservation_id=reservation_id,
        gpu_count=1,
        phase=phase,
        scheduled_false=False,
        deletion_timestamp=deletion_timestamp,
    )


def _lease(res_id: int, *, end_offset_minutes: int, gpu_count: int = 1) -> "reservation":
    """An on-demand lease ending *end_offset_minutes* from NOW."""
    end = NOW + timedelta(minutes=end_offset_minutes)
    return reservation(
        res_id,
        start_utc=end - timedelta(hours=1),
        end_utc=end,
        kind="on_demand",
        with_user=True,
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_count=gpu_count,
    )


class _FakeClient:
    """Records renew calls and returns a scripted RenewalResult."""

    def __init__(self, result_for):
        self.calls: list[int] = []
        self._result_for = result_for  # callable rid -> RenewalResult

    async def renew_reservation(self, reservation_id: int) -> RenewalResult:
        self.calls.append(reservation_id)
        return self._result_for(reservation_id)


def _patch(monkeypatch, m, *, pods, snapshot_ok=True):
    recorders = {"guarantees": [], "renewed_events": [], "denied_events": []}

    async def _snapshot_pods(_key, _group_label_key=None):
        if not snapshot_ok:
            raise RuntimeError("apiserver down")
        return pods

    async def _read_pod(name, namespace):
        return object()

    async def _record_guarantee(pod_name, namespace, fresh_pod, guaranteed_until, now):
        recorders["guarantees"].append((namespace, pod_name, guaranteed_until))

    async def _emit_renewed(pod, name, namespace, reservation_id, guaranteed_until):
        recorders["renewed_events"].append((namespace, name, reservation_id, guaranteed_until))

    async def _emit_denied(pod, name, namespace, reservation_id, ends_at):
        recorders["denied_events"].append((namespace, name, reservation_id, ends_at))

    monkeypatch.setattr(m, "snapshot_tolerated_pods", _snapshot_pods)
    monkeypatch.setattr(m, "read_pod", _read_pod)
    monkeypatch.setattr(m, "_record_guarantee", _record_guarantee)
    monkeypatch.setattr(m, "emit_lease_renewed_event", _emit_renewed)
    monkeypatch.setattr(m, "emit_renewal_denied_event", _emit_denied)
    return recorders


def _renewed_result(res_id: int, new_end: datetime) -> RenewalResult:
    lease = reservation(
        res_id,
        start_utc=new_end - timedelta(hours=2),
        end_utc=new_end,
        kind="on_demand",
        with_user=True,
        gpu_class_label=GPU_CLASS_LABEL,
    )
    return RenewalResult(RenewalOutcome.RENEWED, lease)


class TestRenewalSweep:
    def test_near_expiry_lease_with_live_pod_is_renewed(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)  # inside the 15-min lead window
        state = _state(lease)
        new_end = NOW.replace(minute=0, second=0) + timedelta(hours=2)  # 16:00
        client = _FakeClient(lambda rid: _renewed_result(rid, new_end))
        rec = _patch(monkeypatch, m, pods=[_pod("a", 1)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))

        assert client.calls == [1]
        # State reflects the extended end.
        assert next(r for r in state.reservations if r.id == 1).end_utc == new_end
        # Guarantee re-recorded and LeaseRenewed emitted (to the new end).
        assert rec["guarantees"] == [(USERNAME, "pod-a", new_end)]
        assert [e[2] for e in rec["renewed_events"]] == [1]
        assert rec["denied_events"] == []

    def test_lease_outside_lead_window_is_skipped(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=30)  # 30 min > 15-min lead
        state = _state(lease)
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        _patch(monkeypatch, m, pods=[_pod("a", 1)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == []

    def test_already_expired_lease_is_skipped(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=-1)  # already ended
        state = _state(lease)
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        _patch(monkeypatch, m, pods=[_pod("a", 1)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == []

    def test_lease_without_a_live_pod_is_skipped(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        _patch(monkeypatch, m, pods=[])  # no pod occupies the lease

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == []

    def test_terminating_pod_does_not_renew(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        _patch(monkeypatch, m, pods=[_pod("a", 1, deletion_timestamp=NOW)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == []

    def test_booking_reservation_is_never_renewed(self, monkeypatch):
        m = _main_module(monkeypatch)
        booking = reservation(
            1,
            start_utc=NOW - timedelta(hours=1),
            end_utc=NOW + timedelta(minutes=10),
            kind="booking",
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state = _state(booking)
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        _patch(monkeypatch, m, pods=[_pod("a", 1)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == []

    def test_idempotent_noop_200_merges_but_emits_nothing(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        # App returns the lease unchanged (end not advanced).
        client = _FakeClient(lambda rid: _renewed_result(rid, lease.end_utc))
        rec = _patch(monkeypatch, m, pods=[_pod("a", 1)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == [1]
        assert rec["guarantees"] == []
        assert rec["renewed_events"] == []

    def test_denied_marks_lease_and_emits_once(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        client = _FakeClient(lambda rid: RenewalResult(RenewalOutcome.DENIED))
        rec = _patch(monkeypatch, m, pods=[_pod("a", 1)])

        # First sweep: denied → mark + one event.
        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == [1]
        assert 1 in state.renewal_denied_ids
        assert [e[2] for e in rec["denied_events"]] == [1]

        # Second sweep (still near expiry): suppressed — no new call, no new event.
        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW + timedelta(minutes=1)))
        assert client.calls == [1]
        assert [e[2] for e in rec["denied_events"]] == [1]

    def test_error_leaves_lease_unmarked_and_retries(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        client = _FakeClient(lambda rid: RenewalResult(RenewalOutcome.ERROR))
        _patch(monkeypatch, m, pods=[_pod("a", 1)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW + timedelta(minutes=1)))
        # Not suppressed: retried on the second sweep.
        assert client.calls == [1, 1]
        assert 1 not in state.renewal_denied_ids

    def test_not_renewable_does_not_mark_denied(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        client = _FakeClient(lambda rid: RenewalResult(RenewalOutcome.NOT_RENEWABLE))
        rec = _patch(monkeypatch, m, pods=[_pod("a", 1)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == [1]
        assert 1 not in state.renewal_denied_ids
        assert rec["renewed_events"] == []
        assert rec["denied_events"] == []

    def test_stale_denied_mark_pruned_when_lease_leaves_active_set(self, monkeypatch):
        m = _main_module(monkeypatch)
        # No reservations at all; a stale denied id from a prior window.
        state = _state()
        state.renewal_denied_ids = {1, 2}
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        _patch(monkeypatch, m, pods=[])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert state.renewal_denied_ids == set()

    def test_disabled_flag_is_a_noop(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        # snapshot_ok=False would raise if called; disabled sweep must not snapshot.
        _patch(monkeypatch, m, pods=[_pod("a", 1)], snapshot_ok=False)

        asyncio.run(
            m._run_renewal_sweep(state, client, _config(lease_renewal_enabled=False), now=NOW)
        )
        assert client.calls == []

    def test_snapshot_failure_skips_sweep(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        _patch(monkeypatch, m, pods=[_pod("a", 1)], snapshot_ok=False)

        # Must not raise; must not call the client.
        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert client.calls == []

    def test_not_re_renewed_within_the_same_clock_hour(self, monkeypatch):
        # A large lead keeps the freshly-renewed lease inside the window; the
        # clock-hour guard (not the lead) is what prevents a per-sweep re-renew.
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        new_end = datetime(2024, 1, 15, 15, 52, tzinfo=timezone.utc)  # ~90 min out
        client = _FakeClient(lambda rid: _renewed_result(rid, new_end))
        _patch(monkeypatch, m, pods=[_pod("a", 1)])
        cfg = _config(lease_renewal_lead_minutes=200)

        # First sweep (14:22) renews and records clock-hour 14:00.
        asyncio.run(m._run_renewal_sweep(state, client, cfg, now=NOW))
        assert client.calls == [1]
        assert state.renewal_last_hour[1] == NOW.replace(minute=0, second=0, microsecond=0)

        # Second sweep, same clock hour (14:40): suppressed despite the huge lead.
        asyncio.run(m._run_renewal_sweep(
            state, client, cfg, now=datetime(2024, 1, 15, 14, 40, tzinfo=timezone.utc)))
        assert client.calls == [1]

        # Third sweep, next clock hour (15:05): re-attempted.
        asyncio.run(m._run_renewal_sweep(
            state, client, cfg, now=datetime(2024, 1, 15, 15, 5, tzinfo=timezone.utc)))
        assert client.calls == [1, 1]

    def test_idempotent_noop_also_arms_the_hour_guard(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(1, end_offset_minutes=10)
        state = _state(lease)
        # App returns the lease unchanged (a no-op) — the guard must still arm.
        client = _FakeClient(lambda rid: _renewed_result(rid, lease.end_utc))
        _patch(monkeypatch, m, pods=[_pod("a", 1)])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        asyncio.run(m._run_renewal_sweep(
            state, client, _config(), now=NOW + timedelta(minutes=1)))
        assert client.calls == [1]  # second same-hour sweep suppressed

    def test_stale_last_hour_pruned_when_lease_leaves_active_set(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state()  # no reservations
        state.renewal_last_hour = {
            1: NOW.replace(minute=0, second=0, microsecond=0),
            2: NOW.replace(minute=0, second=0, microsecond=0),
        }
        client = _FakeClient(lambda rid: _renewed_result(rid, NOW + timedelta(hours=2)))
        _patch(monkeypatch, m, pods=[])

        asyncio.run(m._run_renewal_sweep(state, client, _config(), now=NOW))
        assert state.renewal_last_hour == {}
