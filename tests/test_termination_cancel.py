"""Cancelling a reservation once its workload terminates.

Two layers:

- ``ControllerState.classify_vacated_reservation`` — the pure decision: cancel a
  JIT lease whose only pod is gone (even mid-window), or an overstay booking
  whose last pod has gone; keep a live booking or one that still has a pod.
- ``main.pod_watch_loop`` DELETED / terminal-phase handling — the wiring that
  releases occupancy and then issues the ``controller-revoked`` cancel.

No Kubernetes or HTTP I/O; a fake reservation client records cancel calls.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

# ``app.main`` runs ``create_app()`` (→ ``Config.from_env``) at import time, so
# the required env vars must exist before it is imported anywhere below.
os.environ.setdefault("RESERVATION_API_URL", "http://localhost:9999")
os.environ.setdefault("RESERVATION_API_KEY", "test-key-termination")

from app.config import Config
from app.controller import ControllerState
from app.schemas import ReservationResponse

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    USERNAME,
    reservation,
)


# ---------------------------------------------------------------------------
# classify_vacated_reservation (pure)
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _open_lease(res_id: int, *, gpu_count: int = 1) -> ReservationResponse:
    now = _now()
    return reservation(
        res_id,
        start_utc=now - timedelta(minutes=1),
        end_utc=now + timedelta(hours=1),  # still open
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
    )


def _ended_booking(res_id: int, *, gpu_count: int = 1) -> ReservationResponse:
    now = _now()
    return reservation(
        res_id,
        start_utc=now - timedelta(hours=2),
        end_utc=now - timedelta(minutes=5),  # window already closed → overstay
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
    )


class TestClassifyVacatedReservation:
    def test_jit_lease_with_no_pods_is_cancelled_even_mid_window(self):
        lease = _open_lease(500)
        state = ControllerState()
        state.reservations = [lease]
        state.ondemand_reservation_ids = {500}
        # Pod already released → occupancy empty for this id.
        assert state.classify_vacated_reservation(500, _now()) is not None

    def test_live_booking_with_no_pods_is_kept(self):
        # A regular booking (not a lease), window still open, last pod gone.
        # The user may launch another pod, so it must NOT be cancelled.
        booking = _open_lease(501)  # open window, but not in ondemand set
        state = ControllerState()
        state.reservations = [booking]
        assert state.classify_vacated_reservation(501, _now()) is None

    def test_overstay_booking_with_no_pods_is_cancelled(self):
        booking = _ended_booking(502)
        state = ControllerState()
        state.reservations = [booking]
        assert state.classify_vacated_reservation(502, _now()) is not None

    def test_reservation_with_remaining_pod_is_kept(self):
        booking = _ended_booking(503, gpu_count=4)
        state = ControllerState()
        state.reservations = [booking]
        state.record_placement(503, "sibling-pod", 1)  # another pod still holds it
        assert state.classify_vacated_reservation(503, _now()) is None

    def test_lease_with_remaining_pod_is_kept(self):
        lease = _open_lease(504, gpu_count=2)
        state = ControllerState()
        state.reservations = [lease]
        state.ondemand_reservation_ids = {504}
        state.record_placement(504, "sibling-pod", 1)
        assert state.classify_vacated_reservation(504, _now()) is None

    def test_unknown_reservation_is_kept(self):
        state = ControllerState()
        assert state.classify_vacated_reservation(999, _now()) is None

    def test_none_reservation_id_is_kept(self):
        state = ControllerState()
        assert state.classify_vacated_reservation(None, _now()) is None


class TestPruneOndemandIds:
    def test_prunes_ids_that_left_active_set(self):
        state = ControllerState()
        state.reservations = [_open_lease(1)]
        state.ondemand_reservation_ids = {1, 2, 3}
        state.prune_ondemand_ids()
        assert state.ondemand_reservation_ids == {1}


# ---------------------------------------------------------------------------
# pod_watch_loop wiring
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


class _FakeWatcher:
    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


class _FakeClient:
    def __init__(self, *, cancel_result=True):
        self.cancel_result = cancel_result
        self.cancel_calls: list = []

    async def cancel_reservation(self, reservation_id, reason):
        self.cancel_calls.append((reservation_id, reason))
        return self.cancel_result


def _pod(uid: str, *, phase=None, booking_ref=None) -> SimpleNamespace:
    annotations = {"horae/booking-reference": booking_ref} if booking_ref else None
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-" + uid, namespace=USERNAME,
            labels={"gpu-class": GPU_CLASS_LABEL}, annotations=annotations,
        ),
        status=SimpleNamespace(phase=phase),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


def _state_with_lease(res_id: int) -> ControllerState:
    state = ControllerState()
    state.reservations = [_open_lease(res_id)]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.ondemand_reservation_ids = {res_id}
    state.record_placement(res_id, "uid-jit", 1)
    return state


def test_deleted_jit_pod_cancels_its_lease(monkeypatch):
    import app.main as main_module

    state = _state_with_lease(500)
    client = _FakeClient()
    events = [("DELETED", _pod("uid-jit", booking_ref="res-500"))]
    monkeypatch.setattr(main_module, "PodWatcher", lambda **kw: _FakeWatcher(events))

    asyncio.run(main_module.pod_watch_loop(state, client, _config()))

    assert client.cancel_calls == [(500, "controller-revoked")]
    assert all(r.id != 500 for r in state.reservations)
    assert 500 not in state.ondemand_reservation_ids
    assert "uid-jit" not in state.occupancy.get(500, {})


def test_terminal_jit_pod_cancels_its_lease(monkeypatch):
    import app.main as main_module

    state = _state_with_lease(501)
    client = _FakeClient()
    events = [("MODIFIED", _pod("uid-jit", phase="Succeeded", booking_ref="res-501"))]
    monkeypatch.setattr(main_module, "PodWatcher", lambda **kw: _FakeWatcher(events))

    asyncio.run(main_module.pod_watch_loop(state, client, _config()))

    assert client.cancel_calls == [(501, "controller-revoked")]
    assert all(r.id != 501 for r in state.reservations)


def test_deleted_pod_under_live_booking_is_not_cancelled(monkeypatch):
    import app.main as main_module

    # A regular booking (not a lease), window still open: deleting its pod must
    # not cancel it — the user may launch another pod.
    state = ControllerState()
    state.reservations = [_open_lease(600)]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.record_placement(600, "uid-a", 1)
    client = _FakeClient()
    events = [("DELETED", _pod("uid-a", booking_ref="res-600"))]
    monkeypatch.setattr(main_module, "PodWatcher", lambda **kw: _FakeWatcher(events))

    asyncio.run(main_module.pod_watch_loop(state, client, _config()))

    assert client.cancel_calls == []
    assert any(r.id == 600 for r in state.reservations)


def test_deleted_overstay_booking_pod_cancels_when_last(monkeypatch):
    import app.main as main_module

    state = ControllerState()
    state.reservations = [_ended_booking(700)]  # window closed → overstay
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.record_placement(700, "uid-a", 1)
    client = _FakeClient()
    events = [("DELETED", _pod("uid-a", booking_ref="res-700"))]
    monkeypatch.setattr(main_module, "PodWatcher", lambda **kw: _FakeWatcher(events))

    asyncio.run(main_module.pod_watch_loop(state, client, _config()))

    assert client.cancel_calls == [(700, "controller-revoked")]


def test_feature_disabled_skips_cancel(monkeypatch):
    import app.main as main_module

    state = _state_with_lease(800)
    client = _FakeClient()
    events = [("DELETED", _pod("uid-jit", booking_ref="res-800"))]
    monkeypatch.setattr(main_module, "PodWatcher", lambda **kw: _FakeWatcher(events))

    asyncio.run(
        main_module.pod_watch_loop(
            state, client, _config(cancel_vacated_reservations=False)
        )
    )

    assert client.cancel_calls == []
    # Occupancy is still released even with the cancel feature off.
    assert "uid-jit" not in state.occupancy.get(800, {})
