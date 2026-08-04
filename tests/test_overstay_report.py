"""Tests for controller-side overstay reporting (analysis-only, ships dark).

When a controller-admitted pod's life ends (deleted / terminated / preempted),
``main._report_overstay_if_any`` best-effort posts the overstay window to the
app's ``POST /api/reservations/{id}/overstay`` endpoint — but only for a genuine
overstay (the pod ran past its runtime guarantee) and only when
``OVERSTAY_REPORT_ENABLED`` is on. The overstay start is the live ``guarantee_end``
when the reservation is still resolvable, else the pod's frozen
``horae/guaranteed-until`` annotation.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import unittest.mock as mock

os.environ.setdefault("RESERVATION_API_URL", "http://reservations.local")
os.environ.setdefault("RESERVATION_API_KEY", "gpures_test")

import app.main as main_module
from app.config import Config
from app.controller import ControllerState
from app.k8s_client import make_booking_reference

from tests.conftest import make_config, GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME, reservation


NOW = datetime(2026, 7, 19, 17, 30, 0, tzinfo=timezone.utc)


def _config(**overrides) -> Config:
    """Shared builder (tests/conftest.make_config) with this module's default."""
    return make_config(**{"overstay_report_enabled": True} | overrides)


class _FakeClient:
    def __init__(self):
        self.overstay_calls: list = []
        self.cancel_calls: list = []

    async def report_overstay(self, reservation_id, req):
        self.overstay_calls.append((reservation_id, req))
        return True

    async def cancel_reservation(self, reservation_id, reason):
        self.cancel_calls.append((reservation_id, reason))
        return True


def _pod(
    uid: str = "uid-1",
    *,
    reservation_id: int | None,
    gpu_count: int = 2,
    guaranteed_until: datetime | None = None,
    phase: str = "Running",
) -> SimpleNamespace:
    annotations: dict = {}
    if reservation_id is not None:
        annotations["horae/booking-reference"] = make_booking_reference(reservation_id)
    if guaranteed_until is not None:
        annotations["horae/guaranteed-until"] = guaranteed_until.strftime("%Y-%m-%dT%H:%M:%SZ")
    container = SimpleNamespace(
        resources=SimpleNamespace(requests={"nvidia.com/gpu": gpu_count}, limits=None)
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid,
            name="pod-" + uid,
            namespace=USERNAME,
            labels={"gpu-class": GPU_CLASS_LABEL},
            annotations=annotations or None,
        ),
        status=SimpleNamespace(phase=phase, conditions=None),
        spec=SimpleNamespace(
            tolerations=[], containers=[container], scheduling_gates=None, node_name="node-a"
        ),
    )


def _state_with(res_id: int, *, start_utc: datetime, end_utc: datetime, in_list: bool = True) -> ControllerState:
    state = ControllerState()
    if in_list:
        state.reservations = [
            reservation(
                res_id, start_utc=start_utc, end_utc=end_utc,
                kind="on_demand", gpu_class_id=GPU_CLASS_ID,
                gpu_class_label=GPU_CLASS_LABEL, gpu_count=2, username=USERNAME,
            )
        ]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


# ---------------------------------------------------------------------------
# Core helper: _report_overstay_if_any
# ---------------------------------------------------------------------------

def test_reports_when_past_guarantee_via_guarantee_end():
    # Window ended 30 min before now, reservation still in the list → guarantee_end
    # resolves to a past instant → a real overstay.
    ge = NOW - timedelta(minutes=30)
    state = _state_with(500, start_utc=ge - timedelta(hours=1), end_utc=ge)
    client = _FakeClient()
    pod = _pod(reservation_id=500, gpu_count=2)

    asyncio.run(main_module._report_overstay_if_any(state, client, _config(), pod, NOW, "deleted"))

    assert len(client.overstay_calls) == 1
    res_id, req = client.overstay_calls[0]
    assert res_id == 500
    assert req.pod_uid == "uid-1"
    assert req.gpu_count == 2
    assert req.start_utc == ge
    assert req.end_utc == NOW
    assert req.end_reason == "deleted"


def test_no_report_when_within_guarantee():
    # Window still open (ends in the future) → not an overstay.
    state = _state_with(500, start_utc=NOW - timedelta(minutes=5), end_utc=NOW + timedelta(minutes=30))
    client = _FakeClient()
    pod = _pod(reservation_id=500)

    asyncio.run(main_module._report_overstay_if_any(state, client, _config(), pod, NOW, "deleted"))

    assert client.overstay_calls == []


def test_falls_back_to_annotation_when_reservation_gone():
    # Reservation no longer in the list → guarantee_end is None → use the pod's
    # frozen horae/guaranteed-until annotation as the overstay start.
    ge = NOW - timedelta(minutes=45)
    state = _state_with(500, start_utc=ge, end_utc=ge, in_list=False)
    client = _FakeClient()
    pod = _pod(reservation_id=500, guaranteed_until=ge)

    asyncio.run(main_module._report_overstay_if_any(state, client, _config(), pod, NOW, "deleted"))

    assert len(client.overstay_calls) == 1
    _, req = client.overstay_calls[0]
    assert req.start_utc == ge


def test_no_report_when_disabled():
    ge = NOW - timedelta(minutes=30)
    state = _state_with(500, start_utc=ge - timedelta(hours=1), end_utc=ge)
    client = _FakeClient()
    pod = _pod(reservation_id=500)

    asyncio.run(
        main_module._report_overstay_if_any(
            state, client, _config(overstay_report_enabled=False), pod, NOW, "deleted"
        )
    )
    assert client.overstay_calls == []


def test_no_report_without_booking_reference():
    state = _state_with(500, start_utc=NOW - timedelta(hours=2), end_utc=NOW - timedelta(hours=1))
    client = _FakeClient()
    pod = _pod(reservation_id=None)

    asyncio.run(main_module._report_overstay_if_any(state, client, _config(), pod, NOW, "deleted"))
    assert client.overstay_calls == []


def test_no_report_when_unresolvable():
    # Reservation gone AND no annotation → start cannot be resolved → skip.
    state = _state_with(500, start_utc=NOW, end_utc=NOW, in_list=False)
    client = _FakeClient()
    pod = _pod(reservation_id=500, guaranteed_until=None)

    asyncio.run(main_module._report_overstay_if_any(state, client, _config(), pod, NOW, "deleted"))
    assert client.overstay_calls == []


# ---------------------------------------------------------------------------
# Preemption path: _preempt_pod files a "preempted" report before deleting
# ---------------------------------------------------------------------------

def test_preempt_pod_reports_preempted():
    ge = NOW - timedelta(minutes=20)
    state = _state_with(500, start_utc=ge - timedelta(hours=1), end_utc=ge)
    state.record_placement(500, "uid-1", 2)
    client = _FakeClient()
    pod = _pod(reservation_id=500, gpu_count=2)

    async def _drive():
        with mock.patch.object(main_module, "read_pod", return_value=pod), \
             mock.patch.object(main_module, "emit_preempted_event", return_value=None), \
             mock.patch.object(main_module, "delete_pod", return_value=None):
            await main_module._preempt_pod(
                state, USERNAME, "pod-uid-1", "uid-1", "msg",
                client=client, config=_config(), now=NOW,
            )

    asyncio.run(_drive())

    assert len(client.overstay_calls) == 1
    res_id, req = client.overstay_calls[0]
    assert res_id == 500
    assert req.end_reason == "preempted"
    assert req.start_utc == ge


class _FakeWatcher:
    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


def _run_watch(state, client, events):
    def _fake_watcher(**kw):
        return _FakeWatcher(events)

    with mock.patch.object(main_module, "PodWatcher", _fake_watcher):
        asyncio.run(main_module.pod_watch_loop(state, client, _config()))


def test_watch_deleted_overstay_pod_reports():
    ge = datetime.now(timezone.utc) - timedelta(minutes=30)
    state = _state_with(500, start_utc=ge - timedelta(hours=1), end_utc=ge)
    state.record_placement(500, "uid-1", 2)
    client = _FakeClient()

    _run_watch(state, client, [("DELETED", _pod(reservation_id=500, gpu_count=2))])

    assert len(client.overstay_calls) == 1
    res_id, req = client.overstay_calls[0]
    assert res_id == 500
    assert req.end_reason == "deleted"
    # The lease is still torn down as before.
    assert client.cancel_calls == [(500, "pod-terminated")]


def test_preempt_pod_no_client_no_report():
    # Backward-compatible: called without client/config (e.g. older callers/tests)
    # must not attempt a report.
    ge = NOW - timedelta(minutes=20)
    state = _state_with(500, start_utc=ge - timedelta(hours=1), end_utc=ge)
    pod = _pod(reservation_id=500)

    async def _drive():
        with mock.patch.object(main_module, "read_pod", return_value=pod), \
             mock.patch.object(main_module, "emit_preempted_event", return_value=None), \
             mock.patch.object(main_module, "delete_pod", return_value=None):
            await main_module._preempt_pod(state, USERNAME, "pod-uid-1", "uid-1", "msg")

    asyncio.run(_drive())  # must not raise
