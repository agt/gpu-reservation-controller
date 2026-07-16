"""Adopt-before-evict on in-window cancellation (the Continue counterpart).

When the reservation app supersedes a still-running job's reservation via
``POST /api/reservations/{id}/continue``, it pushes the ``superseded``
cancellation together with the replacement booking.  The cancellation handler
must re-link the admitted pod onto that replacement (``POD_ADOPTION_ENABLED``)
instead of evicting it — eviction is only for pods with nowhere to go.

Drives ``main._handle_cancelled_reservations`` directly with monkeypatched
k8s seams (mirroring ``tests/test_adoption.py``'s stub shape).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.config import Config
from app.controller import ControllerState
from app.k8s_client import ToleratedPodInfo

from tests.conftest import GPU_CLASS_LABEL, USERNAME, reservation

NOW = datetime(2024, 1, 15, 15, 15, tzinfo=timezone.utc)


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


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-cancel-adopt")
    import app.main as main_module

    return main_module


def _tolerated(uid: str, res_id: int, *, gpu_count: int = 1) -> ToleratedPodInfo:
    return ToleratedPodInfo(
        namespace=USERNAME, name=f"pod-{uid}", uid=uid, gpu_class=GPU_CLASS_LABEL,
        booking_reference=f"res-{res_id}", reservation_id=res_id,
        gpu_count=gpu_count, phase="Running", scheduled_false=False,
    )


def _cancelled_source(res_id: int) -> "reservation":
    """An in-window reservation the app just cancelled with reason=superseded."""
    return reservation(
        res_id,
        start_utc=NOW - timedelta(hours=1),
        end_utc=NOW + timedelta(hours=1),
        gpu_class_label=GPU_CLASS_LABEL,
        status="cancelled",
        cancelled_by_id=1,
        cancel_reason="superseded",
        kind="on_demand",
        with_user=True,
    )


def _replacement_booking(res_id: int, *, gpu_count: int = 1) -> "reservation":
    """The freshly-minted continue booking, open right now."""
    return reservation(
        res_id,
        start_utc=NOW - timedelta(minutes=1),
        end_utc=NOW + timedelta(hours=2),
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_count=gpu_count,
        kind="booking",
    )


def _install_stubs(monkeypatch, m, snapshot):
    """Stub every k8s seam the handler + adoption path touch; record calls."""
    deleted: list[tuple[str, str]] = []
    cancelled_events: list[tuple[str, str, str]] = []
    patched_refs: list[tuple[str, str]] = []  # (pod name, booking_reference)

    async def _snapshot(_key):
        return list(snapshot)

    async def _delete(name, ns):
        deleted.append((name, ns))

    async def _read(name, ns):
        return SimpleNamespace(status=SimpleNamespace(phase="Running", conditions=None))

    async def _emit_cancelled(pod, name, ns, desc):
        cancelled_events.append((name, ns, desc))

    async def _apply_toleration(name, ns, pod, key, value, booking_reference):
        patched_refs.append((name, booking_reference))

    async def _record_guarantee(name, ns, pod, guaranteed_until, now):
        return None

    async def _emit_relinked(pod, name, ns, res_id, guaranteed_until):
        return None

    monkeypatch.setattr(m, "snapshot_tolerated_pods", _snapshot)
    monkeypatch.setattr(m, "delete_pod", _delete)
    monkeypatch.setattr(m, "read_pod", _read)
    monkeypatch.setattr(m, "emit_reservation_cancelled_event", _emit_cancelled)
    monkeypatch.setattr(m, "apply_toleration", _apply_toleration)
    monkeypatch.setattr(m, "_record_guarantee", _record_guarantee)
    monkeypatch.setattr(m, "emit_overstay_relinked_event", _emit_relinked)
    return deleted, cancelled_events, patched_refs


class TestAdoptBeforeEvict:
    def test_superseded_pod_relinks_to_replacement_booking(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("uid-1", 1)
        deleted, cancelled_events, patched_refs = _install_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        state.gpu_class_labels = {10: GPU_CLASS_LABEL}
        # The caller replaces state.reservations before handling cancellations,
        # so the replacement booking is already the active set.
        state.reservations = [_replacement_booking(2)]
        state.record_placement(1, "uid-1", 1)

        asyncio.run(
            m._handle_cancelled_reservations(state, _config(), [_cancelled_source(1)], NOW)
        )

        assert patched_refs == [("pod-uid-1", "res-2")]  # re-linked, not evicted
        assert deleted == []
        assert cancelled_events == []
        assert state.occupancy.get(2, {}).get("uid-1") == 1  # occupancy re-homed
        assert state.occupancy.get(1, {}) == {}

    def test_evicts_when_adoption_disabled(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("uid-1", 1)
        deleted, cancelled_events, patched_refs = _install_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        state.reservations = [_replacement_booking(2)]
        state.record_placement(1, "uid-1", 1)

        asyncio.run(
            m._handle_cancelled_reservations(
                state, _config(pod_adoption_enabled=False), [_cancelled_source(1)], NOW
            )
        )

        assert patched_refs == []
        assert deleted == [("pod-uid-1", USERNAME)]
        assert len(cancelled_events) == 1

    def test_evicts_when_no_replacement_booking(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("uid-1", 1)
        deleted, cancelled_events, patched_refs = _install_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        state.reservations = []  # nothing to adopt into
        state.record_placement(1, "uid-1", 1)

        asyncio.run(
            m._handle_cancelled_reservations(state, _config(), [_cancelled_source(1)], NOW)
        )

        assert patched_refs == []
        assert deleted == [("pod-uid-1", USERNAME)]
        assert len(cancelled_events) == 1

    def test_replacement_without_budget_still_evicts(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("uid-1", 1, gpu_count=2)
        deleted, cancelled_events, patched_refs = _install_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        # Replacement has only 1 GPU of budget; the pod needs 2 → no adoption.
        state.reservations = [_replacement_booking(2, gpu_count=1)]
        state.record_placement(1, "uid-1", 2)

        asyncio.run(
            m._handle_cancelled_reservations(state, _config(), [_cancelled_source(1)], NOW)
        )

        assert patched_refs == []
        assert deleted == [("pod-uid-1", USERNAME)]
        assert len(cancelled_events) == 1
