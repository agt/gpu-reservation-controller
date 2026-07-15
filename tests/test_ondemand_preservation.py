"""Unit tests for ``ControllerState.preserve_local_ondemand_leases``.

A freshly-granted on-demand lease is upserted into ``state.reservations`` and its
pod admitted the same instant, but a periodic fetch takes its
``GET /api/reservations`` snapshot *before* the lock and then wholesale-replaces
``state.reservations``.  If the lease was granted in that gap the snapshot omits
it, and the replace would drop a still-live lease — leaving its pod's
``guarantee_end`` unresolvable (``None``) and thus wrongly eligible for boundary
preemption.  ``preserve_local_ondemand_leases`` bridges that window; these tests
pin its inclusion/exclusion rules and the resulting guarantee protection.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.schemas import ReservationResponse

from tests.conftest import make_state as _state, reservation

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)


def _lease(
    res_id: int,
    *,
    status: str = "active",
    kind: str = "on_demand",
    start_offset_min: int = -10,
    end_offset_min: int = 50,
    gpu_count: int = 1,
) -> ReservationResponse:
    """A lease/booking whose window straddles NOW by default (live)."""
    return reservation(
        res_id,
        start_utc=NOW + timedelta(minutes=start_offset_min),
        end_utc=NOW + timedelta(minutes=end_offset_min),
        kind=kind,
        status=status,
        gpu_count=gpu_count,
    )


def test_live_lease_absent_from_snapshot_is_preserved():
    """A live on-demand lease the fetch snapshot omits is returned for re-add."""
    lease = _lease(101)
    state = _state(lease)
    # Snapshot from before the grant: it does not mention the lease at all.
    preserved = state.preserve_local_ondemand_leases([], NOW)
    assert [r.id for r in preserved] == [101]


def test_lease_present_in_snapshot_is_not_preserved():
    """When the fetch already reports the lease, there is nothing to bridge."""
    lease = _lease(101)
    state = _state(lease)
    preserved = state.preserve_local_ondemand_leases([lease], NOW)
    assert preserved == []


def test_cancelled_in_snapshot_is_not_preserved():
    """A lease the app reports cancelled carries its id in the full snapshot, so
    the genuine cancellation (incl. the controller's compensating cancel) wins."""
    lease = _lease(101)
    state = _state(lease)
    cancelled = _lease(101, status="cancelled")
    # `fetched` is the full status=all response; the active subset excludes it,
    # but its presence here must still suppress preservation.
    preserved = state.preserve_local_ondemand_leases([cancelled], NOW)
    assert preserved == []


def test_ended_lease_is_not_preserved():
    """An on-demand lease whose window has closed is legitimately past guarantee."""
    ended = _lease(101, start_offset_min=-120, end_offset_min=-10)
    state = _state(ended)
    preserved = state.preserve_local_ondemand_leases([], NOW)
    assert preserved == []


def test_booking_is_not_preserved():
    """Only our own on-demand leases are bridged; a booking is the app's to own."""
    booking = _lease(101, kind="booking")
    state = _state(booking)
    preserved = state.preserve_local_ondemand_leases([], NOW)
    assert preserved == []


def test_locally_cancelled_lease_not_preserved():
    """A lease already dropped to status=cancelled locally is not resurrected."""
    lease = _lease(101, status="cancelled")
    state = _state(lease)
    preserved = state.preserve_local_ondemand_leases([], NOW)
    assert preserved == []


def test_preserved_lease_keeps_pod_guarantee_live():
    """End-to-end: after re-adding the preserved lease, the pod admitted under it
    resolves to a live (future) guarantee instead of None → not preemptable."""
    lease = _lease(202)
    state = _state(lease)

    # Simulate the wholesale fetch replace that omits the just-granted lease,
    # augmented with what the fix re-adds.
    fetched_active: list = []
    preserved = state.preserve_local_ondemand_leases([], NOW)
    state.reservations = fetched_active + preserved

    end = state.guarantee_end(202, now=NOW)
    assert end is not None
    assert end > NOW

    # And the contrast: without the preservation the lease is gone and the pod
    # would resolve to None (the wrongful-preemption path this fix closes).
    state.reservations = fetched_active
    assert state.guarantee_end(202, now=NOW) is None
