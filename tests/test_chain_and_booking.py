"""Unit tests for the chain/claim helpers and booking-reference parsing.

Covers parse_booking_reference (k8s_client) and the pure chain helpers
ControllerState._chain_for / reservations_claimed_by (controller).

No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from app.controller import ControllerState, slot_end, slot_start
from app.k8s_client import parse_booking_reference
from app.schemas import GpuClassBrief, PolicyBrief, ReservationResponse, UserBrief


GPU_CLASS_ID = 10
GPU_CLASS_LABEL = "h100"
OTHER_CLASS_ID = 20
FIXED_DATE = date(2024, 1, 15)
USERNAME = "alice"
# 1 h into res #1's 08:00–10:00 window — used as the chain anchor "now".
NOW = datetime(2024, 1, 15, 9, 0)


def _user_reservation(
    res_id: int,
    *,
    username: str = USERNAME,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_count: int = 2,
    slot_index: int = 0,
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
) -> ReservationResponse:
    return ReservationResponse(
        id=res_id,
        user_id=1,
        user=UserBrief(id=1, username=username),
        group_id=None,
        group=None,
        gpu_class_id=gpu_class_id,
        gpu_class=GpuClassBrief(id=gpu_class_id, name="H100"),
        policy_id=1,
        slot_index=slot_index,
        policy=PolicyBrief(
            id=1,
            name="Test policy",
            start_time=start_time,
            duration_minutes=duration_minutes,
            repeat_count=1,
        ),
        date=reservation_date,
        gpu_count=gpu_count,
        status="active",
        kind="user",
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 1, 1),
    )


def _state(*reservations: ReservationResponse) -> ControllerState:
    state = ControllerState()
    state.reservations = list(reservations)
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


# ---------------------------------------------------------------------------
# parse_booking_reference
# ---------------------------------------------------------------------------


class TestParseBookingReference:
    @pytest.mark.parametrize(
        "reference,expected",
        [
            ("res-42", 42),
            ("ondemand-42", 42),
            ("noshow-42", 42),
            ("res-0", 0),
            ("ondemand-1000000", 1000000),
        ],
    )
    def test_valid_prefixes(self, reference, expected):
        assert parse_booking_reference(reference) == expected

    @pytest.mark.parametrize(
        "reference",
        [None, "", "42", "res-", "res-abc", "unknown-42", "res_42", "noshow-12x"],
    )
    def test_unparseable_returns_none(self, reference):
        assert parse_booking_reference(reference) is None


# ---------------------------------------------------------------------------
# _chain_for
# ---------------------------------------------------------------------------


class TestChainFor:
    def test_no_chain_returns_empty(self):
        r1 = _user_reservation(1)
        state = _state(r1)
        assert state._chain_for(r1, NOW) == []

    def test_single_backtoback(self):
        r1 = _user_reservation(1, slot_index=0)            # 08:00–10:00
        r2 = _user_reservation(2, slot_index=1)            # 10:00–12:00
        state = _state(r1, r2)
        assert [r.id for r in state._chain_for(r1, NOW)] == [2]

    def test_multi_link_in_window_order(self):
        r1 = _user_reservation(1, slot_index=0)            # 08:00–10:00
        r2 = _user_reservation(2, slot_index=1)            # 10:00–12:00
        r3 = _user_reservation(3, slot_index=2)            # 12:00–14:00
        state = _state(r3, r1, r2)  # unsorted input
        assert [r.id for r in state._chain_for(r1, NOW)] == [2, 3]

    def test_gap_breaks_chain(self):
        r1 = _user_reservation(1, start_time="08:00:00", duration_minutes=120)
        r2 = _user_reservation(2, start_time="10:05:00", duration_minutes=120)
        state = _state(r1, r2)
        assert state._chain_for(r1, NOW) == []

    def test_gpu_count_mismatch_breaks_chain(self):
        r1 = _user_reservation(1, slot_index=0, gpu_count=2)
        r2 = _user_reservation(2, slot_index=1, gpu_count=1)
        state = _state(r1, r2)
        assert state._chain_for(r1, NOW) == []

    def test_username_mismatch_breaks_chain(self):
        r1 = _user_reservation(1, slot_index=0, username="alice")
        r2 = _user_reservation(2, slot_index=1, username="bob")
        state = _state(r1, r2)
        assert state._chain_for(r1, NOW) == []

    def test_gpu_class_mismatch_breaks_chain(self):
        r1 = _user_reservation(1, slot_index=0, gpu_class_id=GPU_CLASS_ID)
        r2 = _user_reservation(2, slot_index=1, gpu_class_id=OTHER_CLASS_ID)
        state = _state(r1, r2)
        state.gpu_class_labels[OTHER_CLASS_ID] = "a100"
        assert state._chain_for(r1, NOW) == []

    def test_noshow_link_excluded(self):
        r1 = _user_reservation(1, slot_index=0)
        r2 = _user_reservation(2, slot_index=1)
        state = _state(r1, r2)
        state.noshow_reservation_ids.add(2)
        assert state._chain_for(r1, NOW) == []

    def test_user_none_returns_empty(self):
        r1 = _user_reservation(1)
        no_user = r1.model_copy(update={"user": None, "user_id": None})
        state = _state(no_user)
        assert state._chain_for(no_user, NOW) == []


# ---------------------------------------------------------------------------
# reservations_claimed_by
# ---------------------------------------------------------------------------


class TestReservationsClaimedBy:
    def test_includes_self_with_no_chain(self):
        r1 = _user_reservation(1)
        state = _state(r1)
        assert state.reservations_claimed_by(1, NOW) == {1}

    def test_includes_full_chain(self):
        r1 = _user_reservation(1, slot_index=0)
        r2 = _user_reservation(2, slot_index=1)
        r3 = _user_reservation(3, slot_index=2)
        state = _state(r1, r2, r3)
        assert state.reservations_claimed_by(1, NOW) == {1, 2, 3}

    def test_unknown_id_returns_self_only(self):
        state = _state()
        assert state.reservations_claimed_by(99, NOW) == {99}

    def test_ondemand_reservation_claims_only_self(self):
        r1 = _user_reservation(1).model_copy(
            update={"kind": "ondemand", "user": None, "user_id": None}
        )
        state = _state(r1)
        assert state.reservations_claimed_by(1, NOW) == {1}

    def test_defaults_now_to_wall_clock(self):
        # Far-future reservation so the default now() still sees it as active.
        future = date.today() + timedelta(days=3650)
        r1 = _user_reservation(1, slot_index=0, reservation_date=future)
        r2 = _user_reservation(2, slot_index=1, reservation_date=future)
        state = _state(r1, r2)
        assert state.reservations_claimed_by(1) == {1, 2}
