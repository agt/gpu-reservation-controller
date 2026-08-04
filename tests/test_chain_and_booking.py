"""Unit tests for the chain/claim helpers and booking-reference parsing.

Covers parse_booking_reference (k8s_client) and the pure chain helpers
ControllerState._chain_for / reservations_claimed_by (controller).

No Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from app.k8s_client import parse_booking_reference

from tests.conftest import GPU_CLASS_ID, OTHER_CLASS_ID
from tests.conftest import make_state as _state
from tests.conftest import user_reservation as _user_reservation

# 1 h into res #1's 08:00–10:00 UTC window — used as the chain anchor "now".
NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# parse_booking_reference
# ---------------------------------------------------------------------------


class TestParseBookingReference:
    @pytest.mark.parametrize(
        "reference,expected",
        [
            ("res-42", 42),
            ("res-0", 0),
            ("res-1000000", 1000000),
        ],
    )
    def test_valid_prefixes(self, reference, expected):
        assert parse_booking_reference(reference) == expected

    @pytest.mark.parametrize(
        "reference",
        [None, "", "42", "res-", "res-abc", "unknown-42", "res_42", "ondemand-42", "noshow-42"],
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
        r1 = _user_reservation(1, slot_index=0)            # 08:00–10:00 UTC
        r2 = _user_reservation(2, slot_index=1)            # 10:00–12:00 UTC
        state = _state(r1, r2)
        assert [r.id for r in state._chain_for(r1, NOW)] == [2]

    def test_multi_link_in_window_order(self):
        r1 = _user_reservation(1, slot_index=0)            # 08:00–10:00 UTC
        r2 = _user_reservation(2, slot_index=1)            # 10:00–12:00 UTC
        r3 = _user_reservation(3, slot_index=2)            # 12:00–14:00 UTC
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
            update={"kind": "reclaim", "user": None, "user_id": None}
        )
        state = _state(r1)
        assert state.reservations_claimed_by(1, NOW) == {1}

    def test_defaults_now_to_wall_clock(self):
        # Far-future reservation so the default now(utc) still sees it as active.
        future = date.today() + timedelta(days=3650)
        r1 = _user_reservation(1, slot_index=0, reservation_date=future)
        r2 = _user_reservation(2, slot_index=1, reservation_date=future)
        state = _state(r1, r2)
        assert state.reservations_claimed_by(1) == {1, 2}


# ---------------------------------------------------------------------------
# Unresolvable GPU-class labels must not chain across classes
# ---------------------------------------------------------------------------


class TestChainWithUnresolvableClassLabel:
    """``_chain_for`` resolves the class label on *both* sides of its comparison.

    Every other matcher compares a reservation's label against the *pod's* label,
    which is always a real string. Here both operands were
    ``gpu_class_labels.get(...)``, so two different classes whose labels both
    failed to resolve compared equal (``None == None``) and chained — extending a
    pod's runtime guarantee across a window of a class it never held.
    """

    def _abutting_pair(self):
        """Two back-to-back windows, same user and GPU count, different classes."""
        first = _user_reservation(1, gpu_class_id=GPU_CLASS_ID)
        second = _user_reservation(
            2, gpu_class_id=OTHER_CLASS_ID, start_time="10:00:00"
        )
        return first, second

    def test_does_not_chain_two_unresolvable_classes(self):
        first, second = self._abutting_pair()
        # Neither class is in the label map: both used to resolve to None.
        state = _state(first, second, labels={})

        assert state._chain_for(first, NOW) == []
        # The guarantee is the reservation's own window, not the pair's.
        assert state.compute_guaranteed_until(NOW, first) == first.end_utc

    def test_still_chains_when_both_resolve_to_the_same_label(self):
        """The fix must not break the case chaining exists for."""
        first = _user_reservation(1, gpu_class_id=GPU_CLASS_ID)
        second = _user_reservation(2, gpu_class_id=GPU_CLASS_ID, start_time="10:00:00")
        state = _state(first, second)

        assert [r.id for r in state._chain_for(first, NOW)] == [2]
        assert state.compute_guaranteed_until(NOW, first) == second.end_utc

    def test_does_not_chain_a_resolvable_class_to_an_unresolvable_one(self):
        first, second = self._abutting_pair()
        # Only the anchor's class resolves; the follow-on's does not.
        state = _state(first, second, labels={GPU_CLASS_ID: "h100"})

        assert state._chain_for(first, NOW) == []
        assert state.compute_guaranteed_until(NOW, first) == first.end_utc
