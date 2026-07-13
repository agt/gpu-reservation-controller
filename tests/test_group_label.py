"""Unit tests for the optional REQUIRED_GROUP_LABEL usage-group match constraint.

When ``ControllerState.required_group_label`` is set (populated from the
``REQUIRED_GROUP_LABEL`` env var), the reserved-path matchers add a third
equality gate: a pod's group-label value must equal the reservation's
``group.name``.  When unset the gate is a no-op.  The on-demand path
(``find_ondemand_block`` onto reclaim blocks) stays group-agnostic.

Covers: find_best_reservation, find_open_booking_for, _chain_for,
plan_pod_adoptions, and the group-agnostic on-demand path.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.controller import PodRuntimeView
from tests.conftest import GPU_CLASS_LABEL, make_state, reservation

GROUP_LABEL_KEY = "dsmlp/course"
G1 = "CSE151_SP26_A00"
G2 = "CSE251_SP26_B00"


def _open_booking(res_id, *, group=None, group_id=None, gpu_count=2, username="alice"):
    """A currently-open booking (now-1h … now+1h)."""
    now = datetime.now(timezone.utc)
    return reservation(
        res_id,
        start_utc=now - timedelta(hours=1),
        end_utc=now + timedelta(hours=1),
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_count=gpu_count,
        username=username,
        group=group,
        group_id=group_id,
    )


# ---------------------------------------------------------------------------
# find_best_reservation
# ---------------------------------------------------------------------------


class TestFindBestReservation:
    def test_disabled_matches_regardless_of_group(self):
        # Feature off: the group_label argument is ignored entirely.
        state = make_state(_open_booking(1, group=G1))
        assert state.find_best_reservation("alice", GPU_CLASS_LABEL, None) is not None
        assert state.find_best_reservation("alice", GPU_CLASS_LABEL, "anything") is not None

    def test_enabled_matches_when_group_name_equals_label(self):
        state = make_state(_open_booking(1, group=G1), required_group_label=GROUP_LABEL_KEY)
        res = state.find_best_reservation("alice", GPU_CLASS_LABEL, G1)
        assert res is not None and res.id == 1

    def test_enabled_no_match_on_group_mismatch(self):
        state = make_state(_open_booking(1, group=G1), required_group_label=GROUP_LABEL_KEY)
        assert state.find_best_reservation("alice", GPU_CLASS_LABEL, G2) is None

    def test_enabled_pod_missing_label_never_admitted(self):
        # group_label is None while the feature is on -> matches no grouped booking.
        state = make_state(_open_booking(1, group=G1), required_group_label=GROUP_LABEL_KEY)
        assert state.find_best_reservation("alice", GPU_CLASS_LABEL, None) is None

    def test_enabled_reservation_without_group_never_matches(self):
        # A booking with no group cannot satisfy an enabled group gate.
        state = make_state(_open_booking(1, group=None), required_group_label=GROUP_LABEL_KEY)
        assert state.find_best_reservation("alice", GPU_CLASS_LABEL, G1) is None

    def test_selects_the_matching_group_among_several(self):
        state = make_state(
            _open_booking(1, group=G1),
            _open_booking(2, group=G2),
            required_group_label=GROUP_LABEL_KEY,
        )
        assert state.find_best_reservation("alice", GPU_CLASS_LABEL, G2).id == 2
        assert state.find_best_reservation("alice", GPU_CLASS_LABEL, G1).id == 1


# ---------------------------------------------------------------------------
# find_open_booking_for (adoption match)
# ---------------------------------------------------------------------------


class TestFindOpenBookingFor:
    def test_enabled_requires_group_match(self):
        state = make_state(_open_booking(1, group=G1), required_group_label=GROUP_LABEL_KEY)
        now = datetime.now(timezone.utc)
        assert state.find_open_booking_for("alice", GPU_CLASS_LABEL, now, 1, group_label=G1).id == 1
        assert state.find_open_booking_for("alice", GPU_CLASS_LABEL, now, 1, group_label=G2) is None

    def test_disabled_ignores_group(self):
        state = make_state(_open_booking(1, group=G1))
        now = datetime.now(timezone.utc)
        assert state.find_open_booking_for("alice", GPU_CLASS_LABEL, now, 1).id == 1


# ---------------------------------------------------------------------------
# plan_pod_adoptions threads PodRuntimeView.group_label through
# ---------------------------------------------------------------------------


def _ondemand_view(uid, reservation_id, *, group_label):
    """A within-guarantee on-demand pod (reserved_path=False) — adoption-eligible
    any time once a matching open booking exists."""
    return PodRuntimeView(
        uid=uid,
        namespace="alice",
        name=uid,
        gpu_class=GPU_CLASS_LABEL,
        gpu_count=1,
        reservation_id=reservation_id,
        reserved_path=False,
        node_resident=True,
        terminating=False,
        group_label=group_label,
    )


class TestPlanPodAdoptions:
    def test_upgrade_respects_group(self):
        # Open booking in G1; an on-demand pod tagged G1 is upgraded, G2 is not.
        state = make_state(_open_booking(1, group=G1), required_group_label=GROUP_LABEL_KEY)
        now = datetime.now(timezone.utc)

        matching = _ondemand_view("p-g1", reservation_id=999, group_label=G1)
        assigns = state.plan_pod_adoptions([matching], now)
        assert [(p.uid, r.id) for p, r, _ in assigns] == [("p-g1", 1)]

        mismatched = _ondemand_view("p-g2", reservation_id=999, group_label=G2)
        assert state.plan_pod_adoptions([mismatched], now) == []


# ---------------------------------------------------------------------------
# _chain_for — guarantees never chain across a different group
# ---------------------------------------------------------------------------


class TestChaining:
    def _abutting_pair(self):
        now = datetime.now(timezone.utc)
        first = reservation(
            1,
            start_utc=now - timedelta(hours=1),
            end_utc=now + timedelta(hours=1),
            gpu_class_label=GPU_CLASS_LABEL,
            gpu_count=2,
            group=G1,
        )
        # Abuts first (starts exactly at first.end), same user/class/count.
        second = reservation(
            2,
            start_utc=now + timedelta(hours=1),
            end_utc=now + timedelta(hours=3),
            gpu_class_label=GPU_CLASS_LABEL,
            gpu_count=2,
            group=G2,  # different group
        )
        return first, second, now

    def test_disabled_chains_across_groups(self):
        first, second, now = self._abutting_pair()
        state = make_state(first, second)  # feature off
        chain = state._chain_for(first, now)
        assert [r.id for r in chain] == [2]

    def test_enabled_does_not_chain_across_different_group(self):
        first, second, now = self._abutting_pair()
        state = make_state(first, second, required_group_label=GROUP_LABEL_KEY)
        assert state._chain_for(first, now) == []

    def test_enabled_chains_within_same_group(self):
        first, second, now = self._abutting_pair()
        # Make the follow-on share first's group id.
        second.group_id = first.group_id
        state = make_state(first, second, required_group_label=GROUP_LABEL_KEY)
        assert [r.id for r in state._chain_for(first, now)] == [2]


# ---------------------------------------------------------------------------
# enqueue_pod carries group_label; reconcile_queue re-matches with it
# ---------------------------------------------------------------------------


class TestEnqueueAndReconcile:
    def test_enqueue_respects_group_and_carries_label(self):
        state = make_state(_open_booking(1, group=G1), required_group_label=GROUP_LABEL_KEY)

        # Mismatched group -> not enqueued.
        state.enqueue_pod("u2", "pod-2", "alice", GPU_CLASS_LABEL, 1, G2)
        assert "u2" not in state.task_queue

        # Matching group -> enqueued, and the QueueEntry remembers the label.
        state.enqueue_pod("u1", "pod-1", "alice", GPU_CLASS_LABEL, 1, G1)
        assert state.task_queue["u1"].reservation.id == 1
        assert state.task_queue["u1"].group_label == G1

    def test_reconcile_queue_rematches_with_group(self):
        # Entry's reservation is cancelled; a fresh same-group booking exists.
        b1 = _open_booking(1, group=G1)
        state = make_state(b1, required_group_label=GROUP_LABEL_KEY)
        state.enqueue_pod("u1", "pod-1", "alice", GPU_CLASS_LABEL, 1, G1)

        # Replace reservation 1 with reservation 2 (same group).
        state.reservations = [_open_booking(2, group=G1)]
        state.reconcile_queue()
        assert state.task_queue["u1"].reservation.id == 2
        assert state.task_queue["u1"].group_label == G1

        # Now replace with a different-group booking: entry drops (no re-match).
        state.reservations = [_open_booking(3, group=G2)]
        state.reconcile_queue()
        assert "u1" not in state.task_queue


# ---------------------------------------------------------------------------
# On-demand path is group-agnostic (reclaim blocks have no group)
# ---------------------------------------------------------------------------


class TestOnDemandGroupAgnostic:
    def test_reclaim_block_placed_regardless_of_group(self):
        now = datetime.now(timezone.utc)
        reclaim = reservation(
            5,
            start_utc=now - timedelta(hours=1),
            end_utc=now + timedelta(hours=1),
            kind="reclaim",
            gpu_class_label=GPU_CLASS_LABEL,
            gpu_count=2,
        )
        # Feature on: reclaim blocks (group=None) still place — no group predicate.
        state = make_state(reclaim, required_group_label=GROUP_LABEL_KEY)
        block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
        assert block is not None and block.id == 5
