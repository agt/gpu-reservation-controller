"""Unit tests for reclaim-block merging (reconcile_reclaim_merges).

A "subject block" (open reclaim / no-show / cancelled-in-window window) absorbs a
future, committed (within-guard) reclaim block that abuts it, so an on-demand job
beginning in the subject can run through the merged span.  These tests exercise
only the pure-Python logic in controller.py; no Kubernetes or HTTP calls.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.controller import ControllerState, slot_end
from app.schemas import GpuClassBrief, ReservationResponse

GPU_CLASS_ID = 10
GPU_CLASS_LABEL = "h100"
GUARD_MINUTES = 60


def _block(
    block_id: int,
    start_utc: datetime,
    end_utc: datetime,
    *,
    gpu_count: int = 2,
    kind: str = "reclaim",
    status: str = "active",
    gpu_class_id: int = GPU_CLASS_ID,
    user: bool = False,
) -> ReservationResponse:
    """Build a reservation with an explicit UTC window."""
    return ReservationResponse(
        id=block_id,
        user_id=1 if user else None,
        user=None,
        group_id=None,
        group=None,
        gpu_class_id=gpu_class_id,
        gpu_class=GpuClassBrief(id=gpu_class_id, name="H100"),
        date=start_utc.date(),
        start_utc=start_utc,
        end_utc=end_utc,
        gpu_count=gpu_count,
        status=status,
        kind=kind,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


def _fresh_state() -> ControllerState:
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.reclaim_preempt_guard_minutes = GUARD_MINUTES
    return state


def test_basic_merge_extends_subject_and_stubs_future():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)   # subject end, inside guard
    t2 = t1 + timedelta(hours=2)       # future block end
    subject = _block(1, t0, t1)
    future = _block(2, t1, t2)
    state = _fresh_state()
    state.reservations = [subject, future]

    state.reconcile_reclaim_merges(now)

    assert subject.end_utc == t2
    assert state.merged_stub_ids == {2}
    assert state.reclaim_merges[1].absorbed_ids == [2]
    assert state.reclaim_merges[1].extended_end == t2

    # find_ondemand_block returns the extended subject (not the stub), and its
    # slot_end (used to cap on-demand runtime) reaches the merged end.
    block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
    assert block is not None and block.id == 1
    assert slot_end(block) == t2


def test_no_merge_when_future_block_beyond_guard():
    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=GUARD_MINUTES + 30)  # subject ends beyond guard
    subject = _block(1, now - timedelta(minutes=10), t1)
    future = _block(2, t1, t1 + timedelta(hours=2))
    state = _fresh_state()
    state.reservations = [subject, future]

    state.reconcile_reclaim_merges(now)

    assert subject.end_utc == t1
    assert state.merged_stub_ids == set()
    assert state.reclaim_merges == {}


def test_no_merge_on_gpu_count_or_class_mismatch():
    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=20)
    subject = _block(1, now - timedelta(minutes=10), t1, gpu_count=2)
    count_mismatch = _block(2, t1, t1 + timedelta(hours=2), gpu_count=4)
    class_mismatch = _block(3, t1, t1 + timedelta(hours=2), gpu_class_id=99)
    state = _fresh_state()
    state.gpu_class_labels[99] = "a100"
    state.reservations = [subject, count_mismatch, class_mismatch]

    state.reconcile_reclaim_merges(now)

    assert subject.end_utc == t1
    assert state.merged_stub_ids == set()


def test_no_merge_when_gap_between_blocks():
    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=20)
    subject = _block(1, now - timedelta(minutes=10), t1)
    # Future block starts 5 minutes after the subject ends — not abutting.
    future = _block(2, t1 + timedelta(minutes=5), t1 + timedelta(hours=2))
    state = _fresh_state()
    state.reservations = [subject, future]

    state.reconcile_reclaim_merges(now)

    assert subject.end_utc == t1
    assert state.merged_stub_ids == set()


def test_merge_persists_across_reload_after_subject_original_end():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(hours=2)
    state = _fresh_state()
    state.reservations = [_block(1, t0, t1), _block(2, t1, t2)]
    state.reconcile_reclaim_merges(now)
    assert state.merged_stub_ids == {2}

    # Simulate a reservation reload: fresh objects with the ORIGINAL windows,
    # and advance the clock past the subject's original end (now between t1, t2).
    later = t1 + timedelta(minutes=5)
    fresh_subject = _block(1, t0, t1)
    fresh_future = _block(2, t1, t2)
    state.reservations = [fresh_subject, fresh_future]

    state.reconcile_reclaim_merges(later)

    # The merge must survive: subject re-extended to t2, future still stubbed,
    # so the future block is never independently re-offered (no double-book).
    assert fresh_subject.end_utc == t2
    assert state.merged_stub_ids == {2}
    block = state.find_ondemand_block(GPU_CLASS_LABEL, later, 1, 60)
    assert block is not None and block.id == 1 and slot_end(block) == t2


def test_merge_pruned_once_whole_span_ends():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(hours=2)
    state = _fresh_state()
    state.reservations = [_block(1, t0, t1), _block(2, t1, t2)]
    state.reconcile_reclaim_merges(now)
    assert state.reclaim_merges

    # Advance past the extended end: the record is dropped and the stub cleared.
    after = t2 + timedelta(minutes=1)
    fresh_subject = _block(1, t0, t1)
    fresh_future = _block(2, t1, t2)
    state.reservations = [fresh_subject, fresh_future]
    state.reconcile_reclaim_merges(after)

    assert state.reclaim_merges == {}
    assert state.merged_stub_ids == set()
    assert fresh_subject.end_utc == t1


def test_transitive_chaining_grows_as_blocks_enter_guard():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)           # within guard
    t2 = t1 + timedelta(minutes=40)            # 70 min out — beyond guard at `now`
    t3 = t2 + timedelta(hours=2)
    subject = _block(1, t0, t1)
    mid = _block(2, t1, t2)
    far = _block(3, t2, t3)
    state = _fresh_state()
    state.reservations = [subject, mid, far]

    # First pass: only `mid` is within the guard; `far` (t2 start) is not yet.
    state.reconcile_reclaim_merges(now)
    assert state.merged_stub_ids == {2}
    assert subject.end_utc == t2

    # Later, the clock advances so `far`'s start (t2) falls inside the guard.
    later = t2 - timedelta(minutes=GUARD_MINUTES - 10)
    state.reconcile_reclaim_merges(later)
    assert state.merged_stub_ids == {2, 3}
    assert subject.end_utc == t3
    assert state.reclaim_merges[1].absorbed_ids == [2, 3]


def test_cancelled_block_can_be_subject():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(hours=2)
    cancelled = _block(1, t0, t1, kind="booking", status="cancelled", user=True)
    future = _block(2, t1, t2)
    state = _fresh_state()
    state.cancelled_reservations = {1: cancelled}
    state.reservations = [future]

    state.reconcile_reclaim_merges(now)

    assert cancelled.end_utc == t2
    assert state.merged_stub_ids == {2}


def test_merge_dropped_when_subject_becomes_claimed():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(hours=2)
    subject = _block(1, t0, t1)
    future = _block(2, t1, t2)
    state = _fresh_state()
    state.reservations = [subject, future]
    state.reconcile_reclaim_merges(now)
    assert state.merged_stub_ids == {2}

    # A reserved holder now claims the subject window: the merge is dropped and
    # the future block reverts to an independent reclaim block.
    fresh_subject = _block(1, t0, t1)
    fresh_future = _block(2, t1, t2)
    state.reservations = [fresh_subject, fresh_future]
    state.claimed_reservation_ids = {1}
    state.reconcile_reclaim_merges(now)

    assert state.reclaim_merges == {}
    assert state.merged_stub_ids == set()
    assert fresh_subject.end_utc == t1


def test_no_merge_when_guard_unknown():
    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=20)
    state = _fresh_state()
    state.reclaim_preempt_guard_minutes = None
    state.reservations = [_block(1, now - timedelta(minutes=10), t1),
                          _block(2, t1, t1 + timedelta(hours=2))]

    state.reconcile_reclaim_merges(now)

    assert state.merged_stub_ids == set()
    assert state.reclaim_merges == {}
