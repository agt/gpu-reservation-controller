"""Unit tests for reclaim-block merging (reconcile_reclaim_merges).

A "subject block" (open reclaim / no-show / cancelled-in-window window) absorbs a
future, committed reclaim block that abuts it, so an on-demand job beginning in
the subject can run through the merged span.  A future block counts as committed
only if its start was within ``reclaim_preempt_guard_minutes`` **at the last
reservation fetch** (``last_reservation_fetch_at``), never merely by the tick
clock advancing between fetches.  These tests exercise only the pure-Python logic
in controller.py; no Kubernetes or HTTP calls.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.controller import ControllerState

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL
from tests.conftest import block as _block

GUARD_MINUTES = 60


def _fresh_state(fetch_at: datetime) -> ControllerState:
    """State with the guard configured and reservation data stamped *fetch_at*."""
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.reclaim_preempt_guard_minutes = GUARD_MINUTES
    state.last_reservation_fetch_at = fetch_at
    return state


def test_basic_merge_extends_subject_and_stubs_future():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)   # subject end, inside guard
    t2 = t1 + timedelta(hours=2)       # future block end
    subject = _block(1, t0, t1)
    future = _block(2, t1, t2)
    state = _fresh_state(now)
    state.reservations = [subject, future]

    state.reconcile_reclaim_merges(now)

    # The overlay extends effective_end; the fetched object is not mutated (D4a).
    assert state.effective_end(subject) == t2
    assert subject.end_utc == t1
    assert state.merged_stub_ids == {2}
    assert state.reclaim_merges[1].absorbed_ids == [2]
    assert state.reclaim_merges[1].extended_end == t2

    # find_ondemand_block returns the extended subject (not the stub), and its
    # effective_end (used to cap on-demand runtime) reaches the merged end.
    block = state.find_ondemand_block(GPU_CLASS_LABEL, now, 1, 60)
    assert block is not None and block.id == 1
    assert state.effective_end(block) == t2


def test_no_merge_when_future_block_beyond_guard():
    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=GUARD_MINUTES + 30)  # subject ends beyond guard
    subject = _block(1, now - timedelta(minutes=10), t1)
    future = _block(2, t1, t1 + timedelta(hours=2))
    state = _fresh_state(now)
    state.reservations = [subject, future]

    state.reconcile_reclaim_merges(now)

    assert subject.end_utc == t1
    assert state.merged_stub_ids == set()
    assert state.reclaim_merges == {}


def test_no_merge_when_block_enters_guard_only_by_tick_clock():
    """Regression: a block still preemptible at fetch time must not be merged
    just because the between-fetch tick clock drifts it into the guard window.

    Without a fresh fetch confirming the block is still present, merging it would
    race a last-minute front-end booking the controller has not yet seen.
    """
    fetch_at = datetime.now(timezone.utc)
    t1 = fetch_at + timedelta(minutes=GUARD_MINUTES + 30)  # beyond guard at fetch
    subject = _block(1, fetch_at - timedelta(minutes=10), t1)
    future = _block(2, t1, t1 + timedelta(hours=2))
    state = _fresh_state(fetch_at)
    state.reservations = [subject, future]

    # Fetch-time pass: future block is outside the guard → no merge.
    state.reconcile_reclaim_merges(fetch_at)
    assert state.merged_stub_ids == set()

    # Tick clock advances so the future block is now within the guard by wall
    # clock — but no new fetch happened (last_reservation_fetch_at unchanged).
    later = t1 - timedelta(minutes=10)  # t1 - later = 10 min < guard
    assert (t1 - later).total_seconds() / 60 < GUARD_MINUTES
    state.reconcile_reclaim_merges(later)

    assert state.merged_stub_ids == set()
    assert state.reclaim_merges == {}
    assert subject.end_utc == t1


def test_no_merge_on_gpu_count_or_class_mismatch():
    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=20)
    subject = _block(1, now - timedelta(minutes=10), t1, gpu_count=2)
    count_mismatch = _block(2, t1, t1 + timedelta(hours=2), gpu_count=4)
    class_mismatch = _block(3, t1, t1 + timedelta(hours=2), gpu_class_id=99)
    state = _fresh_state(now)
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
    state = _fresh_state(now)
    state.reservations = [subject, future]

    state.reconcile_reclaim_merges(now)

    assert subject.end_utc == t1
    assert state.merged_stub_ids == set()


def test_merge_persists_across_reload_after_subject_original_end():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(hours=2)
    state = _fresh_state(now)
    state.reservations = [_block(1, t0, t1), _block(2, t1, t2)]
    state.reconcile_reclaim_merges(now)
    assert state.merged_stub_ids == {2}

    # Simulate a reservation reload: fresh objects with the ORIGINAL windows,
    # and advance the clock past the subject's original end (now between t1, t2).
    later = t1 + timedelta(minutes=5)
    fresh_subject = _block(1, t0, t1)
    fresh_future = _block(2, t1, t2)
    state.reservations = [fresh_subject, fresh_future]
    state.last_reservation_fetch_at = later

    state.reconcile_reclaim_merges(later)

    # The merge must survive: subject re-extended to t2, future still stubbed,
    # so the future block is never independently re-offered (no double-book).
    assert state.effective_end(fresh_subject) == t2
    assert state.merged_stub_ids == {2}
    block = state.find_ondemand_block(GPU_CLASS_LABEL, later, 1, 60)
    assert block is not None and block.id == 1 and state.effective_end(block) == t2


def test_merge_pruned_once_whole_span_ends():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(hours=2)
    state = _fresh_state(now)
    state.reservations = [_block(1, t0, t1), _block(2, t1, t2)]
    state.reconcile_reclaim_merges(now)
    assert state.reclaim_merges

    # Advance past the extended end: the record is dropped and the stub cleared.
    after = t2 + timedelta(minutes=1)
    fresh_subject = _block(1, t0, t1)
    fresh_future = _block(2, t1, t2)
    state.reservations = [fresh_subject, fresh_future]
    state.last_reservation_fetch_at = after
    state.reconcile_reclaim_merges(after)

    assert state.reclaim_merges == {}
    assert state.merged_stub_ids == set()
    assert fresh_subject.end_utc == t1


def test_transitive_chaining_grows_on_a_fresh_fetch():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)           # within guard at first fetch
    t2 = t1 + timedelta(minutes=40)            # 70 min out — beyond guard at first fetch
    t3 = t2 + timedelta(hours=2)
    subject = _block(1, t0, t1)
    mid = _block(2, t1, t2)
    far = _block(3, t2, t3)
    state = _fresh_state(now)
    state.reservations = [subject, mid, far]

    # First fetch: only `mid` is within the guard; `far` (t2 start) is not yet.
    state.reconcile_reclaim_merges(now)
    assert state.merged_stub_ids == {2}
    assert state.effective_end(subject) == t2

    # A later fetch sees `far`'s start (t2) fall inside the guard horizon.
    fetch2 = t2 - timedelta(minutes=GUARD_MINUTES - 10)  # horizon now reaches t2
    state.last_reservation_fetch_at = fetch2
    state.reconcile_reclaim_merges(fetch2)
    assert state.merged_stub_ids == {2, 3}
    assert state.effective_end(subject) == t3
    assert state.reclaim_merges[1].absorbed_ids == [2, 3]


def test_cancelled_block_can_be_subject():
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(hours=2)
    cancelled = _block(1, t0, t1, kind="booking", status="cancelled", user=True)
    future = _block(2, t1, t2)
    state = _fresh_state(now)
    state.cancelled_reservations = {1: cancelled}
    state.reservations = [future]

    state.reconcile_reclaim_merges(now)

    assert state.effective_end(cancelled) == t2
    assert state.merged_stub_ids == {2}


def test_claimed_subject_keeps_absorbed_blocks_stubbed():
    """B4: when the subject becomes claimed by a reserved holder the merge must
    NOT be silently dropped — the absorbed blocks stay stubbed (and the merge
    retained) until the whole span ends, or a still-running on-demand job on an
    already-extended deadline would be double-booked.  The claimed subject's own
    window is not extended."""
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(minutes=10)
    t1 = now + timedelta(minutes=30)
    t2 = t1 + timedelta(hours=2)
    subject = _block(1, t0, t1)
    future = _block(2, t1, t2)
    state = _fresh_state(now)
    state.reservations = [subject, future]
    state.reconcile_reclaim_merges(now)
    assert state.merged_stub_ids == {2}

    # A reserved holder now claims the subject window; objects are rebuilt
    # wholesale (mimicking a reservation reload).
    fresh_subject = _block(1, t0, t1)
    fresh_future = _block(2, t1, t2)
    state.reservations = [fresh_subject, fresh_future]
    state.claimed_reservation_ids = {1}
    state.reconcile_reclaim_merges(now)

    assert 1 in state.reclaim_merges           # merge retained
    assert 2 in state.merged_stub_ids          # absorbed block still stubbed
    assert fresh_subject.end_utc == t1         # claimed subject NOT extended


def test_no_merge_when_guard_unknown():
    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=20)
    state = _fresh_state(now)
    state.reclaim_preempt_guard_minutes = None
    state.reservations = [_block(1, now - timedelta(minutes=10), t1),
                          _block(2, t1, t1 + timedelta(hours=2))]

    state.reconcile_reclaim_merges(now)

    assert state.merged_stub_ids == set()
    assert state.reclaim_merges == {}


def test_no_discovery_without_a_fetch_timestamp():
    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=20)
    state = _fresh_state(now)
    state.last_reservation_fetch_at = None  # no fetch has completed yet
    state.reservations = [_block(1, now - timedelta(minutes=10), t1),
                          _block(2, t1, t1 + timedelta(hours=2))]

    state.reconcile_reclaim_merges(now)

    assert state.merged_stub_ids == set()
    assert state.reclaim_merges == {}
