"""Unit tests for mid-window reservation cancellation detection.

Covers:
- detect_cancelled_in_window: various conditions that should and should not trigger
- _canceller_description helper in main.py
"""

from __future__ import annotations

from datetime import date, timedelta

from app.controller import canceller_description, slot_end
from app.schemas import ReservationResponse

from tests.conftest import FIXED_DATE, GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state_with_label
from tests.conftest import reservation, window_from_minutes


# ---------------------------------------------------------------------------
# Constants & factories
# ---------------------------------------------------------------------------


def _user_res(
    res_id: int,
    *,
    start_offset: int = 480,   # 08:00 UTC
    duration: int = 120,
    gpu_count: int = 2,
    user_id: int = 1,
    username: str = USERNAME,
    cancelled_by_id: int | None = None,
    cancel_reason: str | None = None,
    day: date = FIXED_DATE,
) -> ReservationResponse:
    """Booking reservation from a minutes-past-midnight offset; ``status`` follows
    ``cancelled_by_id``.  Delegates to the canonical factory (CODE-REVIEW T1)."""
    s, e = window_from_minutes(start_offset, duration, day)
    return reservation(
        res_id,
        start_utc=s,
        end_utc=e,
        kind="booking",
        username=username,
        user_id=user_id,
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        status="cancelled" if cancelled_by_id else "active",
        cancelled_by_id=cancelled_by_id,
        cancel_reason=cancel_reason,
        day=day,
    )


# ---------------------------------------------------------------------------
# detect_cancelled_in_window
# ---------------------------------------------------------------------------


class TestDetectCancelledInWindow:
    def test_detects_mid_window_cancellation(self):
        prior = _user_res(1, start_offset=0, duration=120)  # was active
        state = _state_with_label()
        state.reservations = [prior]
        res = _user_res(1, start_offset=0, duration=120, cancelled_by_id=99)
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([res], mid_window)
        assert [r.id for r in result] == [1]

    def test_ignores_naturally_expired(self):
        prior = _user_res(1, start_offset=0, duration=60)
        state = _state_with_label()
        state.reservations = [prior]
        res = _user_res(1, start_offset=0, duration=60, cancelled_by_id=99)
        after_end = slot_end(res) + timedelta(minutes=1)
        result = state.detect_cancelled_in_window([res], after_end)
        assert result == []

    def test_ignores_active_status(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=120)  # status="active"
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([res], mid_window)
        assert result == []

    def test_ignores_already_noshow(self):
        prior = _user_res(1, start_offset=0, duration=120)
        state = _state_with_label()
        state.reservations = [prior]
        res = _user_res(1, start_offset=0, duration=120, cancelled_by_id=99)
        state.noshow_reservation_ids.add(1)
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([res], mid_window)
        assert result == []

    def test_ignores_when_not_previously_active(self):
        """A cancellation already reconciled drops its id from state.reservations
        and is never re-detected on a later fetch — the mechanism that makes
        eviction idempotent across cycles without separate bookkeeping."""
        state = _state_with_label()  # reservations empty: id 1 was never active
        res = _user_res(1, start_offset=0, duration=120, cancelled_by_id=99)
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([res], mid_window)
        assert result == []

    def test_mixed_list_returns_only_cancelled_in_window(self):
        active_res = _user_res(1, start_offset=0, duration=120)
        prior_cancelled = _user_res(2, start_offset=0, duration=120)
        state = _state_with_label()
        state.reservations = [active_res, prior_cancelled]
        cancelled_res = _user_res(2, start_offset=0, duration=120, cancelled_by_id=99)
        mid_window = active_res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([active_res, cancelled_res], mid_window)
        assert [r.id for r in result] == [2]

    def test_empty_list_no_detections(self):
        state = _state_with_label()
        res = _user_res(1, start_offset=0, duration=120)
        mid_window = res.start_utc + timedelta(minutes=30)
        result = state.detect_cancelled_in_window([], mid_window)
        assert result == []


# ---------------------------------------------------------------------------
# _canceller_description
# ---------------------------------------------------------------------------


class TestCancellerDescription:
    """Tests for the canceller_description helper.

    The API carries only ``cancelled_by_id`` (RESERVATION-API.md §6 — no nested
    canceller object), so the helper distinguishes self-vs-other and appends the
    machine-readable ``cancel_reason`` when one is recorded.
    """

    def _make_res(
        self,
        user_id: int | None,
        cancelled_by_id: int | None,
        cancel_reason: str | None = None,
    ) -> ReservationResponse:
        return _user_res(
            1,
            user_id=user_id or 1,
            cancelled_by_id=cancelled_by_id,
            cancel_reason=cancel_reason,
        )

    def test_self_cancel_returns_by_user(self):
        res = self._make_res(user_id=7, cancelled_by_id=7)
        assert canceller_description(res) == "by user"

    def test_different_id_returns_by_another_user(self):
        res = self._make_res(user_id=7, cancelled_by_id=99)
        assert canceller_description(res) == "by another user"

    def test_no_canceller_info_returns_by_user(self):
        res = self._make_res(user_id=7, cancelled_by_id=None)
        assert canceller_description(res) == "by user"

    def test_cancel_reason_appended(self):
        res = self._make_res(user_id=7, cancelled_by_id=7, cancel_reason="superseded")
        assert canceller_description(res) == "by user (reason: superseded)"

    def test_controller_cancel_reason_appended(self):
        # A controller (service-key) cancel carries no cancelled_by_id.
        res = self._make_res(user_id=7, cancelled_by_id=None, cancel_reason="no-show")
        assert canceller_description(res) == "by user (reason: no-show)"
