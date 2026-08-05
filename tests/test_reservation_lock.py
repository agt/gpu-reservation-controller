"""The ``reservation_lock`` contract — re-entry, required holds, and scope.

``reservation_lock`` is the one place this daemon's "no locking needed, asyncio
is single-threaded" rule is relaxed, and its contract used to live entirely in
prose: five docstrings said "caller must hold the lock" and nothing checked.

Prose is a bad medium for this particular invariant, because violating it is
**silent**.  ``asyncio.Lock`` is not reentrant, so a coroutine that takes the
lock while already holding it waits on itself forever; ``_on_task_done`` records
only exceptions, so the wedged loop is never marked dead and ``GET /health``
keeps answering 200.  The controller stops admitting pods and nothing says so.

``grep -rn reservation_lock tests/`` returned zero matches before this file.

The repo has no pytest-asyncio: async tests drive ``asyncio.run`` directly and
monkeypatch ``app.main`` module globals.  Every test that could plausibly hang on
a regression is wrapped in ``asyncio.wait_for`` so it fails instead of blocking
CI forever.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from app.controller import ControllerState, LockContractError, _OwnedLock


# A regression in the lock scope shows up as "never returns", so every await in
# this module is bounded.  Generous enough not to flake on a loaded runner.
TIMEOUT = 2.0


def _run(coro):
    """Run *coro* to completion, failing rather than hanging on a regression."""
    async def _bounded():
        return await asyncio.wait_for(coro, timeout=TIMEOUT)

    return asyncio.run(_bounded())


# ---------------------------------------------------------------------------
# _OwnedLock
# ---------------------------------------------------------------------------


class TestOwnedLock:
    def test_acquires_and_releases_like_a_plain_lock(self):
        lock = _OwnedLock()

        async def scenario():
            assert lock.locked() is False
            async with lock:
                assert lock.locked() is True
                assert lock.held_by_current_task() is True
            assert lock.locked() is False
            assert lock.held_by_current_task() is False

        _run(scenario())

    def test_reentry_raises_instead_of_deadlocking(self):
        # The whole point: this used to hang forever, invisibly.
        lock = _OwnedLock()

        async def scenario():
            async with lock:
                with pytest.raises(LockContractError, match="not reentrant"):
                    async with lock:
                        pass  # pragma: no cover - the body never runs

        _run(scenario())

    def test_release_after_a_refused_reentry_still_works(self):
        # A refused re-entry must not leave the outer hold corrupted — the
        # exception propagates through the *inner* __aenter__, which never took
        # ownership, so the outer __aexit__ must still release cleanly.
        lock = _OwnedLock()

        async def scenario():
            with pytest.raises(LockContractError):
                async with lock:
                    async with lock:
                        pass  # pragma: no cover
            assert lock.locked() is False
            async with lock:  # re-acquirable
                assert lock.held_by_current_task() is True

        _run(scenario())

    def test_a_second_task_waits_rather_than_raising(self):
        # Re-entry is a bug; contention between two tasks is the lock doing its
        # job.  The second task must block, not raise.
        lock = _OwnedLock()
        order: list[str] = []

        async def scenario():
            async def holder():
                async with lock:
                    order.append("holder-in")
                    await asyncio.sleep(0.05)
                    order.append("holder-out")

            async def waiter():
                await asyncio.sleep(0.01)  # let the holder go first
                async with lock:
                    order.append("waiter-in")

            await asyncio.gather(holder(), waiter())

        _run(scenario())
        assert order == ["holder-in", "holder-out", "waiter-in"]

    def test_held_by_current_task_is_false_in_another_task(self):
        # The distinction plain Lock.locked() cannot make: someone holds it, but
        # not us.  This is what makes require_reservation_lock meaningful.
        lock = _OwnedLock()

        async def scenario():
            seen: dict[str, bool] = {}

            async def observer():
                seen["locked"] = lock.locked()
                seen["held_by_me"] = lock.held_by_current_task()

            async with lock:
                await asyncio.gather(observer())
            return seen

        seen = _run(scenario())
        assert seen == {"locked": True, "held_by_me": False}


# ---------------------------------------------------------------------------
# ControllerState.require_reservation_lock
# ---------------------------------------------------------------------------


class TestRequireReservationLock:
    def test_passes_while_held(self):
        state = ControllerState()

        async def scenario():
            async with state.reservation_lock:
                state.require_reservation_lock("helper")  # must not raise

        _run(scenario())

    def test_raises_when_not_held(self):
        state = ControllerState()

        async def scenario():
            with pytest.raises(LockContractError, match="requires reservation_lock"):
                state.require_reservation_lock("helper")

        _run(scenario())

    def test_raises_when_another_task_holds_it(self):
        # Not merely "is anyone holding it" — the *calling* task must be.
        state = ControllerState()

        async def scenario():
            async def other():
                with pytest.raises(LockContractError):
                    state.require_reservation_lock("helper")

            async with state.reservation_lock:
                await asyncio.gather(other())

        _run(scenario())

    def test_names_the_helper_in_the_message(self):
        # The message is the whole diagnostic; a bare "lock not held" would send
        # the reader hunting through ten call sites.
        state = ControllerState()

        async def scenario():
            with pytest.raises(LockContractError, match="_adopt_pods"):
                state.require_reservation_lock("_adopt_pods")

        _run(scenario())


# ---------------------------------------------------------------------------
# Every use must be `async with` for the ownership tracking to hold
# ---------------------------------------------------------------------------


@pytest.fixture
def main_module(monkeypatch):
    """Import ``app.main`` with the required env set (the repo's convention)."""
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-lock")
    import app.main as main

    return main


def test_no_direct_acquire_or_release_of_the_reservation_lock(main_module):
    """``_OwnedLock``'s ownership tracking only holds if every use is ``async with``.

    A bare ``reservation_lock.acquire()`` would take the underlying lock without
    recording an owner, so re-entry would go back to deadlocking silently.
    """
    import app.controller

    for module in (main_module, app.controller):
        source = inspect.getsource(module)
        assert "reservation_lock.acquire" not in source, module.__name__
        assert "reservation_lock.release" not in source, module.__name__
