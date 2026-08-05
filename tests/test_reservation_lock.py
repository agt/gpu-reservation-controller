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
import re

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


# ---------------------------------------------------------------------------
# The prose contract and the runtime guard must not drift apart again
# ---------------------------------------------------------------------------


# The docstring phrasings that claim the caller holds the lock: "caller must
# hold", "Caller holds", "the caller holds", "The caller must already hold".
# The optional adverb matters — `_reconcile_after_reservation_change` says
# "must already hold", and a regex without it silently skipped that function.
_CLAIMS_LOCK = re.compile(
    r"caller\s+(?:must\s+)?(?:already\s+)?holds?\b", re.IGNORECASE
)


def _functions_claiming_the_lock(main_module):
    """Every function whose docstring says its caller holds ``reservation_lock``.

    Deduplicated by function object: ``app.main`` imports ``ControllerState``,
    so a method reached through both modules is one function, not two.
    """
    import app.controller

    found: dict[object, str] = {}
    for module in (main_module, app.controller):
        for name, obj in vars(module).items():
            if inspect.isfunction(obj):
                candidates = [(f"{module.__name__}.{name}", obj)]
            elif inspect.isclass(obj):
                candidates = [
                    (f"{obj.__module__}.{obj.__name__}.{m}", fn)
                    for m, fn in vars(obj).items()
                    if inspect.isfunction(fn)
                ]
            else:
                continue
            for qualname, fn in candidates:
                doc = inspect.getdoc(fn) or ""
                if "reservation_lock" in doc and _CLAIMS_LOCK.search(doc):
                    found.setdefault(fn, qualname)
    return found


def test_every_helper_claiming_the_lock_also_requires_it(main_module):
    """A docstring contract must be backed by ``require_reservation_lock``.

    This is how rank 29 happened in the first place: the claim was written
    down, the check never was, and nothing tied the two together — so the
    contract could rot silently while reading exactly as authoritative.

    Modelled on ``tests/test_log_grammar.py``, which fails the build when a log
    call site skips ``kv()``.
    """
    claimants = _functions_claiming_the_lock(main_module)
    assert claimants, "the docstring scan found nothing — has the wording changed?"

    missing = sorted(
        qualname
        for fn, qualname in claimants.items()
        if "require_reservation_lock" not in inspect.getsource(fn)
    )
    assert not missing, (
        "these functions document that their caller holds reservation_lock but "
        "do not call require_reservation_lock(): " + ", ".join(missing)
    )


def test_the_scan_covers_the_helpers_we_expect():
    """Pin the claimant set, so a helper losing its docstring is not silent.

    Without this, deleting the sentence would make the drift test above pass
    vacuously for that function.
    """
    import app.main  # noqa: F401 - imported via the fixture elsewhere

    expected = {
        "_reconcile_after_reservation_change",
        "_handle_cancelled_reservations",
        "_handle_owner_changes",
        "_adopt_pods",
        "_merge_ondemand_into_bookings",
        "_cancel_merged_lease",
        "forecast_preemption_risk",
    }
    found = {
        qualname.rsplit(".", 1)[-1]
        for qualname in _functions_claiming_the_lock(app.main).values()
    }
    assert expected <= found, f"lost the lock contract on: {sorted(expected - found)}"


def test_a_helper_that_acquires_the_lock_is_not_guarded(main_module):
    """``_drain_pending_merge_cancels`` acquires; it must not require.

    The two directions are opposites and the docstrings read almost the same.
    Guarding an acquirer would raise on every ordinary call — the mirror-image
    mistake to the one the guards prevent.
    """
    source = inspect.getsource(main_module._drain_pending_merge_cancels)
    assert "async with state.reservation_lock" in source
    assert "require_reservation_lock" not in source


# ---------------------------------------------------------------------------
# Lock scope: no HTTP under the lock, and a push that does not queue behind it
# ---------------------------------------------------------------------------


class _SlowClassClient:
    """Client whose bulk GPU-class fetch blocks until *gate* is set."""

    def __init__(self, gate: asyncio.Event | None = None):
        self.gate = gate
        self.locked_during_fetch: list[bool] = []

    async def fetch_reservations(self):
        return []

    async def fetch_gpu_classes(self, state=None):
        # Recorded by the caller through the closure below; see the tests.
        if self.gate is not None:
            await self.gate.wait()
        return []

    async def fetch_gpu_class(self, cid):  # pragma: no cover - never reached
        return None


def _config_stub():
    from tests.conftest import make_config

    return make_config()


def test_reconcile_does_not_hold_the_lock_across_the_gpu_class_fetch(main_module):
    """The direct regression test for rank 28.

    The class fetch is one 15 s-timeout HTTP round trip on every cycle, and in
    steady state it was the *only* thing the lock was held across — so a push
    arriving mid-fetch waited it out for no reason at all.
    """
    m = main_module
    state = ControllerState()
    observed: list[bool] = []

    class _Client(_SlowClassClient):
        async def fetch_gpu_classes(self):
            observed.append(state.reservation_lock.locked())
            return []

    _run(m._refresh_reservations(state, _Client(), _config_stub()))

    assert observed == [False], "the GPU-class fetch ran with the lock held"


def test_push_is_not_blocked_by_a_slow_fetch(main_module):
    """The behavioural test — and the one that would have caught the design.

    Drives the endpoint coroutine directly rather than through ``TestClient``:
    ``TestClient`` runs the app on its own event loop, so a gather across it
    would not share a loop with the fetch and could not exercise the contention
    this is about.
    """
    from types import SimpleNamespace

    from app.schemas import ReservationPushRequest

    m = main_module
    state = ControllerState()
    gate = asyncio.Event()
    order: list[str] = []

    class _FetchClient(_SlowClassClient):
        async def fetch_gpu_classes(self):
            order.append("fetch-blocked")
            await gate.wait()
            order.append("fetch-resumed")
            return []

    class _PushClient(_SlowClassClient):
        async def fetch_gpu_classes(self):
            return []

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                controller_state=state,
                reservation_client=_PushClient(),
                config=_config_stub(),
            )
        )
    )

    async def scenario():
        fetch = asyncio.create_task(
            m._refresh_reservations(state, _FetchClient(), _config_stub())
        )
        await asyncio.sleep(0)  # let the fetch reach its blocked call

        # The push must complete while the fetch is still stuck on HTTP.
        await m.push_reservations(ReservationPushRequest(reservations=[]), request)
        order.append("push-done")

        gate.set()
        await fetch

    _run(scenario())

    # "push-done" before "fetch-resumed" is the whole assertion: previously the
    # push could not even start until the fetch released the lock.
    assert order == ["fetch-blocked", "push-done", "fetch-resumed"]
