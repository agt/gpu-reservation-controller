"""Whole-tick exception guard in ``queue_processor_loop``.

The tick body now lives in ``_run_queue_tick`` and the loop guards it the same
way the fetch/preemption/audit loops guard theirs: an exception is logged
(``queue.tick_failed``) and the loop retries next interval instead of dying.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.controller import ControllerState

from tests.test_watch_release import _config


class _Stop(Exception):
    """Sentinel to break out of the infinite loop after the tick under test."""


def test_tick_exception_is_logged_and_loop_survives(monkeypatch, caplog):
    import app.main as main_module

    tick_calls = 0

    async def _boom_tick(state, client, config):
        nonlocal tick_calls
        tick_calls += 1
        raise RuntimeError("tick exploded")

    sleep_calls = 0

    async def _fake_sleep(seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:  # tick 1 ran (and raised); stop before tick 2
            raise _Stop()

    monkeypatch.setattr(main_module, "_run_queue_tick", _boom_tick)
    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)

    with caplog.at_level(logging.ERROR, logger="app.main"):
        with pytest.raises(_Stop):
            asyncio.run(
                main_module.queue_processor_loop(ControllerState(), None, _config())
            )

    # Reaching the second sleep proves the RuntimeError did not kill the loop.
    assert tick_calls == 1
    assert any("queue.tick_failed" in r.getMessage() for r in caplog.records)


def test_extracted_tick_keeps_narrow_snapshot_guard(monkeypatch, caplog):
    """The pure move preserved the pre-existing per-snapshot fail-safe."""
    import app.main as main_module

    async def _fail_snapshot(*a, **kw):
        raise RuntimeError("api down")

    monkeypatch.setattr(main_module, "snapshot_tolerated_pods", _fail_snapshot)

    with caplog.at_level(logging.WARNING, logger="app.main"):
        asyncio.run(main_module._run_queue_tick(ControllerState(), None, _config()))

    assert any("queue.snapshot_failed" in r.getMessage() for r in caplog.records)
