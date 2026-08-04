"""Regression test for CODE-REVIEW-2026-07 B1.

Occupancy is the unified budget map for all admission paths, so releasing a
deleted pod must not be gated on ONDEMAND_LEASE_ENABLED.  With the flag
false a deleted reserved-path pod used to keep consuming budget until the next
reconcile.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.config import Config
from tests.conftest import make_config
from app.controller import ControllerState


def _config(**overrides) -> Config:
    """Shared builder (tests/conftest.make_config) with this module's default."""
    return make_config(**{"ondemand_lease_enabled": False} | overrides)


class _FakeWatcher:
    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


def _pod(uid: str) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-" + uid, namespace="alice",
            labels={"gpu-class": "h100"},
        )
    )


def test_deleted_pod_releases_occupancy_when_ondemand_disabled(monkeypatch):
    import app.main as main_module

    state = ControllerState()
    # A reserved-path pod holding 2 GPUs under reservation #42.
    state.record_placement(42, "uid-del", 2)
    assert state.occupancy.get(42, {}).get("uid-del") == 2

    events = [("DELETED", _pod("uid-del"))]
    monkeypatch.setattr(main_module, "PodWatcher", lambda **kw: _FakeWatcher(events))

    # The DELETED branch never touches the reservation client.
    asyncio.run(main_module.pod_watch_loop(state, None, _config()))

    # Before the fix, release_pod was gated on the on-demand flag, so the slot
    # leaked.  It must now be freed regardless of the flag.
    assert "uid-del" not in state.occupancy.get(42, {})
