"""First direct coverage of ``PodWatcher`` (``app/k8s_client.py``).

The watcher's protocol behaviour — resourceVersion continuation across clean
stream closes, bookmark handling, immediate re-LIST on HTTP 410, the periodic
resync, the bounded drop-oldest queue, and the failure-log throttle — is
exercised with a scripted ``watch.Watch`` fake plus a ``_core_v1`` stub
(the seam precedent is ``tests/test_k8s_capacity.py``).  No kubernetes fake
client, no new dependencies.

Harness notes: the script is a list of per-cycle behaviours; when it is
exhausted the fake stream *blocks* until ``stop()`` is called, which is
exactly what the real long-poll does on a quiet cluster — so the daemon
thread parks instead of spinning through extra LIST cycles that would
confound call-count assertions.  The consumer closes the generator in a
``finally``, which sets the watcher's stop event and calls ``stop()`` on the
current watch.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

import app.k8s_client as k8s_client
from app.k8s_client import PodWatcher

LOGGER = "app.k8s_client"


def _pod(uid: str = "u1", rv: str | None = None, name: str | None = None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid,
            name=name or f"pod-{uid}",
            namespace="alice",
            labels={"gpu-class": "h100"},
            resource_version=rv,
        )
    )


def _ev(event_type: str = "ADDED", uid: str = "u1", rv: str | None = None):
    return {"type": event_type, "object": _pod(uid, rv)}


def _bookmark(rv: str):
    # Bookmark objects arrive as raw dicts, never deserialized models.
    raw = {"metadata": {"resourceVersion": rv}}
    return {"type": "BOOKMARK", "object": raw, "raw_object": raw}


class _FakeWatch:
    """One ``watch.Watch()`` instance; plays the next entry of the shared script.

    Script entries:
      ("yield", [events])            — yield each, then end (clean close)
      ("raise", exc)                 — raise immediately
      ("yield_then_raise", evs, exc) — yield each, then raise
    An exhausted script blocks until ``stop()`` (like a quiet long-poll).
    """

    def __init__(self, harness):
        self._harness = harness
        self._stopped = threading.Event()
        self.stop_called = False
        harness.watches.append(self)

    def stream(self, func, **kwargs):
        self._harness.stream_calls.append(kwargs)
        if self._harness.script:
            entry = self._harness.script.pop(0)
        else:
            entry = ("block",)
        kind = entry[0]
        if kind == "raise":
            raise entry[1]
        if kind in ("yield", "yield_then_raise"):
            yield from entry[1]
        if kind == "yield_then_raise":
            raise entry[2]
        if kind == "block":
            self._harness.exhausted.set()
            self._stopped.wait(timeout=5)

    def stop(self):
        self.stop_called = True
        self._stopped.set()


class _Harness:
    def __init__(self, monkeypatch, script, *, list_pods=(), list_rvs=None):
        self.script = list(script)
        self.stream_calls: list[dict] = []
        self.list_calls = 0
        self.watches: list[_FakeWatch] = []
        self.exhausted = threading.Event()
        self._list_pods = list(list_pods)
        self._list_rvs = list(list_rvs or [])

        def _list(label_selector=None):
            self.list_calls += 1
            rv = (
                self._list_rvs.pop(0)
                if self._list_rvs
                else f"rv-list-{self.list_calls}"
            )
            return SimpleNamespace(
                items=list(self._list_pods),
                metadata=SimpleNamespace(resource_version=rv),
            )

        monkeypatch.setattr(
            k8s_client, "_core_v1",
            SimpleNamespace(list_pod_for_all_namespaces=_list),
        )
        monkeypatch.setattr(
            k8s_client, "watch", SimpleNamespace(Watch=lambda: _FakeWatch(self))
        )


async def _collect(watcher: PodWatcher, n: int, timeout: float = 5.0):
    """Consume exactly *n* events, then close the generator (stop the watcher)."""
    out = []
    agen = watcher.events()
    try:
        while len(out) < n:
            out.append(await asyncio.wait_for(agen.__anext__(), timeout))
    finally:
        await agen.aclose()
    return out


def test_first_cycle_lists_then_streams_with_list_rv(monkeypatch):
    h = _Harness(
        monkeypatch,
        [("yield", [_ev(uid="w1")])],
        list_pods=[_pod("seeded")],
        list_rvs=["rv-100"],
    )
    watcher = PodWatcher(label_selector="gpu-class")
    events = asyncio.run(_collect(watcher, 2))

    # The LIST seed is replayed as ADDED before stream events arrive.
    assert [(t, p.metadata.uid) for t, p in events] == [
        ("ADDED", "seeded"), ("ADDED", "w1"),
    ]
    assert h.list_calls == 1
    first = h.stream_calls[0]
    assert first["resource_version"] == "rv-100"
    assert first["allow_watch_bookmarks"] is True
    assert first["timeout_seconds"] == k8s_client._WATCH_TIMEOUT_S
    assert first["label_selector"] == "gpu-class"


def test_clean_close_resumes_from_last_event_rv_without_relisting(monkeypatch):
    h = _Harness(
        monkeypatch,
        [
            ("yield", [_ev(uid="a", rv="7")]),  # clean close after one event
            ("yield", [_ev(uid="b", rv="8")]),
        ],
    )
    events = asyncio.run(_collect(PodWatcher(), 2))

    assert [p.metadata.uid for _, p in events] == ["a", "b"]
    # One LIST ever: the second (and any parked third) cycle resumed on rv.
    assert h.list_calls == 1
    assert h.stream_calls[1]["resource_version"] == "7"


def test_bookmark_advances_rv_and_is_not_forwarded(monkeypatch):
    h = _Harness(
        monkeypatch,
        [
            ("yield", [_bookmark("9")]),  # clean close after the bookmark
            ("yield", [_ev(uid="after", rv="10")]),
        ],
    )
    events = asyncio.run(_collect(PodWatcher(), 1))

    assert [p.metadata.uid for _, p in events] == ["after"]
    assert h.list_calls == 1
    assert h.stream_calls[1]["resource_version"] == "9"


def test_410_relists_immediately_without_backoff(monkeypatch, caplog):
    h = _Harness(
        monkeypatch,
        [
            ("raise", ApiException(status=410, reason="Gone")),
            ("yield", [_ev(uid="fresh")]),
        ],
        list_pods=[_pod("seeded")],
    )
    # A 5 s backoff would trip the 2 s collection timeout — its absence is
    # what makes this pass quickly.
    watcher = PodWatcher(retry_delay_s=5)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        events = asyncio.run(_collect(watcher, 3, timeout=2.0))

    # Seed replay from both LISTs plus the post-410 stream event.
    assert [p.metadata.uid for _, p in events] == ["seeded", "seeded", "fresh"]
    assert h.list_calls == 2
    assert any("k8s.watch_expired" in r.getMessage() for r in caplog.records)
    # 410 is a protocol signal, not a fault: no watch_error line.
    assert not any("k8s.watch_error" in r.getMessage() for r in caplog.records)


def test_generic_error_relists_after_backoff(monkeypatch, caplog):
    h = _Harness(
        monkeypatch,
        [("raise", RuntimeError("stream died")), ("yield", [_ev(uid="ok")])],
    )
    watcher = PodWatcher(retry_delay_s=0.01)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        events = asyncio.run(_collect(watcher, 1))

    assert [p.metadata.uid for _, p in events] == ["ok"]
    assert h.list_calls == 2  # error invalidates rv → full re-LIST
    warn = [r for r in caplog.records if "k8s.watch_error" in r.getMessage()]
    assert len(warn) == 1 and "fails=1" in warn[0].getMessage()


def test_failure_counter_survives_successful_lists(monkeypatch, caplog):
    """The old code reset the counter right after each successful LIST, so a
    LIST-succeeds/stream-fails pattern warned on every cycle; now only the
    first failure warns and the counter keeps growing until real progress."""
    h = _Harness(
        monkeypatch,
        [
            ("raise", RuntimeError("f1")),
            ("raise", RuntimeError("f2")),
            ("raise", RuntimeError("f3")),
            ("yield", [_ev(uid="works", rv="5")]),  # progress resets the counter
            ("raise", RuntimeError("f4")),
            ("yield", [_ev(uid="again")]),
        ],
    )
    watcher = PodWatcher(retry_delay_s=0.001)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        events = asyncio.run(_collect(watcher, 2))

    assert [p.metadata.uid for _, p in events] == ["works", "again"]
    warnings = [
        r for r in caplog.records
        if "k8s.watch_error" in r.getMessage() and r.levelno == logging.WARNING
    ]
    # fails 1 (first run) and fails 1 (after the reset) — never fails 2/3.
    assert [w.getMessage() for w in warnings if "fails=1" in w.getMessage()] == [
        w.getMessage() for w in warnings
    ]
    assert len(warnings) == 2
    ended = [r for r in caplog.records if "k8s.watch_ended" in r.getMessage()]
    assert len(ended) == 2  # f2 and f3, throttled to DEBUG


def test_periodic_resync_forces_relist(monkeypatch, caplog):
    h = _Harness(
        monkeypatch,
        [
            ("yield", [_ev(uid="a", rv="7")]),  # clean close
            ("yield", [_ev(uid="b", rv="8")]),  # after the resync re-LIST
        ],
        list_pods=[_pod("seeded")],
    )
    watcher = PodWatcher(resync_interval_s=0)  # every clean close is resync-due
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        events = asyncio.run(_collect(watcher, 4))

    # The resync replayed the LIST seed a second time before event b …
    assert [p.metadata.uid for _, p in events] == ["seeded", "a", "seeded", "b"]
    # … and the second stream started from the fresh LIST's rv, not event a's
    # rv=7 (the thread may already be into a further resync cycle by the time
    # we assert, so pin the second cycle rather than counting LISTs).
    assert h.list_calls >= 2
    assert h.stream_calls[1]["resource_version"] == "rv-list-2"
    assert any(
        "k8s.list_pods" in r.getMessage() and "reason=resync" in r.getMessage()
        for r in caplog.records
    )


def test_bounded_queue_drops_oldest_when_consumer_stalls(monkeypatch, caplog):
    h = _Harness(
        monkeypatch,
        [("yield", [_ev(uid="e1"), _ev(uid="e2"), _ev(uid="e3")])],
    )
    watcher = PodWatcher(queue_maxsize=2)

    async def _scenario():
        agen = watcher.events()
        try:
            # Let the producer run to exhaustion before consuming anything, so
            # all three offers hit the queue while it can hold only two.
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: h.exhausted.wait(timeout=5)
            )
            await asyncio.sleep(0.05)  # drain pending call_soon_threadsafe callbacks
            return [
                await asyncio.wait_for(agen.__anext__(), 2),
                await asyncio.wait_for(agen.__anext__(), 2),
            ]
        finally:
            await agen.aclose()

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        events = asyncio.run(_scenario())

    # e1 (the oldest) was dropped; newest state won.
    assert [p.metadata.uid for _, p in events] == ["e2", "e3"]
    dropped = [r for r in caplog.records if "k8s.watch_dropped" in r.getMessage()]
    assert len(dropped) == 1 and "dropped=1" in dropped[0].getMessage()


def test_consumer_close_stops_current_watch(monkeypatch):
    h = _Harness(monkeypatch, [("yield", [_ev(uid="only")])])
    watcher = PodWatcher()
    asyncio.run(_collect(watcher, 1))

    # The generator's finally called stop() on the current Watch (unblocking
    # a parked long-poll on newer client versions).
    assert any(w.stop_called for w in h.watches)


def test_non_dict_events_are_skipped(monkeypatch):
    h = _Harness(monkeypatch, [("yield", [None, _ev(uid="real")])])
    events = asyncio.run(_collect(PodWatcher(), 1))
    assert [p.metadata.uid for _, p in events] == ["real"]
