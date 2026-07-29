"""Coverage for per-unit-of-work trace ids and their propagation to the app.

Two of these assert things that are not obvious from reading the code and would
break silently: that the id actually reaches an outbound request's headers (via
an httpx event hook, not a call site), and that an inbound header is adopted
across a FastAPI ``yield`` dependency into the endpoint's own log records.
"""

import asyncio
import logging

import httpx
import pytest

from app import trace
from app.log_fields import kv


# ---------------------------------------------------------------------------
# Minting and shape
# ---------------------------------------------------------------------------

def test_no_scope_reports_the_not_known_sentinel():
    # "-" is the documented "not known" value for the two envelope fields; a
    # formatter cannot omit a field the way kv() omits an absent one.
    assert trace.current() == "-"


def test_scope_binds_and_restores():
    with trace.scope("sweep") as tid:
        assert trace.current() == tid
    assert trace.current() == "-"


def test_nested_scopes_restore_the_outer_id():
    with trace.scope("queue") as outer:
        with trace.scope("jit") as inner:
            assert inner != outer
            assert trace.current() == inner
        assert trace.current() == outer


def test_id_carries_its_prefix_so_a_line_says_what_produced_it():
    with trace.scope("sweep") as tid:
        assert tid.startswith("sweep-")


def test_ids_are_unique_per_scope():
    ids = set()
    for _ in range(200):
        with trace.scope("pod") as tid:
            ids.add(tid)
    assert len(ids) == 200


@pytest.mark.parametrize("prefix", ["fetch", "queue", "sweep", "audit", "jit", "pod", "startup"])
def test_every_minted_id_satisfies_the_apps_whitelist(prefix):
    """The app silently discards a trace that fails its regex.

    That is the failure mode this guards: propagation would look fine from here
    while the far end logged `trace=-`, and nothing would report it.
    """
    for _ in range(50):
        assert trace._TRACE_RE.match(trace.new_id(prefix)), prefix


def test_a_hostile_prefix_cannot_produce_an_invalid_id():
    tid = trace.new_id("evil id\nwith spaces=and\"quotes")
    assert trace._TRACE_RE.match(tid)


def test_an_absurd_prefix_is_truncated_within_the_cap():
    tid = trace.new_id("x" * 200)
    assert len(tid) <= 36
    assert trace._TRACE_RE.match(tid)


def test_an_empty_prefix_falls_back_rather_than_producing_a_bare_suffix():
    tid = trace.new_id("!!!")
    assert tid.startswith("work-")


# ---------------------------------------------------------------------------
# Adopting an inbound header
# ---------------------------------------------------------------------------

def test_a_wellformed_inbound_id_is_adopted_verbatim():
    with trace.scope("push", inbound="a3f9c2d1b0e7") as tid:
        assert tid == "a3f9c2d1b0e7"


@pytest.mark.parametrize("hostile", [
    "evil\nWARNING forged line",   # the reason this is whitelisted, not escaped
    "evil\r\nfake",
    "has spaces",
    'quote"inside',
    "equals=inside",
    "x" * 37,                      # over the cap
    "",
    None,
])
def test_a_hostile_inbound_id_is_dropped_for_a_local_one(hostile):
    assert trace.adopt(hostile) is None
    with trace.scope("push", inbound=hostile) as tid:
        assert tid.startswith("push-")
        assert trace._TRACE_RE.match(tid)


# ---------------------------------------------------------------------------
# Outbound propagation
# ---------------------------------------------------------------------------

def test_no_header_is_sent_when_no_work_is_in_scope():
    # Otherwise the literal "-" would be echoed to the app as if it were an id.
    assert trace.outbound_headers() == {}


def test_header_is_sent_inside_a_scope():
    with trace.scope("fetch") as tid:
        assert trace.outbound_headers() == {trace.TRACE_HEADER: tid}


def test_the_event_hook_stamps_the_header_on_a_real_request():
    """The hook is the chokepoint — no call site passes the header itself."""
    from app.reservation_client import _attach_trace

    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["trace"] = request.headers.get(trace.TRACE_HEADER)
        return httpx.Response(200, json={})

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://app",
            event_hooks={"request": [_attach_trace]},
        ) as client:
            with trace.scope("jit") as tid:
                await client.get("/api/reservations")
                return tid

    tid = asyncio.run(run())
    assert seen["trace"] == tid


def test_the_event_hook_sends_nothing_outside_a_scope():
    from app.reservation_client import _attach_trace

    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["trace"] = request.headers.get(trace.TRACE_HEADER)
        return httpx.Response(200, json={})

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://app",
            event_hooks={"request": [_attach_trace]},
        ) as client:
            await client.get("/api/reservations")

    asyncio.run(run())
    assert seen["trace"] is None


# ---------------------------------------------------------------------------
# The id reaches the log record
# ---------------------------------------------------------------------------

def test_the_filter_stamps_the_in_scope_id_onto_a_record(caplog, monkeypatch):
    # app.main builds its Config at import; the same env shim other suites use.
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-trace")
    from app.main import _trace_filter

    caplog.handler.addFilter(_trace_filter)
    try:
        with caplog.at_level(logging.INFO, logger="app.main"):
            log = logging.getLogger("app.main")
            with trace.scope("sweep") as tid:
                log.info("%s", kv(event="preempt.boundary"))
            log.info("%s", kv(event="shutdown.complete"))
    finally:
        caplog.handler.removeFilter(_trace_filter)

    in_scope = next(r for r in caplog.records if "preempt.boundary" in r.getMessage())
    out_of_scope = next(r for r in caplog.records if "shutdown.complete" in r.getMessage())
    assert in_scope.req_trace == tid
    # Stamped at emit time, so leaving the scope does not retroactively change it.
    assert out_of_scope.req_trace == "-"


def test_concurrent_tasks_do_not_share_a_trace():
    """Each background loop is its own task, so its trace must be its own.

    A ContextVar copy per task is what makes this true; asserting it here means
    a refactor that hoisted the scope out of the loop body would be caught.
    """
    seen: list[tuple[str, str]] = []

    async def unit(prefix: str) -> None:
        with trace.scope(prefix) as tid:
            await asyncio.sleep(0)          # force interleaving
            seen.append((prefix, trace.current()))
            assert trace.current() == tid

    async def run():
        await asyncio.gather(*(unit(p) for p in ("fetch", "queue", "sweep", "audit")))

    asyncio.run(run())
    assert len(seen) == 4
    assert len({tid for _, tid in seen}) == 4
    for prefix, tid in seen:
        assert tid.startswith(f"{prefix}-")


# ---------------------------------------------------------------------------
# Inbound adoption, end to end through the HTTP stack
# ---------------------------------------------------------------------------
#
# These exercise the one claim that reasoning cannot settle: that a ContextVar
# set inside a FastAPI ``yield`` dependency is still in force when the endpoint
# body runs (same task, so the context is shared). If that were untrue the
# feature would fail silently — the header would be accepted and then ignored.

def _push_main(monkeypatch, token="secret"):
    """Import ``app.main`` with the lifespan's I/O stubbed out."""
    import dataclasses

    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-trace")
    import app.main as main

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(main, "init_k8s", lambda *a, **k: None)
    monkeypatch.setattr(main, "_refresh_reservations", _noop)
    monkeypatch.setattr(main, "reservation_fetch_loop", _noop)
    monkeypatch.setattr(main, "pod_watch_loop", _noop)
    monkeypatch.setattr(main, "queue_processor_loop", _noop)
    monkeypatch.setattr(main, "preemption_loop", _noop)
    monkeypatch.setattr(main, "capacity_audit_loop", _noop)
    orig = main.app.state.config
    monkeypatch.setattr(
        main.app.state, "config", dataclasses.replace(orig, inbound_api_token=token)
    )
    return main


def _push_and_capture_trace(monkeypatch, caplog, headers):
    """POST an empty push and return the trace stamped on its log record."""
    from fastapi.testclient import TestClient

    main = _push_main(monkeypatch)
    caplog.handler.addFilter(main._trace_filter)
    try:
        with caplog.at_level(logging.INFO, logger="app.main"):
            with TestClient(main.app) as client:
                resp = client.post(
                    "/api/reservations/push",
                    json={"reservations": []},
                    headers={"Authorization": "Bearer secret", **headers},
                )
        assert resp.status_code == 200, resp.text
    finally:
        caplog.handler.removeFilter(main._trace_filter)

    applied = [r for r in caplog.records if "event=push.applied" in r.getMessage()]
    assert applied, "the push endpoint must log push.applied"
    return applied[0].req_trace


def test_inbound_trace_reaches_the_endpoints_log_record(monkeypatch, caplog):
    got = _push_and_capture_trace(
        monkeypatch, caplog, {trace.TRACE_HEADER: "a3f9c2d1b0e7"}
    )
    assert got == "a3f9c2d1b0e7"


def test_a_push_without_a_trace_header_gets_a_locally_minted_one(monkeypatch, caplog):
    got = _push_and_capture_trace(monkeypatch, caplog, {})
    assert got.startswith("push-")
    assert trace._TRACE_RE.match(got)


def test_a_hostile_inbound_trace_header_never_reaches_a_log_record(monkeypatch, caplog):
    got = _push_and_capture_trace(
        monkeypatch, caplog, {trace.TRACE_HEADER: "evil-but-way-too-long" * 5}
    )
    assert got.startswith("push-")
    assert trace._TRACE_RE.match(got)


def test_the_scope_does_not_leak_between_requests(monkeypatch, caplog):
    first = _push_and_capture_trace(monkeypatch, caplog, {trace.TRACE_HEADER: "aaaa1111"})
    caplog.clear()
    second = _push_and_capture_trace(monkeypatch, caplog, {})
    assert first == "aaaa1111"
    assert second != "aaaa1111"
    # And nothing is left bound in this test's own context.
    assert trace.current() == "-"
