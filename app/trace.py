"""Per-unit-of-work trace ids, propagated to the reservation app.

The app mints a trace id once per browser page load and stamps it on every log
line of the resulting request (see its ``_RequestLoggingMiddleware``), which is
what makes a multi-step UI flow reconstructible. This module gives the
controller the same thing for *its* units of work — one fetch cycle, one queue
tick, one preemption sweep, one JIT admission batch, one pod event — and then
carries that id across the process boundary.

Why it is worth having: ``rid`` and ``poduid`` already join an app line to a
controller line *about the same object*. A trace joins the lines of one
**operation**, including the ones with no object in common. When a JIT admission
batch grants three leases, the controller's ``lease.granted`` lines and the
three ``reservation.lease_created`` lines the app writes all carry the same
``trace=jit-…`` — so "what did that batch do, on both sides" is one grep instead
of a reconstruction from timestamps.

Propagation is one-directional per call, and needs no cooperation from the app:
the outbound ``X-Client-Trace`` header is exactly the header the app's middleware
already reads, so a controller-initiated request lands in the app's log under the
controller's trace with no app-side change. Inbound works the same way in
reverse — :func:`adopt` takes the header off a request the app makes to *us*, so
a pushed reservation update is logged under the app's trace.

Not to be confused with distributed tracing: there are no spans, no parent/child
relationships and no sampling. It is one opaque correlation id, chosen to fit the
``key=value`` log grammar (see ``docs/LOG-FIELDS.md``).
"""

from __future__ import annotations

import contextvars
import re
import secrets
from contextlib import contextmanager
from typing import Iterator, Optional

__all__ = ["TRACE_HEADER", "adopt", "current", "new_id", "outbound_headers", "scope"]

#: The header the app's request middleware reads. Must match on both sides.
TRACE_HEADER = "X-Client-Trace"

#: The app **discards** any trace that does not match this, falling back to "-",
#: so a value we mint here has to satisfy it or the correlation silently breaks
#: at the far end. Kept identical to the app's ``_TRACE_RE`` deliberately.
_TRACE_RE = re.compile(r"^[A-Za-z0-9_-]{1,36}$")

#: Rendered as the ``trace=`` field of every log line. "-" when no unit of work
#: is in scope — the same "not known" convention the app uses, and the only
#: place besides ``actor=`` where a ``-`` may appear (a formatter cannot omit a
#: field, unlike ``kv()``).
_req_trace: contextvars.ContextVar[str] = contextvars.ContextVar("req_trace", default="-")

# Long enough to not collide across a log retention window, short enough to stay
# readable next to a prefix and well inside the app's 36-character cap.
_SUFFIX_BYTES = 5


def new_id(prefix: str) -> str:
    """Mint a trace id like ``sweep-1a2b3c4d5e``.

    The prefix names the kind of work, so the id is legible on its own: a line
    carrying ``trace=jit-…`` came from an admission batch without needing to
    look at anything else. It is truncated if necessary so the result always
    satisfies the app's whitelist.
    """
    suffix = secrets.token_hex(_SUFFIX_BYTES)
    cleaned = re.sub(r"[^A-Za-z0-9_]", "", prefix)[: 36 - len(suffix) - 1] or "work"
    return f"{cleaned}-{suffix}"


def current() -> str:
    """The trace id in scope, or ``"-"``."""
    return _req_trace.get()


def adopt(raw: Optional[str]) -> Optional[str]:
    """Validate an inbound header value, returning it or ``None``.

    The value arrives from the network, so it is whitelist-matched rather than
    escaped: it is interpolated straight into a log line by the formatter, which
    cannot decide to quote it, and a crafted value containing a newline could
    otherwise forge whole log entries. Anything that does not match is dropped
    in favour of a locally-minted id.
    """
    if raw and _TRACE_RE.match(raw):
        return raw
    return None


@contextmanager
def scope(prefix: str, inbound: Optional[str] = None) -> Iterator[str]:
    """Bind a trace id for the duration of one unit of work.

    Adopts *inbound* when it is a valid header value, else mints one from
    *prefix*. Yields the id in force, and restores the previous value on exit so
    nested or sequential scopes cannot leak into one another.

    Each background loop runs as its own asyncio task, and a task gets a copy of
    the context at creation, so a trace set inside one loop's iteration is
    invisible to the other loops. Tasks spawned *within* a scope inherit it,
    which is what carries the id into the per-pod work a tick fans out.
    """
    trace_id = adopt(inbound) or new_id(prefix)
    token = _req_trace.set(trace_id)
    try:
        yield trace_id
    finally:
        _req_trace.reset(token)


def outbound_headers() -> dict[str, str]:
    """Headers to add to a request to the reservation app.

    Empty when no trace is in scope, so the app sees an absent header and logs
    ``trace=-`` rather than the literal ``"-"`` being echoed back as an id.
    """
    trace_id = current()
    return {TRACE_HEADER: trace_id} if trace_id != "-" else {}
