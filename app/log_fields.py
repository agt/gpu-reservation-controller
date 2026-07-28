"""Render log message bodies as flat ``key=value`` fields.

Every log line in this repo is moving to a single grammar — an ``event=`` noun
followed by ``key=value`` pairs, one field per concept, with no parentheses,
slashes (``pod ns/name`` becomes ``ns=… pod=…``), arrows or ``a..b`` ranges
embedded in a value.  ``docs/LOG-FIELDS.md`` is the canonical field dictionary
and states the grammar in full; this module is the only thing that renders it.

The dictionary is shared with the reservation app, which keeps an identical copy
of both this module and that document — the same arrangement ``API.md`` /
``docs/RESERVATION-API.md`` already use.  The point of sharing it is that a line
from each side can be joined on the same key: ``rid``, ``poduid``, ``cid``.
Keep the two copies in step.

Call sites pass the fields as keyword arguments and let ``%s`` do the
interpolation, so nothing is formatted until the record is actually emitted::

    log.info("%s", kv(event="pod.admitted", ns=ns, pod=name, rid=res.id,
                      clabel=label, gpus=n, until=guarantee_end))

    # pod.admitted ns=jdoe pod=notebook-0 rid=42 clabel=h100 gpus=2 until=...

Why a helper rather than ``extra=`` plus a custom ``Formatter``: call sites stay
explicit and unit-testable, and third-party loggers that know nothing about the
scheme (uvicorn, httpx, the Kubernetes client) keep working through the same
handlers.

**This is also the sanitisation chokepoint.**  Values reaching a log line are
not all trustworthy — pod names, namespaces and annotation values come from
whoever created the pod — and a value containing a newline could otherwise forge
whole log entries.  :func:`scrub` drops non-printable characters from *every*
value, so a call site cannot forget to do it.
"""

from __future__ import annotations

import datetime
import re
from typing import Any

__all__ = ["kv", "scrub"]

# Characters that force a value to be quoted: without quoting they would break
# the parse into fields (space), the key/value split (=), or the quoting itself.
_NEEDS_QUOTING = (" ", "=", '"')

# Keys are developer-supplied literals, so anything outside this set is a typo
# rather than hostile input.  It is still normalised rather than raised on —
# logging must never be the thing that fails a sweep.
_KEY_OK = re.compile(r"[^A-Za-z0-9_.]")


def scrub(value: object, maxlen: int | None = None) -> str:
    """Return *value* as a string with non-printable characters removed.

    Newlines above all: the whole point is that no value can inject a line
    break and forge a log entry.  ``maxlen`` truncates the result when set.
    """
    cleaned = "".join(ch for ch in str(value) if ch.isprintable())
    return cleaned[:maxlen] if maxlen is not None else cleaned


def _render(value: Any) -> str:
    """Render one value to its canonical field form (before quoting)."""
    if isinstance(value, bool):
        # Lowercase, so the grammar reads the same as it does in JSON or YAML
        # rather than leaking Python's True/False.
        return "true" if value else "false"
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        # ISO-8601.  Every datetime in this repo is UTC-aware
        # (``datetime.now(timezone.utc)``), so the offset is always carried.
        return value.isoformat()
    if isinstance(value, float):
        # %g, so a risk score reads 0.33 rather than 0.33000000000000002.
        return f"{value:g}"
    if isinstance(value, (list, tuple)):
        return ",".join(_render(v) for v in value)
    if isinstance(value, (set, frozenset)):
        # Sorted so the same set renders identically on every call — an
        # unordered field makes log diffs and tests needlessly noisy.
        return ",".join(sorted(_render(v) for v in value))
    return str(value)


def kv(**fields: Any) -> str:
    """Render ``fields`` as space-separated ``key=value`` pairs.

    Conventions, all of them load-bearing for the grammar:

    * **Order is call order.**  Keyword arguments preserve insertion order, so
      the call site controls the field order; put ``event`` first.
    * **Absent means omitted.**  A ``None`` or empty value emits no field at
      all, rather than ``key=-`` or ``key=None``.  A key that is not in the
      line was not known — which is how a fail-open guard (a GPU class missing
      from the per-node feasibility map, say) stays distinguishable from one
      that measured zero.
    * **A trailing underscore is stripped from the key**, so Python keywords
      can be used as field names: ``class_="H100"`` emits ``class=H100``.
    * **Values are scrubbed, then quoted only if they need it** — a value
      containing a space, ``=`` or ``"`` is wrapped in double quotes with
      ``\\`` and ``"`` escaped.  Free text (``reason``, ``err``) therefore
      stays on one field without breaking the parse.
    """
    parts: list[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        key = _KEY_OK.sub("_", key.rstrip("_"))
        rendered = _render(value)
        if rendered == "":
            continue
        scrubbed = scrub(rendered)
        if scrubbed == "":
            # The value was non-empty but consisted entirely of characters we
            # refuse to emit.  Emit an empty field rather than dropping it, so
            # the attempt stays visible in the log.
            parts.append(f'{key}=""')
            continue
        if any(c in scrubbed for c in _NEEDS_QUOTING):
            escaped = scrubbed.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'{key}="{escaped}"')
        else:
            parts.append(f"{key}={scrubbed}")
    return " ".join(parts)
