"""Render log message bodies as flat ``key=value`` fields.

Every log line in this repo is moving to a single grammar — an ``event=`` noun
followed by ``key=value`` pairs, one field per concept, with no parentheses,
slashes, arrows or ``a..b`` ranges embedded in a value.  ``docs/LOG-FIELDS.md``
is the canonical field dictionary and states the grammar in full; this module is
the only thing that renders it.

Call sites pass the fields as keyword arguments and let ``%s`` do the
interpolation, so nothing is formatted until the record is actually emitted::

    log.info("%s", kv(event="reservation.created", id=r.id, uid=u.id,
                      user=u.username, gid=g.id, gpus=n, su=cost))

    # reservation.created id=42 uid=7 user=jdoe gid=3 gpus=2 su=12.5

Why a helper rather than ``extra=`` plus a custom ``Formatter``: call sites stay
explicit and unit-testable, and third-party loggers that know nothing about the
scheme (uvicorn, httpx) keep working through the same handlers.

**This is also the sanitisation chokepoint.**  Many values reaching a log line
are attacker-influenced — submitted usernames, OAuth identities, SAML NameIDs,
SICAD roster names — and a value containing a newline could otherwise forge
whole log entries.  :func:`scrub` drops non-printable characters from *every*
value, so a call site cannot forget to do it.
"""

from __future__ import annotations

import datetime
import re
from typing import Any

__all__ = ["kv", "scrub", "changes", "REDACTED"]

#: Marker for a field that changed but whose value must never be logged —
#: passwords, Quill-managed HTML, the inline logo, SMTP credentials.  It appears
#: in ``chg=`` like any other changed field but contributes no ``old.``/``new.``
#: pair, which is the one rule replacing the older ``password changed`` and
#: ``[field changed]`` conventions.
REDACTED = object()

# Characters that force a value to be quoted: without quoting they would break
# the parse into fields (space), the key/value split (=), or the quoting itself.
_NEEDS_QUOTING = (" ", "=", '"')

# Keys are developer-supplied literals, so anything outside this set is a typo
# rather than hostile input.  It is still normalised rather than raised on —
# logging must never be the thing that fails a request.
_KEY_OK = re.compile(r"[^A-Za-z0-9_.]")


def scrub(value: object, maxlen: int | None = None) -> str:
    """Return *value* as a string with non-printable characters removed.

    Newlines above all: the whole point is that no value can inject a line
    break and forge a log entry.  ``maxlen`` truncates the result when set
    (used for the fixed-width actor prefix, which must not be unbounded).
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
        # ISO-8601.  A tz-aware datetime carries its offset; a naive one does
        # not, so call sites should attach tzinfo (in this repo, via
        # ``timezone_utils.site_tz()``) before logging a wall-clock value.
        return value.isoformat()
    if isinstance(value, float):
        # %g, matching the compact form the SU fields already used, and without
        # float repr artefacts like 12.500000000000002.
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
      line was not known.
    * **A trailing underscore is stripped from the key**, so Python keywords
      can be used as field names: ``class_="H100"`` emits ``class=H100`` and
      ``from_=d`` emits ``from=d``.
    * **Values are scrubbed, then quoted only if they need it** — a value
      containing a space, ``=`` or ``"`` is wrapped in double quotes with
      ``\\`` and ``"`` escaped.  Free text (``reason``, ``err``, ``detail``)
      therefore stays on one field without breaking the parse.
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


def changes(pairs: dict) -> dict:
    """Expand a field-change map into ``chg=`` plus ``old.``/``new.`` fields.

    *pairs* maps a field name to an ``(old, new)`` tuple, or to :data:`REDACTED`
    for a field whose value must not be logged.  The result is meant to be
    splatted into :func:`kv`::

        log.info("%s", kv(event="user.updated", id=u.id, **changes({
            "role": ("user", "admin"),
            "password": REDACTED,
        })))
        # user.updated id=7 chg=role,password old.role=user new.role=admin

    An empty map contributes nothing at all, so a no-op update is a line with no
    ``chg=`` field rather than the literal ``no-op`` the prose format used.

    ``None`` renders as ``null`` here, deliberately breaking :func:`kv`'s
    omit-the-absent rule: inside a change pair the value *is* the information, so
    a field going from unset to set must show both sides. Everywhere else an
    absent value means "not known" and is dropped.
    """
    out: dict = {"chg": list(pairs)}
    for field, change in pairs.items():
        if change is REDACTED:
            continue
        old, new = change
        out[f"old.{field}"] = "null" if old is None else old
        out[f"new.{field}"] = "null" if new is None else new
    return out
