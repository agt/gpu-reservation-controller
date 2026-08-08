"""Enforce the key=value log grammar across every call site in ``app/``.

This is the mechanism that stops the grammar decaying. ``docs/LOG-FIELDS.md``
states the rules and ``app/log_fields.py`` renders them, but neither prevents the
next feature from writing ``log.info("did a thing for %s", user)`` and quietly
reintroducing prose — which is exactly how the controller's ``OBSERVABILITY.md``
drifted ~40 log points out of date before this suite existed.

The checks walk the AST rather than running the code, so they cover call sites no
test happens to execute. That matters: a malformed ``kv()`` call raises only when
its record is actually emitted, and the rarely-hit error branches are precisely
the ones nobody exercises.

An identical suite lives at this same path in the sibling repo — keep them in
step along with ``app/log_fields.py`` and ``docs/LOG-FIELDS.md``.
"""

import ast
import pathlib
import re

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_APP_DIR = _REPO_ROOT / "app"
_DICTIONARY = _REPO_ROOT / "docs" / "LOG-FIELDS.md"
_OBSERVABILITY = _REPO_ROOT / "OBSERVABILITY.md"

_LEVELS = {"info", "warning", "error", "debug", "exception", "critical"}
_LOGGER_NAMES = {"log", "logger"}

# Logging kwargs a call site may legitimately pass alongside the rendered body.
_ALLOWED_LOG_KWARGS = {"exc_info", "stacklevel"}

# Every event in this repo is a literal. (The app allows exactly one dynamic
# event, in ``_log_booking_denial``, which renders one of two nouns.)
_DYNAMIC_EVENT_ALLOWED: set[str] = set()


def _iter_log_calls():
    """Yield (relative_path, ast.Call) for every ``log.<level>(...)`` in app/."""
    for path in sorted(_APP_DIR.rglob("*.py")):
        rel = path.relative_to(_REPO_ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr not in _LEVELS:
                continue
            if getattr(func.value, "id", None) not in _LOGGER_NAMES:
                continue
            yield rel, node


def _call_ids():
    return [f"{rel}:{node.lineno}" for rel, node in _iter_log_calls()]


_LOG_CALLS = list(_iter_log_calls())
_CALL_IDS = _call_ids()


def test_the_walker_finds_the_call_sites():
    """Guard against the AST walk silently matching nothing.

    Every check below is vacuously true on an empty list, so a refactor that
    renamed the module-level ``log`` would turn this suite green while enforcing
    nothing at all.
    """
    assert len(_LOG_CALLS) > 50


@pytest.mark.parametrize("rel,node", _LOG_CALLS, ids=_CALL_IDS)
def test_every_log_call_renders_through_kv(rel, node):
    """The body must be ``("%s", kv(...))`` — no prose, no %-interpolation."""
    assert len(node.args) == 2, (
        "expected exactly two positional args: the literal \"%s\" and a kv(...) call"
    )
    fmt, payload = node.args
    assert isinstance(fmt, ast.Constant) and fmt.value == "%s", (
        'first argument must be the literal "%s"; the message body belongs in kv()'
    )
    assert isinstance(payload, ast.Call) and getattr(payload.func, "id", None) == "kv", (
        "second argument must be a kv(...) call"
    )
    assert not payload.args, "kv() takes keyword arguments only"
    for kw in node.keywords:
        assert kw.arg in _ALLOWED_LOG_KWARGS, f"unexpected logging kwarg {kw.arg!r}"


@pytest.mark.parametrize("rel,node", _LOG_CALLS, ids=_CALL_IDS)
def test_event_is_the_first_field_and_is_a_literal(rel, node):
    """``event=`` leads every line and names the thing that happened."""
    payload = node.args[1]
    keywords = [kw for kw in payload.keywords if kw.arg is not None]
    assert keywords, "kv() must carry at least event="
    first = keywords[0]
    assert first.arg == "event", f"event= must be the first field, got {first.arg!r}"

    if isinstance(first.value, ast.Constant):
        event = first.value.value
        assert re.fullmatch(r"[a-z0-9_]+\.[a-z0-9_]+", event), (
            f"event {event!r} must be lower_snake noun.verb"
        )
    else:
        assert rel in _DYNAMIC_EVENT_ALLOWED, (
            f"event= must be a literal (dynamic events are only allowed in "
            f"{sorted(_DYNAMIC_EVENT_ALLOWED)})"
        )


def _documented_keys() -> set[str]:
    """Field names named in the dictionary's tables (first cell of each row)."""
    keys: set[str] = set()
    for line in _DICTIONARY.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| `"):
            continue
        first_cell = line.split("|")[1]
        keys.update(re.findall(r"`([^`]+)`", first_cell))
    return keys


def _emitted_keys():
    """Yield (key, "file:line") for every field name any call site emits."""
    for rel, node in _LOG_CALLS:
        payload = node.args[1]
        for kw in payload.keywords:
            if kw.arg is not None:
                yield kw.arg.rstrip("_"), f"{rel}:{node.lineno}"
                continue
            # **changes({...}) contributes chg= plus old./new. pairs; **{"a": b}
            # is the literal-key escape hatch for dotted names.
            value = kw.value
            if isinstance(value, ast.Call) and getattr(value.func, "id", None) == "changes":
                yield "chg", f"{rel}:{node.lineno}"
            elif isinstance(value, ast.Dict):
                for k in value.keys:
                    if isinstance(k, ast.Constant):
                        yield k.value, f"{rel}:{node.lineno}"


def test_every_emitted_field_is_in_the_dictionary():
    """A new field must be added to docs/LOG-FIELDS.md, not just to the code.

    Two keys for one concept is the failure mode the dictionary exists to
    prevent, and it can only do that if it is complete.
    """
    documented = _documented_keys()
    undocumented: dict[str, str] = {}
    for key, where in _emitted_keys():
        if key == "event":
            continue
        # old.<field> / new.<field> are documented as a pattern, not per field.
        probe = re.sub(r"^(old|new)\..+$", r"\1.<field>", key)
        if probe not in documented:
            undocumented.setdefault(key, where)
    assert not undocumented, (
        "undocumented log fields (add them to docs/LOG-FIELDS.md §4): "
        + ", ".join(f"{k} ({w})" for k, w in sorted(undocumented.items()))
    )


def test_dictionary_and_helper_are_present():
    """The shared files exist where the cross-repo convention says they do.

    Presence only. Whether they still *match* the sibling repo is enforced by
    ``tests/test_shared_artifacts.py`` against ``docs/SHARED-ARTIFACTS.sha256`` —
    this assertion used to carry that name and check nothing, which is how the
    dictionary drifted in both directions at once.
    """
    assert _DICTIONARY.is_file()
    assert (_APP_DIR / "log_fields.py").is_file()


def _emitted_events() -> dict[str, str]:
    """Map every literal ``event=`` value to the first call site that emits it."""
    events: dict[str, str] = {}
    for rel, node in _LOG_CALLS:
        payload = node.args[1]
        first = next((kw for kw in payload.keywords if kw.arg == "event"), None)
        if first is None or not isinstance(first.value, ast.Constant):
            continue
        events.setdefault(first.value.value, f"{rel}:{node.lineno}")
    return events


def test_every_event_is_in_the_observability_doc():
    """A new log point must be added to OBSERVABILITY.md, not just to the code.

    This is the check that exists because the doc drifted ~40 log points out of
    date once already. Adding a log line and forgetting the doc now fails here
    rather than silently rotting.
    """
    doc = _OBSERVABILITY.read_text(encoding="utf-8")
    missing = {
        event: where
        for event, where in _emitted_events().items()
        if f"`{event}`" not in doc
    }
    assert not missing, (
        "log events missing from OBSERVABILITY.md: "
        + ", ".join(f"{e} ({w})" for e, w in sorted(missing.items()))
    )


def test_the_observability_doc_describes_no_events_that_no_longer_exist():
    """The reverse direction: a removed log point must leave the doc too."""
    doc = _OBSERVABILITY.read_text(encoding="utf-8")
    documented = set(re.findall(r"`([a-z0-9_]+\.[a-z0-9_]+)`", doc))
    emitted = set(_emitted_events())
    # The doc also names non-event dotted tokens (module paths, `app.log`), so
    # only flag ones that look like events and are not emitted anywhere.
    stale = {
        d for d in documented - emitted
        if not d.endswith((".py", ".md", ".log", ".json"))
        and not d.startswith(("app.", "docs.", "tests.", "galends."))
    }
    assert not stale, f"OBSERVABILITY.md documents events that are not emitted: {sorted(stale)}"
