"""Coverage for the key=value log-field renderer (``app/log_fields.py``).

The grammar these assert is stated in ``docs/LOG-FIELDS.md``; keep the two in
step. An identical suite lives at this same path in the sibling repo — the
module is duplicated there deliberately, so its behaviour must not drift.
"""

import datetime

import pytest

from app.log_fields import REDACTED, changes, kv, scrub


# ---------------------------------------------------------------------------
# Basic rendering
# ---------------------------------------------------------------------------

def test_fields_render_in_call_order():
    # Call order is the emission order, which is what lets a call site put
    # event= first.
    assert kv(event="reservation.created", id=42, uid=7) == "event=reservation.created id=42 uid=7"


def test_no_fields_renders_empty():
    assert kv() == ""


def test_trailing_underscore_is_stripped_from_key():
    # Lets Python keywords be used as field names.
    assert kv(class_="H100") == "class=H100"
    assert kv(from_="2026-08-01") == "from=2026-08-01"


def test_key_is_normalised_rather_than_raising():
    # A malformed key is a developer typo, but logging must never be the thing
    # that fails a request — so it is normalised, not raised on.
    assert kv(**{"bad key": 1}) == "bad_key=1"
    assert kv(**{"a=b": 1}) == "a_b=1"


# ---------------------------------------------------------------------------
# Absent means omitted
# ---------------------------------------------------------------------------

def test_none_is_omitted_entirely():
    # Not `gid=None`, not `gid=-`: the key simply is not on the line.
    assert kv(uid=7, gid=None, cid=2) == "uid=7 cid=2"


def test_empty_string_is_omitted():
    assert kv(uid=7, user="") == "uid=7"


def test_empty_collection_is_omitted():
    assert kv(uid=7, ids=[]) == "uid=7"


def test_zero_and_false_are_kept():
    # A measured zero is not an absent value — this is the distinction that
    # keeps a fail-open guard readable.
    assert kv(buckets=0, privileged=False) == "buckets=0 privileged=false"


# ---------------------------------------------------------------------------
# Value types
# ---------------------------------------------------------------------------

def test_bool_renders_lowercase():
    assert kv(waived=True, privileged=False) == "waived=true privileged=false"


def test_float_uses_compact_g_format():
    assert kv(su=12.5) == "su=12.5"
    assert kv(su=0.1 + 0.2) == "su=0.3"  # not 0.30000000000000004
    assert kv(su=100.0) == "su=100"


def test_int_renders_bare():
    assert kv(gpus=2) == "gpus=2"


def test_datetime_renders_iso8601_with_offset():
    aware = datetime.datetime(2026, 7, 28, 14, 0, tzinfo=datetime.UTC)
    assert kv(start=aware) == "start=2026-07-28T14:00:00+00:00"


def test_naive_datetime_renders_without_offset():
    # Documented behaviour, not an accident: call sites attach tzinfo. Asserted
    # so a change to that is a deliberate one.
    naive = datetime.datetime(2026, 7, 28, 14, 0)
    assert kv(start=naive) == "start=2026-07-28T14:00:00"


def test_date_renders_iso8601():
    assert kv(from_=datetime.date(2026, 9, 21)) == "from=2026-09-21"


def test_list_is_comma_joined_without_spaces():
    assert kv(ids=[3, 7, 9]) == "ids=3,7,9"


def test_tuple_is_comma_joined():
    assert kv(ids=(3, 7)) == "ids=3,7"


def test_set_is_sorted_for_stable_output():
    # An unordered field would make log diffs and tests needlessly noisy.
    assert kv(allowed={"ucsd.edu", "example.edu"}) == "allowed=example.edu,ucsd.edu"


# ---------------------------------------------------------------------------
# Quoting
# ---------------------------------------------------------------------------

def test_value_with_space_is_quoted():
    assert kv(detail="Group is not active") == 'detail="Group is not active"'


def test_value_with_equals_is_quoted():
    # Otherwise the field would split at the wrong place.
    assert kv(err="HTTP 409: gpus=4 unavailable") == 'err="HTTP 409: gpus=4 unavailable"'


def test_embedded_quote_is_escaped():
    assert kv(detail='say "hi"') == 'detail="say \\"hi\\""'


def test_backslash_is_escaped_before_quotes():
    assert kv(path=r"C:\dir name") == 'path="C:\\\\dir name"'


def test_plain_value_is_not_quoted():
    assert kv(user="jdoe") == "user=jdoe"


# ---------------------------------------------------------------------------
# Sanitisation — the reason this is a chokepoint and not a formatting nicety
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hostile", [
    "jdoe\nWARNING forged line",
    "jdoe\r\nWARNING forged line",
    "jdoe\x00admin",
    "jdoe\x1b[31m",
])
def test_no_value_can_inject_a_line_break_or_control_character(hostile):
    rendered = kv(user=hostile)
    assert "\n" not in rendered
    assert "\r" not in rendered
    assert "\x00" not in rendered
    assert "\x1b" not in rendered


def test_scrubbing_happens_before_quoting():
    # The scrubbed value has no space left, so it must not be quoted either.
    assert kv(user="jdoe\n") == "user=jdoe"


def test_value_that_scrubs_to_nothing_emits_an_empty_field():
    # It was non-empty on the way in, so dropping it silently would hide the
    # attempt; an empty field keeps it visible.
    assert kv(user="\n\r\x00") == 'user=""'


def test_scrub_drops_non_printable_characters():
    assert scrub("a\nb\tc") == "abc"


def test_scrub_truncates_when_maxlen_given():
    assert scrub("abcdef", maxlen=3) == "abc"


def test_scrub_does_not_truncate_by_default():
    # Exception text and denial details are long and must survive intact.
    assert scrub("x" * 500) == "x" * 500


# ---------------------------------------------------------------------------
# The rendered line parses back
# ---------------------------------------------------------------------------

def test_rendered_line_round_trips_through_a_naive_parser():
    import shlex

    line = kv(
        event="reservation.denied",
        status=409,
        gid=12,
        group="cse151b",
        detail="Not enough GPUs available (need 4)",
        gpus=4,
    )
    parsed = dict(token.split("=", 1) for token in shlex.split(line))
    assert parsed == {
        "event": "reservation.denied",
        "status": "409",
        "gid": "12",
        "group": "cse151b",
        "detail": "Not enough GPUs available (need 4)",
        "gpus": "4",
    }


# ---------------------------------------------------------------------------
# changes() — the chg= / old.<f> / new.<f> expansion
# ---------------------------------------------------------------------------

def test_changes_expands_to_chg_plus_old_new_pairs():
    assert kv(event="user.updated", id=7, **changes({"role": ("user", "admin")})) == (
        "event=user.updated id=7 chg=role old.role=user new.role=admin"
    )


def test_changes_lists_every_changed_field_in_chg():
    line = kv(**changes({"role": ("user", "admin"), "is_active": (True, False)}))
    assert line.startswith("chg=role,is_active ")


def test_redacted_field_appears_in_chg_with_no_value():
    # This one rule replaces the older `password changed` / `[field changed]`
    # conventions — the field is named, the value never rendered.
    line = kv(event="user.updated", **changes({"password": REDACTED}))
    assert line == "event=user.updated chg=password"


def test_redacted_and_normal_fields_coexist():
    line = kv(**changes({"email": ("a@x", "b@x"), "password": REDACTED}))
    assert line == "chg=email,password old.email=a@x new.email=b@x"


def test_empty_change_map_contributes_nothing():
    # A no-op update is a line with no chg= field, not the literal "no-op".
    assert kv(event="user.updated", id=7, **changes({})) == "event=user.updated id=7"


def test_none_inside_a_change_pair_renders_as_null():
    # The deliberate exception to "absent means omitted": inside a change pair
    # the value *is* the information, so unset->set must show both sides.
    assert kv(**changes({"notes": (None, "hi")})) == "chg=notes old.notes=null new.notes=hi"
    assert kv(**changes({"notes": ("hi", None)})) == "chg=notes old.notes=hi new.notes=null"


def test_change_values_are_rendered_and_quoted_like_any_other_field():
    line = kv(**changes({"is_active": (True, False), "name": ("a b", "c")}))
    assert "old.is_active=true new.is_active=false" in line
    assert 'old.name="a b" new.name=c' in line
