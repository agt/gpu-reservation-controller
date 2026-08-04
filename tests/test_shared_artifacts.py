"""Fail the build when a cross-repo shared artifact is edited on one side only.

Four files are maintained as byte-identical copies in ``gpu-reservation-app`` and
``gpu-reservation-controller``: ``app/log_fields.py``, ``docs/LOG-FIELDS.md``,
the reservation API contract, and ``SCHEDULING.md``.

The convention used to be prose plus a test named
``test_dictionary_and_helper_are_in_sync_with_the_sibling_repo`` that asserted
only that the local files existed — its docstring conceded the byte comparison
was "a review-time obligation". It was not met: all three documents had drifted,
one of them for 23 days while describing app features that never existed.

The sibling checkout is not available here, so this suite cannot diff the two
repos directly. What it *can* do is make editing a shared file impossible to do
silently: the manifest pins each file's hash, so any edit fails this test until
the author runs ``--update``, and that manifest diff is the reviewer-visible
signal that a sibling PR is owed. See ``docs/SHARED-ARTIFACTS.md``.
"""

import pathlib
import subprocess
import sys

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "scripts" / "sync_shared_artifacts.py"

sys.path.insert(0, str(_REPO_ROOT / "scripts"))
import sync_shared_artifacts as sync  # noqa: E402


def test_shared_artifacts_match_the_manifest():
    """Every shared copy hashes to the value recorded in the manifest.

    A failure here means one of the four files changed. That is allowed — but it
    has to be carried to the sibling repo in the same change, which is what the
    ``--update`` step and its manifest diff force you to acknowledge.
    """
    live, saved = sync.current(), sync.recorded()
    assert saved, "docs/SHARED-ARTIFACTS.sha256 is missing or empty"
    drifted = {n: (saved.get(n), h) for n, h in live.items() if saved.get(n) != h}
    assert not drifted, (
        "shared artifact(s) edited without updating the manifest: "
        + ", ".join(sorted(drifted))
        + "\nRun: python scripts/sync_shared_artifacts.py --update, then copy the "
        "file AND the manifest into the sibling repo (docs/SHARED-ARTIFACTS.md)."
    )


def test_manifest_covers_exactly_the_declared_artifacts():
    """The manifest neither misses a shared file nor pins a retired one."""
    assert set(sync.recorded()) == set(sync._ARTIFACTS), (
        "docs/SHARED-ARTIFACTS.sha256 is out of step with _ARTIFACTS in "
        "scripts/sync_shared_artifacts.py — regenerate it with --update"
    )


def test_every_declared_artifact_exists():
    """A shared file cannot be moved or deleted without updating the mapping."""
    missing = [rel for rel in sync._ARTIFACTS.values() if not (_REPO_ROOT / rel).is_file()]
    assert not missing, f"declared shared artifact(s) not on disk: {missing}"


def test_check_mode_exits_zero_when_in_sync():
    """The CLI the docs tell people to run agrees with the assertions above."""
    result = subprocess.run(
        [sys.executable, str(_SCRIPT), "--check"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_check_mode_detects_a_one_sided_edit(tmp_path, monkeypatch):
    """The guard actually fires — the property the old test never had.

    Rewriting one artifact's recorded hash stands in for the real failure mode:
    a shared file edited in this repo and nowhere else.
    """
    monkeypatch.setattr(
        sync, "recorded", lambda: {**sync.current(), "LOG-FIELDS.md": "0" * 64}
    )
    assert sync.check() == 1
