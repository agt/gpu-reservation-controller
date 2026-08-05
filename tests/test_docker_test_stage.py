"""Guards that the Docker `test` stage carries every file the suite reads.

The suite runs twice: from a checkout, where the whole repository is on disk,
and inside the Dockerfile's `test` stage, which COPYs an explicit subset.  A
test that reads a repo-root path missing from that subset therefore **passes
locally and fails only in CI** — and it fails as a confusing `FileNotFoundError`
during collection rather than as an assertion about the thing under test.

That is not hypothetical: `tests/test_chart_image.py` was added reading `helm/`
and `.github/`, both absent from the stage, and the whole build broke on it.
This guard is the check that would have caught it before the push.

**Limitation, stated rather than hidden:** it recognises the repo's convention of
building paths as `REPO_ROOT` divided by string literals.  A test that reaches
the filesystem some other way (a relative path, a computed string) escapes it.
Extending the convention is cheaper than making the guard clever.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DOCKERFILE = REPO_ROOT / "Dockerfile"
TESTS_DIR = REPO_ROOT / "tests"

# `REPO_ROOT / "helm" / "gpu-reservation-controller"` → captures `helm`.  Only the
# first segment matters: COPY brings a directory in wholesale.
_REPO_ROOT_PATH_RE = re.compile(r'REPO_ROOT\s*/\s*"([^"]+)"')

# `FROM python:3.13-slim AS final` — the boundary past which COPYs build the
# runtime image rather than the test one.
_FINAL_STAGE_RE = re.compile(r"^FROM\s+\S+\s+AS\s+final\b", re.IGNORECASE)


def _test_stage_copied_paths() -> set[str]:
    """Return the repo-root paths available inside the `test` stage.

    Everything before the `final` stage counts: `test` is `FROM deps`, so it
    inherits the requirements files `deps` copied.
    """
    copied: set[str] = set()
    for line in DOCKERFILE.read_text().splitlines():
        stripped = line.strip()
        if _FINAL_STAGE_RE.match(stripped):
            break
        if not stripped.startswith("COPY "):
            continue
        tokens = stripped[len("COPY ") :].split()
        if any(t.startswith("--from=") for t in tokens):
            continue
        # The last token is the destination; the rest are sources.
        for source in tokens[:-1]:
            copied.add(source.rstrip("/"))
    return copied


def _repo_root_paths_read_by_tests() -> dict[str, set[str]]:
    """Return ``{first path segment: {test files that read it}}``."""
    reads: dict[str, set[str]] = {}
    for path in sorted(TESTS_DIR.glob("*.py")):
        for match in _REPO_ROOT_PATH_RE.finditer(path.read_text()):
            segment = match.group(1).split("/")[0]
            reads.setdefault(segment, set()).add(path.name)
    return reads


def test_test_stage_copies_every_path_the_suite_reads() -> None:
    copied = _test_stage_copied_paths()
    reads = _repo_root_paths_read_by_tests()

    missing = {
        segment: sorted(sources)
        for segment, sources in sorted(reads.items())
        if segment not in copied
    }

    assert not missing, (
        "the Dockerfile's `test` stage does not COPY paths the suite reads, so "
        f"these tests pass locally and error in the image: {missing}. "
        "Add a COPY for each to the test stage."
    )


def test_the_guard_can_actually_see_both_sides() -> None:
    """Fail loudly if either half of the guard silently finds nothing.

    Both halves are regex-driven, so a refactor that renamed `REPO_ROOT` or
    reformatted the COPY lines would leave the test above passing vacuously —
    green, and checking nothing at all.  That is the failure mode a drift guard
    can least afford.
    """
    copied = _test_stage_copied_paths()
    reads = _repo_root_paths_read_by_tests()

    assert copied, "parsed no COPY sources from the Dockerfile's test stage"
    assert reads, "found no `REPO_ROOT / \"...\"` reads in tests/"

    # Anchors: files that have read these since well before this guard existed.
    assert "app" in copied and "tests" in copied
    assert "docs" in reads, "expected test_log_grammar.py to read docs/"
