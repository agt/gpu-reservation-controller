"""Guards that the compiled lockfiles stay in step with the requirements files.

The Dockerfile installs from ``requirements.lock`` / ``requirements-dev.lock``
rather than the loose ``requirements*.txt``, so a dependency added to a
requirements file but never compiled into its lock is simply *absent* from every
built image — and from this suite, which runs inside that image.  That is the
drift these tests exist to catch.

Deliberately **offline**: everything here parses files.  No network, no resolver,
no index — so it runs unchanged inside the Docker ``test`` stage, which has no
credentials and should not be reaching out to PyPI anyway.

Equally deliberately, this is *not* a "recompile and diff" check.  That would
fail the build every time any transitive dependency published a release, which
trains people to ignore it; name coverage catches the mistake that actually
happens without the false positives.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

PROD_REQUIREMENTS = REPO_ROOT / "requirements.txt"
DEV_REQUIREMENTS = REPO_ROOT / "requirements-dev.txt"
PROD_LOCK = REPO_ROOT / "requirements.lock"
DEV_LOCK = REPO_ROOT / "requirements-dev.lock"

# A pinned lock entry: `name==version` at the start of a line.  Continuation
# lines (the `# via ...` provenance comments uv emits) are indented, and so are
# excluded by the leading-anchor alone.
_LOCK_ENTRY_RE = re.compile(r"^([A-Za-z0-9._-]+)==([^\s;]+)")

# The distribution name at the head of a requirement, before any extras
# (`uvicorn[standard]`) or specifier (`>=0.30`).
_REQUIREMENT_NAME_RE = re.compile(r"^([A-Za-z0-9._-]+)")


def _canonicalize(name: str) -> str:
    """Normalize a distribution name per PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _read_lock(path: Path) -> dict[str, str]:
    """Return ``{canonical name: version}`` for a compiled lockfile."""
    pins: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if not line or line[0].isspace() or line.startswith("#"):
            continue
        match = _LOCK_ENTRY_RE.match(line)
        assert match is not None, f"{path.name}: unparseable entry {line!r}"
        pins[_canonicalize(match.group(1))] = match.group(2)
    return pins


def _read_requirements(path: Path) -> set[str]:
    """Return the canonical names a requirements file declares directly.

    ``-r``/``-c`` includes and other flags are skipped: this is the file's *own*
    top-level requirements, not the transitive closure.
    """
    names: set[str] = set()
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        match = _REQUIREMENT_NAME_RE.match(line)
        assert match is not None, f"{path.name}: unparseable requirement {raw!r}"
        names.add(_canonicalize(match.group(1)))
    return names


PAIRS = [
    pytest.param(PROD_REQUIREMENTS, PROD_LOCK, id="prod"),
    pytest.param(DEV_REQUIREMENTS, DEV_LOCK, id="dev"),
]


@pytest.mark.parametrize("requirements,lock", PAIRS)
def test_lockfile_exists_and_is_fully_pinned(requirements: Path, lock: Path) -> None:
    """Every lock entry is an exact `==` pin, never a range."""
    assert lock.exists(), f"{lock.name} is missing; regenerate it with uv pip compile"

    pins = _read_lock(lock)
    assert pins, f"{lock.name} pins nothing"

    # `_read_lock` already rejects a non-`==` entry via its regex assert, so this
    # re-states the guarantee against the parsed result: a range that somehow got
    # through would show up as a version containing a comparison operator.
    for name, version in pins.items():
        assert not set(version) & set("<>=!~*"), (
            f"{lock.name}: {name} is pinned to {version!r}, which is not an exact version"
        )


@pytest.mark.parametrize("requirements,lock", PAIRS)
def test_every_declared_requirement_is_locked(requirements: Path, lock: Path) -> None:
    """A dependency added to a requirements file must be compiled into its lock.

    This is the drift that actually happens, and it is silent: the image is built
    from the lock, so an uncompiled dependency is simply missing at runtime.
    """
    declared = _read_requirements(requirements)
    pins = _read_lock(lock)

    missing = sorted(declared - pins.keys())
    assert not missing, (
        f"{requirements.name} declares {missing} but {lock.name} does not pin them. "
        f"Regenerate the locks (see the comment in the Dockerfile's deps stage)."
    )


def test_dev_lock_is_a_superset_of_the_prod_lock() -> None:
    """The dev lock must contain every prod pin at the *same* version.

    ``requirements-dev.lock`` is compiled with ``requirements.lock`` as a
    constraint, which is what makes this structural rather than a coincidence of
    the two having been compiled at the same moment.  Asserting it here is what
    catches someone regenerating one lock and not the other: the `deps` stage
    installs only the dev lock, so a prod pin that drifted there would be tested
    at one version and shipped at another — exactly the split the locks exist to
    close.
    """
    prod = _read_lock(PROD_LOCK)
    dev = _read_lock(DEV_LOCK)

    missing = sorted(prod.keys() - dev.keys())
    assert not missing, f"requirements-dev.lock is missing prod pins: {missing}"

    mismatched = {
        name: (version, dev[name])
        for name, version in prod.items()
        if dev[name] != version
    }
    assert not mismatched, (
        f"requirements-dev.lock disagrees with requirements.lock on {mismatched} "
        f"(prod, dev). Recompile the dev lock with `-c requirements.lock`."
    )


def test_dev_requirements_still_include_prod() -> None:
    """`requirements-dev.txt` must keep including `requirements.txt`.

    The Dockerfile's `deps` stage installs the dev lock *alone* and relies on it
    covering the runtime dependencies too.  Drop the `-r` line and the dev lock
    quietly stops being a superset, so the test stage would run without the
    packages the application imports.
    """
    lines = [
        line.strip()
        for line in DEV_REQUIREMENTS.read_text().splitlines()
        if line.strip()
    ]
    assert "-r requirements.txt" in lines, (
        "requirements-dev.txt must include `-r requirements.txt`; the Dockerfile's "
        "deps stage installs the dev lock alone and depends on it being a superset."
    )


@pytest.mark.parametrize("requirements,lock", PAIRS)
def test_lockfile_records_the_command_that_generated_it(
    requirements: Path, lock: Path
) -> None:
    """The uv header must survive, naming the source file.

    It is the only in-file record of how to regenerate the lock, and its absence
    is the signature of a lock that was hand-edited rather than compiled.
    """
    header = lock.read_text().split("\n", 3)[:3]
    joined = "\n".join(header)
    assert "uv pip compile" in joined, f"{lock.name} has lost its uv provenance header"
    assert requirements.name in joined, (
        f"{lock.name}'s header does not name {requirements.name}; it may have been "
        f"compiled from the wrong source file"
    )
