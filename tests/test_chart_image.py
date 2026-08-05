"""Guards the Helm chart's default image reference.

`helm` is not a test dependency, so these parse the chart's YAML rather than
rendering it.  That is narrower than `helm template` but covers the defect these
tests exist for: the chart shipped an *unqualified* `repository`, which Docker
resolves to ``docker.io/library/gpu-reservation-controller`` — a registry the
CI workflow does not push to and where no such image exists.  Nothing caught it
because nothing here reads the chart at all.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
CHART_DIR = REPO_ROOT / "helm" / "gpu-reservation-controller"
VALUES = CHART_DIR / "values.yaml"
DEPLOYMENT = CHART_DIR / "templates" / "deployment.yaml"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "docker.yml"


@pytest.fixture(scope="module")
def values() -> dict:
    return yaml.safe_load(VALUES.read_text())


def test_image_repository_is_registry_qualified(values: dict) -> None:
    """The default repository must name a registry host.

    Docker treats a single-segment name as a Docker Hub official image, so a
    bare ``gpu-reservation-controller`` silently becomes
    ``docker.io/library/gpu-reservation-controller``.  The failure surfaces at
    pull time as a confusing "not found" against the wrong registry.
    """
    repository = values["image"]["repository"]
    host = repository.split("/")[0]
    assert "." in host or ":" in host or host == "localhost", (
        f"image.repository {repository!r} is not registry-qualified; Docker will "
        f"resolve it to docker.io/library/{repository}"
    )


def test_image_repository_matches_what_ci_publishes(values: dict) -> None:
    """The chart's registry must be the one the workflow pushes to.

    The workflow publishes to ``${REGISTRY}/${IMAGE_NAME}``; a chart pointing at
    a different registry is the same defect as an unqualified name, just harder
    to see.
    """
    workflow = yaml.safe_load(WORKFLOW.read_text())
    registry = workflow["env"]["REGISTRY"]

    repository = values["image"]["repository"]
    assert repository.startswith(f"{registry}/"), (
        f"image.repository {repository!r} does not start with the registry the "
        f"workflow pushes to ({registry!r})"
    )


def test_floating_tag_is_paired_with_always_pull(values: dict) -> None:
    """A floating tag needs `pullPolicy: Always` to be upgradable at all.

    With ``IfNotPresent`` a node that already holds a ``latest`` layer never
    re-pulls, so ``helm upgrade`` generates no pod-spec change and rolls
    nothing — the deployment silently stays on the old image.  Pinning an
    immutable tag instead is fine, and then ``IfNotPresent`` is correct.
    """
    tag = str(values["image"]["tag"])
    policy = values["image"]["pullPolicy"]

    if tag in {"latest", "main", "nightly", ""}:
        assert policy == "Always", (
            f"image.tag {tag!r} is floating, so image.pullPolicy must be Always "
            f"(is {policy!r}); otherwise `helm upgrade` cannot roll a new build"
        )


def test_workflow_publishes_the_tag_the_chart_requests(values: dict) -> None:
    """The chart's default tag must be one the workflow actually produces.

    ``docker/metadata-action``'s ``flavor.latest=auto`` emits ``latest`` only for
    a semver git-tag push, and this repository pushes none — so ``latest`` has to
    be requested explicitly.  Without it the chart's default referred to an image
    that was never published, independently of the registry problem above.
    """
    tag = str(values["image"]["tag"])
    if tag != "latest":
        pytest.skip("chart does not default to `latest`")

    text = WORKFLOW.read_text()
    assert re.search(r"type=raw,value=latest\b", text), (
        "the chart defaults to `latest` but the workflow's metadata-action does "
        "not emit a `latest` tag; add `type=raw,value=latest,enable={{is_default_branch}}`"
    )


def test_deployment_wires_image_pull_secrets() -> None:
    """A private-registry reference needs a credential path.

    GHCR packages are private by default, so a chart that names a `ghcr.io`
    image but renders no `imagePullSecrets` cannot pull it — and there would be
    no way to supply one without editing the template.
    """
    assert "imagePullSecrets" in yaml.safe_load(VALUES.read_text()), (
        "values.yaml declares no imagePullSecrets key"
    )
    assert "imagePullSecrets" in DEPLOYMENT.read_text(), (
        "deployment.yaml never renders imagePullSecrets, so the values key has "
        "no effect"
    )
