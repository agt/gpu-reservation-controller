"""Unit tests for the pure (no-I/O) helper functions in k8s_client.py.

All tests use SimpleNamespace to build minimal mock pod objects — no real
Kubernetes client or cluster is needed.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

import pytest

from app.k8s_client import (
    get_pod_gpu_count,
    get_pod_min_runtime_seconds,
    get_pod_phase,
    pod_has_toleration,
)


# ---------------------------------------------------------------------------
# Builders for mock pod objects
# ---------------------------------------------------------------------------


def _container(gpu_count: Optional[int] = None) -> SimpleNamespace:
    requests = {}
    if gpu_count is not None:
        requests["nvidia.com/gpu"] = str(gpu_count)
    return SimpleNamespace(resources=SimpleNamespace(requests=requests))


def _container_no_resources() -> SimpleNamespace:
    return SimpleNamespace(resources=None)


def _toleration(
    key: str = "gpu-class-reservation",
    value: str = "h100",
    effect: str = "NoSchedule",
) -> SimpleNamespace:
    return SimpleNamespace(key=key, value=value, effect=effect)


def _pod(
    phase: Optional[str] = "Running",
    containers: Optional[list] = None,
    tolerations: Optional[list] = None,
    annotations: Optional[dict] = None,
    namespace: str = "test-ns",
    name: str = "test-pod",
) -> SimpleNamespace:
    """Build a minimal mock pod.

    ``phase=None`` → ``pod.status`` is ``None`` (status object absent).
    Pass ``phase=""`` to get a status object whose phase is an empty string.
    """
    if phase is None:
        status = None
    else:
        status = SimpleNamespace(phase=phase)
    return SimpleNamespace(
        metadata=SimpleNamespace(
            namespace=namespace,
            name=name,
            annotations=annotations,
        ),
        status=status,
        spec=SimpleNamespace(
            containers=containers if containers is not None else [],
            tolerations=tolerations,
        ),
    )


# ---------------------------------------------------------------------------
# get_pod_phase
# ---------------------------------------------------------------------------


class TestGetPodPhase:
    def test_returns_phase_string(self):
        assert get_pod_phase(_pod(phase="Running")) == "Running"

    def test_no_status_returns_empty_string(self):
        assert get_pod_phase(_pod(phase=None)) == ""

    def test_phase_none_returns_empty_string(self):
        pod = SimpleNamespace(status=SimpleNamespace(phase=None))
        assert get_pod_phase(pod) == ""

    def test_phase_empty_string_returns_empty_string(self):
        pod = SimpleNamespace(status=SimpleNamespace(phase=""))
        assert get_pod_phase(pod) == ""

    @pytest.mark.parametrize("phase", ["Pending", "Succeeded", "Failed", "Unknown"])
    def test_various_phases(self, phase: str):
        assert get_pod_phase(_pod(phase=phase)) == phase


# ---------------------------------------------------------------------------
# get_pod_gpu_count
# ---------------------------------------------------------------------------


class TestGetPodGpuCount:
    def test_single_container(self):
        assert get_pod_gpu_count(_pod(containers=[_container(2)])) == 2

    def test_multi_container_sums_gpus(self):
        assert get_pod_gpu_count(_pod(containers=[_container(1), _container(3)])) == 4

    def test_no_gpu_request_returns_zero(self):
        assert get_pod_gpu_count(_pod(containers=[_container(None)])) == 0

    def test_no_containers_returns_zero(self):
        assert get_pod_gpu_count(_pod(containers=[])) == 0

    def test_containers_none_returns_zero(self):
        pod = SimpleNamespace(spec=SimpleNamespace(containers=None))
        assert get_pod_gpu_count(pod) == 0

    def test_no_resources_returns_zero(self):
        assert get_pod_gpu_count(_pod(containers=[_container_no_resources()])) == 0

    def test_non_integer_value_silently_ignored(self):
        bad = SimpleNamespace(resources=SimpleNamespace(requests={"nvidia.com/gpu": "two"}))
        assert get_pod_gpu_count(_pod(containers=[bad])) == 0

    def test_mixed_containers(self):
        """Valid GPU requests are summed; containers without resources contribute 0."""
        pod = _pod(containers=[
            _container(2),
            _container_no_resources(),
            _container(1),
        ])
        assert get_pod_gpu_count(pod) == 3


# ---------------------------------------------------------------------------
# get_pod_min_runtime_seconds
# ---------------------------------------------------------------------------


class TestGetPodMinRuntimeSeconds:
    def test_valid_annotation(self):
        pod = _pod(annotations={"galends/minimum-runtime-seconds": "3600"})
        assert get_pod_min_runtime_seconds(pod) == 3600

    def test_minimum_positive_value(self):
        pod = _pod(annotations={"galends/minimum-runtime-seconds": "1"})
        assert get_pod_min_runtime_seconds(pod) == 1

    def test_missing_annotation_returns_none(self):
        pod = _pod(annotations={})
        assert get_pod_min_runtime_seconds(pod) is None

    def test_no_annotations_object_returns_none(self):
        pod = _pod(annotations=None)
        assert get_pod_min_runtime_seconds(pod) is None

    def test_non_integer_value_returns_none(self):
        pod = _pod(annotations={"galends/minimum-runtime-seconds": "abc"})
        assert get_pod_min_runtime_seconds(pod) is None

    def test_zero_returns_none(self):
        pod = _pod(annotations={"galends/minimum-runtime-seconds": "0"})
        assert get_pod_min_runtime_seconds(pod) is None

    def test_negative_returns_none(self):
        pod = _pod(annotations={"galends/minimum-runtime-seconds": "-100"})
        assert get_pod_min_runtime_seconds(pod) is None


# ---------------------------------------------------------------------------
# pod_has_toleration
# ---------------------------------------------------------------------------


class TestPodHasToleration:
    def test_exact_match_returns_true(self):
        pod = _pod(tolerations=[_toleration()])
        assert pod_has_toleration(pod, "gpu-class-reservation", "h100", "NoSchedule") is True

    def test_key_mismatch_returns_false(self):
        pod = _pod(tolerations=[_toleration(key="other-key")])
        assert pod_has_toleration(pod, "gpu-class-reservation", "h100", "NoSchedule") is False

    def test_value_mismatch_returns_false(self):
        pod = _pod(tolerations=[_toleration(value="a100")])
        assert pod_has_toleration(pod, "gpu-class-reservation", "h100", "NoSchedule") is False

    def test_effect_mismatch_returns_false(self):
        pod = _pod(tolerations=[_toleration(effect="PreferNoSchedule")])
        assert pod_has_toleration(pod, "gpu-class-reservation", "h100", "NoSchedule") is False

    def test_empty_tolerations_list_returns_false(self):
        pod = _pod(tolerations=[])
        assert pod_has_toleration(pod, "gpu-class-reservation", "h100", "NoSchedule") is False

    def test_none_tolerations_returns_false(self):
        pod = _pod(tolerations=None)
        assert pod_has_toleration(pod, "gpu-class-reservation", "h100", "NoSchedule") is False

    def test_finds_match_among_multiple(self):
        pod = _pod(tolerations=[
            _toleration(key="unrelated"),
            _toleration(),  # the matching one
        ])
        assert pod_has_toleration(pod, "gpu-class-reservation", "h100", "NoSchedule") is True
