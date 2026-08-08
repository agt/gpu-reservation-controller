"""Tests for the k8s_client node/pod capacity snapshots.

Covers ``snapshot_node_gpu_capacity`` (per-class totals), its per-node primitive
``snapshot_node_gpu_inventory``, and the ``node_name`` capture added to
``snapshot_tolerated_pods``.  Uses SimpleNamespace stubs and a fake ``_core_v1``
(no real Kubernetes client), matching the ``monkeypatch`` / ``asyncio.run``
convention used for the other async k8s_client wrappers.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import app.k8s_client as k8s_client

TAINT_KEY = "gpu-class-reservation"


def _taint(key: str, value: str) -> SimpleNamespace:
    return SimpleNamespace(key=key, value=value)


def _node(
    name: str,
    *,
    taints: list | None = None,
    allocatable: dict | None = None,
    unschedulable: bool = False,
    deleting: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            deletion_timestamp="2024-01-01T00:00:00Z" if deleting else None,
        ),
        spec=SimpleNamespace(taints=taints, unschedulable=unschedulable),
        status=SimpleNamespace(allocatable=allocatable),
    )


class _FakeCoreV1:
    def __init__(self, nodes: list):
        self._nodes = nodes

    def list_node(self):
        return SimpleNamespace(items=self._nodes)


def _run_snapshot(monkeypatch, nodes: list) -> dict[str, int]:
    monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes))
    return asyncio.run(k8s_client.snapshot_node_gpu_capacity(TAINT_KEY))


class TestSnapshotNodeGpuCapacity:
    def test_sums_allocatable_by_taint_value(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
            _node(
                "n2",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {"h100": 8}

    def test_multiple_classes_bucketed_separately(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
            _node(
                "n2",
                taints=[_taint(TAINT_KEY, "a100")],
                allocatable={"nvidia.com/gpu": "2"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {"h100": 4, "a100": 2}

    def test_node_without_taint_ignored(self, monkeypatch):
        nodes = [
            _node("n1", taints=[], allocatable={"nvidia.com/gpu": "4"}),
            _node("n2", taints=None, allocatable={"nvidia.com/gpu": "4"}),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_other_taint_keys_ignored(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint("some-other-taint", "x")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_unschedulable_node_excluded(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
                unschedulable=True,
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_deleting_node_excluded(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
                deleting=True,
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_missing_allocatable_treated_as_zero(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")], allocatable={}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")], allocatable=None),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {"h100": 0}

    def test_garbage_allocatable_treated_as_zero(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "not-a-number"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {"h100": 0}

    def test_empty_taint_value_ignored(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_no_nodes(self, monkeypatch):
        assert _run_snapshot(monkeypatch, []) == {}


# ---------------------------------------------------------------------------
# snapshot_node_gpu_inventory — the per-node primitive
# ---------------------------------------------------------------------------


def _run_inventory(monkeypatch, nodes: list) -> dict[str, dict[str, int]]:
    monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes))
    return asyncio.run(k8s_client.snapshot_node_gpu_inventory(TAINT_KEY))


class TestSnapshotNodeGpuInventory:
    def test_per_node_breakdown(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "4"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "8"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 4, "n2": 8}}

    def test_multiple_classes_bucketed_by_node(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "4"}),
            _node("n2", taints=[_taint(TAINT_KEY, "a100")], allocatable={"nvidia.com/gpu": "2"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {
            "h100": {"n1": 4},
            "a100": {"n2": 2},
        }

    def test_unschedulable_and_deleting_nodes_excluded(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "4"}, unschedulable=True),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "4"}, deleting=True),
            _node("n3", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "4"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n3": 4}}

    def test_garbage_and_missing_allocatable_treated_as_zero(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "not-a-number"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")], allocatable={}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 0, "n2": 0}}

    def test_untainted_node_ignored(self, monkeypatch):
        nodes = [_node("n1", taints=[], allocatable={"nvidia.com/gpu": "4"})]
        assert _run_inventory(monkeypatch, nodes) == {}

    def test_capacity_is_the_per_class_sum_of_the_inventory(self, monkeypatch):
        """``snapshot_node_gpu_capacity`` must equal the collapsed inventory."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "4"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "8"}),
            _node("n3", taints=[_taint(TAINT_KEY, "a100")], allocatable={"nvidia.com/gpu": "2"}),
        ]
        inventory = _run_inventory(monkeypatch, nodes)
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {
            gpu_class: sum(per_node.values())
            for gpu_class, per_node in inventory.items()
        }
        assert capacity == {"h100": 12, "a100": 2}


# ---------------------------------------------------------------------------
# snapshot_tolerated_pods — node_name capture
# ---------------------------------------------------------------------------


class _FakeCoreV1Pods:
    def __init__(self, pods: list):
        self._pods = pods

    def list_pod_for_all_namespaces(self, label_selector=None):
        return SimpleNamespace(items=self._pods)


def _tolerated_pod(
    name: str, *, uid: str, node_name, gpu_class: str = "h100", gpu_count: int = 1,
    annotations: dict | None = None
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            namespace="alice",
            name=name,
            uid=uid,
            labels={"gpu-class": gpu_class},
            annotations=annotations or {"galends/booking-reference": "res-1"},
            deletion_timestamp=None,
        ),
        status=SimpleNamespace(phase="Running", conditions=None),
        spec=SimpleNamespace(
            node_name=node_name,
            tolerations=[
                SimpleNamespace(key=TAINT_KEY, value=gpu_class, effect="NoSchedule")
            ],
            containers=[
                SimpleNamespace(
                    resources=SimpleNamespace(
                        requests={"nvidia.com/gpu": str(gpu_count)}
                    )
                )
            ],
        ),
    )


class TestSnapshotToleratedPodsNodeName:
    def test_captures_node_name_and_none_when_unscheduled(self, monkeypatch):
        pods = [
            _tolerated_pod("p1", uid="u1", node_name="n1"),
            _tolerated_pod("p2", uid="u2", node_name=None),  # scheduled nowhere yet
        ]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1Pods(pods))
        out = asyncio.run(k8s_client.snapshot_tolerated_pods(TAINT_KEY))
        by_uid = {p.uid: p for p in out}
        assert by_uid["u1"].node_name == "n1"
        assert by_uid["u2"].node_name is None

    def test_captures_termination_warning_annotations(self, monkeypatch):
        pods = [
            _tolerated_pod(
                "p1", uid="u1", node_name="n1",
                annotations={
                    "galends/booking-reference": "res-1",
                    "galends/termination-warning-at": "2024-01-15T10:00:00Z",
                    "galends/termination-warning-risk": "0.50",
                },
            ),
            _tolerated_pod("p2", uid="u2", node_name="n1"),  # no warning annotations
        ]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1Pods(pods))
        out = asyncio.run(k8s_client.snapshot_tolerated_pods(TAINT_KEY))
        by_uid = {p.uid: p for p in out}
        assert by_uid["u1"].termination_warning_at == "2024-01-15T10:00:00Z"
        assert by_uid["u1"].termination_warning_risk == "0.50"
        assert by_uid["u2"].termination_warning_at is None
        assert by_uid["u2"].termination_warning_risk is None
