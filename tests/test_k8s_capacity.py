"""Tests for k8s_client.snapshot_node_gpu_capacity.

Uses SimpleNamespace node stubs and a fake ``_core_v1.list_node`` (no real
Kubernetes client), matching the ``monkeypatch`` / ``asyncio.run`` convention
used for the other async k8s_client wrappers.
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
