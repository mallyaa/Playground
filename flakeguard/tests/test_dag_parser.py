"""Tests for dag_parser."""

from pathlib import Path

import pytest  # type: ignore[reportMissingImports]

from flakeguard.dag_parser import (
    build_dag,
    get_critical_path_length,
    get_fan_in_out_metrics,
    get_topological_order,
    load_manifest,
)


def test_load_manifest_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        load_manifest(Path("/nonexistent/manifest.json"))


def test_load_manifest_missing_nodes(tmp_path: Path) -> None:
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text('{"metadata": {}}')
    with pytest.raises(ValueError, match="nodes"):
        load_manifest(manifest_file)


def test_build_dag_and_topological_order() -> None:
    manifest = {
        "nodes": {
            "model.pkg.a": {"name": "a", "depends_on": {"nodes": []}, "config": {}},
            "model.pkg.b": {"name": "b", "depends_on": {"nodes": ["model.pkg.a"]}, "config": {}},
            "model.pkg.c": {"name": "c", "depends_on": {"nodes": ["model.pkg.a"]}, "config": {}},
        }
    }
    G = build_dag(manifest)
    assert G.number_of_nodes() == 3
    assert G.number_of_edges() == 2
    order = get_topological_order(G)
    assert "model.pkg.a" in order
    idx_a = order.index("model.pkg.a")
    assert order.index("model.pkg.b") > idx_a
    assert order.index("model.pkg.c") > idx_a


def test_fan_in_out() -> None:
    manifest = {
        "nodes": {
            "model.pkg.a": {"name": "a", "depends_on": {"nodes": []}, "config": {}},
            "model.pkg.b": {"name": "b", "depends_on": {"nodes": ["model.pkg.a"]}, "config": {}},
            "model.pkg.c": {"name": "c", "depends_on": {"nodes": ["model.pkg.a"]}, "config": {}},
        }
    }
    G = build_dag(manifest)
    fan = get_fan_in_out_metrics(G)
    assert fan["model.pkg.a"]["fan_in"] == 0
    assert fan["model.pkg.a"]["fan_out"] == 2
    assert fan["model.pkg.b"]["fan_in"] == 1
    assert fan["model.pkg.b"]["fan_out"] == 0


def test_critical_path_length() -> None:
    manifest = {
        "nodes": {
            "model.pkg.a": {"name": "a", "depends_on": {"nodes": []}, "config": {}},
            "model.pkg.b": {"name": "b", "depends_on": {"nodes": ["model.pkg.a"]}, "config": {}},
            "model.pkg.c": {"name": "c", "depends_on": {"nodes": ["model.pkg.b"]}, "config": {}},
        }
    }
    G = build_dag(manifest)
    assert get_critical_path_length(G) == 2  # a -> b -> c = 2 edges


def test_build_dag_ignores_non_models() -> None:
    manifest = {
        "nodes": {
            "model.pkg.a": {"name": "a", "depends_on": {"nodes": []}, "config": {}},
            "test.pkg.t1": {"name": "t1", "depends_on": {"nodes": ["model.pkg.a"]}, "config": {}},
        }
    }
    G = build_dag(manifest)
    assert G.number_of_nodes() == 1
    assert "model.pkg.a" in G
    assert "test.pkg.t1" not in G
