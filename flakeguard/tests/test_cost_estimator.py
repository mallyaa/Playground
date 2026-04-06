"""Tests for cost_estimator."""

from pathlib import Path

import networkx as nx

from flakeguard.config import FlakeguardConfig, WarehouseSize
from flakeguard.cost_estimator import (
    cost_per_model,
    compute_dag_costs,
    top_expensive_models,
)
from flakeguard.dag_parser import build_dag


def test_cost_per_model() -> None:
    # 3600 sec = 1 hour -> 1 * credits_per_hour
    assert cost_per_model(3600.0, WarehouseSize.XS) == 1.0
    assert cost_per_model(3600.0, WarehouseSize.M) == 4.0
    assert cost_per_model(1800.0, WarehouseSize.M) == 2.0


def test_compute_dag_costs() -> None:
    manifest = {
        "nodes": {
            "model.pkg.a": {"name": "a", "depends_on": {"nodes": []}, "config": {}},
            "model.pkg.b": {"name": "b", "depends_on": {"nodes": ["model.pkg.a"]}, "config": {}},
        }
    }
    G = build_dag(manifest)
    config = FlakeguardConfig(project_path=Path(__file__).parent, warehouse=WarehouseSize.M, default_execution_seconds=60.0)
    cost_per, downstream, total = compute_dag_costs(G, config)
    assert len(cost_per) == 2
    assert total == cost_per["model.pkg.a"] + cost_per["model.pkg.b"]
    assert downstream["model.pkg.a"] >= downstream["model.pkg.b"]
    assert downstream["model.pkg.a"] == cost_per["model.pkg.a"] + cost_per["model.pkg.b"]


def test_top_expensive_models() -> None:
    G = nx.DiGraph()
    G.add_node("m1", name="m1")
    G.add_node("m2", name="m2")
    cost = {"m1": 10.0, "m2": 5.0}
    top = top_expensive_models(cost, G, top_n=2)
    assert top[0][2] == 10.0
    assert top[1][2] == 5.0
