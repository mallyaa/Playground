"""Tests for simulator."""

from pathlib import Path

from flakeguard.config import FlakeguardConfig, WarehouseSize
from flakeguard.dag_parser import build_dag
from flakeguard.simulator import run_scaling_simulation, SimulationRow


def test_run_scaling_simulation() -> None:
    manifest = {
        "nodes": {
            "model.pkg.a": {"name": "a", "depends_on": {"nodes": []}, "config": {}},
            "model.pkg.b": {"name": "b", "depends_on": {"nodes": ["model.pkg.a"]}, "config": {}},
        }
    }
    G = build_dag(manifest)
    config = FlakeguardConfig(project_path=Path(__file__).parent, warehouse=WarehouseSize.M, default_execution_seconds=30.0)
    rows = run_scaling_simulation(G, config, concurrency=2)
    assert len(rows) >= 1
    for r in rows:
        assert isinstance(r, SimulationRow)
        assert r.num_models == 2
        assert r.total_runtime_seconds > 0
        assert r.total_credits > 0
