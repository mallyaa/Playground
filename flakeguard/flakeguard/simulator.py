"""Warehouse scaling simulator: runtime and cost vs warehouse size and concurrency."""

from dataclasses import dataclass
from typing import Any

import networkx as nx

from flakeguard.config import CREDITS_PER_HOUR, FlakeguardConfig, WarehouseSize


@dataclass
class SimulationRow:
    """Single row of simulation: one warehouse size outcome."""

    warehouse: str
    total_runtime_seconds: float
    total_credits: float
    concurrency: int
    num_models: int


def _default_execution_times(G: nx.DiGraph, config: FlakeguardConfig) -> dict[str, float]:
    """Map node_id -> execution_time_seconds from config."""
    return {
        n: config.get_execution_time(G.nodes[n].get("name", n))
        for n in G
    }


def _critical_path_runtime(
    G: nx.DiGraph,
    execution_times: dict[str, float],
) -> float:
    """Total runtime if we run in topological order with one worker (critical path time)."""
    order = list(nx.topological_sort(G))
    if not order:
        return 0.0
    # Simple model: each node runs after its deps. Runtime = max over paths of sum of times.
    # Longest path weight (node weight = execution time).
    try:
        path = nx.dag_longest_path(G)
        return sum(execution_times.get(n, 0.0) for n in path)
    except (nx.NetworkXError, nx.NetworkXNotImplemented):
        return sum(execution_times.values())


def _simulate_sequential_runtime(
    G: nx.DiGraph,
    execution_times: dict[str, float],
) -> float:
    """Total runtime if all models run sequentially (sum of all execution times)."""
    return sum(execution_times.get(n, 0.0) for n in G)


def _simulate_concurrent_runtime(
    G: nx.DiGraph,
    execution_times: dict[str, float],
    concurrency: int,
) -> float:
    """Rough simulation: total runtime with limited concurrency.

    Simplified: we assume tasks in topological order, and we run up to `concurrency`
    tasks at a time when their deps are done. This is approximated by:
    total_work / concurrency bounded below by critical_path_time.
    """
    total_work = sum(execution_times.get(n, 0.0) for n in G)
    critical = _critical_path_runtime(G, execution_times)
    if concurrency <= 0:
        concurrency = 1
    parallel_estimate = total_work / concurrency
    return max(parallel_estimate, critical)


def run_scaling_simulation(
    G: nx.DiGraph,
    config: FlakeguardConfig,
    warehouse_sizes: list[WarehouseSize] | None = None,
    concurrency: int = 4,
) -> list[SimulationRow]:
    """Simulate total runtime and credit burn for different warehouse sizes.

    Assumes execution time is independent of warehouse size for MVP (mock).
    In reality, larger warehouses often run faster; we only vary credits_per_hour here.

    Args:
        G: DAG from dag_parser.build_dag().
        config: Flakeguard config (execution times).
        warehouse_sizes: Which sizes to compare; default XS, S, M.
        concurrency: Assumed concurrent model runs.

    Returns:
        List of SimulationRow for each warehouse size.
    """
    if warehouse_sizes is None:
        warehouse_sizes = [WarehouseSize.XS, WarehouseSize.S, WarehouseSize.M]

    execution_times = _default_execution_times(G, config)
    total_runtime = _simulate_concurrent_runtime(G, execution_times, concurrency)

    rows: list[SimulationRow] = []
    for wh in warehouse_sizes:
        credits_per_hour = CREDITS_PER_HOUR.get(wh, CREDITS_PER_HOUR[WarehouseSize.M])
        total_credits = (total_runtime / 3600.0) * credits_per_hour
        rows.append(
            SimulationRow(
                warehouse=wh.value,
                total_runtime_seconds=total_runtime,
                total_credits=total_credits,
                concurrency=concurrency,
                num_models=G.number_of_nodes(),
            )
        )
    return rows


def simulation_comparison_table(rows: list[SimulationRow]) -> list[dict[str, Any]]:
    """Return rows as list of dicts for CLI table or dashboard."""
    return [
        {
            "Warehouse": r.warehouse,
            "Runtime (sec)": round(r.total_runtime_seconds, 2),
            "Credits": round(r.total_credits, 4),
            "Concurrency": r.concurrency,
            "Models": r.num_models,
        }
        for r in rows
    ]
