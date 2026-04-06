"""Cost estimator: compute credit cost per model and DAG-level metrics."""

from typing import Any

import networkx as nx

from flakeguard.config import CREDITS_PER_HOUR, FlakeguardConfig, WarehouseSize


def cost_per_model(
    execution_time_seconds: float,
    warehouse: WarehouseSize,
) -> float:
    """Compute credit cost for a single model run.

    Formula: (execution_time_seconds / 3600) * credits_per_hour

    Args:
        execution_time_seconds: Model execution time in seconds.
        warehouse: Warehouse size for credit rate.

    Returns:
        Estimated credits consumed.
    """
    credits_per_hour = CREDITS_PER_HOUR.get(warehouse, CREDITS_PER_HOUR[WarehouseSize.M])
    return (execution_time_seconds / 3600.0) * credits_per_hour


def compute_dag_costs(
    G: nx.DiGraph,
    config: FlakeguardConfig,
) -> tuple[dict[str, float], dict[str, float], float]:
    """Compute cost per model, downstream cost impact, and total DAG cost.

    Downstream cost impact for a node = sum of costs of all nodes that depend on it
    (directly or indirectly).

    Args:
        G: DAG from dag_parser.build_dag().
        config: Flakeguard config (warehouse, execution times).

    Returns:
        (cost_per_model, downstream_cost_per_model, total_dag_cost).
    """
    cost_per_model_map: dict[str, float] = {}
    for node in G:
        node_attrs = G.nodes[node]
        exec_sec = node_attrs.get("execution_time_seconds")
        if exec_sec is None:
            exec_sec = config.get_execution_time(node_attrs.get("name", node))
        cost_per_model_map[node] = cost_per_model(
            exec_sec,
            config.warehouse,
        )

    # Downstream: for each node, sum cost of all descendants
    downstream: dict[str, float] = {}
    for node in G:
        descendants = nx.descendants(G, node)
        downstream[node] = cost_per_model_map[node] + sum(
            cost_per_model_map.get(d, 0.0) for d in descendants
        )

    total = sum(cost_per_model_map.values())
    return cost_per_model_map, downstream, total


def top_expensive_models(
    cost_per_model: dict[str, float],
    G: nx.DiGraph,
    top_n: int = 5,
) -> list[tuple[str, str, float]]:
    """Return top N most expensive models by cost.

    Args:
        cost_per_model: Map node_id -> cost (credits).
        G: DAG (for node names).
        top_n: Number of models to return.

    Returns:
        List of (node_id, model_name, cost) sorted by cost descending.
    """
    with_names = [
        (nid, G.nodes[nid].get("name", nid), cost)
        for nid, cost in cost_per_model.items()
    ]
    with_names.sort(key=lambda x: x[2], reverse=True)
    return with_names[:top_n]


def top_downstream_impact(
    downstream_cost_per_model: dict[str, float],
    G: nx.DiGraph,
    top_n: int = 5,
) -> list[tuple[str, str, float]]:
    """Return top N models by downstream cost impact (change here affects many)."""

    with_names = [
        (nid, G.nodes[nid].get("name", nid), cost)
        for nid, cost in downstream_cost_per_model.items()
    ]
    with_names.sort(key=lambda x: x[2], reverse=True)
    return with_names[:top_n]
