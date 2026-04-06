"""DAG parser for dbt manifest.json: build dependency graph and compute metrics."""

from pathlib import Path
from typing import Any

import networkx as nx

from flakeguard.config import FlakeguardConfig


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """Load and parse dbt manifest.json.

    Args:
        manifest_path: Path to target/manifest.json.

    Returns:
        Parsed manifest dictionary.

    Raises:
        FileNotFoundError: If manifest does not exist.
        ValueError: If file is not valid JSON or missing required keys.
    """
    import json

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    raw = manifest_path.read_text(encoding="utf-8")
    data = json.loads(raw)

    if "nodes" not in data:
        raise ValueError("Manifest missing 'nodes' key")

    return data


def _is_model_node(unique_id: str) -> bool:
    """Return True if node is a dbt model (not test, seed, snapshot, etc.)."""
    return unique_id.startswith("model.")


def _model_name_from_id(unique_id: str) -> str:
    """Extract short model name from unique_id (e.g. model.pkg.name -> name)."""
    return unique_id.split(".")[-1] if "." in unique_id else unique_id


def build_dag(manifest: dict[str, Any]) -> nx.DiGraph:
    """Build a directed graph from manifest nodes (models only).

    Nodes are model unique_ids. Edges go from dependency -> dependent.
    Node attributes: name, materialization (when available).

    Args:
        manifest: Parsed manifest from load_manifest().

    Returns:
        networkx DiGraph with model nodes and dependency edges.
    """
    G = nx.DiGraph()
    nodes = manifest.get("nodes", {})

    for uid, node in nodes.items():
        if not _is_model_node(uid):
            continue
        config = node.get("config", {}) or {}
        materialization = config.get("materialized", "view")
        if isinstance(materialization, dict):
            materialization = materialization.get("value", "view")
        name = node.get("name", _model_name_from_id(uid))
        # Optional: mock or imported execution time (seconds) for cost estimation
        execution_time_seconds = node.get("execution_time_seconds")
        attrs = {
            "name": name,
            "materialization": materialization,
            "raw": node,
        }
        if execution_time_seconds is not None:
            attrs["execution_time_seconds"] = float(execution_time_seconds)
        G.add_node(uid, **attrs)

    for uid, node in nodes.items():
        if not _is_model_node(uid):
            continue
        depends_on = node.get("depends_on") or {}
        parent_ids = depends_on.get("nodes") or []
        for parent_id in parent_ids:
            if _is_model_node(parent_id) and G.has_node(parent_id):
                G.add_edge(parent_id, uid)

    return G


def get_topological_order(G: nx.DiGraph) -> list[str]:
    """Return model unique_ids in topological (dependency) order.

    Args:
        G: DAG from build_dag().

    Returns:
        List of node IDs such that dependencies come before dependents.

    Raises:
        nx.NetworkXError: If graph has a cycle.
    """
    return list(nx.topological_sort(G))


def get_fan_in_out_metrics(G: nx.DiGraph) -> dict[str, dict[str, int]]:
    """Compute fan-in and fan-out per model.

    Fan-in: number of direct dependencies (in-edges).
    Fan-out: number of direct dependents (out-edges).

    Args:
        G: DAG from build_dag().

    Returns:
        Dict mapping node_id -> {"fan_in": int, "fan_out": int}.
    """
    result: dict[str, dict[str, int]] = {}
    for node in G:
        result[node] = {
            "fan_in": G.in_degree(node),
            "fan_out": G.out_degree(node),
        }
    return result


def get_critical_path_length(G: nx.DiGraph) -> int:
    """Length of longest path in the DAG (number of edges).

    Represents critical path length in terms of dependency depth.

    Args:
        G: DAG from build_dag().

    Returns:
        Number of edges in the longest path, or 0 if empty.
    """
    if G.number_of_nodes() == 0:
        return 0
    try:
        path = nx.dag_longest_path(G)
        return len(path) - 1 if len(path) > 1 else 0
    except (nx.NetworkXError, nx.NetworkXNotImplemented):
        return 0


def parse_dbt_project(config: FlakeguardConfig) -> tuple[nx.DiGraph, dict[str, Any]]:
    """Load manifest and build DAG from FlakeguardConfig.

    Args:
        config: Flakeguard configuration (project path, manifest path).

    Returns:
        (DAG graph, raw manifest).
    """
    manifest_path = config.get_manifest_path()
    manifest = load_manifest(manifest_path)
    G = build_dag(manifest)
    return G, manifest
