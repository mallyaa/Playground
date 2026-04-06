"""CLI entrypoint for flakeguard: analyze dbt project and report cost + lint."""

from __future__ import annotations

from pathlib import Path

import typer

from flakeguard.config import FlakeguardConfig, WarehouseSize
from flakeguard.cost_estimator import (
    compute_dag_costs,
    top_downstream_impact,
    top_expensive_models,
)
from flakeguard.dag_parser import (
    get_critical_path_length,
    get_fan_in_out_metrics,
    get_topological_order,
    parse_dbt_project,
)
from flakeguard.simulator import run_scaling_simulation, simulation_comparison_table
from flakeguard.sql_linter import (
    LintFinding,
    gate_result,
    lint_manifest_models,
    severity_score,
)

app = typer.Typer(
    name="flakeguard",
    help="Cost-intelligence for dbt/Snowflake: DAG analysis, cost estimation, SQL lint, scaling simulation.",
)


def _print_lint_report(findings: list[LintFinding], threshold: int) -> bool:
    """Print grouped lint report and return True if gate passed."""
    gate = gate_result(findings, threshold=threshold)

    typer.echo("\n=== SQL Lint Report ===")
    if gate.passed:
        typer.echo(f"  GATE PASS  score={gate.score}  threshold={gate.threshold}")
    else:
        typer.echo(f"  GATE FAIL  score={gate.score}  threshold={gate.threshold}")

    typer.echo(f"  Total findings: {len(findings)}")
    typer.echo(f"  Avg severity weight: {severity_score(findings):.2f}")

    if gate.findings_by_severity:
        typer.echo("  Breakdown:")
        for sev in ["critical", "high", "medium", "low", "info"]:
            cnt = gate.findings_by_severity.get(sev, 0)
            if cnt:
                typer.echo(f"    {sev.upper():10s}: {cnt}")

    if not findings:
        typer.echo("  No anti-patterns detected.")
        return gate.passed

    # Group by severity, print details
    by_sev: dict[str, list[LintFinding]] = {}
    for f in findings:
        by_sev.setdefault(f.severity.value, []).append(f)

    for sev in ["critical", "high", "medium", "low", "info"]:
        group = by_sev.get(sev, [])
        if not group:
            continue
        typer.echo(f"\n  ── {sev.upper()} ({len(group)}) ──")
        for f in group:
            model = (f.model_id or "").split(".")[-1] or "?"
            typer.echo(f"  [{f.rule_id}] {model}")
            typer.echo(f"    {f.message}")
            typer.echo(f"    → {f.suggestion}")
            if f.offending_sql:
                snippet = f.offending_sql.replace("\n", " ")[:120]
                typer.echo(f"    SQL: {snippet}")
            if f.estimated_impact:
                typer.echo(f"    Impact: {f.estimated_impact}")

    return gate.passed


def _run_analyze(
    project_path: Path,
    warehouse: WarehouseSize,
    manifest_path: Path | None,
    concurrency: int,
    fail_threshold: int,
) -> None:
    config = FlakeguardConfig(
        project_path=project_path,
        manifest_path=manifest_path,
        warehouse=warehouse,
    )

    try:
        G, manifest = parse_dbt_project(config)
    except FileNotFoundError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    # DAG summary
    order = get_topological_order(G)
    fan = get_fan_in_out_metrics(G)
    critical_len = get_critical_path_length(G)
    typer.echo("=== DAG Summary ===")
    typer.echo(f"  Models: {G.number_of_nodes()}")
    typer.echo(f"  Edges: {G.number_of_edges()}")
    typer.echo(f"  Topological length: {len(order)}")
    typer.echo(f"  Critical path length: {critical_len}")

    # Cost
    cost_per_model, downstream_cost, total_cost = compute_dag_costs(G, config)
    typer.echo("\n=== Cost Summary ===")
    typer.echo(f"  Warehouse: {warehouse.value}")
    typer.echo(f"  Total DAG cost (credits): {total_cost:.4f}")
    top = top_expensive_models(cost_per_model, G, top_n=5)
    typer.echo("  Top 5 expensive models:")
    for nid, name, cost in top:
        typer.echo(f"    - {name}: {cost:.4f} credits")
    top_down = top_downstream_impact(downstream_cost, G, top_n=3)
    typer.echo("  Top 3 by downstream impact:")
    for nid, name, cost in top_down:
        typer.echo(f"    - {name}: {cost:.4f} credits (self + downstream)")

    # Lint (enterprise)
    findings = lint_manifest_models(manifest)
    gate_passed = _print_lint_report(findings, fail_threshold)

    # Simulation
    sim_rows = run_scaling_simulation(G, config, concurrency=concurrency)
    table = simulation_comparison_table(sim_rows)
    typer.echo("\n=== Warehouse Scaling Simulation ===")
    typer.echo(f"  Concurrency: {concurrency}")
    headers = list(table[0].keys()) if table else []
    typer.echo("  " + " | ".join(headers))
    for row in table:
        typer.echo("  " + " | ".join(str(row[h]) for h in headers))

    if not gate_passed:
        typer.echo(f"\nLint gate FAILED (score exceeded --fail-threshold {fail_threshold}).")
        raise typer.Exit(2)
    typer.echo("\nDone.")


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to dbt project root.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    ),
    warehouse: WarehouseSize = typer.Option(WarehouseSize.M, "--warehouse", "-w"),
    manifest_path: Path | None = typer.Option(
        None, "--manifest", "-m", path_type=Path, exists=True, file_okay=True, dir_okay=False,
    ),
    concurrency: int = typer.Option(4, "--concurrency", "-c", min=1),
    fail_threshold: int = typer.Option(
        20, "--fail-threshold", "-t",
        help="Lint gate threshold; exit code 2 if score exceeds this.",
    ),
) -> None:
    """Analyze dbt project: DAG summary, cost, lint warnings, simulation comparison."""
    if ctx.invoked_subcommand is None:
        _run_analyze(project_path, warehouse, manifest_path, concurrency, fail_threshold)


@app.command("analyze")
def analyze_cmd(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to dbt project root.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    ),
    warehouse: WarehouseSize = typer.Option(WarehouseSize.M, "--warehouse", "-w"),
    manifest_path: Path | None = typer.Option(
        None, "--manifest", "-m", path_type=Path, exists=True, file_okay=True, dir_okay=False,
    ),
    concurrency: int = typer.Option(4, "--concurrency", "-c", min=1),
    fail_threshold: int = typer.Option(
        20, "--fail-threshold", "-t",
        help="Lint gate threshold; exit code 2 if score exceeds this.",
    ),
) -> None:
    """Same as default: DAG summary, cost, lint, simulation."""
    _run_analyze(project_path, warehouse, manifest_path, concurrency, fail_threshold)


if __name__ == "__main__":
    app()
