# flakeguard

**Cost-intelligence engine for dbt/Snowflake pipelines:** model DAG analysis, warehouse cost estimation, SQL anti-pattern detection, and warehouse scaling tradeoff simulation.

No Snowflake connection required for the MVP — uses `target/manifest.json` and mock execution times.

---

## Features (MVP v1)

| Feature | Description |
|--------|-------------|
| **DAG Parser** | Reads `target/manifest.json`, extracts models and `depends_on`, builds dependency graph (networkx). Computes topological order, fan-in/fan-out, critical path length. |
| **Cost Estimator** | Simplified cost model: `cost = (execution_time_seconds / 3600) * credits_per_hour`. Outputs cost per model, total DAG cost, downstream propagation cost, top 5 expensive models. |
| **SQL Anti-Pattern Detection** | Uses sqlglot to flag: `SELECT *`, missing WHERE on large tables, cross joins, excessive nested subqueries. Returns severity and suggested optimization. |
| **Warehouse Scaling Simulator** | Simulates total runtime and credit burn for XS / S / M (and optional L/XL) with configurable concurrency. Comparison table: cost vs runtime. |
| **CLI** | `flakeguard analyze ./dbt_project --warehouse M` — DAG summary, cost, lint report, simulation table. |
| **Streamlit Dashboard** | Tabs: DAG visualization, cost breakdown, lint report, scaling simulation. |
| **GitHub Action** | On PR, run cost estimation and post markdown summary (optional comment). |

---

## Install

```bash
cd flakeguard
pip install -e .
# or
pip install -r requirements.txt
```

Requires **Python 3.11+**.

---

## Quick Start

**→ Step-by-step: [RUN_LOCALLY.md](RUN_LOCALLY.md)** (setup, CLI, dashboard, tests)

1. **CLI** (use sample manifest if you don’t have a dbt project):

   ```bash
   # From repo root, with bundled sample manifest
   mkdir -p target && cp examples/sample_manifest.json target/manifest.json
   flakeguard --warehouse M .
   # or: flakeguard analyze . --warehouse M
   ```

   Or point at your dbt project (options before path):

   ```bash
   flakeguard --warehouse M /path/to/dbt_project
   flakeguard analyze /path/to/dbt_project --concurrency 4
   ```

2. **Dashboard**:

   ```bash
   streamlit run dashboard/app.py
   ```

   Open the URL (e.g. http://localhost:8501). Use the sidebar to set manifest path (default: `examples/sample_manifest.json`).

3. **Tests**:

   ```bash
   pip install -e ".[dev]"
   pytest tests/ -v
   ```

---

## Architecture

- **dag_parser** — Load manifest, build networkx DAG, topological order, fan-in/fan-out, critical path.
- **cost_estimator** — Per-model and total cost; downstream cost impact; top-N expensive models.
- **sql_linter** — sqlglot-based rules: select star, no WHERE, cross join, excessive subqueries.
- **simulator** — Warehouse size × concurrency → runtime and credits; comparison table.
- **cli** — Typer app: `analyze` command.
- **dashboard** — Streamlit app with four tabs.

All modules are stateless and take config/manifest/DAG as inputs; no live Snowflake connection in MVP.

---

## Tradeoff Analysis (for resume / README)

- **Warehouse size**: Larger warehouses cost more per hour but can reduce runtime; the simulator compares credit burn across XS/S/M for the same DAG.
- **Concurrency**: Higher concurrency can reduce wall-clock time but is bounded by critical path; the simulator uses a simple parallel estimate.
- **SQL patterns**: SELECT *, missing WHERE, and cross joins increase data scanned and runtime; the linter highlights these for optimization.

---

## Future Roadmap

- Snowflake query history integration for real execution times.
- Incremental vs full refresh detection and cost impact.
- More lint rules (e.g. distinct without limit, large CTEs).
- Optional PR comment with cost diff vs base branch.

---

## License

MIT.
