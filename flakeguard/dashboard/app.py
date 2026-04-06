"""
Streamlit dashboard for flakeguard: DAG, cost, lint, scaling simulation.
Run from repo root: streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flakeguard.config import FlakeguardConfig, LintConfig, WarehouseSize
from flakeguard.cost_estimator import (
    compute_dag_costs,
    top_downstream_impact,
    top_expensive_models,
)
from flakeguard.dag_parser import (
    build_dag,
    get_critical_path_length,
    get_fan_in_out_metrics,
    get_topological_order,
    load_manifest,
)
from flakeguard.simulator import run_scaling_simulation, simulation_comparison_table
from flakeguard.sql_linter import (
    Category,
    LintFinding,
    Severity,
    gate_result,
    lint_manifest_models,
    severity_score,
)


def _load_manifest_from_source(
    uploaded_file,
    path_str: str,
) -> tuple[dict | None, str | None]:
    """Return (manifest_dict, error_message_or_none)."""
    if uploaded_file is not None:
        try:
            raw = uploaded_file.read().decode("utf-8")
            data = json.loads(raw)
            if "nodes" not in data:
                return None, "Uploaded JSON is missing 'nodes' key."
            return data, None
        except Exception as exc:
            return None, f"Failed to parse uploaded file: {exc}"
    path = Path(path_str)
    if not path.exists():
        return None, f"Manifest not found: {path}"
    try:
        return load_manifest(path), None
    except Exception as exc:
        return None, f"Failed to load manifest: {exc}"


def main() -> None:
    st.set_page_config(page_title="flakeguard", page_icon="❄️", layout="wide")
    st.title("❄️ flakeguard")
    st.caption("Cost-intelligence for dbt/Snowflake pipelines")

    # ── Sidebar ──────────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Manifest source")
        uploaded = st.file_uploader(
            "Upload manifest.json",
            type=["json"],
            help="Upload a dbt target/manifest.json file",
        )
        default_manifest = Path(__file__).resolve().parent.parent / "examples" / "ecommerce_manifest.json"
        manifest_path_str = st.text_input(
            "Or enter file path",
            value=str(default_manifest),
            help="Path to target/manifest.json on disk",
        )
        st.divider()
        warehouse = st.selectbox(
            "Warehouse size",
            [w.value for w in WarehouseSize],
            index=2,
        )
        wh_enum = WarehouseSize(warehouse)
        concurrency = st.slider("Simulation concurrency", 1, 16, 4)
        st.divider()
        st.subheader("Lint gate")
        gate_threshold = st.number_input(
            "Gate threshold",
            min_value=0,
            max_value=500,
            value=20,
            help="Weighted score above which the quality gate FAILs",
        )

    # ── Load manifest ────────────────────────────────────────────────────
    manifest, err = _load_manifest_from_source(uploaded, manifest_path_str)
    if err:
        st.error(err)
        st.stop()
    assert manifest is not None

    try:
        G = build_dag(manifest)
    except Exception as exc:
        st.error(f"Failed to build DAG: {exc}")
        st.stop()

    manifest_path = Path(manifest_path_str) if uploaded is None else Path(tempfile.gettempdir())
    project_path = manifest_path.parent.parent
    config = FlakeguardConfig(
        project_path=project_path,
        manifest_path=manifest_path if manifest_path.exists() else None,
        warehouse=wh_enum,
    )

    # ── Tabs ─────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "DAG Visualization",
        "Cost Breakdown",
        "SQL Lint Report",
        "Scaling Simulation",
    ])

    # ── Tab 1: DAG ───────────────────────────────────────────────────────
    with tab1:
        st.subheader("DAG Visualization")
        order = get_topological_order(G)
        fan = get_fan_in_out_metrics(G)
        critical_len = get_critical_path_length(G)
        c1, c2, c3 = st.columns(3)
        c1.metric("Models", G.number_of_nodes())
        c2.metric("Edges", G.number_of_edges())
        c3.metric("Critical path", critical_len)
        rows = []
        for nid in order:
            name = G.nodes[nid].get("name", nid)
            mat = G.nodes[nid].get("materialization", "?")
            fi, fo = fan[nid]["fan_in"], fan[nid]["fan_out"]
            rows.append({"Model": name, "Materialization": mat, "Fan-in": fi, "Fan-out": fo})
        st.dataframe(rows, use_container_width=True, hide_index=True)
        st.text("Topological order: " + " → ".join(G.nodes[n].get("name", n) for n in order))

    # ── Tab 2: Cost ──────────────────────────────────────────────────────
    with tab2:
        st.subheader("Cost Breakdown")
        cost_per_model, downstream_cost, total_cost = compute_dag_costs(G, config)
        st.metric("Total DAG cost (credits)", f"{total_cost:.4f}")
        top = top_expensive_models(cost_per_model, G, top_n=10)
        st.write("**Top expensive models**")
        cost_df = pd.DataFrame([{"Model": name, "Cost (credits)": round(c, 4)} for _, name, c in top])
        st.dataframe(cost_df, use_container_width=True, hide_index=True)
        top_down = top_downstream_impact(downstream_cost, G, top_n=5)
        st.write("**Top by downstream impact**")
        down_df = pd.DataFrame([{"Model": name, "Downstream cost": round(c, 4)} for _, name, c in top_down])
        st.dataframe(down_df, use_container_width=True, hide_index=True)
        names = [x[1] for x in top]
        costs = [x[2] for x in top]
        st.bar_chart(dict(zip(names, costs)))

    # ── Tab 3: Lint Report ───────────────────────────────────────────────
    with tab3:
        st.subheader("SQL Anti-Pattern Report")

        findings: list[LintFinding] = lint_manifest_models(manifest)
        gate = gate_result(findings, threshold=gate_threshold)

        # Gate banner
        if gate.passed:
            st.success(f"**GATE PASS** — Score: {gate.score} / {gate.threshold}")
        else:
            st.error(f"**GATE FAIL** — Score: {gate.score} / {gate.threshold}")

        # Summary metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("Total findings", len(findings))
        m2.metric("Weighted score", gate.score)
        m3.metric("Avg severity", f"{severity_score(findings):.2f}")

        if not findings:
            st.success("No anti-patterns detected.")
        else:
            # Severity / category breakdowns
            sev_counts = {}
            cat_counts = {}
            model_counts: dict[str, int] = {}
            for f in findings:
                sev_counts[f.severity.value] = sev_counts.get(f.severity.value, 0) + 1
                cat_counts[f.category.value] = cat_counts.get(f.category.value, 0) + 1
                m_name = (f.model_id or "unknown").split(".")[-1]
                model_counts[m_name] = model_counts.get(m_name, 0) + 1

            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.write("**Findings by severity**")
                sev_order = ["critical", "high", "medium", "low", "info"]
                sev_df = pd.DataFrame([
                    {"Severity": s, "Count": sev_counts.get(s, 0)}
                    for s in sev_order if sev_counts.get(s, 0) > 0
                ])
                if not sev_df.empty:
                    st.bar_chart(sev_df.set_index("Severity"))
            with chart_col2:
                st.write("**Findings by category**")
                cat_df = pd.DataFrame([
                    {"Category": c, "Count": n}
                    for c, n in sorted(cat_counts.items())
                ])
                if not cat_df.empty:
                    st.bar_chart(cat_df.set_index("Category"))

            st.write("**Top 5 models by finding count**")
            top_models = sorted(model_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            st.dataframe(
                pd.DataFrame(top_models, columns=["Model", "Findings"]),
                use_container_width=True,
                hide_index=True,
            )

            # Filters
            st.divider()
            st.write("**Filter findings**")
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                sev_filter = st.multiselect(
                    "Severity",
                    options=sev_order,
                    default=list(sev_counts.keys()),
                )
            with fc2:
                cat_options = sorted({f.category.value for f in findings})
                cat_filter = st.multiselect(
                    "Category",
                    options=cat_options,
                    default=cat_options,
                )
            with fc3:
                model_options = sorted({(f.model_id or "unknown").split(".")[-1] for f in findings})
                model_filter = st.multiselect(
                    "Model",
                    options=model_options,
                    default=model_options,
                )

            filtered = [
                f for f in findings
                if f.severity.value in sev_filter
                and f.category.value in cat_filter
                and (f.model_id or "unknown").split(".")[-1] in model_filter
            ]

            # Findings table
            table_data = []
            for f in filtered:
                table_data.append({
                    "Rule": f.rule_id,
                    "Severity": f.severity.value.upper(),
                    "Category": f.category.value,
                    "Model": (f.model_id or "").split(".")[-1],
                    "Message": f.message[:120],
                    "Impact": f.estimated_impact[:80] if f.estimated_impact else "",
                })
            if table_data:
                st.dataframe(
                    pd.DataFrame(table_data),
                    use_container_width=True,
                    hide_index=True,
                )

            # Expandable details
            st.divider()
            st.write(f"**Detailed findings** ({len(filtered)} shown)")
            for i, f in enumerate(filtered):
                label = f"[{f.severity.value.upper()}] {f.rule_id} — {(f.model_id or '').split('.')[-1]}"
                with st.expander(label, expanded=False):
                    st.markdown(f"**{f.message}**")
                    if f.offending_sql:
                        st.code(f.offending_sql, language="sql")
                    st.markdown(f"**Suggestion:** {f.suggestion}")
                    if f.estimated_impact:
                        st.caption(f"Impact: {f.estimated_impact}")
                    if f.doc_url:
                        st.caption(f"[Documentation]({f.doc_url})")
                    detail_cols = st.columns(3)
                    detail_cols[0].caption(f"Category: {f.category.value}")
                    detail_cols[1].caption(f"File: {f.file_path or '—'}")
                    detail_cols[2].caption(f"Line: {f.line_number or '—'}")

    # ── Tab 4: Simulation ────────────────────────────────────────────────
    with tab4:
        st.subheader("Warehouse Scaling Simulation")
        sim_rows = run_scaling_simulation(G, config, concurrency=concurrency)
        table = simulation_comparison_table(sim_rows)
        st.dataframe(table, use_container_width=True, hide_index=True)
        df = pd.DataFrame(table)
        col1, col2 = st.columns(2)
        with col1:
            st.bar_chart(df.set_index("Warehouse")["Credits"])
        with col2:
            st.bar_chart(df.set_index("Warehouse")["Runtime (sec)"])

    st.sidebar.caption("flakeguard enterprise · SQL quality gate for dbt pipelines")


if __name__ == "__main__":
    main()
