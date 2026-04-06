"""dbt-practice lint rules (E301 – E302)."""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from flakeguard.rules.base import BaseRule
from flakeguard.sql_linter import Category, LintFinding, Severity


# ---------------------------------------------------------------------------
# E301 – Full-refresh table with high execution time
# ---------------------------------------------------------------------------

class FullRefreshLargeTableRule(BaseRule):
    rule_id = "E301_full_refresh_large"
    category = Category.DBT_PRACTICE
    severity = Severity.HIGH
    description = "materialized='table' with high runtime — consider incremental."

    RUNTIME_THRESHOLD_SECONDS = 120.0

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        materialization = meta.get("materialization", "view")
        exec_time = meta.get("execution_time_seconds")
        if materialization != "table":
            return []
        if exec_time is None or exec_time < self.RUNTIME_THRESHOLD_SECONDS:
            return []
        model_name = (meta.get("model_id") or "").split(".")[-1] or "model"
        return [self._finding(
            message=(
                f"'{model_name}' is materialized as TABLE (full refresh) "
                f"with execution time {exec_time}s (threshold: {self.RUNTIME_THRESHOLD_SECONDS}s)"
            ),
            suggestion=(
                "Convert to incremental materialization with an is_incremental() guard "
                "and a reliable timestamp or surrogate key for merge. This avoids "
                "full re-scan/rebuild on every run and can cut cost 10–50x for append-heavy tables."
            ),
            meta=meta,
            offending_sql=f"{{ config(materialized='table') }}  -- {model_name}",
            estimated_impact=f"Full rebuild every run: ~{exec_time:.0f}s of warehouse time wasted",
            doc_url="https://docs.getdbt.com/docs/build/incremental-models",
        )]


# ---------------------------------------------------------------------------
# E302 – Incremental without is_incremental()
# ---------------------------------------------------------------------------

class IncrementalMissingIsIncrementalRule(BaseRule):
    rule_id = "E302_incremental_no_guard"
    category = Category.DBT_PRACTICE
    severity = Severity.HIGH
    description = "materialized='incremental' but raw_code has no is_incremental() guard."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        materialization = meta.get("materialization", "view")
        if materialization != "incremental":
            return []
        raw_code = meta.get("raw_code", "")
        if "is_incremental()" in raw_code:
            return []
        model_name = (meta.get("model_id") or "").split(".")[-1] or "model"
        return [self._finding(
            message=(
                f"'{model_name}' is incremental but raw_code has no is_incremental() guard"
            ),
            suggestion=(
                "Add {% if is_incremental() %} ... {% endif %} around your WHERE clause "
                "to filter only new/changed rows on incremental runs. Without it, the "
                "model does a full scan every time, negating the incremental benefit."
            ),
            meta=meta,
            offending_sql=f"{{ config(materialized='incremental') }}  -- missing is_incremental()",
            estimated_impact="Every run is effectively a full refresh; incremental provides no benefit",
            doc_url="https://docs.getdbt.com/docs/build/incremental-models#how-do-i-use-the-is_incremental-function",
        )]
