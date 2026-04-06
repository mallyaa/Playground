"""dbt-practice lint rules (E301 – E306).

E301-E302: SQL-level rules (run via BaseRule.check against parsed AST).
E303-E306: Manifest-level test coverage rules (run via run_test_coverage_rules
           against node metadata, not SQL). These enforce schema test coverage
           statically from manifest.json -- zero Snowflake runtime.
"""

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


# ===========================================================================
# Manifest-level test coverage rules (E303 – E306)
#
# These do NOT subclass BaseRule (they don't parse SQL).  They inspect the
# test_coverage dict built from manifest test nodes.  Called by
# lint_manifest_models via run_test_coverage_rules().
# ===========================================================================

_TYPED_SNAPSHOT_MATERIALIZATIONS = {"table", "incremental"}


def _make_finding(
    rule_id: str,
    severity: Severity,
    message: str,
    suggestion: str,
    meta: dict[str, Any],
    *,
    estimated_impact: str = "",
    doc_url: str | None = None,
) -> LintFinding:
    return LintFinding(
        rule_id=rule_id,
        category=Category.DBT_PRACTICE,
        severity=severity,
        message=message,
        suggestion=suggestion,
        offending_sql="",
        model_id=meta.get("model_id"),
        file_path=meta.get("file_path"),
        estimated_impact=estimated_impact,
        doc_url=doc_url,
    )


def run_test_coverage_rules(meta: dict[str, Any]) -> list[LintFinding]:
    """Run E303-E306 test coverage rules against a model's metadata.

    These rules enforce that TYPED and SNAPSHOT models have schema tests
    defined in the manifest -- the same tests you removed from dbt run
    to save runtime.  flakeguard checks them statically in <1 second.

    Args:
        meta: Node metadata dict with keys: model_id, file_path,
              materialization, test_coverage (dict of test_type -> [test_ids]).

    Returns:
        List of LintFinding for missing test coverage.
    """
    findings: list[LintFinding] = []
    materialization = meta.get("materialization", "view")
    coverage = meta.get("test_coverage", {})
    model_name = (meta.get("model_id") or "").split(".")[-1] or "model"
    file_path = meta.get("file_path") or ""

    is_typed_or_snapshot = (
        materialization in _TYPED_SNAPSHOT_MATERIALIZATIONS
        or "typed" in file_path.lower()
        or "snapshot" in file_path.lower()
    )

    # E303: No tests at all on TYPED/SNAPSHOT model
    if is_typed_or_snapshot and not coverage:
        findings.append(_make_finding(
            rule_id="E303_no_tests",
            severity=Severity.HIGH,
            message=f"'{model_name}' has no schema tests defined (TYPED/SNAPSHOT model)",
            suggestion=(
                "Add at minimum not_null and unique tests in schema.yml for key columns. "
                "flakeguard checks this statically from manifest.json -- zero dbt runtime "
                "impact. These tests document data contracts and catch issues before "
                "client file delivery."
            ),
            meta=meta,
            estimated_impact="No data quality validation; bad data reaches client deliverables undetected",
            doc_url="https://docs.getdbt.com/docs/build/data-tests",
        ))
        return findings  # Don't pile on E304-E306 if there are zero tests

    # E304: Missing not_null tests on TYPED/SNAPSHOT model
    if is_typed_or_snapshot and "not_null" not in coverage:
        findings.append(_make_finding(
            rule_id="E304_missing_not_null",
            severity=Severity.MEDIUM,
            message=f"'{model_name}' has no not_null tests (TYPED/SNAPSHOT model)",
            suggestion=(
                "Add not_null tests for primary key and critical columns in schema.yml. "
                "NULLs in key columns cause silent join failures and incorrect aggregations "
                "in downstream SNAPSHOT and VIEW layers."
            ),
            meta=meta,
            estimated_impact="NULL keys cause silent data loss in joins and aggregations",
            doc_url="https://docs.getdbt.com/reference/resource-properties/data-tests#not_null",
        ))

    # E305: Missing accepted_values tests on TYPED/SNAPSHOT model
    if is_typed_or_snapshot and "accepted_values" not in coverage:
        findings.append(_make_finding(
            rule_id="E305_missing_accepted_values",
            severity=Severity.MEDIUM,
            message=f"'{model_name}' has no accepted_values tests (TYPED/SNAPSHOT model)",
            suggestion=(
                "Add accepted_values tests for status, type, and category columns in "
                "schema.yml. This enforces the data contract between source systems and "
                "client deliverables -- catching unexpected values before they reach "
                "HMRO/RAW/Beta Access files. flakeguard validates this statically; "
                "no dbt runtime cost."
            ),
            meta=meta,
            estimated_impact="Unexpected enum values propagate to client files undetected",
            doc_url="https://docs.getdbt.com/reference/resource-properties/data-tests#accepted_values",
        ))

    # E306: Missing unique test on TYPED/SNAPSHOT model
    if is_typed_or_snapshot and "unique" not in coverage:
        findings.append(_make_finding(
            rule_id="E306_missing_unique_test",
            severity=Severity.MEDIUM,
            message=f"'{model_name}' has no unique key test (TYPED/SNAPSHOT model)",
            suggestion=(
                "Add a unique test on the primary key column in schema.yml. "
                "Duplicate rows in TYPED/SNAPSHOT models cause double-counting in "
                "reports and duplicate records in client file deliveries."
            ),
            meta=meta,
            estimated_impact="Duplicate rows cascade to reports and client deliverables",
            doc_url="https://docs.getdbt.com/reference/resource-properties/data-tests#unique",
        ))

    return findings
