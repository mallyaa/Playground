"""Enterprise SQL anti-pattern detection engine.

Thin orchestration layer: parses SQL via sqlglot, delegates to the pluggable
rule engine in flakeguard.rules, and exposes the public API consumed by CLI
and dashboard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import sqlglot
from sqlglot import exp


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class Category(str, Enum):
    PERFORMANCE = "performance"
    CORRECTNESS = "correctness"
    STYLE = "style"
    DBT_PRACTICE = "dbt_practice"


# Numeric weights used by gate scoring
SEVERITY_WEIGHT: dict[Severity, int] = {
    Severity.CRITICAL: 10,
    Severity.HIGH: 5,
    Severity.MEDIUM: 2,
    Severity.LOW: 1,
    Severity.INFO: 0,
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class LintFinding:
    """Rich, granular lint finding produced by a rule."""

    rule_id: str
    category: Category
    severity: Severity
    message: str
    suggestion: str
    offending_sql: str = ""
    model_id: Optional[str] = None
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    estimated_impact: str = ""
    doc_url: Optional[str] = None


# Backward-compat alias so existing imports keep working
LintWarning = LintFinding


@dataclass
class GateResult:
    """Pass / fail result for a pre-deployment quality gate."""

    passed: bool
    score: int
    threshold: int
    summary: str
    findings_by_severity: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Gate scoring
# ---------------------------------------------------------------------------

def gate_result(
    findings: list[LintFinding],
    threshold: int = 20,
) -> GateResult:
    """Compute weighted gate score and return pass/fail.

    Weights: CRITICAL=10, HIGH=5, MEDIUM=2, LOW=1, INFO=0.
    """
    score = sum(SEVERITY_WEIGHT.get(f.severity, 0) for f in findings)
    by_sev: dict[str, int] = {}
    for f in findings:
        by_sev[f.severity.value] = by_sev.get(f.severity.value, 0) + 1
    passed = score <= threshold
    status = "PASS" if passed else "FAIL"
    summary = f"Gate {status}: score {score} (threshold {threshold})"
    return GateResult(
        passed=passed,
        score=score,
        threshold=threshold,
        summary=summary,
        findings_by_severity=by_sev,
    )


def severity_score(findings: list[LintFinding]) -> float:
    """Average severity score (backward-compat helper for dashboard)."""
    if not findings:
        return 0.0
    total = sum(SEVERITY_WEIGHT.get(f.severity, 0) for f in findings)
    return total / len(findings)


# ---------------------------------------------------------------------------
# Core lint functions
# ---------------------------------------------------------------------------

def _node_meta(node: dict[str, Any]) -> dict[str, Any]:
    """Extract metadata from a manifest node for rule context."""
    config = node.get("config", {}) or {}
    materialization = config.get("materialized", "view")
    if isinstance(materialization, dict):
        materialization = materialization.get("value", "view")
    return {
        "model_id": node.get("unique_id"),
        "file_path": node.get("original_file_path") or node.get("path"),
        "materialization": materialization,
        "execution_time_seconds": node.get("execution_time_seconds"),
        "raw_code": node.get("raw_code", ""),
    }


def lint_sql(
    sql: str,
    model_id: Optional[str] = None,
    file_path: Optional[str] = None,
    *,
    node_meta: Optional[dict[str, Any]] = None,
) -> list[LintFinding]:
    """Parse SQL and run all registered rules.

    Args:
        sql: Raw or compiled SQL string.
        model_id: Optional dbt unique_id.
        file_path: Optional original file path.
        node_meta: Optional dict with materialization, execution_time, raw_code.

    Returns:
        List of LintFinding from all rules.
    """
    if not sql or not sql.strip():
        return []

    try:
        parsed = sqlglot.parse_one(sql, dialect="snowflake")
    except Exception:
        return [
            LintFinding(
                rule_id="E000_parse_error",
                category=Category.CORRECTNESS,
                severity=Severity.CRITICAL,
                message="SQL could not be parsed by sqlglot (Snowflake dialect)",
                suggestion="Check SQL syntax; ensure Jinja is compiled before linting.",
                offending_sql=sql[:200],
                model_id=model_id,
                file_path=file_path,
                estimated_impact="Model cannot be analyzed",
            )
        ]

    meta = node_meta or {}
    meta.setdefault("model_id", model_id)
    meta.setdefault("file_path", file_path)

    from flakeguard.rules.registry import run_all
    return run_all(parsed, meta)


def lint_model_node(node: dict[str, Any]) -> list[LintFinding]:
    """Lint a single manifest model node."""
    sql = (
        node.get("compiled_code")
        or node.get("raw_code")
        or node.get("compiled_sql")
        or node.get("raw_sql")
        or ""
    )
    meta = _node_meta(node)
    return lint_sql(sql, node_meta=meta)


def lint_manifest_models(manifest: dict[str, Any]) -> list[LintFinding]:
    """Lint all model nodes in a dbt manifest."""
    findings: list[LintFinding] = []
    nodes = manifest.get("nodes", {})
    for uid, node in nodes.items():
        if not uid.startswith("model."):
            continue
        findings.extend(lint_model_node(node))
    return findings
