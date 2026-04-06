"""Backward-compat tests for sql_linter public API."""

from flakeguard.sql_linter import (
    LintFinding,
    LintWarning,
    Severity,
    lint_sql,
    lint_manifest_models,
    severity_score,
    gate_result,
)


def test_lint_sql_select_star() -> None:
    findings = lint_sql("select * from t")
    rule_ids = [f.rule_id for f in findings]
    assert any("select_star" in r for r in rule_ids)


def test_lint_sql_no_where() -> None:
    findings = lint_sql("select a from t")
    rule_ids = [f.rule_id for f in findings]
    assert any("missing_where" in r or "no_where" in r for r in rule_ids)


def test_lint_sql_cross_join() -> None:
    findings = lint_sql("select * from a cross join b")
    rule_ids = [f.rule_id for f in findings]
    assert any("cross_join" in r for r in rule_ids)


def test_lint_sql_empty() -> None:
    assert lint_sql("") == []
    assert lint_sql("   ") == []


def test_severity_score_empty() -> None:
    assert severity_score([]) == 0.0


def test_lint_warning_alias() -> None:
    """LintWarning is an alias for LintFinding."""
    assert LintWarning is LintFinding


def test_lint_manifest_models_basic() -> None:
    manifest = {
        "nodes": {
            "model.pkg.a": {
                "unique_id": "model.pkg.a",
                "name": "a",
                "config": {"materialized": "view"},
                "depends_on": {"nodes": []},
                "raw_code": "select * from t",
            },
            "test.pkg.t1": {
                "unique_id": "test.pkg.t1",
                "name": "t1",
                "config": {},
                "depends_on": {"nodes": []},
                "raw_code": "select 1",
            },
        }
    }
    findings = lint_manifest_models(manifest)
    assert len(findings) > 0
    assert all(isinstance(f, LintFinding) for f in findings)
    # Only model nodes linted, not test nodes
    assert all("model." in (f.model_id or "") for f in findings)
