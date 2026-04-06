"""Comprehensive tests for the enterprise rule engine (15 rules)."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from flakeguard.sql_linter import (
    Category,
    LintFinding,
    Severity,
    gate_result,
    lint_sql,
    severity_score,
)
from flakeguard.rules.performance_rules import (
    SelectStarRule,
    MissingWhereRule,
    CrossJoinRule,
    ImplicitCartesianRule,
    OrInJoinRule,
    LeadingWildcardLikeRule,
    NonSargablePredicateRule,
    OrderByWithoutLimitRule,
    FanOutJoinRule,
)
from flakeguard.rules.correctness_rules import (
    NestedSubqueryRule,
    DistinctOverGroupByRule,
    UnionVsUnionAllRule,
    UnusedCTERule,
)
from flakeguard.rules.dbt_rules import (
    FullRefreshLargeTableRule,
    IncrementalMissingIsIncrementalRule,
    run_test_coverage_rules,
)

META = {"model_id": "model.test.example", "file_path": "models/example.sql"}


def _parse(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect="snowflake")


# ── E101 SelectStar ──────────────────────────────────────────────────────

def test_e101_select_star_triggers() -> None:
    findings = SelectStarRule().check(_parse("select * from t"), META)
    assert len(findings) >= 1
    assert findings[0].rule_id == "E101_select_star"
    assert findings[0].category == Category.PERFORMANCE


def test_e101_explicit_columns_clean() -> None:
    findings = SelectStarRule().check(_parse("select a, b from t"), META)
    assert findings == []


# ── E102 MissingWhere ────────────────────────────────────────────────────

def test_e102_no_where_triggers() -> None:
    findings = MissingWhereRule().check(_parse("select a from t"), META)
    assert len(findings) == 1
    assert "E102" in findings[0].rule_id


def test_e102_with_where_clean() -> None:
    findings = MissingWhereRule().check(_parse("select a from t where a > 1"), META)
    assert findings == []


# ── E103 CrossJoin ───────────────────────────────────────────────────────

def test_e103_cross_join_triggers() -> None:
    findings = CrossJoinRule().check(_parse("select * from a cross join b"), META)
    assert len(findings) >= 1
    assert "E103" in findings[0].rule_id
    assert findings[0].severity in (Severity.HIGH, Severity.CRITICAL)


def test_e103_inner_join_clean() -> None:
    findings = CrossJoinRule().check(_parse("select * from a join b on a.id = b.id"), META)
    assert findings == []


# ── E104 ImplicitCartesian ───────────────────────────────────────────────

def test_e104_comma_from_triggers() -> None:
    findings = ImplicitCartesianRule().check(_parse("select * from a, b"), META)
    assert len(findings) >= 1
    assert "E104" in findings[0].rule_id


def test_e104_single_table_clean() -> None:
    findings = ImplicitCartesianRule().check(_parse("select * from a"), META)
    assert findings == []


# ── E105 OrInJoin ────────────────────────────────────────────────────────

def test_e105_or_in_join_triggers() -> None:
    sql = "select * from a join b on a.id = b.id or a.name = b.name"
    findings = OrInJoinRule().check(_parse(sql), META)
    assert len(findings) >= 1
    assert "E105" in findings[0].rule_id


def test_e105_and_join_clean() -> None:
    sql = "select * from a join b on a.id = b.id and a.name = b.name"
    findings = OrInJoinRule().check(_parse(sql), META)
    assert findings == []


# ── E106 LeadingWildcardLike ─────────────────────────────────────────────

def test_e106_leading_wildcard_triggers() -> None:
    findings = LeadingWildcardLikeRule().check(_parse("select * from t where name like '%foo'"), META)
    assert len(findings) >= 1
    assert "E106" in findings[0].rule_id


def test_e106_trailing_wildcard_clean() -> None:
    findings = LeadingWildcardLikeRule().check(_parse("select * from t where name like 'foo%'"), META)
    assert findings == []


# ── E107 NonSargablePredicate ────────────────────────────────────────────

def test_e107_function_in_where_triggers() -> None:
    sql = "select * from t where upper(name) = 'FOO'"
    findings = NonSargablePredicateRule().check(_parse(sql), META)
    assert len(findings) >= 1
    assert "E107" in findings[0].rule_id


def test_e107_plain_predicate_clean() -> None:
    findings = NonSargablePredicateRule().check(_parse("select * from t where name = 'foo'"), META)
    assert findings == []


# ── E108 OrderByWithoutLimit ─────────────────────────────────────────────

def test_e108_orderby_in_subquery_triggers() -> None:
    sql = "select * from (select * from t order by id) x"
    findings = OrderByWithoutLimitRule().check(_parse(sql), META)
    assert len(findings) >= 1
    assert "E108" in findings[0].rule_id


def test_e108_orderby_with_limit_clean() -> None:
    sql = "select * from (select * from t order by id limit 10) x"
    findings = OrderByWithoutLimitRule().check(_parse(sql), META)
    assert findings == []


# ── E109 FanOutJoin ──────────────────────────────────────────────────────

def test_e109_multiple_joins_no_group_triggers() -> None:
    sql = "select * from a join b on a.id = b.aid join c on b.id = c.bid"
    findings = FanOutJoinRule().check(_parse(sql), META)
    assert len(findings) >= 1
    assert "E109" in findings[0].rule_id


def test_e109_join_with_group_clean() -> None:
    sql = "select a.id, count(*) from a join b on a.id = b.aid join c on b.id = c.bid group by a.id"
    findings = FanOutJoinRule().check(_parse(sql), META)
    assert findings == []


# ── E201 NestedSubquery ──────────────────────────────────────────────────

def test_e201_deep_nesting_triggers() -> None:
    sql = "select * from (select * from (select * from (select 1) a) b) c"
    findings = NestedSubqueryRule().check(_parse(sql), META)
    assert len(findings) >= 1
    assert "E201" in findings[0].rule_id


def test_e201_shallow_clean() -> None:
    findings = NestedSubqueryRule().check(_parse("select * from (select 1) x"), META)
    assert findings == []


# ── E202 DistinctOverGroupBy ─────────────────────────────────────────────

def test_e202_distinct_plus_groupby_triggers() -> None:
    sql = "select distinct a, count(*) from t group by a"
    findings = DistinctOverGroupByRule().check(_parse(sql), META)
    assert len(findings) >= 1
    assert "E202" in findings[0].rule_id


def test_e202_distinct_only_clean() -> None:
    findings = DistinctOverGroupByRule().check(_parse("select distinct a from t"), META)
    assert findings == []


# ── E203 UnionVsUnionAll ─────────────────────────────────────────────────

def test_e203_union_dedup_triggers() -> None:
    sql = "select a from t1 union select a from t2"
    findings = UnionVsUnionAllRule().check(_parse(sql), META)
    assert len(findings) >= 1
    assert "E203" in findings[0].rule_id


def test_e203_union_all_clean() -> None:
    sql = "select a from t1 union all select a from t2"
    findings = UnionVsUnionAllRule().check(_parse(sql), META)
    assert findings == []


# ── E204 UnusedCTE ───────────────────────────────────────────────────────

def test_e204_unused_cte_triggers() -> None:
    sql = "with foo as (select 1), bar as (select 2) select * from foo"
    findings = UnusedCTERule().check(_parse(sql), META)
    assert len(findings) >= 1
    assert "E204" in findings[0].rule_id
    assert "bar" in findings[0].message


def test_e204_all_used_clean() -> None:
    sql = "with foo as (select 1) select * from foo"
    findings = UnusedCTERule().check(_parse(sql), META)
    assert findings == []


# ── E301 FullRefreshLargeTable ───────────────────────────────────────────

def test_e301_full_refresh_high_runtime_triggers() -> None:
    meta = {**META, "materialization": "table", "execution_time_seconds": 200}
    findings = FullRefreshLargeTableRule().check(_parse("select 1"), meta)
    assert len(findings) == 1
    assert "E301" in findings[0].rule_id


def test_e301_view_clean() -> None:
    meta = {**META, "materialization": "view", "execution_time_seconds": 200}
    findings = FullRefreshLargeTableRule().check(_parse("select 1"), meta)
    assert findings == []


def test_e301_table_low_runtime_clean() -> None:
    meta = {**META, "materialization": "table", "execution_time_seconds": 30}
    findings = FullRefreshLargeTableRule().check(_parse("select 1"), meta)
    assert findings == []


# ── E302 IncrementalMissingIsIncremental ─────────────────────────────────

def test_e302_incremental_no_guard_triggers() -> None:
    meta = {**META, "materialization": "incremental", "raw_code": "select * from t"}
    findings = IncrementalMissingIsIncrementalRule().check(_parse("select 1"), meta)
    assert len(findings) == 1
    assert "E302" in findings[0].rule_id


def test_e302_incremental_with_guard_clean() -> None:
    meta = {**META, "materialization": "incremental", "raw_code": "{% if is_incremental() %} where updated > max {% endif %}"}
    findings = IncrementalMissingIsIncrementalRule().check(_parse("select 1"), meta)
    assert findings == []


def test_e302_non_incremental_clean() -> None:
    meta = {**META, "materialization": "table", "raw_code": "select * from t"}
    findings = IncrementalMissingIsIncrementalRule().check(_parse("select 1"), meta)
    assert findings == []


# ── Integration: lint_sql runs all rules ─────────────────────────────────

def test_lint_sql_integration() -> None:
    findings = lint_sql("select * from a cross join b")
    rule_ids = {f.rule_id for f in findings}
    assert "E101_select_star" in rule_ids
    assert "E103_cross_join" in rule_ids
    assert "E102_missing_where" in rule_ids


def test_lint_sql_empty() -> None:
    assert lint_sql("") == []
    assert lint_sql("   ") == []


# ── Gate scoring ─────────────────────────────────────────────────────────

def test_gate_pass() -> None:
    findings = [
        LintFinding("x", Category.PERFORMANCE, Severity.LOW, "m", "s"),
        LintFinding("x", Category.PERFORMANCE, Severity.LOW, "m", "s"),
    ]
    gate = gate_result(findings, threshold=10)
    assert gate.passed is True
    assert gate.score == 2


def test_gate_fail() -> None:
    findings = [
        LintFinding("x", Category.PERFORMANCE, Severity.CRITICAL, "m", "s"),
        LintFinding("x", Category.PERFORMANCE, Severity.CRITICAL, "m", "s"),
        LintFinding("x", Category.PERFORMANCE, Severity.HIGH, "m", "s"),
    ]
    gate = gate_result(findings, threshold=20)
    assert gate.passed is False
    assert gate.score == 25


def test_severity_score_backward_compat() -> None:
    assert severity_score([]) == 0.0
    findings = [
        LintFinding("x", Category.PERFORMANCE, Severity.HIGH, "m", "s"),
        LintFinding("x", Category.PERFORMANCE, Severity.LOW, "m", "s"),
    ]
    avg = severity_score(findings)
    assert avg == 3.0  # (5 + 1) / 2


# ── E303 No Tests ────────────────────────────────────────────────────────

def test_e303_no_tests_on_table_triggers() -> None:
    meta = {**META, "materialization": "table", "test_coverage": {}}
    findings = run_test_coverage_rules(meta)
    assert any(f.rule_id == "E303_no_tests" for f in findings)


def test_e303_no_tests_on_view_clean() -> None:
    meta = {**META, "materialization": "view", "test_coverage": {}, "file_path": "models/marts/report.sql"}
    findings = run_test_coverage_rules(meta)
    assert not any(f.rule_id == "E303_no_tests" for f in findings)


def test_e303_with_tests_clean() -> None:
    meta = {**META, "materialization": "table", "test_coverage": {"not_null": ["test.x.not_null.abc"]}}
    findings = run_test_coverage_rules(meta)
    assert not any(f.rule_id == "E303_no_tests" for f in findings)


# ── E304 Missing not_null ────────────────────────────────────────────────

def test_e304_missing_not_null_triggers() -> None:
    meta = {**META, "materialization": "table", "test_coverage": {"unique": ["test.x.unique.abc"]}}
    findings = run_test_coverage_rules(meta)
    assert any(f.rule_id == "E304_missing_not_null" for f in findings)


def test_e304_has_not_null_clean() -> None:
    meta = {**META, "materialization": "table", "test_coverage": {"not_null": ["test.x.not_null.abc"]}}
    findings = run_test_coverage_rules(meta)
    assert not any(f.rule_id == "E304_missing_not_null" for f in findings)


# ── E305 Missing accepted_values ────────────────────────────────────────

def test_e305_missing_accepted_values_triggers() -> None:
    meta = {**META, "materialization": "incremental", "test_coverage": {"not_null": ["test.x.not_null.abc"]}}
    findings = run_test_coverage_rules(meta)
    assert any(f.rule_id == "E305_missing_accepted_values" for f in findings)


def test_e305_has_accepted_values_clean() -> None:
    meta = {**META, "materialization": "table", "test_coverage": {"accepted_values": ["test.x.av.abc"]}}
    findings = run_test_coverage_rules(meta)
    assert not any(f.rule_id == "E305_missing_accepted_values" for f in findings)


def test_e305_typed_in_path_triggers() -> None:
    meta = {
        "model_id": "model.test.example",
        "file_path": "models/typed/dim_account.sql",
        "materialization": "view",
        "test_coverage": {"not_null": ["test.x.not_null.abc"]},
    }
    findings = run_test_coverage_rules(meta)
    assert any(f.rule_id == "E305_missing_accepted_values" for f in findings)


# ── E306 Missing unique test ────────────────────────────────────────────

def test_e306_missing_unique_triggers() -> None:
    meta = {**META, "materialization": "table", "test_coverage": {"not_null": ["test.x.not_null.abc"]}}
    findings = run_test_coverage_rules(meta)
    assert any(f.rule_id == "E306_missing_unique_test" for f in findings)


def test_e306_has_unique_clean() -> None:
    meta = {**META, "materialization": "table", "test_coverage": {"unique": ["test.x.unique.abc"]}}
    findings = run_test_coverage_rules(meta)
    assert not any(f.rule_id == "E306_missing_unique_test" for f in findings)


# ── E303-E306 integration: lint_manifest_models picks up test coverage ──

def test_lint_manifest_models_test_coverage() -> None:
    from flakeguard.sql_linter import lint_manifest_models
    manifest = {
        "nodes": {
            "model.test.uncovered_table": {
                "name": "uncovered_table",
                "unique_id": "model.test.uncovered_table",
                "config": {"materialized": "table"},
                "depends_on": {"nodes": []},
                "original_file_path": "models/typed/uncovered_table.sql",
                "raw_code": "select id, status from source.raw",
                "compiled_code": "select id, status from source.raw",
            },
            "model.test.covered_table": {
                "name": "covered_table",
                "unique_id": "model.test.covered_table",
                "config": {"materialized": "table"},
                "depends_on": {"nodes": []},
                "original_file_path": "models/typed/covered_table.sql",
                "raw_code": "select id, status from source.raw",
                "compiled_code": "select id, status from source.raw",
            },
            "test.test.not_null_covered_table_id.xyz": {
                "name": "not_null_covered_table_id",
                "unique_id": "test.test.not_null_covered_table_id.xyz",
                "resource_type": "test",
                "test_metadata": {"name": "not_null"},
                "depends_on": {"nodes": ["model.test.covered_table"]},
                "config": {},
                "raw_code": "select count(*) from {{ model }} where id is null",
            },
            "test.test.unique_covered_table_id.xyz": {
                "name": "unique_covered_table_id",
                "unique_id": "test.test.unique_covered_table_id.xyz",
                "resource_type": "test",
                "test_metadata": {"name": "unique"},
                "depends_on": {"nodes": ["model.test.covered_table"]},
                "config": {},
                "raw_code": "select count(*) from ...",
            },
            "test.test.accepted_values_covered_table_status.xyz": {
                "name": "accepted_values_covered_table_status",
                "unique_id": "test.test.accepted_values_covered_table_status.xyz",
                "resource_type": "test",
                "test_metadata": {"name": "accepted_values"},
                "depends_on": {"nodes": ["model.test.covered_table"]},
                "config": {},
                "raw_code": "select count(*) from ...",
            },
        }
    }
    findings = lint_manifest_models(manifest)
    rule_ids = [f.rule_id for f in findings]

    # uncovered_table should trigger E303
    e303 = [f for f in findings if f.rule_id == "E303_no_tests" and "uncovered_table" in (f.model_id or "")]
    assert len(e303) == 1

    # covered_table should NOT trigger E303-E306
    covered_test_cov = [f for f in findings if f.rule_id.startswith("E30") and f.model_id == "model.test.covered_table"]
    assert len(covered_test_cov) == 0
