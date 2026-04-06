"""Performance-related lint rules (E101 – E109)."""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from flakeguard.rules.base import BaseRule
from flakeguard.sql_linter import Category, LintFinding, Severity


# ---------------------------------------------------------------------------
# E101 – SELECT *
# ---------------------------------------------------------------------------

class SelectStarRule(BaseRule):
    rule_id = "E101_select_star"
    category = Category.PERFORMANCE
    severity = Severity.MEDIUM
    description = "Flags every SELECT that uses *, reporting source tables."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        for select in parsed.find_all(exp.Select):
            stars = [
                col for col in select.expressions
                if isinstance(col, exp.Star) or (isinstance(col, exp.Column) and col.name == "*")
            ]
            if not stars:
                continue
            tables = [t.alias_or_name for t in select.find_all(exp.Table)]
            table_list = ", ".join(tables[:5]) or "unknown"
            findings.append(self._finding(
                message=f"SELECT * pulls all columns from: {table_list}",
                suggestion=(
                    f"Replace SELECT * with explicit columns needed downstream. "
                    f"Reduces bytes scanned, improves micro-partition pruning on Snowflake, "
                    f"and prevents breakage when upstream schema changes."
                ),
                meta=meta,
                offending_sql=self._snippet(select),
                estimated_impact=f"Full column scan on {len(tables)} table(s); excess network I/O",
                doc_url="https://docs.snowflake.com/en/user-guide/performance-query-select",
            ))
        return findings


# ---------------------------------------------------------------------------
# E102 – Missing WHERE / QUALIFY
# ---------------------------------------------------------------------------

class MissingWhereRule(BaseRule):
    rule_id = "E102_missing_where"
    category = Category.PERFORMANCE
    severity = Severity.MEDIUM
    description = "Flags top-level queries reading tables without WHERE or QUALIFY."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        if parsed.find(exp.Where) or parsed.find(exp.Qualify):
            return []
        from_clause = parsed.find(exp.From)
        if not from_clause:
            return []
        tables = [t.alias_or_name for t in parsed.find_all(exp.Table)]
        if not tables:
            return []
        return [self._finding(
            message=f"No WHERE or QUALIFY clause; full table scan on: {', '.join(tables[:5])}",
            suggestion=(
                "Add a WHERE filter (e.g. date predicate) to enable partition pruning, "
                "or use incremental materialization with an is_incremental() guard."
            ),
            meta=meta,
            offending_sql=self._snippet(parsed, max_len=300),
            estimated_impact="Potential full scan of all micro-partitions",
            doc_url="https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions",
        )]


# ---------------------------------------------------------------------------
# E103 – Explicit CROSS JOIN
# ---------------------------------------------------------------------------

class CrossJoinRule(BaseRule):
    rule_id = "E103_cross_join"
    category = Category.PERFORMANCE
    severity = Severity.HIGH
    description = "Explicit CROSS JOIN producing a Cartesian product."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        for join in parsed.find_all(exp.Join):
            if getattr(join, "side", None) == "CROSS" or getattr(join, "kind", None) == "CROSS":
                right = join.find(exp.Table)
                right_name = right.alias_or_name if right else "?"
                findings.append(self._finding(
                    message=f"CROSS JOIN with '{right_name}' produces a Cartesian product",
                    suggestion=(
                        f"Replace with INNER JOIN or LEFT JOIN on an explicit key. "
                        f"If intentional (e.g. date spine), add a comment and limit result size."
                    ),
                    meta=meta,
                    offending_sql=self._snippet(join),
                    estimated_impact="Row count = left_rows * right_rows; potential OOM or warehouse timeout",
                    doc_url="https://docs.snowflake.com/en/sql-reference/constructs/join",
                    severity_override=Severity.CRITICAL if not meta.get("file_path", "").endswith("_spine.sql") else Severity.HIGH,
                ))
        return findings


# ---------------------------------------------------------------------------
# E104 – Implicit Cartesian (comma-separated FROM)
# ---------------------------------------------------------------------------

class ImplicitCartesianRule(BaseRule):
    rule_id = "E104_implicit_cartesian"
    category = Category.PERFORMANCE
    severity = Severity.HIGH
    description = "Comma-separated FROM without explicit JOIN condition."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        # sqlglot rewrites "FROM a, b" into "FROM a JOIN b" (no kind, side, or ON).
        for join in parsed.find_all(exp.Join):
            has_on = join.args.get("on") is not None
            has_using = join.args.get("using") is not None
            kind = getattr(join, "kind", None) or ""
            side = getattr(join, "side", None) or ""
            # A comma-join has no kind, no side, and no ON/USING
            if not kind and not side and not has_on and not has_using:
                right = join.find(exp.Table)
                right_name = right.alias_or_name if right else "?"
                findings.append(self._finding(
                    message=f"Implicit Cartesian: comma-join with '{right_name}' (no ON condition)",
                    suggestion=(
                        "Use explicit JOIN ... ON syntax. Comma-separated FROM "
                        "performs a Cartesian product and relies on WHERE for filtering, "
                        "which is error-prone and blocks optimizer pushdown."
                    ),
                    meta=meta,
                    offending_sql=self._snippet(join),
                    estimated_impact="Accidental Cartesian product if WHERE condition is missing or incomplete",
                ))
        return findings


# ---------------------------------------------------------------------------
# E105 – OR in JOIN ON clause
# ---------------------------------------------------------------------------

class OrInJoinRule(BaseRule):
    rule_id = "E105_or_in_join"
    category = Category.PERFORMANCE
    severity = Severity.MEDIUM
    description = "OR inside a JOIN ON clause prevents hash/merge join pushdown."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        for join in parsed.find_all(exp.Join):
            on_clause = join.args.get("on")
            if on_clause is None:
                continue
            if on_clause.find(exp.Or):
                findings.append(self._finding(
                    message="OR condition in JOIN ON clause prevents efficient join pushdown",
                    suggestion=(
                        "Refactor into separate JOINs combined with UNION ALL, or "
                        "rewrite as two equality conditions where possible. "
                        "Snowflake cannot use hash join with OR predicates."
                    ),
                    meta=meta,
                    offending_sql=self._snippet(on_clause),
                    estimated_impact="Falls back to nested-loop join; runtime scales quadratically",
                ))
        return findings


# ---------------------------------------------------------------------------
# E106 – Leading wildcard LIKE
# ---------------------------------------------------------------------------

class LeadingWildcardLikeRule(BaseRule):
    rule_id = "E106_leading_wildcard_like"
    category = Category.PERFORMANCE
    severity = Severity.LOW
    description = "LIKE pattern starting with '%' prevents search optimization."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        for like_node in parsed.find_all(exp.Like):
            pattern = like_node.expression
            if isinstance(pattern, exp.Literal) and pattern.is_string:
                val = pattern.this
                if isinstance(val, str) and val.startswith("%"):
                    findings.append(self._finding(
                        message=f"LIKE '{val}' — leading wildcard prevents search optimization",
                        suggestion=(
                            "Use SEARCH OPTIMIZATION or reverse-index pattern if full-text "
                            "matching is needed, or restructure the filter to avoid leading %."
                        ),
                        meta=meta,
                        offending_sql=self._snippet(like_node),
                        estimated_impact="Cannot use search optimization; full scan of string column",
                    ))
        return findings


# ---------------------------------------------------------------------------
# E107 – Non-sargable predicate (function on column in WHERE)
# ---------------------------------------------------------------------------

class NonSargablePredicateRule(BaseRule):
    rule_id = "E107_non_sargable_predicate"
    category = Category.PERFORMANCE
    severity = Severity.MEDIUM
    description = "Function wrapping a column in WHERE prevents partition pruning."

    _SARGABLE_FUNCS = {"date_trunc", "trunc"}

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        where = parsed.find(exp.Where)
        if not where:
            return findings
        for eq in where.find_all((exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ)):
            left = eq.left
            if isinstance(left, exp.Anonymous) or (
                isinstance(left, exp.Func)
                and not isinstance(left, (exp.Column, exp.Literal))
            ):
                func_name = getattr(left, "sql_name", lambda: "")()
                if func_name.lower() in self._SARGABLE_FUNCS:
                    continue
                findings.append(self._finding(
                    message=f"Function '{self._snippet(left, 80)}' wraps a column in WHERE",
                    suggestion=(
                        "Move the function to the right-hand side or create a computed column. "
                        "Snowflake cannot prune micro-partitions when a function wraps the filter column."
                    ),
                    meta=meta,
                    offending_sql=self._snippet(eq),
                    estimated_impact="Partition pruning disabled for this predicate",
                    doc_url="https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions#partition-pruning",
                ))
        return findings


# ---------------------------------------------------------------------------
# E108 – ORDER BY without LIMIT in subquery / CTE
# ---------------------------------------------------------------------------

class OrderByWithoutLimitRule(BaseRule):
    rule_id = "E108_orderby_no_limit"
    category = Category.PERFORMANCE
    severity = Severity.LOW
    description = "ORDER BY in subquery/CTE without LIMIT triggers a needless sort."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        for subq in parsed.find_all(exp.Subquery):
            inner = subq.this
            if inner and inner.find(exp.Order) and not inner.find(exp.Limit):
                findings.append(self._finding(
                    message="ORDER BY in subquery without LIMIT — sort has no effect on outer query",
                    suggestion=(
                        "Remove the ORDER BY or add a LIMIT. Snowflake does not guarantee "
                        "order propagation from subqueries; the sort wastes compute."
                    ),
                    meta=meta,
                    offending_sql=self._snippet(subq, max_len=200),
                    estimated_impact="Wasted sort step; may spill to remote storage on large datasets",
                ))
        for cte in parsed.find_all(exp.CTE):
            inner = cte.this
            if inner and inner.find(exp.Order) and not inner.find(exp.Limit):
                findings.append(self._finding(
                    message=f"ORDER BY in CTE '{cte.alias}' without LIMIT — sort has no effect",
                    suggestion="Remove the ORDER BY or add LIMIT to make the sort intentional.",
                    meta=meta,
                    offending_sql=self._snippet(cte, max_len=200),
                    estimated_impact="Unnecessary sort in CTE materialization",
                ))
        return findings


# ---------------------------------------------------------------------------
# E109 – Fan-out JOIN (many-to-many pattern)
# ---------------------------------------------------------------------------

class FanOutJoinRule(BaseRule):
    rule_id = "E109_fan_out_join"
    category = Category.PERFORMANCE
    severity = Severity.HIGH
    description = "JOIN without aggregation that likely fans out rows."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        joins = list(parsed.find_all(exp.Join))
        if not joins:
            return findings
        has_group = parsed.find(exp.Group) is not None
        has_distinct = any(
            getattr(s, "args", {}).get("distinct") for s in parsed.find_all(exp.Select)
        )
        if has_group or has_distinct:
            return findings
        # Heuristic: multiple joins without GROUP BY or DISTINCT
        if len(joins) >= 2:
            table_names = [
                (j.find(exp.Table).alias_or_name if j.find(exp.Table) else "?")
                for j in joins
            ]
            findings.append(self._finding(
                message=f"Multiple JOINs ({', '.join(table_names)}) without GROUP BY or DISTINCT",
                suggestion=(
                    "Verify join cardinality. If any join is one-to-many, the result set fans out. "
                    "Add GROUP BY, DISTINCT, or QUALIFY ROW_NUMBER() to control output row count."
                ),
                meta=meta,
                offending_sql=self._snippet(parsed, max_len=300),
                estimated_impact="Row count may multiply with each join; potential data explosion",
                severity_override=Severity.MEDIUM,
            ))
        return findings
