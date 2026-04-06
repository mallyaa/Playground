"""Correctness-related lint rules (E201 – E204)."""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from flakeguard.rules.base import BaseRule
from flakeguard.sql_linter import Category, LintFinding, Severity


# ---------------------------------------------------------------------------
# E201 – Excessive nested subqueries
# ---------------------------------------------------------------------------

class NestedSubqueryRule(BaseRule):
    rule_id = "E201_nested_subquery"
    category = Category.CORRECTNESS
    severity = Severity.MEDIUM
    description = "Deeply nested subqueries hurt readability and may block optimizer."

    MAX_DEPTH = 2

    def _max_depth(self, node: exp.Expression, current: int = 0) -> int:
        best = current
        for child in node.iter_expressions():
            if isinstance(child, exp.Subquery):
                best = max(best, self._max_depth(child, current + 1))
            else:
                best = max(best, self._max_depth(child, current))
        return best

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        depth = self._max_depth(parsed)
        if depth <= self.MAX_DEPTH:
            return []
        return [self._finding(
            message=f"Subquery nesting depth is {depth} (threshold: {self.MAX_DEPTH})",
            suggestion=(
                f"Refactor nested subqueries into CTEs (WITH clauses) or intermediate dbt models. "
                f"CTEs improve readability and give Snowflake's optimizer more flexibility."
            ),
            meta=meta,
            offending_sql=self._snippet(parsed, max_len=300),
            estimated_impact=f"Optimizer may not inline deeply nested subqueries; readability degrades",
        )]


# ---------------------------------------------------------------------------
# E202 – DISTINCT on an already-grouped query
# ---------------------------------------------------------------------------

class DistinctOverGroupByRule(BaseRule):
    rule_id = "E202_distinct_over_groupby"
    category = Category.CORRECTNESS
    severity = Severity.LOW
    description = "SELECT DISTINCT on an already-grouped result is redundant."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        for select in parsed.find_all(exp.Select):
            is_distinct = select.args.get("distinct")
            has_group = select.find(exp.Group) is not None
            if is_distinct and has_group:
                findings.append(self._finding(
                    message="SELECT DISTINCT combined with GROUP BY — the GROUP BY already guarantees uniqueness",
                    suggestion=(
                        "Remove DISTINCT; GROUP BY output rows are already unique on "
                        "the grouped columns. The extra DISTINCT adds a redundant sort/hash step."
                    ),
                    meta=meta,
                    offending_sql=self._snippet(select, max_len=200),
                    estimated_impact="Redundant dedup pass; wastes compute",
                ))
        return findings


# ---------------------------------------------------------------------------
# E203 – UNION where UNION ALL likely intended
# ---------------------------------------------------------------------------

class UnionVsUnionAllRule(BaseRule):
    rule_id = "E203_union_vs_union_all"
    category = Category.CORRECTNESS
    severity = Severity.MEDIUM
    description = "UNION (dedup) used where UNION ALL is likely intended."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        for union in parsed.find_all(exp.Union):
            # exp.Union with distinct=True means UNION (not ALL)
            is_distinct_union = union.args.get("distinct", False)
            if is_distinct_union:
                findings.append(self._finding(
                    message="UNION (with implicit dedup) — did you mean UNION ALL?",
                    suggestion=(
                        "UNION performs a costly sort + dedup. If the branches are already "
                        "distinct or dedup is unnecessary, use UNION ALL to avoid the overhead."
                    ),
                    meta=meta,
                    offending_sql=self._snippet(union, max_len=300),
                    estimated_impact="Extra sort + dedup step; can double query runtime on large sets",
                ))
        return findings


# ---------------------------------------------------------------------------
# E204 – Unused CTE
# ---------------------------------------------------------------------------

class UnusedCTERule(BaseRule):
    rule_id = "E204_unused_cte"
    category = Category.CORRECTNESS
    severity = Severity.LOW
    description = "CTE defined but never referenced in the query body."

    def check(self, parsed: exp.Expression, meta: dict[str, Any]) -> list[LintFinding]:
        findings: list[LintFinding] = []
        # Collect CTE aliases
        cte_aliases: list[str] = []
        for cte in parsed.find_all(exp.CTE):
            alias = cte.alias
            if alias:
                cte_aliases.append(alias)
        if not cte_aliases:
            return findings

        try:
            full_sql = parsed.sql(dialect="snowflake")
        except Exception:
            return findings

        for alias in cte_aliases:
            # Check if alias appears as a table reference outside the CTE definition
            referenced = False
            for table in parsed.find_all(exp.Table):
                if table.alias_or_name == alias:
                    referenced = True
                    break
            if not referenced:
                findings.append(self._finding(
                    message=f"CTE '{alias}' is defined but never referenced",
                    suggestion=(
                        f"Remove the unused CTE '{alias}' to reduce query complexity. "
                        f"Snowflake may still execute unused CTEs, wasting compute."
                    ),
                    meta=meta,
                    offending_sql=f"WITH {alias} AS (...)",
                    estimated_impact="Dead code; potential wasted compute if Snowflake materializes it",
                ))
        return findings
