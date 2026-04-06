"""Base class for all lint rules."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sqlglot import exp

from flakeguard.sql_linter import Category, LintFinding, Severity


class BaseRule(ABC):
    """Abstract base for a single lint rule.

    Subclasses must define class-level ``rule_id``, ``category``, ``severity``,
    and implement ``check()``.
    """

    rule_id: str
    category: Category
    severity: Severity
    description: str = ""

    @abstractmethod
    def check(
        self,
        parsed: exp.Expression,
        meta: dict[str, Any],
    ) -> list[LintFinding]:
        """Run this rule against a parsed SQL expression.

        Args:
            parsed: sqlglot AST root of one SQL statement.
            meta: Context dict with keys model_id, file_path,
                  materialization, execution_time_seconds, raw_code.

        Returns:
            Zero or more LintFinding instances.
        """
        ...

    # Helpers available to all rules -----------------------------------------

    def _finding(
        self,
        message: str,
        suggestion: str,
        meta: dict[str, Any],
        *,
        offending_sql: str = "",
        estimated_impact: str = "",
        doc_url: str | None = None,
        line_number: int | None = None,
        severity_override: Severity | None = None,
    ) -> LintFinding:
        """Convenience builder so rules don't repeat boilerplate."""
        return LintFinding(
            rule_id=self.rule_id,
            category=self.category,
            severity=severity_override or self.severity,
            message=message,
            suggestion=suggestion,
            offending_sql=offending_sql,
            model_id=meta.get("model_id"),
            file_path=meta.get("file_path"),
            line_number=line_number,
            estimated_impact=estimated_impact,
            doc_url=doc_url,
        )

    @staticmethod
    def _snippet(node: exp.Expression, max_len: int = 200) -> str:
        """Return a short SQL snippet for ``offending_sql``."""
        try:
            sql = node.sql(dialect="snowflake")
        except Exception:
            sql = str(node)
        if len(sql) > max_len:
            return sql[: max_len - 3] + "..."
        return sql
