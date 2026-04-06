"""Rule registry: auto-discovers BaseRule subclasses and runs them."""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from flakeguard.rules.base import BaseRule
from flakeguard.sql_linter import LintFinding

# Import rule modules so subclasses register themselves.
import flakeguard.rules.performance_rules as _perf  # noqa: F401
import flakeguard.rules.correctness_rules as _corr  # noqa: F401
import flakeguard.rules.dbt_rules as _dbt  # noqa: F401

_RULE_CACHE: list[BaseRule] | None = None


def _discover_rules() -> list[BaseRule]:
    """Instantiate one instance of every BaseRule subclass."""
    global _RULE_CACHE
    if _RULE_CACHE is not None:
        return _RULE_CACHE

    rules: list[BaseRule] = []
    seen: set[type] = set()

    def _walk(cls: type) -> None:
        for sub in cls.__subclasses__():
            if sub not in seen and not getattr(sub, "__abstractmethods__", set()):
                seen.add(sub)
                rules.append(sub())
            _walk(sub)

    _walk(BaseRule)
    _RULE_CACHE = rules
    return _RULE_CACHE


def run_all(
    parsed: exp.Expression,
    meta: dict[str, Any],
) -> list[LintFinding]:
    """Run every registered rule against *parsed* and return aggregated findings."""
    findings: list[LintFinding] = []
    for rule in _discover_rules():
        try:
            findings.extend(rule.check(parsed, meta))
        except Exception:
            findings.append(
                LintFinding(
                    rule_id=rule.rule_id,
                    category=rule.category,
                    severity=rule.severity,
                    message=f"Rule {rule.rule_id} raised an internal error",
                    suggestion="File a bug report.",
                    model_id=meta.get("model_id"),
                    file_path=meta.get("file_path"),
                )
            )
    return findings
