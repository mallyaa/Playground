"""Configuration models and defaults for flakeguard."""

from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class WarehouseSize(str, Enum):
    """Snowflake warehouse size tiers with mock credit/hour rates."""

    XS = "XS"
    S = "S"
    M = "M"
    L = "L"
    XL = "XL"


# Mock credits per hour (typical Snowflake pricing tier approximations)
CREDITS_PER_HOUR: dict[WarehouseSize, float] = {
    WarehouseSize.XS: 1.0,
    WarehouseSize.S: 2.0,
    WarehouseSize.M: 4.0,
    WarehouseSize.L: 8.0,
    WarehouseSize.XL: 16.0,
}


class FlakeguardConfig(BaseModel):
    """Root configuration for flakeguard analysis."""

    project_path: Path = Field(..., description="Path to dbt project root")
    manifest_path: Optional[Path] = Field(
        default=None,
        description="Override path to target/manifest.json; default: project_path/target/manifest.json",
    )
    warehouse: WarehouseSize = Field(
        default=WarehouseSize.M,
        description="Warehouse size for cost estimation",
    )
    default_execution_seconds: float = Field(
        default=60.0,
        description="Default execution time per model when not provided",
    )
    execution_time_overrides: dict[str, float] = Field(
        default_factory=dict,
        description="Model name -> execution_time_seconds overrides",
    )

    def get_manifest_path(self) -> Path:
        """Resolve manifest.json path."""
        if self.manifest_path is not None:
            return self.manifest_path
        return self.project_path / "target" / "manifest.json"

    def get_execution_time(self, model_name: str) -> float:
        """Return execution time in seconds for a model."""
        return self.execution_time_overrides.get(
            model_name, self.default_execution_seconds
        )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class LintConfig(BaseModel):
    """Configuration for the SQL lint engine and quality gate."""

    gate_threshold: int = Field(
        default=20,
        description="Weighted score threshold; above this the gate FAILs. "
        "Weights: CRITICAL=10, HIGH=5, MEDIUM=2, LOW=1, INFO=0.",
    )
    disabled_rules: list[str] = Field(
        default_factory=list,
        description="Rule IDs to skip (e.g. ['E106_leading_wildcard_like']).",
    )
    full_refresh_runtime_threshold: float = Field(
        default=120.0,
        description="Seconds above which a full-refresh table triggers E301.",
    )
    nested_subquery_max_depth: int = Field(
        default=2,
        description="Maximum subquery nesting depth before E201 fires.",
    )
