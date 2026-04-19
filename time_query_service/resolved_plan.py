from __future__ import annotations

from datetime import date as Date
from typing import Any, Literal

from pydantic import ConfigDict, Field, model_validator

from time_query_service.time_plan import PairExpansion, StrictModel


ResolvedReasonKind = Literal[
    "llm_hard_fail",
    "unsupported_calendar_grain_rolling",
    "unsupported_anchor_semantics",
    "semantic_conflict",
    "calendar_data_missing",
    "all_sources_degraded",
]
TreeRole = Literal["atom", "union", "grouped_member", "filtered_collection", "derived", "derived_source"]
ComparisonDegradedReason = Literal[
    "subject_needs_clarification",
    "reference_needs_clarification",
    "both_need_clarification",
]


class Interval(StrictModel):
    start: Date
    end: Date
    end_inclusive: bool

    @model_validator(mode="after")
    def validate_bounds(self) -> "Interval":
        if self.start > self.end:
            raise ValueError("interval start must be <= end")
        return self


class TreeLabels(StrictModel):
    model_config = ConfigDict(extra="allow")

    absolute_core_time: Interval | None = None
    source_unit_id: str | None = None
    degraded: bool | None = None
    degraded_source_reason_kind: ResolvedReasonKind | None = None
    derivation_transform_summary: Any | None = None


class IntervalTree(StrictModel):
    role: TreeRole
    intervals: list[Interval] = Field(default_factory=list)
    children: list["IntervalTree"] = Field(default_factory=list)
    labels: TreeLabels = Field(default_factory=TreeLabels)


class ResolvedNode(StrictModel):
    tree: IntervalTree | None = None
    needs_clarification: bool = False
    reason_kind: ResolvedReasonKind | None = None
    derived_from: list[str] | None = None

    @model_validator(mode="after")
    def validate_consistency(self) -> "ResolvedNode":
        if self.needs_clarification and self.reason_kind is None:
            raise ValueError("degraded resolved node requires reason_kind")
        if not self.needs_clarification and self.reason_kind is not None:
            raise ValueError("healthy resolved node must not carry reason_kind")

        if self.tree is not None and self.tree.role == "derived":
            child_source_ids = [child.labels.source_unit_id for child in self.tree.children]
            if any(source_id is None for source_id in child_source_ids):
                raise ValueError("derived node children must all declare labels.source_unit_id")
            normalized = child_source_ids
            declared = self.derived_from or []
            if declared != normalized:
                raise ValueError(
                    f"derived_from must equal ordered child source_unit_id list: declared={declared}, child_labels={normalized}"
                )
        elif self.derived_from not in (None, []):
            raise ValueError("standalone resolved node must not carry derived_from")
        return self


class ResolvedComparisonPair(StrictModel):
    subject_unit_id: str
    reference_unit_id: str
    expansion: PairExpansion | None = None
    degraded: bool
    degraded_reason: ComparisonDegradedReason | None = None
    subject_absolute_core_time: Interval | None = None
    reference_absolute_core_time: Interval | None = None


class ResolvedComparison(StrictModel):
    comparison_id: str
    pairs: list[ResolvedComparisonPair] = Field(default_factory=list)


class ResolvedPlan(StrictModel):
    nodes: dict[str, ResolvedNode]
    comparisons: list[ResolvedComparison] = Field(default_factory=list)
