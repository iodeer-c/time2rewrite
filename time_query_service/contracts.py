from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


NodeKind = Literal[
    "explicit_window",
    "relative_window",
    "holiday_window",
    "offset_window",
    "reference_window",
    "window_with_regular_grain",
    "window_with_calendar_selector",
    "calendar_selector_only",
]

ReasonCode = Literal[
    "relative_time",
    "rolling_or_to_date",
    "holiday_or_business_calendar",
    "offset_from_anchor",
    "structural_enumeration",
    "already_explicit_natural_period",
    "shared_prefix_explicit",
    "same_period_reference",
]

RelationType = Literal[
    "year_over_year",
    "period_over_period",
    "same_period_reference",
    "generic_compare",
]

Direction = Literal["subject_to_reference", "reference_to_subject", "symmetric"]
ComparisonRole = Literal["subject", "reference", "peer"]
Alignment = Literal["same_period", "same_grain", "same_window"]
CalendarSelectorType = Literal["holiday", "workday", "trading_day", "business_day", "custom"]
CalendarMode = Literal["statutory", "configured"]
WindowType = Literal["named_period", "date_range", "single_date"]
CalendarUnit = Literal["day", "week", "month", "quarter", "half", "year"]
RelativeType = Literal["single_relative", "to_date", "rolling_window"]
RelativeDirection = Literal["current", "previous", "next", "last"]
OffsetDirection = Literal["before", "after"]
OffsetUnit = Literal["day", "week", "month", "quarter", "year"]
RegularGrain = Literal["day", "month", "quarter", "year"]
ScopeMode = Literal["implicit_current_context", "external_context"]
NestedWindowKind = Literal[
    "explicit_window",
    "relative_window",
    "holiday_window",
    "reference_window",
    "offset_window",
]


class YearRef(StrictModel):
    mode: Literal["absolute", "relative"]
    year: int | None = None
    offset: int | None = None

    @model_validator(mode="after")
    def validate_payload(self) -> "YearRef":
        if self.mode == "absolute" and self.year is None:
            raise ValueError("absolute year_ref requires year")
        if self.mode == "relative" and self.offset is None:
            raise ValueError("relative year_ref requires offset")
        return self


class OffsetSpec(StrictModel):
    direction: OffsetDirection
    value: int
    unit: OffsetUnit


class ShiftSpec(StrictModel):
    unit: OffsetUnit
    value: int


class CalendarSelectorSpec(StrictModel):
    selector_type: CalendarSelectorType
    selector_key: str | None = None

    @model_validator(mode="after")
    def validate_selector(self) -> "CalendarSelectorSpec":
        if self.selector_type == "custom" and not self.selector_key:
            raise ValueError("custom calendar selector requires selector_key")
        return self


class NestedWindowSpec(StrictModel):
    kind: NestedWindowKind
    value: dict[str, Any]


class ExplicitWindowResolutionSpec(StrictModel):
    window_type: WindowType
    calendar_unit: CalendarUnit
    year_ref: YearRef | None = None
    month: int | None = None
    quarter: int | None = None
    half: int | None = None
    start_date: date | None = None
    end_date: date | None = None


class RelativeWindowResolutionSpec(StrictModel):
    relative_type: RelativeType
    unit: OffsetUnit
    direction: RelativeDirection
    value: int
    include_today: bool = False


class HolidayWindowResolutionSpec(StrictModel):
    holiday_key: str
    year_ref: YearRef
    calendar_mode: CalendarMode = "configured"


class InlineOffsetBase(StrictModel):
    source: Literal["inline"]
    window: NestedWindowSpec


class NodeRefOffsetBase(StrictModel):
    source: Literal["node_ref"]
    node_id: str


OffsetBase = InlineOffsetBase | NodeRefOffsetBase


class OffsetWindowResolutionSpec(StrictModel):
    base: OffsetBase
    offset: OffsetSpec


class ReferenceWindowResolutionSpec(StrictModel):
    reference_node_id: str
    alignment: Alignment
    shift: ShiftSpec


class WindowWithRegularGrainResolutionSpec(StrictModel):
    window: NestedWindowSpec
    grain: RegularGrain


class WindowWithCalendarSelectorResolutionSpec(StrictModel):
    window: NestedWindowSpec
    selector: CalendarSelectorSpec


class CalendarSelectorOnlyResolutionSpec(StrictModel):
    selector: CalendarSelectorSpec
    scope_mode: ScopeMode


ResolutionSpec = (
    ExplicitWindowResolutionSpec
    | RelativeWindowResolutionSpec
    | HolidayWindowResolutionSpec
    | OffsetWindowResolutionSpec
    | ReferenceWindowResolutionSpec
    | WindowWithRegularGrainResolutionSpec
    | WindowWithCalendarSelectorResolutionSpec
    | CalendarSelectorOnlyResolutionSpec
)

NODE_KIND_TO_SPEC_MODEL: dict[NodeKind, type[ResolutionSpec]] = {
    "explicit_window": ExplicitWindowResolutionSpec,
    "relative_window": RelativeWindowResolutionSpec,
    "holiday_window": HolidayWindowResolutionSpec,
    "offset_window": OffsetWindowResolutionSpec,
    "reference_window": ReferenceWindowResolutionSpec,
    "window_with_regular_grain": WindowWithRegularGrainResolutionSpec,
    "window_with_calendar_selector": WindowWithCalendarSelectorResolutionSpec,
    "calendar_selector_only": CalendarSelectorOnlyResolutionSpec,
}


class ComparisonMember(StrictModel):
    node_id: str
    role: ComparisonRole


class ComparisonGroup(StrictModel):
    group_id: str
    relation_type: RelationType
    anchor_text: str
    anchor_ordinal: int
    direction: Direction
    members: list[ComparisonMember]


class Interval(StrictModel):
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_bounds(self) -> "Interval":
        if self.start_date > self.end_date:
            raise ValueError("interval start_date must be on or before end_date")
        return self


class ClarificationItem(StrictModel):
    node_id: str
    render_text: str
    ordinal: int
    display_exact_time: str
    surface_fragments: list[str] = Field(default_factory=list)
    intervals: list[Interval] = Field(default_factory=list)


class ClarificationNode(StrictModel):
    node_id: str
    render_text: str
    ordinal: int
    needs_clarification: bool
    node_kind: NodeKind
    reason_code: ReasonCode
    resolution_spec: Any
    surface_fragments: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_resolution_spec(self) -> "ClarificationNode":
        spec_model = NODE_KIND_TO_SPEC_MODEL[self.node_kind]
        self.resolution_spec = spec_model.model_validate(self.resolution_spec)
        return self


class ClarificationPlan(StrictModel):
    nodes: list[ClarificationNode]
    comparison_groups: list[ComparisonGroup] = Field(default_factory=list)
