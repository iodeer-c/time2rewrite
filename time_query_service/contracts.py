from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from time_query_service.clarification_contract import (
    SUPPORTED_CALENDAR_SELECTOR_TYPES,
    SUPPORTED_EXPLICIT_PERIOD_UNITS,
    SUPPORTED_OFFSET_UNITS,
    SUPPORTED_REFERENCE_ALIGNMENTS,
    SUPPORTED_RELATIVE_TYPES,
    SUPPORTED_SINGLE_RELATIVE_UNITS,
    SUPPORTED_TO_DATE_UNITS,
)


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
Alignment = Literal["same_period"]
CalendarSelectorType = Literal["holiday", "workday", "business_day"]
CalendarMode = Literal["statutory", "configured"]
WindowType = Literal["named_period", "named_period_range", "date_range", "single_date"]
CalendarUnit = Literal["day", "week", "month", "quarter", "half", "year"]
RelativeType = Literal["single_relative", "to_date"]
RelativeDirection = Literal["current", "previous"]
OffsetDirection = Literal["before", "after"]
RelativeUnit = Literal["day", "week", "month", "quarter", "year"]
OffsetUnit = Literal["day"]
ShiftUnit = Literal["day", "week", "month", "quarter", "year"]
RegularGrain = Literal["day", "month", "quarter", "year"]
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


class NamedPeriodPoint(StrictModel):
    year_ref: YearRef
    month: int | None = None
    quarter: int | None = None
    half: int | None = None


class OffsetSpec(StrictModel):
    direction: OffsetDirection
    value: int
    unit: OffsetUnit

    @model_validator(mode="after")
    def validate_offset(self) -> "OffsetSpec":
        if self.unit not in SUPPORTED_OFFSET_UNITS:
            raise ValueError(f"Unsupported offset unit: {self.unit}")
        return self


class ShiftSpec(StrictModel):
    unit: ShiftUnit
    value: int


class CalendarSelectorSpec(StrictModel):
    selector_type: CalendarSelectorType
    selector_key: str | None = None

    @model_validator(mode="after")
    def validate_selector(self) -> "CalendarSelectorSpec":
        if self.selector_type not in SUPPORTED_CALENDAR_SELECTOR_TYPES:
            raise ValueError(f"Unsupported calendar selector type: {self.selector_type}")
        if self.selector_key is not None:
            raise ValueError("selector_key is not supported by the admitted ClarificationPlan contract")
        return self


class NestedWindowSpec(StrictModel):
    kind: NestedWindowKind
    value: Any

    @model_validator(mode="after")
    def validate_value(self) -> "NestedWindowSpec":
        self.value = validate_resolution_spec_for_kind(self.kind, self.value)
        return self


class ExplicitWindowResolutionSpec(StrictModel):
    window_type: WindowType
    calendar_unit: CalendarUnit
    year_ref: YearRef | None = None
    month: int | None = None
    quarter: int | None = None
    half: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    start_period: NamedPeriodPoint | None = None
    end_period: NamedPeriodPoint | None = None

    @model_validator(mode="after")
    def validate_payload(self) -> "ExplicitWindowResolutionSpec":
        if self.window_type == "single_date":
            if self.calendar_unit != "day":
                raise ValueError("single_date explicit_window requires calendar_unit=day")
            if self.start_date is None:
                raise ValueError("single_date explicit_window requires start_date")
            if any(
                value is not None
                for value in (self.end_date, self.year_ref, self.month, self.quarter, self.half, self.start_period, self.end_period)
            ):
                raise ValueError("single_date explicit_window only supports start_date")
            return self

        if self.window_type == "date_range":
            if self.calendar_unit != "day":
                raise ValueError("date_range explicit_window requires calendar_unit=day")
            if self.start_date is None or self.end_date is None:
                raise ValueError("date_range explicit_window requires start_date and end_date")
            if any(
                value is not None
                for value in (self.year_ref, self.month, self.quarter, self.half, self.start_period, self.end_period)
            ):
                raise ValueError("date_range explicit_window only supports start_date and end_date")
            return self

        if self.calendar_unit not in SUPPORTED_EXPLICIT_PERIOD_UNITS:
            raise ValueError(
                f"{self.window_type} explicit_window requires calendar_unit in {SUPPORTED_EXPLICIT_PERIOD_UNITS}"
            )

        if self.window_type == "named_period":
            if self.year_ref is None:
                raise ValueError("named_period explicit_window requires year_ref")
            if any(
                value is not None for value in (self.start_date, self.end_date, self.start_period, self.end_period)
            ):
                raise ValueError("named_period explicit_window does not support date or period range endpoints")
            self._validate_single_period_fields()
            return self

        if self.window_type != "named_period_range":
            raise ValueError(f"Unsupported explicit window_type: {self.window_type}")

        if self.start_period is None or self.end_period is None:
            raise ValueError("named_period_range explicit_window requires start_period and end_period")
        if any(
            value is not None
            for value in (self.year_ref, self.month, self.quarter, self.half, self.start_date, self.end_date)
        ):
            raise ValueError("named_period_range explicit_window only supports start_period and end_period")
        self._validate_period_point("start_period", self.start_period)
        self._validate_period_point("end_period", self.end_period)
        return self

    def _validate_single_period_fields(self) -> None:
        if self.calendar_unit == "year":
            if any(value is not None for value in (self.month, self.quarter, self.half)):
                raise ValueError("year named_period explicit_window does not support month/quarter/half fields")
            return
        if self.calendar_unit == "month":
            if self.month is None or self.quarter is not None or self.half is not None:
                raise ValueError("month named_period explicit_window requires month only")
            return
        if self.calendar_unit == "quarter":
            if self.quarter is None or self.month is not None or self.half is not None:
                raise ValueError("quarter named_period explicit_window requires quarter only")
            return
        if self.calendar_unit == "half":
            if self.half is None or self.month is not None or self.quarter is not None:
                raise ValueError("half named_period explicit_window requires half only")
            return
        raise ValueError(f"Unsupported named_period calendar_unit: {self.calendar_unit}")

    def _validate_period_point(self, label: str, point: NamedPeriodPoint) -> None:
        if self.calendar_unit == "year":
            if any(value is not None for value in (point.month, point.quarter, point.half)):
                raise ValueError(f"{label} for year range cannot include month/quarter/half")
            return
        if self.calendar_unit == "month":
            if point.month is None or point.quarter is not None or point.half is not None:
                raise ValueError(f"{label} for month range requires month only")
            return
        if self.calendar_unit == "quarter":
            if point.quarter is None or point.month is not None or point.half is not None:
                raise ValueError(f"{label} for quarter range requires quarter only")
            return
        if self.calendar_unit == "half":
            if point.half is None or point.month is not None or point.quarter is not None:
                raise ValueError(f"{label} for half range requires half only")
            return
        raise ValueError(f"Unsupported named_period_range calendar_unit: {self.calendar_unit}")


class RelativeWindowResolutionSpec(StrictModel):
    relative_type: RelativeType
    unit: RelativeUnit
    direction: RelativeDirection
    value: int
    include_today: bool = False

    @model_validator(mode="after")
    def validate_payload(self) -> "RelativeWindowResolutionSpec":
        if self.relative_type not in SUPPORTED_RELATIVE_TYPES:
            raise ValueError(f"Unsupported relative_type: {self.relative_type}")
        if self.relative_type == "single_relative":
            if self.direction != "previous" or self.unit not in SUPPORTED_SINGLE_RELATIVE_UNITS:
                raise ValueError("single_relative only supports previous day/week/month/quarter/year windows")
            return self
        if self.direction != "current" or self.unit not in SUPPORTED_TO_DATE_UNITS:
            raise ValueError("to_date only supports current month/quarter/year windows")
        return self


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

    @model_validator(mode="after")
    def validate_alignment(self) -> "ReferenceWindowResolutionSpec":
        if self.alignment not in SUPPORTED_REFERENCE_ALIGNMENTS:
            raise ValueError(f"Unsupported reference alignment: {self.alignment}")
        return self


class WindowWithRegularGrainResolutionSpec(StrictModel):
    window: NestedWindowSpec
    grain: RegularGrain


class WindowWithCalendarSelectorResolutionSpec(StrictModel):
    window: NestedWindowSpec
    selector: CalendarSelectorSpec


ResolutionSpec = (
    ExplicitWindowResolutionSpec
    | RelativeWindowResolutionSpec
    | HolidayWindowResolutionSpec
    | OffsetWindowResolutionSpec
    | ReferenceWindowResolutionSpec
    | WindowWithRegularGrainResolutionSpec
    | WindowWithCalendarSelectorResolutionSpec
)

NODE_KIND_TO_SPEC_MODEL: dict[NodeKind, type[ResolutionSpec]] = {
    "explicit_window": ExplicitWindowResolutionSpec,
    "relative_window": RelativeWindowResolutionSpec,
    "holiday_window": HolidayWindowResolutionSpec,
    "offset_window": OffsetWindowResolutionSpec,
    "reference_window": ReferenceWindowResolutionSpec,
    "window_with_regular_grain": WindowWithRegularGrainResolutionSpec,
    "window_with_calendar_selector": WindowWithCalendarSelectorResolutionSpec,
}


def validate_resolution_spec_for_kind(kind: NestedWindowKind | NodeKind, payload: Any) -> ResolutionSpec:
    spec_model = NODE_KIND_TO_SPEC_MODEL[kind]
    return spec_model.model_validate(payload)


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
        self.resolution_spec = validate_resolution_spec_for_kind(self.node_kind, self.resolution_spec)
        return self


class ClarificationPlan(StrictModel):
    nodes: list[ClarificationNode]
    comparison_groups: list[ComparisonGroup] = Field(default_factory=list)
