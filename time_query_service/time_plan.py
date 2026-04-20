from __future__ import annotations

from datetime import date as Date
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


PreResolverReasonKind = Literal[
    "llm_hard_fail",
    "unsupported_calendar_grain_rolling",
    "unsupported_anchor_semantics",
    "semantic_conflict",
]
PeriodType = Literal["year", "quarter", "half_year", "month", "week", "day"]
RollingEndpoint = Literal["today", "yesterday", "this_month_end", "previous_complete"]
DayClass = Literal["workday", "weekend", "holiday", "statutory_holiday", "makeup_workday"]
CalendarEventScope = Literal["statutory", "consecutive_rest"]
MappedRangeMode = Literal["bounded_pair", "period_to_date", "rolling_map"]
MemberSelectionSelector = Literal["first", "last", "nth", "first_n", "last_n"]
TreeComparisonSingleScope = Literal["per_group", "global"]
OffsetUnit = Literal["day", "week", "month", "quarter", "half_year", "year"]


class SurfaceFragment(StrictModel):
    start: int
    end: int

    @model_validator(mode="after")
    def validate_offsets(self) -> "SurfaceFragment":
        if self.start < 0:
            raise ValueError("surface fragment start must be non-negative")
        if self.end <= self.start:
            raise ValueError("surface fragment end must be greater than start")
        return self


class NamedPeriod(StrictModel):
    kind: Literal["named_period"]
    period_type: PeriodType
    year: int | None = None
    quarter: Literal[1, 2, 3, 4] | None = None
    half: Literal[1, 2] | None = None
    month: int | None = None
    iso_week: int | None = None
    date: Date | None = None

    @model_validator(mode="after")
    def validate_period_fields(self) -> "NamedPeriod":
        if self.period_type == "year":
            return self
        if self.year is None:
            raise ValueError("named_period requires year for non-year period types")
        if self.period_type == "quarter" and self.quarter is None:
            raise ValueError("quarter named_period requires quarter")
        if self.period_type == "half_year" and self.half is None:
            raise ValueError("half_year named_period requires half")
        if self.period_type == "month" and self.month is None:
            raise ValueError("month named_period requires month")
        if self.period_type == "week" and self.iso_week is None:
            raise ValueError("week named_period requires iso_week")
        if self.period_type == "day" and self.date is None:
            raise ValueError("day named_period requires date")
        return self


class DateRange(StrictModel):
    kind: Literal["date_range"]
    start_date: Date
    end_date: Date
    end_inclusive: bool = True

    @model_validator(mode="after")
    def validate_range(self) -> "DateRange":
        if self.start_date > self.end_date:
            raise ValueError("date_range start_date must be <= end_date")
        return self


class RelativeWindow(StrictModel):
    kind: Literal["relative_window"]
    grain: Literal["day", "week", "month", "quarter", "half_year", "year"]
    offset_units: int


class RollingWindow(StrictModel):
    kind: Literal["rolling_window"]
    length: int
    unit: Literal["day", "week", "month", "quarter", "half_year", "year"]
    endpoint: RollingEndpoint
    include_endpoint: bool = True

    @model_validator(mode="after")
    def validate_length(self) -> "RollingWindow":
        if self.length <= 0:
            raise ValueError("rolling_window length must be > 0")
        return self


class RollingByCalendarUnit(StrictModel):
    kind: Literal["rolling_by_calendar_unit"]
    length: int
    day_class: DayClass
    endpoint: RollingEndpoint
    include_endpoint: bool = True

    @model_validator(mode="after")
    def validate_length(self) -> "RollingByCalendarUnit":
        if self.length <= 0:
            raise ValueError("rolling_by_calendar_unit length must be > 0")
        return self


class ScheduleYearRef(StrictModel):
    year: int | None = None
    source_unit_id: str | None = None

    @model_validator(mode="after")
    def validate_ref(self) -> "ScheduleYearRef":
        if (self.year is None) == (self.source_unit_id is None):
            raise ValueError("schedule_year_ref requires exactly one of year or source_unit_id")
        return self


class CalendarEvent(StrictModel):
    kind: Literal["calendar_event"]
    region: str
    event_key: str
    schedule_year_ref: ScheduleYearRef
    scope: CalendarEventScope


class HolidayEventCollection(StrictModel):
    kind: Literal["holiday_event_collection"]
    parent: RelativeWindow
    region: str
    scope: Literal["consecutive_rest"] = "consecutive_rest"
    selector: Literal["all"] = "all"

    @model_validator(mode="after")
    def validate_parent_scope(self) -> "HolidayEventCollection":
        if self.parent.grain != "year":
            raise ValueError("holiday_event_collection only supports year-scoped relative parents")
        return self


class EnumerationSet(StrictModel):
    kind: Literal["enumeration_set"]
    grain: Literal["year", "quarter", "half_year", "month", "week", "day", "calendar_event"]
    members: list["EnumerationMemberAnchor"]

    @model_validator(mode="after")
    def validate_members(self) -> "EnumerationSet":
        if len(self.members) < 2:
            raise ValueError("enumeration_set requires at least two members")
        return self


class GroupedTemporalValue(StrictModel):
    kind: Literal["grouped_temporal_value"]
    parent: "GroupedParentAnchor"
    child_grain: Literal["quarter", "half_year", "month", "week", "day"]
    selector: Literal["all", "first", "last", "nth", "first_n", "last_n"] = "all"


class MappedRange(StrictModel):
    kind: Literal["mapped_range"]
    mode: MappedRangeMode
    start: Any | None = None
    end: Any | None = None
    period_grain: Literal["day", "week", "month", "quarter", "half_year", "year"] | None = None
    anchor_ref: Any | None = None
    length: int | None = None
    unit: Literal["day", "week", "month", "quarter", "half_year", "year"] | None = None
    endpoint_set: Any | None = None
    include_endpoint: bool | None = None


EnumerationMemberAnchor = Annotated[NamedPeriod | DateRange | CalendarEvent, Field(discriminator="kind")]
GroupedParentAnchor = Annotated[
    NamedPeriod | DateRange | RelativeWindow | RollingWindow | CalendarEvent | MappedRange, Field(discriminator="kind")
]
Anchor = Annotated[
    NamedPeriod
    | DateRange
    | RelativeWindow
    | RollingWindow
    | RollingByCalendarUnit
    | EnumerationSet
    | GroupedTemporalValue
    | CalendarEvent
    | HolidayEventCollection
    | MappedRange,
    Field(discriminator="kind"),
]


class GrainExpansion(StrictModel):
    kind: Literal["grain_expansion"]
    target_grain: Literal["day", "week", "month", "quarter", "half_year", "year"]
    scope: TreeComparisonSingleScope | None = None


class CalendarFilter(StrictModel):
    kind: Literal["calendar_filter"]
    day_class: DayClass


class MemberSelection(StrictModel):
    kind: Literal["member_selection"]
    selector: MemberSelectionSelector
    n: int | None = None
    scope: TreeComparisonSingleScope | None = None

    @model_validator(mode="after")
    def validate_selector(self) -> "MemberSelection":
        if self.selector in {"nth", "first_n", "last_n"} and self.n is None:
            raise ValueError(f"{self.selector} selection requires n")
        return self


class Offset(StrictModel):
    kind: Literal["offset"]
    value: int
    unit: OffsetUnit


Modifier = Annotated[GrainExpansion | CalendarFilter | MemberSelection | Offset, Field(discriminator="kind")]


class Carrier(StrictModel):
    anchor: Anchor
    modifiers: list[Modifier] = Field(default_factory=list)


class DerivationTransform(StrictModel):
    model_config = ConfigDict(extra="allow")

    kind: str


class DerivationSource(StrictModel):
    source_unit_id: str
    transform: DerivationTransform


class StandaloneContent(StrictModel):
    content_kind: Literal["standalone"]
    carrier: Carrier | None = None


class DerivedContent(StrictModel):
    content_kind: Literal["derived"]
    sources: list[DerivationSource]

    @model_validator(mode="after")
    def validate_sources(self) -> "DerivedContent":
        if not self.sources:
            raise ValueError("derived content requires non-empty sources")
        return self


Content = Annotated[StandaloneContent | DerivedContent, Field(discriminator="content_kind")]


class PairExpansion(StrictModel):
    source_pair_index: int
    expansion_index: int
    expansion_cardinality: int
    subject_core_index: int | None = None
    reference_core_index: int | None = None

    @model_validator(mode="after")
    def validate_indices(self) -> "PairExpansion":
        if self.expansion_cardinality <= 0:
            raise ValueError("expansion_cardinality must be > 0")
        if self.expansion_index < 0 or self.expansion_index >= self.expansion_cardinality:
            raise ValueError("expansion_index must be within expansion_cardinality")
        return self


class ComparisonPair(StrictModel):
    subject_unit_id: str
    reference_unit_id: str
    expansion: PairExpansion | None = None


class Comparison(StrictModel):
    comparison_id: str
    anchor_text: str
    pairs: list[ComparisonPair]

    @model_validator(mode="after")
    def validate_pairs(self) -> "Comparison":
        if not self.pairs:
            raise ValueError("comparison requires at least one pair")
        seen: set[tuple[Any, ...]] = set()
        for pair in self.pairs:
            signature = (
                pair.subject_unit_id,
                pair.reference_unit_id,
                None if pair.expansion is None else tuple(pair.expansion.model_dump(mode="python").items()),
            )
            if signature in seen:
                raise ValueError("comparison pairs must be unique")
            seen.add(signature)
        return self


class Unit(StrictModel):
    unit_id: str
    render_text: str
    surface_fragments: list[SurfaceFragment]
    content: Content
    needs_clarification: bool = False
    reason_kind: PreResolverReasonKind | None = None

    @model_validator(mode="after")
    def validate_xor_semantics(self) -> "Unit":
        if self.needs_clarification:
            if self.reason_kind is None:
                raise ValueError("degraded unit requires reason_kind")
        elif self.reason_kind is not None:
            raise ValueError("healthy unit must not carry reason_kind")

        if isinstance(self.content, StandaloneContent):
            if not self.needs_clarification and self.content.carrier is None:
                raise ValueError("healthy standalone unit requires carrier")
        elif isinstance(self.content, DerivedContent):
            if not self.content.sources:
                raise ValueError("derived unit requires non-empty sources")
        return self


class TimePlan(StrictModel):
    query: str
    system_date: Date
    timezone: str
    units: list[Unit]
    comparisons: list[Comparison] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unit_ids(self) -> "TimePlan":
        unit_ids = [unit.unit_id for unit in self.units]
        if len(unit_ids) != len(set(unit_ids)):
            raise ValueError("unit_id values must be unique")
        return self
