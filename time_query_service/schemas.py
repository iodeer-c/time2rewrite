from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


TimeUnit = Literal["day", "week", "month", "quarter", "half_year", "year"]
LiteralPeriodUnit = Literal["year", "month", "quarter", "half_year"]
SliceMode = Literal["first", "last"]
SliceUnit = Literal["day", "week", "month", "quarter", "year"]
SelectSubperiodUnit = Literal["day", "week", "month", "quarter", "half_year", "year"]
EnumerateSubperiodUnit = SelectSubperiodUnit
CalendarEventScope = Literal["consecutive_rest", "statutory"]
RangeEdge = Literal["start", "end"]
CalendarDayKind = Literal["workday", "restday", "holiday"]
AnchorName = Literal["system_date", "system_datetime"]
SelectSegmentMode = Literal["first", "last", "nth", "nth_from_end"]


class AnchorExpr(StrictModel):
    op: Literal["anchor"]
    name: AnchorName


class CurrentPeriodExpr(StrictModel):
    op: Literal["current_period"]
    unit: TimeUnit


class LiteralDateExpr(StrictModel):
    op: Literal["literal_date"]
    date: str

    @model_validator(mode="after")
    def validate_date_format(self) -> "LiteralDateExpr":
        datetime.strptime(self.date, "%Y-%m-%d")
        return self


class LiteralDatetimeExpr(StrictModel):
    op: Literal["literal_datetime"]
    datetime: str

    @model_validator(mode="after")
    def validate_datetime_format(self) -> "LiteralDatetimeExpr":
        datetime.strptime(self.datetime, "%Y-%m-%d %H:%M:%S")
        return self


class LiteralPeriodExpr(StrictModel):
    op: Literal["literal_period"]
    unit: LiteralPeriodUnit
    year: int
    month: int | None = Field(default=None, ge=1, le=12)
    quarter: int | None = Field(default=None, ge=1, le=4)
    half: int | None = Field(default=None, ge=1, le=2)

    @model_validator(mode="after")
    def validate_fields_for_unit(self) -> "LiteralPeriodExpr":
        if self.unit == "year":
            if any(value is not None for value in (self.month, self.quarter, self.half)):
                raise ValueError("literal_period year must not include month, quarter, or half")
            return self
        if self.unit == "month":
            if self.month is None:
                raise ValueError("literal_period month requires month")
            if self.quarter is not None or self.half is not None:
                raise ValueError("literal_period month must not include quarter or half")
            return self
        if self.unit == "quarter":
            if self.quarter is None:
                raise ValueError("literal_period quarter requires quarter")
            if self.month is not None or self.half is not None:
                raise ValueError("literal_period quarter must not include month or half")
            return self
        if self.half is None:
            raise ValueError("literal_period half_year requires half")
        if self.month is not None or self.quarter is not None:
            raise ValueError("literal_period half_year must not include month or quarter")
        return self


class ShiftExpr(StrictModel):
    op: Literal["shift"]
    unit: TimeUnit
    value: int
    base: "Expr"


class RollingExpr(StrictModel):
    op: Literal["rolling"]
    unit: TimeUnit
    value: int = Field(ge=1)
    anchor: Literal["system_date"] | None = Field(default=None, exclude_if=lambda value: value is None)
    anchor_expr: Expr | None = Field(default=None, exclude_if=lambda value: value is None)
    include_anchor: StrictBool | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def validate_anchor_fields(self) -> "RollingExpr":
        has_legacy_anchor = self.anchor is not None
        has_local_anchor = self.anchor_expr is not None
        if has_legacy_anchor == has_local_anchor:
            raise ValueError("rolling requires exactly one of anchor or anchor_expr")
        if has_local_anchor and self.include_anchor is None:
            self.include_anchor = False
        return self


class RollingHoursExpr(StrictModel):
    op: Literal["rolling_hours"]
    value: int = Field(ge=1)


class RollingMinutesExpr(StrictModel):
    op: Literal["rolling_minutes"]
    value: int = Field(ge=1)
    anchor_expr: "Expr"


class RollingBusinessDaysExpr(StrictModel):
    op: Literal["rolling_business_days"]
    region: str = "CN"
    value: int = Field(ge=1)
    anchor_expr: "Expr"
    include_anchor: StrictBool = False


class BoundedRangeExpr(StrictModel):
    op: Literal["bounded_range"]
    start: "Expr"
    end: "Expr"


class PeriodToDateExpr(StrictModel):
    op: Literal["period_to_date"]
    unit: TimeUnit
    anchor_expr: "Expr"


class CalendarEventRangeExpr(StrictModel):
    op: Literal["calendar_event_range"]
    region: str = "CN"
    event_key: str
    schedule_year: int | None = Field(default=None, exclude_if=lambda value: value is None)
    schedule_year_expr: Expr | None = Field(default=None, exclude_if=lambda value: value is None)
    scope: CalendarEventScope

    @model_validator(mode="after")
    def validate_schedule_year_fields(self) -> "CalendarEventRangeExpr":
        has_scalar_year = self.schedule_year is not None
        has_expr_year = self.schedule_year_expr is not None
        if has_scalar_year == has_expr_year:
            raise ValueError("calendar_event_range requires exactly one of schedule_year or schedule_year_expr")
        return self


class RangeEdgeExpr(StrictModel):
    op: Literal["range_edge"]
    edge: RangeEdge
    base: "Expr"


class BusinessDayOffsetExpr(StrictModel):
    op: Literal["business_day_offset"]
    region: str = "CN"
    value: int
    base: "Expr"

    @model_validator(mode="after")
    def validate_nonzero_value(self) -> "BusinessDayOffsetExpr":
        if self.value == 0:
            raise ValueError("business_day_offset value must not be zero")
        return self


class EnumerateCalendarDaysExpr(StrictModel):
    op: Literal["enumerate_calendar_days"]
    region: str = "CN"
    day_kind: CalendarDayKind
    base: "Expr"


class EnumerateMakeupWorkdaysExpr(StrictModel):
    op: Literal["enumerate_makeup_workdays"]
    region: str = "CN"
    event_key: str
    schedule_year: int | None = Field(default=None, exclude_if=lambda value: value is None)
    schedule_year_expr: Expr | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def validate_schedule_year_fields(self) -> "EnumerateMakeupWorkdaysExpr":
        has_scalar_year = self.schedule_year is not None
        has_expr_year = self.schedule_year_expr is not None
        if has_scalar_year == has_expr_year:
            raise ValueError("enumerate_makeup_workdays requires exactly one of schedule_year or schedule_year_expr")
        return self


class CurrentHourExpr(StrictModel):
    op: Literal["current_hour"]


class SelectHourExpr(StrictModel):
    op: Literal["select_hour"]
    hour: int = Field(ge=0, le=23)
    base: "Expr"


class SliceHoursExpr(StrictModel):
    op: Literal["slice_hours"]
    mode: SliceMode
    count: int = Field(ge=1, le=24)
    base: "Expr"


class EnumerateHoursExpr(StrictModel):
    op: Literal["enumerate_hours"]
    base: "Expr"


class SelectWeekdayExpr(StrictModel):
    op: Literal["select_weekday"]
    weekday: int = Field(ge=1, le=7)
    base: "Expr"


class SelectWeekendExpr(StrictModel):
    op: Literal["select_weekend"]
    base: "Expr"


class SelectMonthExpr(StrictModel):
    op: Literal["select_month"]
    month: int = Field(ge=1, le=12)
    base: "Expr"


class SelectQuarterExpr(StrictModel):
    op: Literal["select_quarter"]
    quarter: int = Field(ge=1, le=4)
    base: "Expr"


class SelectHalfYearExpr(StrictModel):
    op: Literal["select_half_year"]
    half: int = Field(ge=1, le=2)
    base: "Expr"


class SelectSegmentExpr(StrictModel):
    op: Literal["select_segment"]
    mode: SelectSegmentMode
    index: int | None = Field(default=None, ge=1)
    base: "Expr"

    @model_validator(mode="after")
    def validate_index_requirements(self) -> "SelectSegmentExpr":
        requires_index = self.mode in {"nth", "nth_from_end"}
        if requires_index and self.index is None:
            raise ValueError("select_segment index is required when mode is nth or nth_from_end")
        if not requires_index and self.index is not None:
            raise ValueError("select_segment index must be omitted when mode is first or last")
        return self


class SegmentsBoundsExpr(StrictModel):
    op: Literal["segments_bounds"]
    base: "Expr"


class SelectSubperiodExpr(StrictModel):
    op: Literal["select_subperiod"]
    unit: SelectSubperiodUnit
    index: int = Field(ge=1)
    complete_only: StrictBool = False
    base: "Expr"


class EnumerateSubperiodsExpr(StrictModel):
    op: Literal["enumerate_subperiods"]
    unit: EnumerateSubperiodUnit
    complete_only: StrictBool = False
    base: "Expr"


OccurrenceKind = Literal["weekday", "weekend"]
OccurrenceOrdinal = Literal["last", "nth_from_end"] | Annotated[int, Field(ge=1)]


class SelectOccurrenceExpr(StrictModel):
    op: Literal["select_occurrence"]
    kind: OccurrenceKind
    ordinal: OccurrenceOrdinal
    index: int | None = Field(default=None, ge=1)
    weekday: int | None = Field(default=None, ge=1, le=7)
    base: "Expr"

    @model_validator(mode="after")
    def validate_kind_specific_fields(self) -> "SelectOccurrenceExpr":
        if self.kind == "weekday" and self.weekday is None:
            raise ValueError("weekday is required when kind=weekday")
        if self.kind == "weekend" and self.weekday is not None:
            raise ValueError("weekday must be omitted when kind=weekend")
        if self.ordinal == "nth_from_end" and self.index is None:
            raise ValueError("index is required when ordinal=nth_from_end")
        if self.ordinal != "nth_from_end" and self.index is not None:
            raise ValueError("index must be omitted unless ordinal=nth_from_end")
        return self


class ReferenceExpr(StrictModel):
    op: Literal["reference"]
    ref: str


class SliceSubperiodsExpr(StrictModel):
    op: Literal["slice_subperiods"]
    mode: SliceMode
    unit: SliceUnit
    count: int = Field(ge=1)
    complete_only: StrictBool = False
    base: "Expr"


class SliceSegmentsExpr(StrictModel):
    op: Literal["slice_segments"]
    mode: SliceMode
    count: int = Field(ge=1)
    base: "Expr"


Expr = Annotated[
    AnchorExpr
    | CurrentPeriodExpr
    | LiteralDateExpr
    | LiteralDatetimeExpr
    | LiteralPeriodExpr
    | ShiftExpr
    | RollingExpr
    | RollingHoursExpr
    | RollingMinutesExpr
    | RollingBusinessDaysExpr
    | BoundedRangeExpr
    | PeriodToDateExpr
    | CalendarEventRangeExpr
    | RangeEdgeExpr
    | BusinessDayOffsetExpr
    | EnumerateCalendarDaysExpr
    | EnumerateMakeupWorkdaysExpr
    | CurrentHourExpr
    | SelectHourExpr
    | SliceHoursExpr
    | EnumerateHoursExpr
    | SelectWeekdayExpr
    | SelectWeekendExpr
    | SelectMonthExpr
    | SelectQuarterExpr
    | SelectHalfYearExpr
    | SelectSegmentExpr
    | SegmentsBoundsExpr
    | SelectSubperiodExpr
    | EnumerateSubperiodsExpr
    | SelectOccurrenceExpr
    | ReferenceExpr
    | SliceSubperiodsExpr
    | SliceSegmentsExpr,
    Field(discriminator="op"),
]

ShiftExpr.model_rebuild()
RollingExpr.model_rebuild()
LiteralPeriodExpr.model_rebuild()
RollingMinutesExpr.model_rebuild()
RollingBusinessDaysExpr.model_rebuild()
BoundedRangeExpr.model_rebuild()
PeriodToDateExpr.model_rebuild()
RangeEdgeExpr.model_rebuild()
BusinessDayOffsetExpr.model_rebuild()
EnumerateCalendarDaysExpr.model_rebuild()
EnumerateMakeupWorkdaysExpr.model_rebuild()
SelectHourExpr.model_rebuild()
SliceHoursExpr.model_rebuild()
EnumerateHoursExpr.model_rebuild()
SelectWeekdayExpr.model_rebuild()
SelectWeekendExpr.model_rebuild()
SelectMonthExpr.model_rebuild()
SelectQuarterExpr.model_rebuild()
SelectHalfYearExpr.model_rebuild()
SelectSegmentExpr.model_rebuild()
SegmentsBoundsExpr.model_rebuild()
SelectSubperiodExpr.model_rebuild()
EnumerateSubperiodsExpr.model_rebuild()
SelectOccurrenceExpr.model_rebuild()
SliceSubperiodsExpr.model_rebuild()
SliceSegmentsExpr.model_rebuild()


class TimeExpression(StrictModel):
    id: str
    text: str
    expr: Expr


class ParsedTimeExpressions(StrictModel):
    rolling_includes_today: StrictBool = False
    time_expressions: list[TimeExpression] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unique_ids(self) -> "ParsedTimeExpressions":
        seen = set()
        for item in self.time_expressions:
            if item.id in seen:
                raise ValueError(f"Duplicate time expression id: {item.id}")
            seen.add(item.id)
        return self


class ResolvedTimeExpression(StrictModel):
    id: str
    text: str
    source_id: str | None = None
    source_text: str | None = None
    start_time: str
    end_time: str
    timezone: str
    is_partial: bool | None = None


class ResolvedTimeExpressionGroup(StrictModel):
    id: str
    text: str
    source_id: str | None = None
    source_text: str | None = None
    start_time: str
    end_time: str
    timezone: str
    is_partial: bool | None = None
    children: list["ResolvedTimeExpressionGroup"] = Field(default_factory=list)


class RewriteHint(StrictModel):
    topology: Literal["discrete_set"]
    member_grain: str
    is_contiguous: bool
    preferred_rendering: Literal["default", "member_list"]


class NoMatchResult(StrictModel):
    source_id: str
    source_text: str
    reason: Literal["calendar_filter_empty"]
    expr_op: str
    day_kind: CalendarDayKind | Literal["makeup_workday"] | None = None
    event_key: str | None = None
    schedule_year: int | None = None


class ResolvedMetadata(StrictModel):
    calendar_version: str | None = None
    enumerated_counts: dict[str, int] | None = None
    rewrite_hints: dict[str, RewriteHint] | None = None
    no_match_results: list[NoMatchResult] | None = None


class ResolvedTimeExpressions(StrictModel):
    resolved_time_expressions: list[ResolvedTimeExpression] = Field(default_factory=list)
    resolved_time_expression_groups: list[ResolvedTimeExpressionGroup] = Field(default_factory=list)
    metadata: ResolvedMetadata | None = None


ResolvedTimeExpressionGroup.model_rebuild()


RewriteResultShape = Literal["single", "per_window", "aggregate", "compare", "date_identification"]
RewriteSlotRole = Literal[
    "filter_range",
    "enumeration_grain",
    "compare_left",
    "compare_right",
    "condition_modifier",
    "date_target",
    "default_window",
]
RewriteMatchMode = Literal["exact_text", "nth_occurrence"]
RewriteEditAction = Literal["replace_source_span", "preserve_and_supplement"]


class SemanticAnchorSlot(StrictModel):
    slot_id: str
    source_text: str
    role: RewriteSlotRole
    preserve_original_time_scaffold: bool = False
    occurrence_index: int | None = Field(default=None, ge=1)


class SemanticAnchor(StrictModel):
    result_shape: RewriteResultShape
    slots: list[SemanticAnchorSlot]
    compare_group_id: str | None = None

    @model_validator(mode="after")
    def validate_slots(self) -> "SemanticAnchor":
        if not self.slots:
            raise ValueError("semantic anchor requires at least one slot")
        return self


class ExecutionSpecConstraints(StrictModel):
    preserve_non_time_text: bool = True
    preserve_question_style: bool = True
    forbid_new_business_words: bool = True
    forbid_change_result_shape: bool = True
    forbid_drop_compare_binding: bool = True
    allow_parenthetical_only_when_specified: bool = True
    must_normalize_whitespace: bool = True


class ExecutionSpecSlot(StrictModel):
    slot_id: str
    source_text: str
    role: RewriteSlotRole
    match_mode: RewriteMatchMode = "exact_text"
    occurrence_index: int | None = Field(default=None, ge=1)
    render_mode: str
    rendered_time: str
    edit_action: RewriteEditAction
    preserve_original_time_scaffold: bool = False


class ExecutionSpec(StrictModel):
    result_shape: RewriteResultShape
    slots: list[ExecutionSpecSlot]
    constraints: ExecutionSpecConstraints = Field(default_factory=ExecutionSpecConstraints)

    @model_validator(mode="after")
    def validate_slots(self) -> "ExecutionSpec":
        if not self.slots:
            raise ValueError("execution spec requires at least one slot")
        return self


class RewriteRoutingState(str, Enum):
    CONSTRAINED_EXECUTION = "constrained_execution"
    FALLBACK_FULL_REWRITE = "fallback_full_rewrite"
    REWRITE_ABSTAINED_NO_MATCH = "rewrite_abstained_no_match"
    ORIGINAL_QUERY_FALLBACK = "original_query_fallback"


class RewriteRoutingResult(StrictModel):
    state: RewriteRoutingState
    execution_spec: ExecutionSpec | None = None
    semantic_anchor: SemanticAnchor | None = None
    rewritten_query: str | None = None


class TemporalContextRequest(StrictModel):
    system_date: str | None = None
    system_datetime: str | None = None
    timezone: str = "Asia/Shanghai"

    @model_validator(mode="after")
    def validate_temporal_context(self) -> "TemporalContextRequest":
        if self.system_datetime is None:
            if self.system_date is None:
                raise ValueError("system_date is required when system_datetime is omitted")
            return self

        try:
            parsed_datetime = datetime.strptime(self.system_datetime, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ValueError("system_datetime must use format YYYY-MM-DD HH:MM:SS") from exc

        if self.system_date is not None and self.system_date != parsed_datetime.strftime("%Y-%m-%d"):
            raise ValueError("system_date must match the date portion of system_datetime")

        return self


class ParseQueryRequest(TemporalContextRequest):
    query: str


class ResolveQueryRequest(TemporalContextRequest):
    parsed_time_expressions: ParsedTimeExpressions


class RewriteQueryRequest(StrictModel):
    original_query: str
    resolved_time_expressions: ResolvedTimeExpressions


class RewriteQueryResponse(StrictModel):
    rewritten_query: str | None


class PipelineRequest(TemporalContextRequest):
    query: str
    rewrite: bool = False


class PipelineResponse(StrictModel):
    parsed_time_expressions: ParsedTimeExpressions
    resolved_time_expressions: ResolvedTimeExpressions
    rewritten_query: str | None
