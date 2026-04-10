from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


TimeUnit = Literal["day", "week", "month", "quarter", "half_year", "year"]
SliceMode = Literal["first", "last"]
SliceUnit = Literal["day", "week", "month", "quarter", "year"]
SelectSubperiodUnit = Literal["day", "week", "month", "quarter", "half_year", "year"]
EnumerateSubperiodUnit = SelectSubperiodUnit
CalendarEventScope = Literal["consecutive_rest", "statutory"]
RangeEdge = Literal["start", "end"]
CalendarDayKind = Literal["workday", "restday", "holiday"]


class AnchorExpr(StrictModel):
    op: Literal["anchor"]
    name: Literal["system_date"]


class CurrentPeriodExpr(StrictModel):
    op: Literal["current_period"]
    unit: TimeUnit


class ShiftExpr(StrictModel):
    op: Literal["shift"]
    unit: TimeUnit
    value: int
    base: "Expr"


class RollingExpr(StrictModel):
    op: Literal["rolling"]
    unit: TimeUnit
    value: int = Field(ge=1)
    anchor: Literal["system_date"]


class RollingHoursExpr(StrictModel):
    op: Literal["rolling_hours"]
    value: int = Field(ge=1)


class BoundedRangeExpr(StrictModel):
    op: Literal["bounded_range"]
    start: "Expr"
    end: "Expr"


class CalendarEventRangeExpr(StrictModel):
    op: Literal["calendar_event_range"]
    region: str = "CN"
    event_key: str
    year: int
    scope: CalendarEventScope


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
    year: int


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


class SelectSubperiodExpr(StrictModel):
    op: Literal["select_subperiod"]
    unit: SelectSubperiodUnit
    index: int = Field(ge=1)
    base: "Expr"


class EnumerateSubperiodsExpr(StrictModel):
    op: Literal["enumerate_subperiods"]
    unit: EnumerateSubperiodUnit
    base: "Expr"


OccurrenceKind = Literal["weekday", "weekend"]
OccurrenceOrdinal = Literal["last"] | Annotated[int, Field(ge=1)]


class SelectOccurrenceExpr(StrictModel):
    op: Literal["select_occurrence"]
    kind: OccurrenceKind
    ordinal: OccurrenceOrdinal
    weekday: int | None = Field(default=None, ge=1, le=7)
    base: "Expr"

    @model_validator(mode="after")
    def validate_kind_specific_fields(self) -> "SelectOccurrenceExpr":
        if self.kind == "weekday" and self.weekday is None:
            raise ValueError("weekday is required when kind=weekday")
        if self.kind == "weekend" and self.weekday is not None:
            raise ValueError("weekday must be omitted when kind=weekend")
        return self


class ReferenceExpr(StrictModel):
    op: Literal["reference"]
    ref: str


class SliceSubperiodsExpr(StrictModel):
    op: Literal["slice_subperiods"]
    mode: SliceMode
    unit: SliceUnit
    count: int = Field(ge=1)
    base: "Expr"


Expr = Annotated[
    AnchorExpr
    | CurrentPeriodExpr
    | ShiftExpr
    | RollingExpr
    | RollingHoursExpr
    | BoundedRangeExpr
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
    | SelectSubperiodExpr
    | EnumerateSubperiodsExpr
    | SelectOccurrenceExpr
    | ReferenceExpr
    | SliceSubperiodsExpr,
    Field(discriminator="op"),
]

ShiftExpr.model_rebuild()
BoundedRangeExpr.model_rebuild()
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
SelectSubperiodExpr.model_rebuild()
EnumerateSubperiodsExpr.model_rebuild()
SelectOccurrenceExpr.model_rebuild()
SliceSubperiodsExpr.model_rebuild()


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


class ResolvedMetadata(StrictModel):
    calendar_version: str | None = None
    enumerated_counts: dict[str, int] | None = None


class ResolvedTimeExpressions(StrictModel):
    resolved_time_expressions: list[ResolvedTimeExpression] = Field(default_factory=list)
    metadata: ResolvedMetadata | None = None


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
    rewritten_query: str


class PipelineRequest(TemporalContextRequest):
    query: str
    rewrite: bool = False


class PipelineResponse(StrictModel):
    parsed_time_expressions: ParsedTimeExpressions
    resolved_time_expressions: ResolvedTimeExpressions
    rewritten_query: str | None
