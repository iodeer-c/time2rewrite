from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


TimeUnit = Literal["day", "week", "month", "quarter", "half_year", "year"]
SliceMode = Literal["first", "last"]
SliceUnit = Literal["day", "week", "month", "quarter"]
SelectSubperiodUnit = Literal["day", "week", "month", "quarter", "half_year"]


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
    | SelectWeekdayExpr
    | SelectWeekendExpr
    | SelectMonthExpr
    | SelectQuarterExpr
    | SelectHalfYearExpr
    | SelectSubperiodExpr
    | SelectOccurrenceExpr
    | ReferenceExpr
    | SliceSubperiodsExpr,
    Field(discriminator="op"),
]

ShiftExpr.model_rebuild()
SelectWeekdayExpr.model_rebuild()
SelectWeekendExpr.model_rebuild()
SelectMonthExpr.model_rebuild()
SelectQuarterExpr.model_rebuild()
SelectHalfYearExpr.model_rebuild()
SelectSubperiodExpr.model_rebuild()
SelectOccurrenceExpr.model_rebuild()
SliceSubperiodsExpr.model_rebuild()


class TimeExpression(StrictModel):
    id: str
    text: str
    expr: Expr


class ParsedTimeExpressions(StrictModel):
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
    start_time: str
    end_time: str
    timezone: str


class ResolvedTimeExpressions(StrictModel):
    resolved_time_expressions: list[ResolvedTimeExpression] = Field(default_factory=list)


class ParseQueryRequest(StrictModel):
    query: str
    system_date: str
    timezone: str = "Asia/Shanghai"


class ResolveQueryRequest(StrictModel):
    parsed_time_expressions: ParsedTimeExpressions
    system_date: str
    timezone: str = "Asia/Shanghai"


class RewriteQueryRequest(StrictModel):
    original_query: str
    resolved_time_expressions: ResolvedTimeExpressions


class RewriteQueryResponse(StrictModel):
    rewritten_query: str


class PipelineRequest(StrictModel):
    query: str
    system_date: str
    timezone: str = "Asia/Shanghai"
    rewrite: bool = False


class PipelineResponse(StrictModel):
    parsed_time_expressions: ParsedTimeExpressions
    resolved_time_expressions: ResolvedTimeExpressions
    rewritten_query: str | None
