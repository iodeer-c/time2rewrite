from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import Field

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.contracts import (
    CalendarSelectorSpec,
    ClarificationItem,
    ClarificationPlan,
    ClarificationNode,
    ExplicitWindowResolutionSpec,
    HolidayWindowResolutionSpec,
    Interval,
    NestedWindowSpec,
    OffsetWindowResolutionSpec,
    ReferenceWindowResolutionSpec,
    RelativeWindowResolutionSpec,
    StrictModel,
    YearRef,
    WindowWithCalendarSelectorResolutionSpec,
)


class ResolutionResult(StrictModel):
    items: list[ClarificationItem] = Field(default_factory=list)


def resolve_plan(
    *,
    plan: ClarificationPlan | dict[str, Any],
    system_date: str | None = None,
    system_datetime: str | None = None,
    timezone: str = "Asia/Shanghai",
    business_calendar: BusinessCalendarPort | None = None,
) -> ResolutionResult:
    normalized_plan = plan if isinstance(plan, ClarificationPlan) else ClarificationPlan.model_validate(plan)
    anchor_date = _resolve_anchor_date(system_date=system_date, system_datetime=system_datetime, timezone=timezone)
    node_lookup = {node.node_id: node for node in normalized_plan.nodes}
    resolved_by_node_id: dict[str, list[Interval]] = {}

    items: list[ClarificationItem] = []
    for node in normalized_plan.nodes:
        intervals = _resolve_node_intervals(
            node=node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        if not node.needs_clarification:
            continue
        items.append(_build_clarification_item(node=node, intervals=intervals))
    return ResolutionResult(items=items)


def _resolve_node_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> list[Interval]:
    cached = resolved_by_node_id.get(node.node_id)
    if cached is not None:
        return cached

    if node.node_kind == "relative_window":
        intervals = _resolve_relative_window_intervals(node=node, anchor_date=anchor_date)
    elif node.node_kind == "holiday_window":
        intervals = _resolve_holiday_window_intervals(
            node=node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
        )
    elif node.node_kind == "explicit_window":
        intervals = _resolve_explicit_window_intervals(node=node, anchor_date=anchor_date)
    elif node.node_kind == "reference_window":
        intervals = _resolve_reference_window_intervals(
            node=node,
            anchor_date=anchor_date,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
            business_calendar=business_calendar,
        )
    elif node.node_kind == "offset_window":
        intervals = _resolve_offset_window_intervals(
            node=node,
            anchor_date=anchor_date,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
            business_calendar=business_calendar,
        )
    elif node.node_kind == "window_with_calendar_selector":
        spec = WindowWithCalendarSelectorResolutionSpec.model_validate(node.resolution_spec)
        base_intervals = _resolve_inline_window_intervals(
            window=spec.window,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        if len(base_intervals) != 1:
            raise ValueError("Current resolver slice only supports single-interval calendar selector windows.")
        start_date = base_intervals[0].start_date
        end_date = base_intervals[0].end_date
        matched_dates = _filter_calendar_dates(
            start_date=start_date,
            end_date=end_date,
            selector=spec.selector,
            business_calendar=business_calendar,
        )
        intervals = _compress_dates_to_intervals(matched_dates)
    else:
        raise ValueError(f"Unsupported node_kind for current resolver slice: {node.node_kind}")

    resolved_by_node_id[node.node_id] = intervals
    return intervals


def _build_clarification_item(*, node: ClarificationNode, intervals: list[Interval]) -> ClarificationItem:
    return ClarificationItem(
        node_id=node.node_id,
        render_text=node.render_text,
        ordinal=node.ordinal,
        display_exact_time=_render_intervals(intervals),
        surface_fragments=node.surface_fragments,
        intervals=intervals,
    )


def _resolve_relative_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
) -> list[Interval]:
    spec = RelativeWindowResolutionSpec.model_validate(node.resolution_spec)
    if spec.relative_type == "single_relative" and spec.unit == "day" and spec.direction == "previous":
        target_date = anchor_date - timedelta(days=spec.value)
        return [Interval(start_date=target_date, end_date=target_date)]
    if spec.relative_type == "single_relative" and spec.unit == "week" and spec.direction == "previous":
        current_week_monday = anchor_date - timedelta(days=anchor_date.weekday())
        start_date = current_week_monday - timedelta(weeks=spec.value)
        end_date = start_date + timedelta(days=6)
        return [Interval(start_date=start_date, end_date=end_date)]
    if spec.relative_type == "single_relative" and spec.unit == "month" and spec.direction == "previous":
        target_year = anchor_date.year
        target_month = anchor_date.month - spec.value
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        end_day = calendar.monthrange(target_year, target_month)[1]
        return [Interval(start_date=date(target_year, target_month, 1), end_date=date(target_year, target_month, end_day))]
    if spec.relative_type == "to_date" and spec.unit == "month" and spec.direction == "current":
        start_date = anchor_date.replace(day=1)
        end_date = anchor_date if spec.include_today else anchor_date - timedelta(days=1)
        return [Interval(start_date=start_date, end_date=end_date)]
    raise ValueError(
        "Current resolver slice only supports previous single-day/week/month relative windows "
        "and current month-to-date windows."
    )


def _resolve_holiday_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
) -> list[Interval]:
    if business_calendar is None:
        raise ValueError("Business calendar is required for holiday_window resolution.")

    spec = HolidayWindowResolutionSpec.model_validate(node.resolution_spec)
    schedule_year = _resolve_year_ref(spec.year_ref, anchor_date=anchor_date)
    scope = "statutory" if spec.calendar_mode == "statutory" else "consecutive_rest"
    holiday_range = business_calendar.get_event_span(
        region="CN",
        event_key=spec.holiday_key,
        schedule_year=schedule_year,
        scope=scope,
    )
    if holiday_range is None:
        raise ValueError(
            f"Missing business calendar data for holiday={spec.holiday_key}, schedule_year={schedule_year}."
        )
    return [Interval(start_date=holiday_range[0], end_date=holiday_range[1])]


def _resolve_explicit_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
) -> list[Interval]:
    spec = ExplicitWindowResolutionSpec.model_validate(node.resolution_spec)
    if spec.window_type == "single_date":
        if spec.start_date is None:
            raise ValueError("single_date explicit_window requires start_date")
        return [Interval(start_date=spec.start_date, end_date=spec.start_date)]
    if spec.window_type == "date_range":
        if spec.start_date is None or spec.end_date is None:
            raise ValueError("date_range explicit_window requires start_date and end_date")
        return [Interval(start_date=spec.start_date, end_date=spec.end_date)]
    if spec.window_type != "named_period":
        raise ValueError(f"Unsupported explicit window_type: {spec.window_type}")

    year = _resolve_year_ref(spec.year_ref or YearRef(mode="absolute", year=anchor_date.year), anchor_date=anchor_date)
    if spec.calendar_unit == "year":
        return [Interval(start_date=date(year, 1, 1), end_date=date(year, 12, 31))]
    if spec.calendar_unit == "month":
        if spec.month is None:
            raise ValueError("month explicit_window requires month")
        month_last_day = calendar.monthrange(year, spec.month)[1]
        return [Interval(start_date=date(year, spec.month, 1), end_date=date(year, spec.month, month_last_day))]
    if spec.calendar_unit == "quarter":
        if spec.quarter is None:
            raise ValueError("quarter explicit_window requires quarter")
        start_month = (spec.quarter - 1) * 3 + 1
        end_month = start_month + 2
        end_day = calendar.monthrange(year, end_month)[1]
        return [Interval(start_date=date(year, start_month, 1), end_date=date(year, end_month, end_day))]
    if spec.calendar_unit == "half":
        if spec.half is None:
            raise ValueError("half explicit_window requires half")
        start_month = 1 if spec.half == 1 else 7
        end_month = 6 if spec.half == 1 else 12
        end_day = calendar.monthrange(year, end_month)[1]
        return [Interval(start_date=date(year, start_month, 1), end_date=date(year, end_month, end_day))]
    raise ValueError(f"Unsupported explicit calendar_unit for current resolver slice: {spec.calendar_unit}")


def _resolve_reference_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
    business_calendar: BusinessCalendarPort | None,
) -> list[Interval]:
    spec = ReferenceWindowResolutionSpec.model_validate(node.resolution_spec)
    reference_node = node_lookup.get(spec.reference_node_id)
    if reference_node is None:
        raise ValueError(f"Missing reference node: {spec.reference_node_id}")
    reference_intervals = _resolve_node_intervals(
        node=reference_node,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )
    return [_shift_interval(interval, unit=spec.shift.unit, value=spec.shift.value) for interval in reference_intervals]


def _resolve_offset_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
    business_calendar: BusinessCalendarPort | None,
) -> list[Interval]:
    spec = OffsetWindowResolutionSpec.model_validate(node.resolution_spec)
    if spec.offset.unit != "day":
        raise ValueError("Current resolver slice only supports day-based offset windows.")

    base_intervals = _resolve_offset_base_intervals(
        spec=spec,
        anchor_date=anchor_date,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
        business_calendar=business_calendar,
    )
    if len(base_intervals) != 1:
        raise ValueError("Current resolver slice only supports single-interval offset bases.")

    base_interval = base_intervals[0]
    count = spec.offset.value
    if spec.offset.direction == "after":
        start_date = base_interval.end_date + timedelta(days=1)
        end_date = start_date + timedelta(days=count - 1)
    else:
        end_date = base_interval.start_date - timedelta(days=1)
        start_date = end_date - timedelta(days=count - 1)
    return [Interval(start_date=start_date, end_date=end_date)]


def _resolve_offset_base_intervals(
    *,
    spec: OffsetWindowResolutionSpec,
    anchor_date: date,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
    business_calendar: BusinessCalendarPort | None,
) -> list[Interval]:
    if spec.base.source == "node_ref":
        reference_node = node_lookup.get(spec.base.node_id)
        if reference_node is None:
            raise ValueError(f"Missing offset base node: {spec.base.node_id}")
        return _resolve_node_intervals(
            node=reference_node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )

    return _resolve_inline_window_intervals(
        window=spec.base.window,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )


def _resolve_anchor_date(
    *,
    system_date: str | None,
    system_datetime: str | None,
    timezone: str,
) -> date:
    if system_date:
        return date.fromisoformat(system_date)
    if system_datetime:
        return datetime.fromisoformat(system_datetime).date()
    return datetime.now(ZoneInfo(timezone)).date()


def _resolve_inline_window_intervals(
    *,
    window: NestedWindowSpec,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> list[Interval]:
    inline_node = ClarificationNode(
        node_id="__inline__",
        render_text="__inline__",
        ordinal=0,
        needs_clarification=False,
        node_kind=window.kind,
        reason_code="structural_enumeration",
        resolution_spec=window.value,
        surface_fragments=[],
    )
    return _resolve_node_intervals(
        node=inline_node,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )


def _resolve_year_ref(year_ref: YearRef, *, anchor_date: date) -> int:
    if year_ref.mode == "absolute":
        assert year_ref.year is not None
        return year_ref.year
    assert year_ref.offset is not None
    return anchor_date.year + year_ref.offset


def _filter_calendar_dates(
    *,
    start_date: date,
    end_date: date,
    selector: CalendarSelectorSpec,
    business_calendar: BusinessCalendarPort | None,
) -> list[date]:
    if business_calendar is None:
        raise ValueError("Business calendar is required for calendar-sensitive selectors.")

    matched: list[date] = []
    cursor = start_date
    while cursor <= end_date:
        status = business_calendar.get_day_status(region="CN", d=cursor)
        if selector.selector_type == "workday" and status.is_workday:
            matched.append(cursor)
        elif selector.selector_type == "holiday" and status.is_holiday:
            matched.append(cursor)
        elif selector.selector_type == "business_day" and status.is_workday:
            matched.append(cursor)
        elif selector.selector_type == "trading_day":
            raise ValueError("trading_day selector is not implemented yet.")
        elif selector.selector_type == "custom":
            raise ValueError("custom calendar selectors are not implemented yet.")
        cursor += timedelta(days=1)
    return matched


def _compress_dates_to_intervals(dates: list[date]) -> list[Interval]:
    if not dates:
        return []

    intervals: list[Interval] = []
    current_start = dates[0]
    current_end = dates[0]

    for current in dates[1:]:
        if current == current_end + timedelta(days=1):
            current_end = current
            continue
        intervals.append(Interval(start_date=current_start, end_date=current_end))
        current_start = current
        current_end = current

    intervals.append(Interval(start_date=current_start, end_date=current_end))
    return intervals


def _shift_interval(interval: Interval, *, unit: str, value: int) -> Interval:
    if unit == "day":
        return Interval(
            start_date=interval.start_date + timedelta(days=value),
            end_date=interval.end_date + timedelta(days=value),
        )
    if unit == "week":
        return Interval(
            start_date=interval.start_date + timedelta(weeks=value),
            end_date=interval.end_date + timedelta(weeks=value),
        )
    if unit == "month":
        return Interval(
            start_date=_add_months(interval.start_date, value),
            end_date=_add_months(interval.end_date, value),
        )
    if unit == "quarter":
        return Interval(
            start_date=_add_months(interval.start_date, value * 3),
            end_date=_add_months(interval.end_date, value * 3),
        )
    if unit == "year":
        return Interval(
            start_date=_add_years(interval.start_date, value),
            end_date=_add_years(interval.end_date, value),
        )
    raise ValueError(f"Unsupported shift unit: {unit}")


def _add_months(value: date, months: int) -> date:
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _add_years(value: date, years: int) -> date:
    target_year = value.year + years
    if value.month == 2 and value.day == 29 and not calendar.isleap(target_year):
        return date(target_year, 2, 28)
    return date(target_year, value.month, value.day)


def _render_intervals(intervals: list[Interval]) -> str:
    return "、".join(_render_interval(interval) for interval in intervals)


def _render_interval(interval: Interval) -> str:
    if interval.start_date == interval.end_date:
        return _format_date(interval.start_date)
    return f"{_format_date(interval.start_date)}至{_format_date(interval.end_date)}"


def _format_date(value: date) -> str:
    return f"{value.year}年{value.month}月{value.day}日"
