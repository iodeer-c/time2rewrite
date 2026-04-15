from __future__ import annotations

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
    Interval,
    NestedWindowSpec,
    RelativeWindowResolutionSpec,
    StrictModel,
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

    items: list[ClarificationItem] = []
    for node in normalized_plan.nodes:
        if not node.needs_clarification:
            continue
        items.append(
            _resolve_node(
                node=node,
                anchor_date=anchor_date,
                timezone=timezone,
                business_calendar=business_calendar,
            )
        )
    return ResolutionResult(items=items)


def _resolve_node(
    *,
    node: ClarificationNode,
    anchor_date: date,
    timezone: str,
    business_calendar: BusinessCalendarPort | None,
) -> ClarificationItem:
    if node.node_kind != "window_with_calendar_selector":
        raise ValueError(f"Unsupported node_kind for current resolver slice: {node.node_kind}")

    spec = WindowWithCalendarSelectorResolutionSpec.model_validate(node.resolution_spec)
    start_date, end_date = _resolve_nested_window(spec.window, anchor_date=anchor_date)
    matched_dates = _filter_calendar_dates(
        start_date=start_date,
        end_date=end_date,
        selector=spec.selector,
        business_calendar=business_calendar,
    )
    intervals = _compress_dates_to_intervals(matched_dates)

    return ClarificationItem(
        node_id=node.node_id,
        render_text=node.render_text,
        ordinal=node.ordinal,
        display_exact_time=_render_intervals(intervals),
        surface_fragments=node.surface_fragments,
        intervals=intervals,
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


def _resolve_nested_window(window: NestedWindowSpec, *, anchor_date: date) -> tuple[date, date]:
    if window.kind != "relative_window":
        raise ValueError(f"Unsupported nested window kind for current resolver slice: {window.kind}")

    spec = RelativeWindowResolutionSpec.model_validate(window.value)
    if spec.relative_type != "to_date" or spec.unit != "month" or spec.direction != "current":
        raise ValueError("Current resolver slice only supports current month-to-date windows.")

    start_date = anchor_date.replace(day=1)
    end_date = anchor_date if spec.include_today else anchor_date - timedelta(days=1)
    return start_date, end_date


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


def _render_intervals(intervals: list[Interval]) -> str:
    return "、".join(_render_interval(interval) for interval in intervals)


def _render_interval(interval: Interval) -> str:
    if interval.start_date == interval.end_date:
        return _format_date(interval.start_date)
    return f"{_format_date(interval.start_date)}至{_format_date(interval.end_date)}"


def _format_date(value: date) -> str:
    return f"{value.year}年{value.month}月{value.day}日"
