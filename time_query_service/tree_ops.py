from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Callable, Literal

from pydantic import TypeAdapter

from time_query_service.derivation_registry import get_derivation_transform_spec
from time_query_service.resolved_plan import Interval, IntervalTree, TreeLabels
from time_query_service.time_plan import (
    Anchor,
    CalendarEvent,
    Carrier,
    DateRange,
    DatetimeRange,
    EnumerationSet,
    GrainExpansion,
    GroupedTemporalValue,
    HolidayEventCollection,
    MappedRange,
    NamedPeriod,
    Offset,
    RelativeWindow,
    RollingByCalendarUnit,
    RollingWindow,
)


def floor_to_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def day_start(value: date | datetime) -> datetime:
    base = value.date() if isinstance(value, datetime) else value
    return datetime(base.year, base.month, base.day, 0, 0, 0)


def day_end(value: date | datetime) -> datetime:
    base = value.date() if isinstance(value, datetime) else value
    return datetime(base.year, base.month, base.day, 23, 0, 0)


def now_date(system_datetime: date | datetime) -> date:
    if isinstance(system_datetime, datetime):
        return system_datetime.date()
    return system_datetime


def now_hour(system_datetime: date | datetime) -> datetime:
    if isinstance(system_datetime, datetime):
        return floor_to_hour(system_datetime)
    return day_start(system_datetime)


def structural_grain(anchor_or_carrier: object) -> str:
    if isinstance(anchor_or_carrier, Carrier):
        grain = structural_grain(anchor_or_carrier.anchor)
        for modifier in anchor_or_carrier.modifiers:
            if isinstance(modifier, GrainExpansion):
                grain = modifier.target_grain
        return grain
    if isinstance(anchor_or_carrier, NamedPeriod):
        return anchor_or_carrier.period_type
    if isinstance(anchor_or_carrier, DateRange):
        return "day"
    if isinstance(anchor_or_carrier, DatetimeRange):
        return "hour"
    if isinstance(anchor_or_carrier, RelativeWindow):
        return anchor_or_carrier.grain
    if isinstance(anchor_or_carrier, RollingWindow):
        return anchor_or_carrier.unit
    if isinstance(anchor_or_carrier, RollingByCalendarUnit):
        return "day"
    if isinstance(anchor_or_carrier, EnumerationSet):
        return "day" if anchor_or_carrier.grain == "calendar_event" else anchor_or_carrier.grain
    if isinstance(anchor_or_carrier, GroupedTemporalValue):
        return anchor_or_carrier.child_grain
    if isinstance(anchor_or_carrier, CalendarEvent):
        return "day"
    if isinstance(anchor_or_carrier, HolidayEventCollection):
        return "day"
    if isinstance(anchor_or_carrier, MappedRange):
        if anchor_or_carrier.mode == "bounded_pair":
            if _is_current_time_bounded_pair_expr(anchor_or_carrier.end):
                return _mapped_range_expr_precision(anchor_or_carrier.start)
            if _mapped_range_expr_precision(anchor_or_carrier.start) == "hour":
                return "hour"
            if _mapped_range_expr_precision(anchor_or_carrier.end) == "hour":
                return "hour"
            return "day"
        if anchor_or_carrier.mode == "period_to_date":
            return anchor_or_carrier.period_grain or "day"
        return "day"
    raise TypeError(f"Unsupported structural_grain target: {type(anchor_or_carrier)!r}")


def display_precision(anchor_or_carrier: object) -> Literal["day", "hour"]:
    return "hour" if structural_grain(anchor_or_carrier) == "hour" else "day"


def _is_current_time_bounded_pair_expr(expr: object) -> bool:
    if expr == "system_datetime":
        return True
    coerced = _coerce_mapped_range_expr(expr)
    return isinstance(coerced, RelativeWindow) and coerced.grain in {"day", "hour"} and coerced.offset_units == 0


def _mapped_range_expr_precision(expr: object) -> str:
    if expr == "system_datetime":
        return "day"
    coerced = _coerce_mapped_range_expr(expr)
    if coerced is None:
        return "day"
    if isinstance(coerced, DatetimeRange):
        return "hour"
    if isinstance(coerced, RelativeWindow):
        return "hour" if coerced.grain == "hour" else "day"
    if isinstance(coerced, RollingWindow):
        return "hour" if coerced.unit == "hour" else "day"
    if isinstance(coerced, MappedRange):
        return display_precision(coerced)
    return "hour" if structural_grain(coerced) == "hour" else "day"


def _coerce_mapped_range_expr(expr: object) -> object | None:
    if isinstance(
        expr,
        (
            NamedPeriod,
            DateRange,
            DatetimeRange,
            RelativeWindow,
            RollingWindow,
            RollingByCalendarUnit,
            EnumerationSet,
            GroupedTemporalValue,
            CalendarEvent,
            HolidayEventCollection,
            MappedRange,
        ),
    ):
        return expr
    if isinstance(expr, dict):
        try:
            return TypeAdapter(Anchor).validate_python(expr)
        except Exception:
            return None
    return None


def shift_tree(tree: IntervalTree, transform: dict[str, object]) -> IntervalTree:
    kind = transform.get("kind")
    offset = int(transform.get("offset", 0))
    spec = get_derivation_transform_spec(str(kind))
    if spec is None:
        raise ValueError(f"Unsupported derivation transform: {kind}")
    months = spec["month_stride"] * offset
    return _shift_tree_months(tree, months)


def filter_tree(tree: IntervalTree, predicate: Callable[[IntervalTree], bool]) -> IntervalTree:
    kept_children = [child for child in tree.children if predicate(child)]
    intervals = [child.labels.absolute_core_time for child in kept_children if child.labels.absolute_core_time is not None]
    labels = tree.labels.model_copy(deep=True)
    _assert_consistent_child_precision(kept_children, expected=labels.display_precision, context="filter_tree")
    if kept_children and tree.role != "filtered_collection" and tree.role != "atom":
        if len(kept_children) == 1:
            labels.absolute_core_time = kept_children[0].labels.absolute_core_time
        elif intervals:
            labels.absolute_core_time = _bounding_interval(intervals)
    return IntervalTree(role=tree.role, intervals=intervals or tree.intervals, children=kept_children, labels=labels)


def select_tree(tree: IntervalTree, selector: str, *, n: int | None = None) -> IntervalTree:
    children = list(tree.children)
    if not children:
        raise ValueError("select_tree requires children")
    if selector == "first":
        selected = children[:1]
    elif selector == "last":
        selected = children[-1:]
    elif selector == "nth":
        if n is None or n <= 0:
            raise ValueError("nth selection requires n > 0")
        selected = children[n - 1 : n]
    elif selector == "first_n":
        if n is None or n <= 0:
            raise ValueError("first_n selection requires n > 0")
        selected = children[:n]
    elif selector == "last_n":
        if n is None or n <= 0:
            raise ValueError("last_n selection requires n > 0")
        selected = children[-n:]
    else:
        raise ValueError(f"Unsupported selector: {selector}")

    intervals = [child.labels.absolute_core_time for child in selected if child.labels.absolute_core_time is not None]
    labels = tree.labels.model_copy(deep=True)
    _assert_consistent_child_precision(selected, expected=labels.display_precision, context="select_tree")
    if selector in {"first", "last", "nth"}:
        labels.absolute_core_time = intervals[0]
    else:
        labels.absolute_core_time = _bounding_interval(intervals)
    return IntervalTree(role=tree.role, intervals=intervals, children=selected, labels=labels)


def _shift_tree_months(tree: IntervalTree, months: int) -> IntervalTree:
    shifted_intervals = [_shift_interval_months(interval, months) for interval in tree.intervals]
    shifted_children = [_shift_tree_months(child, months) for child in tree.children]
    labels = tree.labels.model_copy(deep=True)
    if labels.absolute_core_time is not None:
        labels.absolute_core_time = _shift_interval_months(labels.absolute_core_time, months)
    _assert_consistent_child_precision(shifted_children, expected=labels.display_precision, context="_shift_tree_months")
    return IntervalTree(role=tree.role, intervals=shifted_intervals, children=shifted_children, labels=labels)


def _shift_interval_months(interval: Interval, months: int) -> Interval:
    return Interval(
        start=_add_months_clipped(interval.start, months),
        end=_add_months_clipped(interval.end, months),
        end_inclusive=interval.end_inclusive,
    )

def _add_months_clipped(value: date | datetime, months: int) -> date | datetime:
    month_index = (value.month - 1) + months
    year = value.year + month_index // 12
    month = (month_index % 12) + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    if isinstance(value, datetime):
        return value.replace(year=year, month=month, day=day)
    return date(year, month, day)


def _bounding_interval(intervals: list[Interval]) -> Interval:
    if not intervals:
        raise ValueError("bounding interval requires at least one interval")
    return Interval(start=intervals[0].start, end=intervals[-1].end, end_inclusive=True)


def require_tree_display_precision(tree: IntervalTree, *, context: str) -> Literal["day", "hour"]:
    precision = tree.labels.display_precision
    if precision is None:
        raise ValueError(f"{context} requires tree.labels.display_precision")
    return precision


def _assert_consistent_child_precision(
    children: list[IntervalTree],
    *,
    expected: Literal["day", "hour"] | None,
    context: str,
) -> None:
    if expected is None:
        if children:
            raise ValueError(f"{context} requires parent display_precision when children are present")
        return
    for child in children:
        child_precision = child.labels.display_precision
        if child_precision is None:
            raise ValueError(f"{context} requires child display_precision")
        if child_precision != expected:
            raise ValueError(
                f"{context} requires consistent display_precision: parent={expected}, child={child_precision}"
            )
