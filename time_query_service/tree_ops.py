from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Callable

from time_query_service.derivation_registry import get_derivation_transform_spec
from time_query_service.resolved_plan import Interval, IntervalTree, TreeLabels


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
    return IntervalTree(role=tree.role, intervals=shifted_intervals, children=shifted_children, labels=labels)


def _shift_interval_months(interval: Interval, months: int) -> Interval:
    return Interval(
        start=_add_months_clipped(interval.start, months),
        end=_add_months_clipped(interval.end, months),
        end_inclusive=interval.end_inclusive,
    )


def _add_months_clipped(value: date, months: int) -> date:
    month_index = (value.month - 1) + months
    year = value.year + month_index // 12
    month = (month_index % 12) + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _bounding_interval(intervals: list[Interval]) -> Interval:
    if not intervals:
        raise ValueError("bounding interval requires at least one interval")
    return Interval(start=intervals[0].start, end=intervals[-1].end, end_inclusive=True)
