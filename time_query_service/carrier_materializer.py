from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Any

from pydantic import TypeAdapter

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.resolved_plan import Interval, IntervalTree, TreeLabels
from time_query_service.time_plan import (
    Anchor,
    CalendarEvent,
    CalendarFilter,
    Carrier,
    DateRange,
    DatetimeRange,
    EnumerationSet,
    GrainExpansion,
    GroupedTemporalValue,
    HolidayEventCollection,
    MappedRange,
    MemberSelection,
    Modifier,
    NamedPeriod,
    Offset,
    RelativeWindow,
    RollingByCalendarUnit,
    RollingWindow,
)
from time_query_service.tree_ops import day_end, day_start, display_precision, floor_to_hour, now_date, now_hour


def materialize_carrier(
    carrier: Carrier,
    *,
    system_datetime: datetime | date,
    business_calendar: BusinessCalendarPort,
    region: str = "CN",
) -> IntervalTree:
    system_datetime = _coerce_system_datetime(system_datetime)
    tree = materialize_anchor(
        carrier.anchor,
        system_datetime=system_datetime,
        business_calendar=business_calendar,
        region=region,
    )
    for modifier in carrier.modifiers:
        tree = apply_modifier(
            tree,
            modifier,
            system_datetime=system_datetime,
            business_calendar=business_calendar,
            region=region,
        )
    return tree


def materialize_anchor(
    anchor: object,
    *,
    system_datetime: datetime | date,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> IntervalTree:
    system_datetime = _coerce_system_datetime(system_datetime)
    if isinstance(anchor, NamedPeriod):
        return _atom_tree(_materialize_named_period(anchor), precision=display_precision(anchor))
    if isinstance(anchor, DateRange):
        return _atom_tree(
            Interval(start=day_start(anchor.start_date), end=day_end(anchor.end_date), end_inclusive=anchor.end_inclusive),
            precision=display_precision(anchor),
        )
    if isinstance(anchor, DatetimeRange):
        return _atom_tree(
            Interval(start=anchor.start_datetime, end=anchor.end_datetime, end_inclusive=anchor.end_inclusive),
            precision=display_precision(anchor),
        )
    if isinstance(anchor, RelativeWindow):
        return _atom_tree(_materialize_relative_window(anchor, system_datetime), precision=display_precision(anchor))
    if isinstance(anchor, RollingWindow):
        return _atom_tree(_materialize_rolling_window(anchor, system_datetime), precision=display_precision(anchor))
    if isinstance(anchor, RollingByCalendarUnit):
        return _materialize_rolling_by_calendar_unit(anchor, system_datetime, business_calendar, region)
    if isinstance(anchor, EnumerationSet):
        return _materialize_enumeration_set(anchor, system_datetime, business_calendar, region)
    if isinstance(anchor, CalendarEvent):
        if anchor.schedule_year_ref.year is None:
            raise NotImplementedError("calendar_event schedule_year_ref from source_unit_id is not implemented yet")
        span = business_calendar.get_event_span(
            region=anchor.region,
            event_key=anchor.event_key,
            schedule_year=anchor.schedule_year_ref.year,
            scope=anchor.scope,
        )
        if span is None:
            raise ValueError(
                f"Missing calendar event span for {anchor.region}/{anchor.event_key}/{anchor.schedule_year_ref.year}/{anchor.scope}"
            )
        return _atom_tree(
            Interval(start=day_start(span[0]), end=day_end(span[1]), end_inclusive=True),
            precision=display_precision(anchor),
        )
    if isinstance(anchor, HolidayEventCollection):
        return _materialize_holiday_event_collection(anchor, system_datetime, business_calendar, region)
    if isinstance(anchor, GroupedTemporalValue):
        return _materialize_grouped_temporal_value(anchor, system_datetime, business_calendar, region)
    if isinstance(anchor, MappedRange):
        return _materialize_mapped_range(anchor, system_datetime, business_calendar, region)
    raise NotImplementedError(f"Unsupported anchor type: {type(anchor)!r}")


def apply_modifier(
    tree: IntervalTree,
    modifier: Modifier,
    *,
    system_datetime: datetime | date,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> IntervalTree:
    system_datetime = _coerce_system_datetime(system_datetime)
    if isinstance(modifier, CalendarFilter):
        return _apply_calendar_filter(tree, modifier, business_calendar=business_calendar, region=region)
    if isinstance(modifier, GrainExpansion):
        return _apply_grain_expansion(tree, modifier)
    if isinstance(modifier, MemberSelection):
        return _apply_member_selection(tree, modifier)
    if isinstance(modifier, Offset):
        return _shift_tree(tree, modifier.value, modifier.unit)
    raise NotImplementedError(f"Unsupported modifier type: {type(modifier)!r}")


def _materialize_named_period(anchor: NamedPeriod) -> Interval:
    if anchor.period_type == "year":
        if anchor.year is None:
            raise ValueError("year named_period requires year")
        return Interval(start=day_start(date(anchor.year, 1, 1)), end=day_end(date(anchor.year, 12, 31)), end_inclusive=True)
    if anchor.year is None:
        raise ValueError("named_period requires year for non-year period types")
    if anchor.period_type == "quarter":
        if anchor.quarter is None:
            raise ValueError("quarter named_period requires quarter")
        month = 1 + (anchor.quarter - 1) * 3
        start = date(anchor.year, month, 1)
        end = _end_of_month(anchor.year, month + 2)
        return Interval(start=day_start(start), end=day_end(end), end_inclusive=True)
    if anchor.period_type == "half_year":
        if anchor.half is None:
            raise ValueError("half_year named_period requires half")
        start_month = 1 if anchor.half == 1 else 7
        end_month = 6 if anchor.half == 1 else 12
        return Interval(
            start=day_start(date(anchor.year, start_month, 1)),
            end=day_end(_end_of_month(anchor.year, end_month)),
            end_inclusive=True,
        )
    if anchor.period_type == "month":
        if anchor.month is None:
            raise ValueError("month named_period requires month")
        return Interval(
            start=day_start(date(anchor.year, anchor.month, 1)),
            end=day_end(_end_of_month(anchor.year, anchor.month)),
            end_inclusive=True,
        )
    if anchor.period_type == "week":
        if anchor.iso_week is None:
            raise ValueError("week named_period requires iso_week")
        start = date.fromisocalendar(anchor.year, anchor.iso_week, 1)
        end = date.fromisocalendar(anchor.year, anchor.iso_week, 7)
        return Interval(start=day_start(start), end=day_end(end), end_inclusive=True)
    if anchor.period_type == "day":
        if anchor.date is None:
            raise ValueError("day named_period requires date")
        return Interval(start=day_start(anchor.date), end=day_end(anchor.date), end_inclusive=True)
    raise NotImplementedError(f"Unsupported named_period type: {anchor.period_type}")


def _materialize_relative_window(anchor: RelativeWindow, system_datetime: datetime) -> Interval:
    base_date = now_date(system_datetime)
    if anchor.grain == "day":
        target = base_date + timedelta(days=anchor.offset_units)
        return Interval(start=day_start(target), end=day_end(target), end_inclusive=True)
    if anchor.grain == "hour":
        target = now_hour(system_datetime) + timedelta(hours=anchor.offset_units)
        return Interval(start=target, end=target, end_inclusive=True)
    if anchor.grain == "week":
        anchor_day = base_date + timedelta(days=anchor.offset_units * 7)
        start = _start_of_iso_week(anchor_day)
        end = start + timedelta(days=6)
        return Interval(start=day_start(start), end=day_end(end), end_inclusive=True)
    if anchor.grain == "month":
        anchor_day = _add_months_clipped(base_date, anchor.offset_units)
        return Interval(
            start=day_start(date(anchor_day.year, anchor_day.month, 1)),
            end=day_end(_end_of_month(anchor_day.year, anchor_day.month)),
            end_inclusive=True,
        )
    if anchor.grain == "quarter":
        anchor_day = _add_months_clipped(base_date, anchor.offset_units * 3)
        quarter = ((anchor_day.month - 1) // 3) + 1
        return _materialize_named_period(
            NamedPeriod(kind="named_period", period_type="quarter", year=anchor_day.year, quarter=quarter)
        )
    if anchor.grain == "half_year":
        anchor_day = _add_months_clipped(base_date, anchor.offset_units * 6)
        half = 1 if anchor_day.month <= 6 else 2
        return _materialize_named_period(
            NamedPeriod(kind="named_period", period_type="half_year", year=anchor_day.year, half=half)
        )
    if anchor.grain == "year":
        anchor_day = _add_months_clipped(base_date, anchor.offset_units * 12)
        return _materialize_named_period(NamedPeriod(kind="named_period", period_type="year", year=anchor_day.year))
    raise NotImplementedError(f"Unsupported relative grain: {anchor.grain}")


def _materialize_rolling_window(anchor: RollingWindow, system_datetime: datetime) -> Interval:
    if anchor.unit == "hour":
        reference_hour = _rolling_reference_hour(anchor.endpoint, system_datetime)
        if not anchor.include_endpoint:
            reference_hour = reference_hour - timedelta(hours=1)
        start = reference_hour - timedelta(hours=anchor.length - 1)
        return Interval(start=start, end=reference_hour, end_inclusive=True)

    reference_day = _rolling_reference_day(anchor.endpoint, anchor.unit, now_date(system_datetime))
    if anchor.endpoint == "previous_complete" and anchor.include_endpoint:
        return _materialize_previous_complete_window(length=anchor.length, unit=anchor.unit, reference_day=reference_day)
    return _materialize_rolling_window_from_reference(
        length=anchor.length,
        unit=anchor.unit,
        reference_day=reference_day,
        include_endpoint=anchor.include_endpoint,
    )


def _materialize_rolling_by_calendar_unit(
    anchor: RollingByCalendarUnit,
    system_datetime: datetime,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> IntervalTree:
    reference_day = _rolling_by_calendar_reference_day(anchor.endpoint, now_date(system_datetime))
    if not anchor.include_endpoint:
        reference_day = reference_day - timedelta(days=1)

    matched_days: list[date] = []
    cursor = reference_day
    while len(matched_days) < anchor.length:
        status = business_calendar.get_day_status(region=region, d=cursor)
        if _matches_day_class(status, anchor.day_class):
            matched_days.append(cursor)
        cursor = cursor - timedelta(days=1)
    matched_days.reverse()
    children = [_atom_tree(Interval(start=day_start(day), end=day_end(day), end_inclusive=True), precision="day") for day in matched_days]
    aggregate = Interval(start=day_start(matched_days[0]), end=day_end(matched_days[-1]), end_inclusive=True)
    return IntervalTree(
        role="filtered_collection",
        intervals=[aggregate],
        children=children,
        labels=TreeLabels(absolute_core_time=aggregate, display_precision="day"),
    )


def _materialize_rolling_window_from_reference(
    *,
    length: int,
    unit: str,
    reference_day: date,
    include_endpoint: bool,
) -> Interval:
    if not include_endpoint:
        reference_day = _shift_grain(reference_day, unit, 1)
    start = _shift_grain(reference_day, unit, length) + timedelta(days=1)
    return Interval(start=day_start(start), end=day_end(reference_day), end_inclusive=True)


def _materialize_enumeration_set(
    anchor: EnumerationSet,
    system_datetime: datetime,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> IntervalTree:
    children = [
        materialize_anchor(member, system_datetime=system_datetime, business_calendar=business_calendar, region=region)
        for member in anchor.members
    ]
    intervals = [child.labels.absolute_core_time for child in children if child.labels.absolute_core_time is not None]
    labels = TreeLabels(
        absolute_core_time=_bounding_interval(intervals) if intervals else None,
        display_precision=_shared_child_precision(children, context="enumeration_set"),
    )
    return IntervalTree(role="union", intervals=intervals, children=children, labels=labels)


def _resolve_expression_intervals(
    expr: Any,
    system_datetime: datetime,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> list[Interval]:
    if expr is None:
        raise ValueError("mapped_range expression is required")
    if expr == "system_datetime":
        current_day = now_date(system_datetime)
        return [Interval(start=day_start(current_day), end=day_end(current_day), end_inclusive=True)]
    anchor = _coerce_anchor(expr)
    tree = materialize_anchor(anchor, system_datetime=system_datetime, business_calendar=business_calendar, region=region)
    if tree.role in {"union", "grouped_member"}:
        return list(tree.intervals)
    if tree.labels.absolute_core_time is None:
        raise ValueError("mapped_range expression resolved without absolute_core_time")
    return [tree.labels.absolute_core_time]


def _validate_bounded_pair_expression(expr: Any) -> None:
    if expr is None:
        raise ValueError("mapped_range expression is required")
    if expr == "system_datetime":
        return

    anchor = _coerce_anchor(expr)
    if isinstance(anchor, (NamedPeriod, DateRange, DatetimeRange)):
        return
    if isinstance(anchor, RelativeWindow) and anchor.grain in {"day", "hour"} and anchor.offset_units == 0:
        return
    if isinstance(anchor, EnumerationSet):
        for member in anchor.members:
            _validate_bounded_pair_expression(member)
        return
    raise NotImplementedError(f"mapped_range bounded_pair does not support endpoint type {type(anchor).__name__}")


def _coerce_anchor(expr: Any) -> Anchor:
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
    return TypeAdapter(Anchor).validate_python(expr)


def _materialize_grouped_temporal_value(
    anchor: GroupedTemporalValue,
    system_datetime: datetime,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> IntervalTree:
    parent_tree = materialize_anchor(anchor.parent, system_datetime=system_datetime, business_calendar=business_calendar, region=region)
    base = parent_tree.labels.absolute_core_time
    if base is None:
        raise ValueError("grouped_temporal_value parent must resolve to a continuous interval")
    precision = "hour" if anchor.child_grain == "hour" else "day"
    children = [_atom_tree(bucket, precision=precision) for bucket in _partition_interval_by_grain(base, anchor.child_grain)]
    intervals = [child.labels.absolute_core_time for child in children if child.labels.absolute_core_time is not None]
    return IntervalTree(
        role="grouped_member",
        intervals=intervals,
        children=children,
        labels=TreeLabels(absolute_core_time=base, display_precision=precision),
    )


def _materialize_holiday_event_collection(
    anchor: HolidayEventCollection,
    system_datetime: datetime,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> IntervalTree:
    parent_tree = materialize_anchor(anchor.parent, system_datetime=system_datetime, business_calendar=business_calendar, region=region)
    base = parent_tree.labels.absolute_core_time
    if base is None:
        raise ValueError("holiday_event_collection parent must resolve to a continuous interval")

    blocks = business_calendar.list_holiday_vacation_blocks(region=anchor.region, schedule_year=base.start.year)
    children = [
        _atom_tree(Interval(start=day_start(block.start), end=day_end(block.end), end_inclusive=True), precision="day")
        for block in blocks
    ]
    intervals = [child.labels.absolute_core_time for child in children if child.labels.absolute_core_time is not None]
    return IntervalTree(
        role="grouped_member",
        intervals=intervals,
        children=children,
        labels=TreeLabels(absolute_core_time=base, display_precision="day"),
    )


def _materialize_mapped_range(
    anchor: MappedRange,
    system_datetime: datetime,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> IntervalTree:
    if anchor.mode == "bounded_pair":
        _validate_bounded_pair_expression(anchor.start)
        _validate_bounded_pair_expression(anchor.end)
        start_intervals = _resolve_expression_intervals(anchor.start, system_datetime, business_calendar, region)
        if _is_current_time_bounded_pair_endpoint(anchor.end):
            end_intervals = [
                _current_time_interval_for_bounded_pair(_bounded_pair_expr_precision(anchor.start), system_datetime)
                for start_interval in start_intervals
            ]
        else:
            end_intervals = _resolve_expression_intervals(anchor.end, system_datetime, business_calendar, region)
        if len(start_intervals) != len(end_intervals):
            raise ValueError("mapped_range bounded_pair requires equal start/end cardinality")
        ranges = [
            _bounded_pair_interval(start, end)
            for start, end in zip(start_intervals, end_intervals, strict=True)
        ]
        return _interval_list_tree(ranges, precision=_bounded_pair_expr_precision(anchor.start))
    if anchor.mode == "period_to_date":
        if anchor.period_grain is None:
            raise ValueError("mapped_range period_to_date requires period_grain")
        ranges = [
            _period_to_date_interval(anchor.period_grain, interval.end)
            for interval in _resolve_expression_intervals(anchor.anchor_ref, system_datetime, business_calendar, region)
        ]
        return _interval_list_tree(ranges, precision="day")
    if anchor.mode == "rolling_map":
        if anchor.length is None or anchor.unit is None:
            raise ValueError("mapped_range rolling_map requires length and unit")
        ranges = [
            _materialize_rolling_window_from_reference(
                length=anchor.length,
                unit=anchor.unit,
                reference_day=interval.end.date(),
                include_endpoint=True if anchor.include_endpoint is None else anchor.include_endpoint,
            )
            for interval in _resolve_expression_intervals(anchor.endpoint_set, system_datetime, business_calendar, region)
        ]
        return _interval_list_tree(ranges, precision="day")
    raise NotImplementedError(f"Unsupported mapped_range mode: {anchor.mode}")


def _apply_calendar_filter(
    tree: IntervalTree,
    modifier: CalendarFilter,
    *,
    business_calendar: BusinessCalendarPort,
    region: str,
) -> IntervalTree:
    base = tree.labels.absolute_core_time
    if base is None:
        raise ValueError("calendar_filter requires a continuous parent absolute_core_time")

    matched_days: list[date] = []
    if tree.children and all(_is_single_day_atom(child) for child in tree.children):
        for child in tree.children:
            day = child.labels.absolute_core_time.start.date()
            status = business_calendar.get_day_status(region=region, d=day)
            if _matches_day_class(status, modifier.day_class):
                matched_days.append(day)
    else:
        cursor = base.start.date()
        while cursor <= base.end.date():
            status = business_calendar.get_day_status(region=region, d=cursor)
            if _matches_day_class(status, modifier.day_class):
                matched_days.append(cursor)
            cursor = cursor + timedelta(days=1)

    children = [_atom_tree(Interval(start=day_start(day), end=day_end(day), end_inclusive=True), precision="day") for day in matched_days]
    aggregate = Interval(start=base.start, end=base.end, end_inclusive=True)
    return IntervalTree(
        role="filtered_collection",
        intervals=[aggregate],
        children=children,
        labels=TreeLabels(absolute_core_time=aggregate, display_precision="day"),
    )


def _apply_grain_expansion(tree: IntervalTree, modifier: GrainExpansion) -> IntervalTree:
    base = tree.labels.absolute_core_time
    if base is None:
        raise ValueError("grain_expansion requires a continuous parent absolute_core_time")
    precision = "hour" if modifier.target_grain == "hour" else "day"
    children = [_atom_tree(bucket, precision=precision) for bucket in _partition_interval_by_grain(base, modifier.target_grain)]
    intervals = [child.labels.absolute_core_time for child in children if child.labels.absolute_core_time is not None]
    return IntervalTree(
        role="grouped_member",
        intervals=intervals,
        children=children,
        labels=TreeLabels(absolute_core_time=base, display_precision=precision),
    )


def _apply_member_selection(tree: IntervalTree, modifier: MemberSelection) -> IntervalTree:
    if not tree.children:
        raise ValueError("member_selection requires a tree with children")
    children = list(tree.children)
    selector = modifier.selector
    if selector == "first":
        selected = children[:1]
    elif selector == "last":
        selected = children[-1:]
    elif selector == "nth":
        if modifier.n is None or modifier.n <= 0:
            raise ValueError("nth member_selection requires n > 0")
        selected = children[modifier.n - 1 : modifier.n]
    elif selector == "first_n":
        if modifier.n is None or modifier.n <= 0:
            raise ValueError("first_n member_selection requires n > 0")
        selected = children[: modifier.n]
    elif selector == "last_n":
        if modifier.n is None or modifier.n <= 0:
            raise ValueError("last_n member_selection requires n > 0")
        selected = children[-modifier.n :]
    else:
        raise NotImplementedError(f"Unsupported member_selection selector: {selector}")

    intervals = [child.labels.absolute_core_time for child in selected if child.labels.absolute_core_time is not None]
    absolute_core_time = None
    if selector in {"first", "last", "nth"} and len(intervals) == 1:
        absolute_core_time = intervals[0]
    elif intervals:
        absolute_core_time = _bounding_interval(intervals)

    return IntervalTree(
        role=tree.role,
        intervals=intervals,
        children=selected,
        labels=TreeLabels(absolute_core_time=absolute_core_time, display_precision=tree.labels.display_precision),
    )


def _shift_tree(tree: IntervalTree, value: int, unit: str) -> IntervalTree:
    shifted_intervals = [_shift_interval(interval, value, unit) for interval in tree.intervals]
    shifted_children = [_shift_tree(child, value, unit) for child in tree.children]
    labels = tree.labels.model_copy(deep=True)
    if labels.absolute_core_time is not None:
        labels.absolute_core_time = _shift_interval(labels.absolute_core_time, value, unit)
    return IntervalTree(role=tree.role, intervals=shifted_intervals, children=shifted_children, labels=labels)


def _shift_interval(interval: Interval, value: int, unit: str) -> Interval:
    start = _shift_datetime_forward(interval.start, unit, value)
    end = _shift_datetime_forward(interval.end, unit, value)
    return Interval(start=start, end=end, end_inclusive=interval.end_inclusive)


def _bounded_pair_interval(start: Interval, end: Interval) -> Interval:
    normalized_end = end
    while normalized_end.end < start.start:
        normalized_end = _shift_interval(normalized_end, 1, "year")
    return Interval(start=start.start, end=normalized_end.end, end_inclusive=True)


def _is_current_time_bounded_pair_endpoint(expr: Any) -> bool:
    if expr == "system_datetime":
        return True
    try:
        anchor = _coerce_anchor(expr)
    except Exception:
        return False
    return isinstance(anchor, RelativeWindow) and anchor.grain in {"day", "hour"} and anchor.offset_units == 0


def _current_time_interval_for_bounded_pair(start_precision: str, system_datetime: datetime) -> Interval:
    if start_precision == "hour":
        current_hour = now_hour(system_datetime)
        return Interval(start=current_hour, end=current_hour, end_inclusive=True)
    current_day = now_date(system_datetime)
    return Interval(start=day_start(current_day), end=day_end(current_day), end_inclusive=True)


def _bounded_pair_expr_precision(expr: Any) -> str:
    if expr == "system_datetime":
        return "day"
    anchor = _coerce_anchor(expr)
    if isinstance(anchor, EnumerationSet):
        return _shared_member_precision(
            [display_precision(member) for member in anchor.members],
            context="bounded_pair enumeration endpoint",
        )
    return display_precision(anchor)


def _matches_day_class(status: object, day_class: str) -> bool:
    if day_class == "workday":
        return bool(getattr(status, "is_workday"))
    if day_class == "holiday":
        return bool(getattr(status, "is_holiday"))
    if day_class == "statutory_holiday":
        return bool(getattr(status, "is_statutory_holiday"))
    if day_class == "makeup_workday":
        return bool(getattr(status, "is_makeup_workday"))
    if day_class == "weekend":
        return not bool(getattr(status, "is_workday")) and not bool(getattr(status, "is_holiday"))
    raise NotImplementedError(f"Unsupported day_class: {day_class}")


def _atom_tree(interval: Interval, *, precision: str) -> IntervalTree:
    return IntervalTree(
        role="atom",
        intervals=[interval],
        children=[],
        labels=TreeLabels(absolute_core_time=interval, display_precision=precision),
    )


def _is_single_day_atom(tree: IntervalTree) -> bool:
    interval = tree.labels.absolute_core_time
    return tree.role == "atom" and interval is not None and interval.start.date() == interval.end.date()


def _bounding_interval(intervals: list[Interval]) -> Interval:
    if not intervals:
        raise ValueError("bounding interval requires at least one interval")
    return Interval(start=intervals[0].start, end=intervals[-1].end, end_inclusive=True)


def _shared_child_precision(children: list[IntervalTree], *, context: str) -> str | None:
    return _shared_member_precision(
        [child.labels.display_precision for child in children],
        context=context,
        allow_empty=True,
    )


def _shared_member_precision(
    precisions: list[str | None],
    *,
    context: str,
    allow_empty: bool = False,
) -> str | None:
    concrete = {precision for precision in precisions if precision is not None}
    if not concrete:
        if allow_empty:
            return None
        raise ValueError(f"{context} requires at least one precision value")
    if len(concrete) != 1:
        raise ValueError(f"{context} requires a single display_precision, got {sorted(concrete)}")
    return concrete.pop()


def _period_to_date_interval(period_grain: str, anchor_day: date) -> Interval:
    anchor_date = anchor_day.date() if isinstance(anchor_day, datetime) else anchor_day
    if period_grain == "day":
        start = anchor_date
    elif period_grain == "week":
        start = _start_of_iso_week(anchor_date)
    elif period_grain == "month":
        start = date(anchor_date.year, anchor_date.month, 1)
    elif period_grain == "quarter":
        start = date(anchor_date.year, 1 + ((anchor_date.month - 1) // 3) * 3, 1)
    elif period_grain == "half_year":
        start = date(anchor_date.year, 1 if anchor_date.month <= 6 else 7, 1)
    elif period_grain == "year":
        start = date(anchor_date.year, 1, 1)
    else:
        raise NotImplementedError(f"Unsupported period_to_date grain: {period_grain}")
    return Interval(start=day_start(start), end=day_end(anchor_date), end_inclusive=True)


def _interval_list_tree(intervals: list[Interval], *, precision: str) -> IntervalTree:
    if not intervals:
        raise ValueError("mapped_range requires at least one interval")
    if len(intervals) == 1:
        return _atom_tree(intervals[0], precision=precision)
    children = [_atom_tree(interval, precision=precision) for interval in intervals]
    return IntervalTree(
        role="grouped_member",
        intervals=list(intervals),
        children=children,
        labels=TreeLabels(absolute_core_time=_bounding_interval(intervals), display_precision=precision),
    )


def _partition_interval_by_grain(interval: Interval, grain: str) -> list[Interval]:
    if grain == "hour":
        return _partition_by_hour(interval)
    if grain == "day":
        return _partition_by_day(interval)
    if grain == "week":
        return _partition_by_week(interval)
    if grain == "month":
        return _partition_by_month(interval)
    if grain == "quarter":
        return _partition_by_quarter(interval)
    if grain == "half_year":
        return _partition_by_half_year(interval)
    if grain == "year":
        return _partition_by_year(interval)
    raise NotImplementedError(f"Unsupported partition grain: {grain}")


def _partition_by_day(interval: Interval) -> list[Interval]:
    buckets: list[Interval] = []
    cursor = interval.start.date()
    while cursor <= interval.end.date():
        buckets.append(Interval(start=day_start(cursor), end=day_end(cursor), end_inclusive=True))
        cursor += timedelta(days=1)
    return buckets


def _partition_by_hour(interval: Interval) -> list[Interval]:
    buckets: list[Interval] = []
    cursor = floor_to_hour(interval.start)
    end = floor_to_hour(interval.end)
    while cursor <= end:
        buckets.append(Interval(start=cursor, end=cursor, end_inclusive=True))
        cursor += timedelta(hours=1)
    return buckets


def _partition_by_week(interval: Interval) -> list[Interval]:
    buckets: list[Interval] = []
    interval_start = interval.start.date()
    interval_end = interval.end.date()
    cursor = _start_of_iso_week(interval_start)
    while cursor <= interval_end:
        bucket_start = max(cursor, interval_start)
        bucket_end = min(cursor + timedelta(days=6), interval_end)
        if bucket_start <= bucket_end:
            buckets.append(Interval(start=day_start(bucket_start), end=day_end(bucket_end), end_inclusive=True))
        cursor += timedelta(days=7)
    return buckets


def _partition_by_month(interval: Interval) -> list[Interval]:
    buckets: list[Interval] = []
    interval_start = interval.start.date()
    interval_end = interval.end.date()
    cursor = date(interval_start.year, interval_start.month, 1)
    while cursor <= interval_end:
        natural_end = _end_of_month(cursor.year, cursor.month)
        bucket_start = max(cursor, interval_start)
        bucket_end = min(natural_end, interval_end)
        if bucket_start <= bucket_end:
            buckets.append(Interval(start=day_start(bucket_start), end=day_end(bucket_end), end_inclusive=True))
        cursor = _add_months_clipped(cursor, 1).replace(day=1)
    return buckets


def _partition_by_quarter(interval: Interval) -> list[Interval]:
    buckets: list[Interval] = []
    interval_start = interval.start.date()
    interval_end = interval.end.date()
    start_month = 1 + ((interval_start.month - 1) // 3) * 3
    cursor = date(interval_start.year, start_month, 1)
    while cursor <= interval_end:
        natural_end = _end_of_month(cursor.year, cursor.month + 2)
        bucket_start = max(cursor, interval_start)
        bucket_end = min(natural_end, interval_end)
        if bucket_start <= bucket_end:
            buckets.append(Interval(start=day_start(bucket_start), end=day_end(bucket_end), end_inclusive=True))
        cursor = _add_months_clipped(cursor, 3).replace(day=1)
    return buckets


def _partition_by_half_year(interval: Interval) -> list[Interval]:
    buckets: list[Interval] = []
    interval_start = interval.start.date()
    interval_end = interval.end.date()
    start_month = 1 if interval_start.month <= 6 else 7
    cursor = date(interval_start.year, start_month, 1)
    while cursor <= interval_end:
        natural_end = date(cursor.year, 6, 30) if cursor.month == 1 else date(cursor.year, 12, 31)
        bucket_start = max(cursor, interval_start)
        bucket_end = min(natural_end, interval_end)
        if bucket_start <= bucket_end:
            buckets.append(Interval(start=day_start(bucket_start), end=day_end(bucket_end), end_inclusive=True))
        cursor = _add_months_clipped(cursor, 6).replace(day=1)
    return buckets


def _partition_by_year(interval: Interval) -> list[Interval]:
    buckets: list[Interval] = []
    interval_start = interval.start.date()
    interval_end = interval.end.date()
    cursor = date(interval_start.year, 1, 1)
    while cursor <= interval_end:
        natural_end = date(cursor.year, 12, 31)
        bucket_start = max(cursor, interval_start)
        bucket_end = min(natural_end, interval_end)
        if bucket_start <= bucket_end:
            buckets.append(Interval(start=day_start(bucket_start), end=day_end(bucket_end), end_inclusive=True))
        cursor = date(cursor.year + 1, 1, 1)
    return buckets


def _rolling_reference_day(endpoint: str, unit: str, anchor_date: date) -> date:
    if endpoint == "today":
        return anchor_date
    if endpoint == "yesterday":
        return anchor_date - timedelta(days=1)
    if endpoint == "this_month_end":
        return _end_of_month(anchor_date.year, anchor_date.month)
    if endpoint == "previous_complete":
        if unit == "day":
            return anchor_date - timedelta(days=1)
        if unit == "week":
            return _start_of_iso_week(anchor_date) - timedelta(days=1)
        if unit == "month":
            return date(anchor_date.year, anchor_date.month, 1) - timedelta(days=1)
        if unit == "quarter":
            quarter_start_month = 1 + ((anchor_date.month - 1) // 3) * 3
            return date(anchor_date.year, quarter_start_month, 1) - timedelta(days=1)
        if unit == "half_year":
            half_start_month = 1 if anchor_date.month <= 6 else 7
            return date(anchor_date.year, half_start_month, 1) - timedelta(days=1)
        if unit == "year":
            return date(anchor_date.year, 1, 1) - timedelta(days=1)
    raise NotImplementedError(f"Unsupported rolling endpoint/unit combination: {endpoint}/{unit}")


def _rolling_reference_hour(endpoint: str, system_datetime: datetime) -> datetime:
    if endpoint == "today":
        return now_hour(system_datetime)
    if endpoint == "yesterday":
        return now_hour(system_datetime - timedelta(days=1))
    raise NotImplementedError(f"Unsupported rolling hour endpoint: {endpoint}")


def _rolling_by_calendar_reference_day(endpoint: str, anchor_date: date) -> date:
    if endpoint == "today":
        return anchor_date
    if endpoint == "yesterday":
        return anchor_date - timedelta(days=1)
    if endpoint == "this_month_end":
        return _end_of_month(anchor_date.year, anchor_date.month)
    if endpoint == "previous_complete":
        return anchor_date - timedelta(days=1)
    raise NotImplementedError(f"Unsupported rolling_by_calendar endpoint: {endpoint}")


def _materialize_previous_complete_window(*, length: int, unit: str, reference_day: date) -> Interval:
    if unit == "day":
        return Interval(start=day_start(reference_day), end=day_end(reference_day), end_inclusive=True)
    if unit == "week":
        start = _start_of_iso_week(reference_day) - timedelta(days=7 * (length - 1))
        return Interval(start=day_start(start), end=day_end(reference_day), end_inclusive=True)
    if unit == "month":
        period_start = date(reference_day.year, reference_day.month, 1)
        start = _add_months_clipped(period_start, -(length - 1))
        return Interval(start=day_start(start), end=day_end(reference_day), end_inclusive=True)
    if unit == "quarter":
        quarter_start_month = 1 + ((reference_day.month - 1) // 3) * 3
        period_start = date(reference_day.year, quarter_start_month, 1)
        start = _add_months_clipped(period_start, -(3 * (length - 1)))
        return Interval(start=day_start(start), end=day_end(reference_day), end_inclusive=True)
    if unit == "half_year":
        half_start_month = 1 if reference_day.month <= 6 else 7
        period_start = date(reference_day.year, half_start_month, 1)
        start = _add_months_clipped(period_start, -(6 * (length - 1)))
        return Interval(start=day_start(start), end=day_end(reference_day), end_inclusive=True)
    if unit == "year":
        start = date(reference_day.year - (length - 1), 1, 1)
        return Interval(start=day_start(start), end=day_end(reference_day), end_inclusive=True)
    raise NotImplementedError(f"Unsupported previous_complete rolling unit: {unit}")


def _shift_grain(reference_day: date, unit: str, count: int) -> date:
    if unit == "day":
        return reference_day - timedelta(days=count)
    if unit == "week":
        return reference_day - timedelta(days=7 * count)
    if unit == "month":
        return _add_months_clipped(reference_day, -count)
    if unit == "quarter":
        return _add_months_clipped(reference_day, -(3 * count))
    if unit == "half_year":
        return _add_months_clipped(reference_day, -(6 * count))
    if unit == "year":
        return _add_months_clipped(reference_day, -(12 * count))
    raise NotImplementedError(f"Unsupported shift grain: {unit}")


def _shift_grain_forward(reference_day: date, unit: str, count: int) -> date:
    if unit == "day":
        return reference_day + timedelta(days=count)
    if unit == "week":
        return reference_day + timedelta(days=7 * count)
    if unit == "month":
        return _add_months_clipped(reference_day, count)
    if unit == "quarter":
        return _add_months_clipped(reference_day, 3 * count)
    if unit == "half_year":
        return _add_months_clipped(reference_day, 6 * count)
    if unit == "year":
        return _add_months_clipped(reference_day, 12 * count)
    raise NotImplementedError(f"Unsupported forward shift grain: {unit}")


def _shift_datetime_forward(reference: datetime, unit: str, count: int) -> datetime:
    if unit == "hour":
        return reference + timedelta(hours=count)
    if unit == "day":
        return reference + timedelta(days=count)
    if unit == "week":
        return reference + timedelta(days=7 * count)
    if unit == "month":
        return _add_months_clipped(reference, count)
    if unit == "quarter":
        return _add_months_clipped(reference, 3 * count)
    if unit == "half_year":
        return _add_months_clipped(reference, 6 * count)
    if unit == "year":
        return _add_months_clipped(reference, 12 * count)
    raise NotImplementedError(f"Unsupported forward shift grain: {unit}")


def _add_months_clipped(value: date | datetime, months: int) -> date | datetime:
    month_index = (value.month - 1) + months
    year = value.year + month_index // 12
    month = (month_index % 12) + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    if isinstance(value, datetime):
        return value.replace(year=year, month=month, day=day)
    return date(year, month, day)


def _start_of_iso_week(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _end_of_month(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _coerce_system_datetime(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        return value
    return day_start(value)
