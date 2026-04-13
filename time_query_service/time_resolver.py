from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from time_query_service.business_calendar import BusinessCalendarPort, JsonBusinessCalendar
from time_query_service.config import get_business_calendar_root, get_slice_subperiod_max_counts
from time_query_service.schemas import ParsedTimeExpressions, ResolvedTimeExpressions


@dataclass(frozen=True)
class TimeRange:
    start: datetime
    end: datetime
    grain: str | None = None
    slicing_grain: str | None = None
    is_partial: bool | None = None
    natural_grain: str | None = None

    @property
    def subperiod_parent_grain(self) -> str | None:
        return self.natural_grain or self.slicing_grain or self.grain


@dataclass(frozen=True)
class TimeSegmentSet:
    members: tuple[TimeRange, ...]

    def __post_init__(self) -> None:
        ordered = tuple(sorted(self.members, key=lambda item: (item.start, item.end)))
        deduped: list[TimeRange] = []
        for member in ordered:
            if deduped:
                previous = deduped[-1]
                if member.start == previous.start and member.end == previous.end:
                    continue
                if member.start <= previous.end:
                    raise ValueError("TimeSegmentSet members must not overlap.")
            deduped.append(member)
        object.__setattr__(self, "members", tuple(deduped))


@dataclass(frozen=True)
class TimeGroup:
    parent: TimeRange
    value: "TemporalValue"


@dataclass(frozen=True)
class TimeGroupedSet:
    groups: tuple[TimeGroup, ...]


TemporalValue = TimeRange | TimeSegmentSet | TimeGroupedSet


def start_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=23, minute=59, second=59, microsecond=0)


def start_of_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def end_of_hour(dt: datetime) -> datetime:
    return dt.replace(minute=59, second=59, microsecond=0)


def point_in_time(dt: datetime) -> TimeRange:
    return TimeRange(dt, dt, grain="datetime")


def current_period(system_dt: datetime, unit: str) -> TimeRange:
    if unit == "day":
        return TimeRange(start_of_day(system_dt), end_of_day(system_dt), grain="day")

    if unit == "week":
        monday = start_of_day(system_dt - timedelta(days=system_dt.weekday()))
        sunday = end_of_day(monday + timedelta(days=6))
        return TimeRange(monday, sunday, grain="week")

    if unit == "month":
        first = system_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day = calendar.monthrange(system_dt.year, system_dt.month)[1]
        last = system_dt.replace(day=last_day, hour=23, minute=59, second=59, microsecond=0)
        return TimeRange(first, last, grain="month")

    if unit == "quarter":
        q_start_month = ((system_dt.month - 1) // 3) * 3 + 1
        first = system_dt.replace(month=q_start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        end_month = q_start_month + 2
        last_day = calendar.monthrange(system_dt.year, end_month)[1]
        last = system_dt.replace(month=end_month, day=last_day, hour=23, minute=59, second=59, microsecond=0)
        return TimeRange(first, last, grain="quarter")

    if unit == "half_year":
        start_month = 1 if system_dt.month <= 6 else 7
        end_month = 6 if start_month == 1 else 12
        first = system_dt.replace(month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day = calendar.monthrange(system_dt.year, end_month)[1]
        last = system_dt.replace(month=end_month, day=last_day, hour=23, minute=59, second=59, microsecond=0)
        return TimeRange(first, last, grain="half_year")

    if unit == "year":
        first = system_dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        last = system_dt.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=0)
        return TimeRange(first, last, grain="year")

    raise ValueError(f"Unsupported unit: {unit}")


def current_hour(system_dt: datetime) -> TimeRange:
    return TimeRange(start_of_hour(system_dt), end_of_hour(system_dt), grain="hour")


def rolling_hours_range(system_dt: datetime, value: int) -> TimeRange:
    return TimeRange(system_dt - timedelta(hours=value), system_dt, grain=None, slicing_grain="rolling_hours")


def rolling_minutes_range(anchor_dt: datetime, value: int) -> TimeRange:
    return TimeRange(anchor_dt - timedelta(minutes=value), anchor_dt, grain=None, slicing_grain="rolling_minutes")


def add_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def add_years(dt: datetime, years: int) -> datetime:
    target_year = dt.year + years
    if dt.month == 2 and dt.day == 29 and not calendar.isleap(target_year):
        return dt.replace(year=target_year, day=28)
    return dt.replace(year=target_year)


def shift_month_like(base: TimeRange, months: int) -> TimeRange:
    start = add_months(base.start, months).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_candidate = add_months(base.end, months)
    end_day = calendar.monthrange(end_candidate.year, end_candidate.month)[1]
    end = end_candidate.replace(day=end_day, hour=23, minute=59, second=59, microsecond=0)
    return TimeRange(
        start,
        end,
        grain=base.grain,
        slicing_grain=base.slicing_grain,
        is_partial=base.is_partial,
    )


def shift_year_like(base: TimeRange, years: int) -> TimeRange:
    start = add_years(base.start, years)
    end = add_years(base.end, years)
    return TimeRange(
        start,
        end,
        grain=base.grain,
        slicing_grain=base.slicing_grain,
        is_partial=base.is_partial,
    )


def shift_range(base: TimeRange, unit: str, value: int) -> TimeRange:
    if unit == "day":
        delta = timedelta(days=value)
        return TimeRange(
            base.start + delta,
            base.end + delta,
            grain=base.grain,
            slicing_grain=base.slicing_grain,
            is_partial=base.is_partial,
        )

    if unit == "week":
        delta = timedelta(weeks=value)
        return TimeRange(
            base.start + delta,
            base.end + delta,
            grain=base.grain,
            slicing_grain=base.slicing_grain,
            is_partial=base.is_partial,
        )

    if unit == "month":
        return shift_month_like(base, value)

    if unit == "quarter":
        return shift_month_like(base, value * 3)

    if unit == "half_year":
        return shift_month_like(base, value * 6)

    if unit == "year":
        return shift_year_like(base, value)

    raise ValueError(f"Unsupported unit: {unit}")


def add_days(dt: datetime, days: int) -> datetime:
    return dt + timedelta(days=days)


def rolling_range(system_dt: datetime, unit: str, value: int) -> TimeRange:
    end = end_of_day(system_dt)

    if unit == "day":
        start_date = add_days(system_dt, -(value - 1))
        return TimeRange(start_of_day(start_date), end, grain=None, slicing_grain=unit)

    if unit == "week":
        start_date = add_days(system_dt, -(value * 7) + 1)
        return TimeRange(start_of_day(start_date), end, grain=None, slicing_grain=unit)

    if unit == "month":
        start_date = add_days(add_months(system_dt, -value), 1)
        return TimeRange(start_of_day(start_date), end, grain=None, slicing_grain=unit)

    if unit == "quarter":
        start_date = add_days(add_months(system_dt, -(value * 3)), 1)
        return TimeRange(start_of_day(start_date), end, grain=None, slicing_grain=unit)

    if unit == "half_year":
        start_date = add_days(add_months(system_dt, -(value * 6)), 1)
        return TimeRange(start_of_day(start_date), end, grain=None, slicing_grain=unit)

    if unit == "year":
        start_date = add_days(add_years(system_dt, -value), 1)
        return TimeRange(start_of_day(start_date), end, grain=None, slicing_grain=unit)

    raise ValueError(f"Unsupported rolling unit: {unit}")


def bounded_range(start: TimeRange, end: TimeRange) -> TimeRange:
    if start.start > end.end:
        raise ValueError("bounded_range start must be on or before end.")
    return TimeRange(start.start, end.end, grain=None, slicing_grain="bounded_range")


def period_to_date_range(unit: str, anchor: TimeRange) -> TimeRange:
    anchor_end = _require_day_or_point_anchor(anchor, "period_to_date")
    period = _natural_period_containing(anchor.start, unit)
    is_partial = anchor_end != period.end
    if not is_partial:
        return period
    return TimeRange(
        period.start,
        anchor_end,
        grain=None,
        slicing_grain="bounded_range",
        is_partial=True,
        natural_grain=unit,
    )


def _segment_set(*members: TimeRange) -> TimeSegmentSet:
    return TimeSegmentSet(tuple(members))


def _flatten_temporal_value(value: TemporalValue) -> list[TimeRange]:
    if isinstance(value, TimeRange):
        return [value]
    if isinstance(value, TimeSegmentSet):
        return list(value.members)
    flattened: list[TimeRange] = []
    for group in value.groups:
        flattened.extend(_flatten_temporal_value(group.value))
    return flattened


def _ordered_members(value: TemporalValue) -> list[TimeRange]:
    return _flatten_temporal_value(value)


def _member_grain(member: TimeRange) -> str:
    return member.subperiod_parent_grain or member.grain or "range"


def _members_are_contiguous(members: list[TimeRange]) -> bool:
    if len(members) <= 1:
        return True
    for previous, current in zip(members, members[1:]):
        if current.start > previous.end + timedelta(seconds=1):
            return False
    return True


def _build_rewrite_hint(value: TemporalValue) -> dict[str, Any] | None:
    members = _flatten_temporal_value(value)
    if len(members) <= 1:
        return None

    member_grains = {_member_grain(member) for member in members}
    member_grain = next(iter(member_grains)) if len(member_grains) == 1 else "range"
    is_contiguous = _members_are_contiguous(members)
    preferred_rendering = "member_list" if member_grain == "day" and not is_contiguous else "default"
    return {
        "topology": "discrete_set",
        "member_grain": member_grain,
        "is_contiguous": is_contiguous,
        "preferred_rendering": preferred_rendering,
    }


def _has_temporal_members(value: TemporalValue) -> bool:
    if isinstance(value, TimeRange):
        return True
    if isinstance(value, TimeSegmentSet):
        return bool(value.members)
    return bool(value.groups)


def _require_time_range(value: TemporalValue, op_name: str) -> TimeRange:
    if not isinstance(value, TimeRange):
        raise ValueError(f"{op_name} requires a single continuous range.")
    return value


def _map_over_members(value: TemporalValue, fn: Callable[[TimeRange], TemporalValue]) -> TemporalValue:
    if isinstance(value, TimeRange):
        return fn(value)
    if isinstance(value, TimeSegmentSet):
        return TimeGroupedSet(tuple(TimeGroup(parent=member, value=fn(member)) for member in value.members))
    return TimeGroupedSet(
        tuple(
            TimeGroup(
                parent=group.parent,
                value=_map_over_members(group.value, fn),
            )
            for group in value.groups
        )
    )


def _map_leaves(value: TemporalValue, fn: Callable[[TimeRange], TemporalValue]) -> TemporalValue:
    if isinstance(value, TimeRange):
        return fn(value)
    if isinstance(value, TimeSegmentSet):
        parts: list[TimeRange] = []
        for member in value.members:
            parts.extend(_flatten_temporal_value(fn(member)))
        return TimeSegmentSet(tuple(parts))
    return TimeGroupedSet(
        tuple(
            TimeGroup(
                parent=group.parent,
                value=_map_leaves(group.value, fn),
            )
            for group in value.groups
        )
    )


def _apply_within_groups(
    value: TemporalValue,
    fn: Callable[[TimeRange | TimeSegmentSet], TemporalValue],
) -> TemporalValue:
    if isinstance(value, (TimeRange, TimeSegmentSet)):
        return fn(value)
    return TimeGroupedSet(
        tuple(
            TimeGroup(
                parent=group.parent,
                value=_apply_within_groups(group.value, fn),
            )
            for group in value.groups
        )
    )


def _normalize_expr(expr: Any) -> dict[str, Any]:
    if hasattr(expr, "model_dump"):
        return expr.model_dump(mode="python")
    return dict(expr)


def _extract_calendar_filter_context(expr_payload: Mapping[str, Any]) -> dict[str, Any] | None:
    op = expr_payload.get("op")
    if op == "enumerate_calendar_days":
        context: dict[str, Any] = {
            "day_kind": expr_payload.get("day_kind"),
            "event_key": None,
            "schedule_year": None,
        }
        base = expr_payload.get("base")
        if isinstance(base, Mapping) and base.get("op") == "calendar_event_range":
            context["event_key"] = base.get("event_key")
            schedule_year = base.get("schedule_year")
            if isinstance(schedule_year, int):
                context["schedule_year"] = schedule_year
        return context
    if op == "enumerate_makeup_workdays":
        context = {
            "day_kind": "makeup_workday",
            "event_key": expr_payload.get("event_key"),
            "schedule_year": None,
        }
        schedule_year = expr_payload.get("schedule_year")
        if isinstance(schedule_year, int):
            context["schedule_year"] = schedule_year
        return context
    for value in expr_payload.values():
        if isinstance(value, Mapping) and "op" in value:
            context = _extract_calendar_filter_context(value)
            if context is not None:
                return context
        if isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping) and "op" in item:
                    context = _extract_calendar_filter_context(item)
                    if context is not None:
                        return context
    return None


def _build_no_match_result(item_id: str, text: str, expr_payload: Mapping[str, Any]) -> dict[str, Any] | None:
    context = _extract_calendar_filter_context(expr_payload)
    if context is None:
        return None
    return {
        "source_id": item_id,
        "source_text": text,
        "reason": "calendar_filter_empty",
        "expr_op": expr_payload["op"],
        "day_kind": context["day_kind"],
        "event_key": context["event_key"],
        "schedule_year": context["schedule_year"],
    }


def _should_convert_empty_selection_error_to_no_match(expr_payload: Mapping[str, Any], exc: ValueError) -> bool:
    if _build_no_match_result("t", "x", expr_payload) is None:
        return False
    message = str(exc)
    return "at least one segment" in message or "only 0 are available" in message


def _natural_month_range(year: int, month: int, tzinfo) -> TimeRange:
    first = datetime(year, month, 1, 0, 0, 0, tzinfo=tzinfo)
    last_day = calendar.monthrange(year, month)[1]
    last = datetime(year, month, last_day, 23, 59, 59, tzinfo=tzinfo)
    return TimeRange(first, last, grain="month")


def _natural_quarter_range(year: int, quarter: int, tzinfo) -> TimeRange:
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    first = datetime(year, start_month, 1, 0, 0, 0, tzinfo=tzinfo)
    last_day = calendar.monthrange(year, end_month)[1]
    last = datetime(year, end_month, last_day, 23, 59, 59, tzinfo=tzinfo)
    return TimeRange(first, last, grain="quarter")


def _natural_half_year_range(year: int, half: int, tzinfo) -> TimeRange:
    start_month = 1 if half == 1 else 7
    end_month = 6 if half == 1 else 12
    first = datetime(year, start_month, 1, 0, 0, 0, tzinfo=tzinfo)
    last_day = calendar.monthrange(year, end_month)[1]
    last = datetime(year, end_month, last_day, 23, 59, 59, tzinfo=tzinfo)
    return TimeRange(first, last, grain="half_year")


def _is_natural_year_range(base: TimeRange) -> bool:
    return (
        base.start.year == base.end.year
        and base.start.month == 1
        and base.start.day == 1
        and base.start == datetime(base.start.year, 1, 1, 0, 0, 0, tzinfo=base.start.tzinfo)
        and base.end.month == 12
        and base.end.day == 31
        and base.end == datetime(base.end.year, 12, 31, 23, 59, 59, tzinfo=base.end.tzinfo)
    )


def _is_year_valued(base: TimeRange) -> bool:
    return base.start.year == base.end.year and (
        base.grain == "year"
        or base.natural_grain == "year"
        or base.slicing_grain == "year"
        or _is_natural_year_range(base)
    )


def _require_single_year_base(base: TemporalValue, op_name: str) -> TimeRange:
    # Absolute week expressions such as `2026年第3周` are modeled as
    # `select_subperiod(unit="week", index=3, base=literal_period(unit="year", year=2026))`,
    # so any single natural year range must be accepted as a valid week parent.
    if not isinstance(base, TimeRange) or not _is_year_valued(base):
        raise ValueError(f"{op_name} requires a year-valued base.")
    return base


def _extract_year_members(base: TemporalValue, op_name: str) -> list[TimeRange]:
    if isinstance(base, TimeRange):
        return [_require_single_year_base(base, op_name)]
    if isinstance(base, TimeSegmentSet):
        members = list(base.members)
        if not members:
            raise ValueError(f"{op_name} requires at least one year-valued base.")
        for member in members:
            if not _is_year_valued(member):
                raise ValueError(f"{op_name} requires a year-valued base.")
        return members
    raise ValueError(f"{op_name} requires a year-valued base.")


def _clip_range_to_window(mapped: TimeRange, window: TimeRange) -> TimeRange | None:
    overlap_start = max(mapped.start, window.start)
    overlap_end = min(mapped.end, window.end)
    if overlap_start > overlap_end:
        return None
    if overlap_start == mapped.start and overlap_end == mapped.end:
        return mapped
    natural_grain = mapped.natural_grain or mapped.grain
    return TimeRange(
        overlap_start,
        overlap_end,
        grain=None,
        slicing_grain=mapped.slicing_grain,
        is_partial=True,
        natural_grain=natural_grain,
    )


def _bound_year_mapped_value(source_member: TimeRange, value: TemporalValue) -> TemporalValue:
    if isinstance(value, TimeRange):
        clipped = _clip_range_to_window(value, source_member)
        if clipped is None:
            return TimeSegmentSet(tuple())
        return clipped
    if isinstance(value, TimeSegmentSet):
        clipped_members = [
            clipped
            for member in value.members
            if (clipped := _clip_range_to_window(member, source_member)) is not None
        ]
        return TimeSegmentSet(tuple(clipped_members))

    bounded_groups: list[TimeGroup] = []
    for group in value.groups:
        clipped_parent = _clip_range_to_window(group.parent, source_member)
        if clipped_parent is None:
            continue
        clipped_value = _bound_year_mapped_value(clipped_parent, group.value)
        if _has_temporal_members(clipped_value):
            bounded_groups.append(TimeGroup(parent=clipped_parent, value=clipped_value))
    return TimeGroupedSet(tuple(bounded_groups))


def _map_year_valued_base(
    base: TemporalValue,
    op_name: str,
    fn: Callable[[TimeRange], TemporalValue],
) -> TemporalValue:
    if isinstance(base, TimeGroupedSet):
        bounded_groups: list[TimeGroup] = []
        for group in base.groups:
            mapped_value = _map_year_valued_base(group.value, op_name, fn)
            if _has_temporal_members(mapped_value):
                bounded_groups.append(TimeGroup(parent=group.parent, value=mapped_value))
        return TimeGroupedSet(tuple(bounded_groups))
    if isinstance(base, TimeRange):
        source_member = _require_single_year_base(base, op_name)
        return _bound_year_mapped_value(source_member, fn(source_member))
    members = _extract_year_members(base, op_name)
    bounded_groups: list[TimeGroup] = []
    for member in members:
        mapped_value = _bound_year_mapped_value(member, fn(member))
        if _has_temporal_members(mapped_value):
            bounded_groups.append(TimeGroup(parent=member, value=mapped_value))
    return TimeGroupedSet(tuple(bounded_groups))


def _validate_subperiod_request(base: TimeRange, unit: str, count: int) -> str:
    base_grain = base.subperiod_parent_grain
    if base_grain is None:
        raise ValueError("Unsupported subperiod slicing: base range does not have a natural grain.")

    if base_grain == unit:
        return base_grain

    if base.grain is None and base.slicing_grain is not None and base.slicing_grain != "bounded_range":
        if unit == base.slicing_grain:
            return base_grain

    if base_grain == "bounded_range":
        if unit not in {"day", "week", "month", "quarter", "half_year", "year"}:
            raise ValueError(f"Unsupported subperiod slicing: {base_grain} -> {unit}")
        return base_grain

    max_counts = get_slice_subperiod_max_counts()
    child_limits = max_counts.get(base_grain)
    if child_limits is None or unit not in child_limits:
        raise ValueError(f"Unsupported subperiod slicing: {base_grain} -> {unit}")

    max_allowed = child_limits[unit]
    if base.grain == base_grain and count > max_allowed:
        raise ValueError(
            f"Requested {count} {unit} subperiods from {base_grain}, "
            f"which exceeds configured maximum {max_allowed}."
        )
    return base_grain


def _split_by_days(base: TimeRange) -> list[TimeRange]:
    parts = []
    cursor = base.start
    while cursor <= base.end:
        parts.append(TimeRange(start_of_day(cursor), end_of_day(cursor), grain="day"))
        cursor = cursor + timedelta(days=1)
    return parts


def _split_by_weeks(base: TimeRange) -> list[TimeRange]:
    first_monday_offset = (7 - base.start.weekday()) % 7
    first_monday = start_of_day(base.start + timedelta(days=first_monday_offset))
    if first_monday > base.end:
        return []

    parts = []
    cursor = first_monday
    while cursor <= base.end:
        part_start = start_of_day(cursor)
        part_end = end_of_day(min(cursor + timedelta(days=6), base.end))
        parts.append(TimeRange(part_start, part_end, grain="week"))
        cursor = cursor + timedelta(days=7)
    return parts


def _natural_period_containing(dt: datetime, unit: str) -> TimeRange:
    if unit == "day":
        return TimeRange(start_of_day(dt), end_of_day(dt), grain="day")
    if unit == "week":
        return current_period(dt, "week")
    if unit == "month":
        return _natural_month_range(dt.year, dt.month, dt.tzinfo)
    if unit == "quarter":
        return _natural_quarter_range(dt.year, ((dt.month - 1) // 3) + 1, dt.tzinfo)
    if unit == "half_year":
        return _natural_half_year_range(dt.year, 1 if dt.month <= 6 else 2, dt.tzinfo)
    if unit == "year":
        return current_period(dt, "year")
    raise ValueError(f"Unsupported unit: {unit}")


def _next_natural_period_start(period_start: datetime, unit: str) -> datetime:
    if unit == "day":
        return period_start + timedelta(days=1)
    if unit == "week":
        return period_start + timedelta(days=7)
    if unit == "month":
        return add_months(period_start, 1).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == "quarter":
        return add_months(period_start, 3).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == "half_year":
        return add_months(period_start, 6).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == "year":
        return add_years(period_start, 1).replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unsupported unit: {unit}")


def _split_clipped_subperiods(base: TimeRange, unit: str) -> list[TimeRange]:
    parts: list[TimeRange] = []
    current = _natural_period_containing(base.start, unit)
    while current.start <= base.end:
        overlap_start = max(current.start, base.start)
        overlap_end = min(current.end, base.end)
        if overlap_start <= overlap_end:
            is_partial = overlap_start != current.start or overlap_end != current.end
            parts.append(
                TimeRange(
                    overlap_start,
                    overlap_end,
                    grain=None if is_partial else unit,
                    is_partial=is_partial,
                    natural_grain=unit if is_partial else None,
                )
            )
        current = _natural_period_containing(_next_natural_period_start(current.start, unit), unit)
    return parts


def split_into_subperiods(base: TimeRange, unit: str) -> list[TimeRange]:
    _validate_subperiod_request(base, unit, 1)

    if base.grain == unit or base.natural_grain == unit:
        return [base]

    if base.grain is None and base.slicing_grain is not None:
        return _split_clipped_subperiods(base, unit)

    if base.grain == "week" and unit == "day":
        return _split_by_days(base)

    if base.grain in {"month", "quarter", "half_year", "year"} and unit == "day":
        return _split_by_days(base)

    if base.grain in {"month", "quarter", "half_year", "year"} and unit == "week":
        return _split_by_weeks(base)

    if base.grain == "quarter" and unit == "month":
        return [
            _natural_month_range(base.start.year, base.start.month + offset, base.start.tzinfo)
            for offset in range(3)
        ]

    if base.grain == "half_year" and unit == "month":
        return [
            _natural_month_range(add_months(base.start, offset).year, add_months(base.start, offset).month, base.start.tzinfo)
            for offset in range(6)
        ]

    if base.grain == "half_year" and unit == "quarter":
        start_quarter = ((base.start.month - 1) // 3) + 1
        return [
            _natural_quarter_range(base.start.year, quarter, base.start.tzinfo)
            for quarter in range(start_quarter, start_quarter + 2)
        ]

    if base.grain == "year" and unit == "month":
        return [
            _natural_month_range(base.start.year, month, base.start.tzinfo)
            for month in range(1, 13)
        ]

    if base.grain == "year" and unit == "half_year":
        return [
            _natural_half_year_range(base.start.year, half, base.start.tzinfo)
            for half in (1, 2)
        ]

    if base.grain == "year" and unit == "quarter":
        return [
            _natural_quarter_range(base.start.year, quarter, base.start.tzinfo)
            for quarter in range(1, 5)
        ]

    raise ValueError(f"Unsupported subperiod slicing: {base.grain} -> {unit}")


def _filter_complete_subperiods(parts: list[TimeRange], complete_only: bool) -> list[TimeRange]:
    if not complete_only:
        return parts
    return [part for part in parts if not part.is_partial]


def _aggregate_partial_flag(parts: list[TimeRange]) -> bool | None:
    partial_flags = [part.is_partial for part in parts if part.is_partial is not None]
    if not partial_flags:
        return None
    return any(partial_flags)


def _continuous_parent_range(
    start: datetime,
    end: datetime,
    *,
    slicing_grain: str = "bounded_range",
    is_partial: bool | None = None,
) -> TimeRange:
    return TimeRange(
        start,
        end,
        grain=None,
        slicing_grain=slicing_grain,
        is_partial=is_partial,
    )


def _is_point_range(base: TimeRange) -> bool:
    return base.start == base.end


def _is_full_day_range(base: TimeRange) -> bool:
    return (
        base.start.date() == base.end.date()
        and base.start == start_of_day(base.start)
        and base.end == end_of_day(base.start)
    )


def _require_natural_day_base(base: TimeRange, op_name: str) -> None:
    if base.grain != "day":
        raise ValueError(f"{op_name} requires a natural day base, got {base.grain!r}.")
    if base.start.date() != base.end.date():
        raise ValueError(f"{op_name} requires a natural day base.")
    if base.start != start_of_day(base.start) or base.end != end_of_day(base.start):
        raise ValueError(f"{op_name} requires a natural day base.")


def _require_single_day_base(base: TimeRange, op_name: str) -> None:
    if base.start.date() != base.end.date():
        raise ValueError(f"{op_name} requires a single-day base.")


def _split_into_hours(base: TimeRange) -> list[TimeRange]:
    _require_natural_day_base(base, "hour operation")
    return [
        TimeRange(
            base.start + timedelta(hours=hour),
            base.start + timedelta(hours=hour, minutes=59, seconds=59),
            grain="hour",
        )
        for hour in range(24)
    ]


def _split_clipped_hours(base: TimeRange) -> list[TimeRange]:
    parts: list[TimeRange] = []
    cursor = start_of_hour(base.start)
    while cursor <= base.end:
        natural_start = start_of_hour(cursor)
        natural_end = end_of_hour(cursor)
        overlap_start = max(natural_start, base.start)
        overlap_end = min(natural_end, base.end)
        if overlap_start <= overlap_end:
            is_partial = overlap_start != natural_start or overlap_end != natural_end
            parts.append(
                TimeRange(
                    overlap_start,
                    overlap_end,
                    grain="hour" if not is_partial else None,
                    is_partial=is_partial,
                    natural_grain="hour" if is_partial else None,
                )
            )
        cursor = natural_start + timedelta(hours=1)
    return parts


def enumerate_hours_segments(base: TimeRange) -> list[TimeRange]:
    if base.grain == "day":
        return _split_into_hours(base)
    return _split_clipped_hours(base)


def select_hour_range(base: TimeRange, hour: int) -> TimeRange:
    if base.grain == "day":
        return _split_into_hours(base)[hour]
    if base.grain is not None:
        raise ValueError("select_hour requires a natural day base or a single-day bounded range.")

    _require_single_day_base(base, "select_hour")
    for part in _split_clipped_hours(base):
        if start_of_hour(part.start).hour == hour:
            return part
    raise ValueError(f"select_hour could not find hour {hour} in base range.")


def slice_hours_range(base: TimeRange, mode: str, count: int) -> TimeRange:
    parts = _split_into_hours(base) if base.grain == "day" else _split_clipped_hours(base)
    if count > len(parts):
        raise ValueError(f"Requested {count} hours, but only {len(parts)} are available.")
    selected = parts[:count] if mode == "first" else parts[-count:]
    if not selected:
        raise ValueError("No hours selected.")
    if len(selected) == 1:
        part = selected[0]
        return TimeRange(
            part.start,
            part.end,
            grain=part.grain,
            is_partial=part.is_partial,
        )
    return TimeRange(
        selected[0].start,
        selected[-1].end,
        grain=None,
        is_partial=_aggregate_partial_flag(selected),
    )


def slice_subperiods_range(base: TimeRange, mode: str, unit: str, count: int, complete_only: bool = False) -> TimeRange:
    base_grain = _validate_subperiod_request(base, unit, count)
    parts = _filter_complete_subperiods(split_into_subperiods(base, unit), complete_only)

    if count > len(parts):
        raise ValueError(
            f"Requested {count} {unit} subperiods from {base_grain}, but only {len(parts)} are available."
        )

    selected = parts[:count] if mode == "first" else parts[-count:]
    if not selected:
        raise ValueError("No subperiods selected.")
    if len(selected) == 1:
        return selected[0]

    return _continuous_parent_range(
        selected[0].start,
        selected[-1].end,
        is_partial=_aggregate_partial_flag(selected),
    )


def select_subperiod_range(base: TimeRange, unit: str, index: int, complete_only: bool = False) -> TimeRange:
    base_grain = _validate_subperiod_request(base, unit, index)
    parts = _filter_complete_subperiods(split_into_subperiods(base, unit), complete_only)

    if index > len(parts):
        raise ValueError(
            f"Requested subperiod index {index} for {base_grain} -> {unit}, "
            f"but only {len(parts)} subperiods are available."
        )

    return parts[index - 1]


def _select_segment_value(value: TimeRange | TimeSegmentSet, mode: str, index: int | None) -> TemporalValue:
    members = [value] if isinstance(value, TimeRange) else list(value.members)
    if not members:
        raise ValueError("select_segment requires at least one segment.")
    if mode == "first":
        return members[0]
    if mode == "last":
        return members[-1]
    assert index is not None
    if index > len(members):
        raise ValueError("select_segment requested ordinal is out of range.")
    if mode == "nth":
        return members[index - 1]
    return members[-index]


def _slice_segments_value(value: TimeRange | TimeSegmentSet, mode: str, count: int) -> TimeSegmentSet:
    members = [value] if isinstance(value, TimeRange) else list(value.members)
    if count > len(members):
        raise ValueError(f"Requested {count} segments, but only {len(members)} are available.")
    selected = members[:count] if mode == "first" else members[-count:]
    return TimeSegmentSet(tuple(selected))


def _segments_bounds_value(value: TimeRange | TimeSegmentSet) -> TimeRange:
    if isinstance(value, TimeRange):
        return value
    if not value.members:
        raise ValueError("segments_bounds requires at least one segment.")
    if len(value.members) == 1:
        return value.members[0]
    return _continuous_parent_range(value.members[0].start, value.members[-1].end)


def _is_day_addressable_continuous_parent(base: TimeRange) -> bool:
    if base.subperiod_parent_grain in {"month", "quarter", "half_year", "year", "bounded_range"}:
        return True
    return base.grain is None and base.slicing_grain == "bounded_range"


def _require_week_base(base: TimeRange, op_name: str) -> None:
    if base.subperiod_parent_grain != "week":
        raise ValueError(f"{op_name} requires a week base, got {base.subperiod_parent_grain!r}.")


def _weekday_occurrences_within_parent(base: TimeRange, weekday: int) -> list[TimeRange]:
    occurrences: list[TimeRange] = []
    cursor = start_of_day(base.start)
    while cursor <= base.end:
        if cursor.isoweekday() == weekday:
            occurrences.append(TimeRange(start_of_day(cursor), end_of_day(cursor), grain="day"))
        cursor = cursor + timedelta(days=1)
    return occurrences


def _weekend_occurrences_within_parent(base: TimeRange) -> list[TimeRange]:
    occurrences: list[TimeRange] = []
    cursor = start_of_day(base.start)
    current_start: datetime | None = None
    current_end: datetime | None = None

    while cursor <= base.end:
        if cursor.isoweekday() in {6, 7}:
            if current_start is None:
                current_start = start_of_day(cursor)
            current_end = end_of_day(cursor)
        elif current_start is not None and current_end is not None:
            occurrences.append(TimeRange(current_start, current_end, grain=None))
            current_start = None
            current_end = None
        cursor = cursor + timedelta(days=1)

    if current_start is not None and current_end is not None:
        occurrences.append(TimeRange(current_start, current_end, grain=None))

    return occurrences


def select_occurrence_range(
    base: TimeRange,
    kind: str,
    ordinal: int | str,
    weekday: int | None = None,
    *,
    from_end: bool = False,
) -> TimeRange:
    parent_grain = base.subperiod_parent_grain
    if not _is_day_addressable_continuous_parent(base):
        raise ValueError(
            "select_occurrence requires a day-addressable continuous parent with grain month, quarter, half_year, year, or bounded_range."
        )

    if kind == "weekday":
        if weekday is None:
            raise ValueError("select_occurrence(kind='weekday') requires weekday.")
        occurrences = _weekday_occurrences_within_parent(base, weekday)
    elif kind == "weekend":
        occurrences = _weekend_occurrences_within_parent(base)
    else:
        raise ValueError(f"Unsupported occurrence kind: {kind}")

    if not occurrences:
        raise ValueError(f"No {kind} occurrences found within {base.grain}.")

    if ordinal == "last":
        return occurrences[-1]

    if ordinal > len(occurrences):
        raise ValueError(
            f"Requested occurrence {ordinal} for {kind} within {parent_grain}, "
            f"but only {len(occurrences)} are available."
        )

    if from_end:
        return occurrences[-ordinal]
    return occurrences[ordinal - 1]


def _ensure_single_day(base: TimeRange, op_name: str) -> date:
    if base.start.date() != base.end.date():
        raise ValueError(f"{op_name} requires a single-day base.")
    return base.start.date()


def _require_exact_datetime_anchor(base: TimeRange, op_name: str) -> datetime:
    if not _is_point_range(base):
        raise ValueError(f"{op_name} requires an exact datetime anchor.")
    return base.start


def _require_single_day_anchor(base: TimeRange, op_name: str) -> date:
    if not _is_full_day_range(base):
        raise ValueError(f"{op_name} requires a single-day anchor.")
    return base.start.date()


def _require_day_or_point_anchor(base: TimeRange, op_name: str) -> datetime:
    if _is_point_range(base):
        return base.start
    if _is_full_day_range(base):
        return base.end
    raise ValueError(f"{op_name} requires a single day or exact datetime anchor.")


def _map_anchor_value(
    value: TemporalValue,
    op_name: str,
    builder: Callable[[TimeRange], TemporalValue],
) -> TemporalValue:
    if isinstance(value, TimeGroupedSet):
        return TimeGroupedSet(
            tuple(
                TimeGroup(parent=group.parent, value=_map_anchor_value(group.value, op_name, builder))
                for group in value.groups
            )
        )
    if isinstance(value, TimeRange):
        return builder(value)
    return TimeGroupedSet(tuple(TimeGroup(parent=member, value=builder(member)) for member in value.members))


def _zip_temporal_values(
    start_value: TemporalValue,
    end_value: TemporalValue,
    op_name: str,
    fn: Callable[[TimeRange, TimeRange], TemporalValue],
) -> TemporalValue:
    if isinstance(start_value, TimeGroupedSet) and isinstance(end_value, TimeGroupedSet):
        if len(start_value.groups) != len(end_value.groups):
            raise ValueError(f"{op_name} paired boundary sets require equal cardinality.")
        groups: list[TimeGroup] = []
        for start_group, end_group in zip(start_value.groups, end_value.groups, strict=True):
            groups.append(
                TimeGroup(
                    parent=start_group.parent,
                    value=_zip_temporal_values(start_group.value, end_group.value, op_name, fn),
                )
            )
        return TimeGroupedSet(tuple(groups))
    if isinstance(start_value, TimeGroupedSet) or isinstance(end_value, TimeGroupedSet):
        raise ValueError(f"{op_name} paired boundary sets require matching shapes.")
    if isinstance(start_value, TimeRange) and isinstance(end_value, TimeRange):
        return fn(start_value, end_value)
    if isinstance(start_value, TimeRange) or isinstance(end_value, TimeRange):
        raise ValueError(f"{op_name} paired boundary sets require equal cardinality.")

    start_members = list(start_value.members)
    end_members = list(end_value.members)
    if len(start_members) != len(end_members):
        raise ValueError(f"{op_name} paired boundary sets require equal cardinality.")
    return TimeGroupedSet(
        tuple(
            TimeGroup(parent=start_member, value=fn(start_member, end_member))
            for start_member, end_member in zip(start_members, end_members, strict=True)
        )
    )


def range_edge(base: TimeRange, edge: str) -> TimeRange:
    if edge == "start":
        target = base.start
    else:
        target = base.end
    return TimeRange(start_of_day(target), end_of_day(target), grain="day")


def business_day_offset_range(
    *,
    base: TimeRange,
    value: int,
    region: str,
    business_calendar: BusinessCalendarPort,
    calendar_versions: set[str] | None = None,
) -> TimeRange:
    anchor = _ensure_single_day(base, "business_day_offset")
    remaining = abs(value)
    step = 1 if value > 0 else -1
    cursor = anchor

    while True:
        status = business_calendar.get_day_status(region=region, d=cursor)
        if calendar_versions is not None:
            calendar_versions.update(status.calendar_versions)
        if status.is_workday:
            remaining -= 1
            if remaining == 0:
                target = datetime.combine(cursor, datetime.min.time(), tzinfo=base.start.tzinfo)
                return TimeRange(start_of_day(target), end_of_day(target), grain="day")
        cursor = cursor + timedelta(days=step)


def rolling_business_day_segments(
    *,
    anchor: TimeRange,
    region: str,
    value: int,
    include_anchor: bool,
    business_calendar: BusinessCalendarPort,
    calendar_versions: set[str] | None = None,
) -> TimeSegmentSet:
    cursor = _require_single_day_anchor(anchor, "rolling_business_days")
    if not include_anchor:
        cursor = cursor - timedelta(days=1)

    matched: list[date] = []
    while len(matched) < value:
        status = business_calendar.get_day_status(region=region, d=cursor)
        if calendar_versions is not None:
            calendar_versions.update(status.calendar_versions)
        if status.is_workday:
            matched.append(cursor)
        cursor = cursor - timedelta(days=1)

    matched.reverse()
    return _dates_to_atomic_ranges(matched, anchor.start.tzinfo)


def _expr_uses_business_calendar(expr: Mapping[str, Any] | Any) -> bool:
    payload = _normalize_expr(expr)
    if payload.get("op") in {
        "calendar_event_range",
        "business_day_offset",
        "enumerate_calendar_days",
        "enumerate_makeup_workdays",
        "rolling_business_days",
    }:
        return True
    for value in payload.values():
        if hasattr(value, "model_dump"):
            if _expr_uses_business_calendar(value):
                return True
        elif isinstance(value, Mapping):
            if _expr_uses_business_calendar(value):
                return True
    return False


def _normalize_nested_expr_value(value: Any, default_include_anchor: bool) -> Any:
    if hasattr(value, "model_dump") or isinstance(value, Mapping):
        payload = _normalize_expr(value)
        if "op" in payload:
            return _normalize_legacy_rolling_expr(payload, default_include_anchor)
        return {
            key: _normalize_nested_expr_value(nested_value, default_include_anchor)
            for key, nested_value in payload.items()
        }
    if isinstance(value, list):
        return [_normalize_nested_expr_value(item, default_include_anchor) for item in value]
    return value


def _normalize_legacy_rolling_expr(expr: Mapping[str, Any] | Any, default_include_anchor: bool) -> dict[str, Any]:
    payload = _normalize_expr(expr)
    if payload.get("op") == "rolling":
        if "anchor_expr" in payload:
            normalized = {
                key: _normalize_nested_expr_value(value, default_include_anchor)
                for key, value in payload.items()
            }
            if normalized.get("include_anchor") is None:
                normalized["include_anchor"] = False
            return normalized
        if payload.get("anchor") == "system_date":
            return {
                "op": "rolling",
                "unit": payload["unit"],
                "value": payload["value"],
                "anchor_expr": {"op": "anchor", "name": "system_date"},
                "include_anchor": default_include_anchor,
            }
    return {
        key: _normalize_nested_expr_value(value, default_include_anchor)
        for key, value in payload.items()
    }


def eval_expr(
    expr: Mapping[str, Any] | Any,
    system_dt: datetime,
    context: Mapping[str, TemporalValue] | None = None,
    business_calendar: BusinessCalendarPort | None = None,
    calendar_versions: set[str] | None = None,
    rolling_anchor_dt: datetime | None = None,
    system_has_time: bool = False,
    reference_resolver: Callable[[str], TemporalValue] | None = None,
) -> TemporalValue:
    payload = _normalize_expr(expr)
    op = payload["op"]

    if op == "anchor":
        if payload["name"] == "system_date":
            return TimeRange(start_of_day(system_dt), end_of_day(system_dt), grain="day")
        if not system_has_time:
            raise ValueError("system_datetime is required for anchor(system_datetime).")
        return point_in_time(system_dt)

    if op == "current_period":
        return current_period(system_dt, payload["unit"])

    if op == "literal_date":
        target = datetime.strptime(payload["date"], "%Y-%m-%d").replace(tzinfo=system_dt.tzinfo)
        return TimeRange(start_of_day(target), end_of_day(target), grain="day")

    if op == "literal_datetime":
        target = datetime.strptime(payload["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=system_dt.tzinfo)
        return point_in_time(target)

    if op == "literal_period":
        if payload["unit"] == "year":
            target = datetime(payload["year"], 1, 1, tzinfo=system_dt.tzinfo)
            return current_period(target, "year")
        if payload["unit"] == "month":
            target = datetime(payload["year"], payload["month"], 1, tzinfo=system_dt.tzinfo)
            return current_period(target, "month")
        if payload["unit"] == "quarter":
            return _natural_quarter_range(payload["year"], payload["quarter"], system_dt.tzinfo)
        return _natural_half_year_range(payload["year"], payload["half"], system_dt.tzinfo)

    if op == "current_hour":
        if not system_has_time:
            raise ValueError("current_hour requires system_datetime.")
        return current_hour(system_dt)

    if op == "rolling_hours":
        if not system_has_time:
            raise ValueError("rolling_hours requires system_datetime.")
        return rolling_hours_range(system_dt, payload["value"])

    if op == "rolling_minutes":
        anchor_value = eval_expr(
            payload["anchor_expr"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_anchor_value(
            anchor_value,
            "rolling_minutes",
            lambda anchor_range: rolling_minutes_range(
                _require_exact_datetime_anchor(anchor_range, "rolling_minutes"),
                payload["value"],
            ),
        )

    if op == "shift":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(base, lambda segment: shift_range(segment, payload["unit"], payload["value"]))

    if op == "rolling":
        if "anchor_expr" in payload:
            anchor_value = eval_expr(
                payload["anchor_expr"],
                system_dt,
                context,
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
                rolling_anchor_dt=rolling_anchor_dt,
                system_has_time=system_has_time,
                reference_resolver=reference_resolver,
            )
            return _map_anchor_value(
                anchor_value,
                "rolling",
                lambda anchor_range: rolling_range(
                    datetime.combine(
                        _ensure_single_day(anchor_range, "rolling anchor")
                        if payload.get("include_anchor", False)
                        else _ensure_single_day(anchor_range, "rolling anchor") - timedelta(days=1),
                        datetime.min.time(),
                        tzinfo=anchor_range.start.tzinfo,
                    ),
                    payload["unit"],
                    payload["value"],
                ),
            )
        return rolling_range(rolling_anchor_dt or system_dt, payload["unit"], payload["value"])

    if op == "rolling_business_days":
        if business_calendar is None:
            raise ValueError("Business calendar is required for rolling_business_days.")
        anchor_value = eval_expr(
            payload["anchor_expr"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_anchor_value(
            anchor_value,
            "rolling_business_days",
            lambda anchor_range: rolling_business_day_segments(
                anchor=anchor_range,
                region=payload["region"],
                value=payload["value"],
                include_anchor=payload["include_anchor"],
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
            ),
        )

    if op == "bounded_range":
        start_value = eval_expr(
            payload["start"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        end_value = eval_expr(
            payload["end"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _zip_temporal_values(
            start_value,
            end_value,
            "bounded_range",
            lambda start, end: bounded_range(
                _require_time_range(start, "bounded_range start"),
                _require_time_range(end, "bounded_range end"),
            ),
        )

    if op == "period_to_date":
        anchor_value = eval_expr(
            payload["anchor_expr"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_anchor_value(
            anchor_value,
            "period_to_date",
            lambda anchor_range: period_to_date_range(payload["unit"], anchor_range),
        )

    if op == "calendar_event_range":
        if business_calendar is None:
            raise ValueError("Business calendar is required for calendar_event_range.")
        def _calendar_event_for_year(schedule_year: int) -> TimeRange:
            version = business_calendar.calendar_version_for_schedule_year(
                region=payload["region"],
                schedule_year=schedule_year,
            )
            if version is None:
                raise ValueError(
                    "Missing business calendar data for event query: "
                    f"region={payload['region']}, schedule_year={schedule_year}, "
                    f"event_key={payload['event_key']}, scope={payload['scope']}"
                )
            span = business_calendar.get_event_span(
                region=payload["region"],
                event_key=payload["event_key"],
                schedule_year=schedule_year,
                scope=payload["scope"],
            )
            if span is None:
                if payload["scope"] == "statutory":
                    raise ValueError(
                        f"Missing statutory business calendar data for region={payload['region']}, "
                        f"event_key={payload['event_key']}, schedule_year={schedule_year}"
                    )
                raise ValueError(
                    f"Missing business calendar data for region={payload['region']}, "
                    f"event_key={payload['event_key']}, schedule_year={schedule_year}, scope={payload['scope']}"
                )
            if calendar_versions is not None:
                calendar_versions.add(version)
            start, end = span
            start_dt = datetime.combine(start, datetime.min.time(), tzinfo=system_dt.tzinfo)
            end_dt = datetime.combine(end, datetime.min.time(), tzinfo=system_dt.tzinfo)
            return _continuous_parent_range(start_of_day(start_dt), end_of_day(end_dt))

        if payload.get("schedule_year_expr") is not None:
            schedule_year_value = eval_expr(
                payload["schedule_year_expr"],
                system_dt,
                context,
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
                rolling_anchor_dt=rolling_anchor_dt,
                system_has_time=system_has_time,
                reference_resolver=reference_resolver,
            )
            return _map_year_valued_base(
                schedule_year_value,
                "calendar_event_range",
                lambda member: _calendar_event_for_year(member.start.year),
            )
        return _calendar_event_for_year(payload["schedule_year"])

    if op == "range_edge":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(base, lambda segment: range_edge(segment, payload["edge"]))

    if op == "business_day_offset":
        if business_calendar is None:
            raise ValueError("Business calendar is required for business_day_offset.")
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(
            base,
            lambda segment: business_day_offset_range(
                base=segment,
                value=payload["value"],
                region=payload["region"],
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
            ),
        )

    if op == "enumerate_calendar_days":
        if business_calendar is None:
            raise ValueError("Business calendar is required for enumerate_calendar_days.")
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(
            base,
            lambda segment: _dates_to_atomic_ranges(
                _filter_calendar_dates(
                    base=segment,
                    region=payload["region"],
                    day_kind=payload["day_kind"],
                    business_calendar=business_calendar,
                    calendar_versions=calendar_versions,
                ),
                segment.start.tzinfo,
            ),
        )

    if op == "enumerate_makeup_workdays":
        if business_calendar is None:
            raise ValueError("Business calendar is required for enumerate_makeup_workdays.")
        def _makeup_workdays_for_year(schedule_year: int) -> TimeSegmentSet:
            version = business_calendar.calendar_version_for_schedule_year(
                region=payload["region"],
                schedule_year=schedule_year,
            )
            if version is None:
                raise ValueError(
                    f"Missing business calendar data for region={payload['region']}, schedule_year={schedule_year}"
                )
            if calendar_versions is not None:
                calendar_versions.add(version)
            matched_dates = business_calendar.list_makeup_workdays(
                region=payload["region"],
                event_key=payload["event_key"],
                schedule_year=schedule_year,
            )
            return _dates_to_atomic_ranges(matched_dates, system_dt.tzinfo)

        if payload.get("schedule_year_expr") is not None:
            schedule_year_value = eval_expr(
                payload["schedule_year_expr"],
                system_dt,
                context,
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
                rolling_anchor_dt=rolling_anchor_dt,
                system_has_time=system_has_time,
                reference_resolver=reference_resolver,
            )
            return _map_year_valued_base(
                schedule_year_value,
                "enumerate_makeup_workdays",
                lambda member: _makeup_workdays_for_year(member.start.year),
            )
        return _makeup_workdays_for_year(payload["schedule_year"])

    if op == "enumerate_hours":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(base, lambda segment: TimeSegmentSet(tuple(enumerate_hours_segments(segment))))

    if op == "enumerate_subperiods":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(
            base,
            lambda segment: TimeSegmentSet(
                tuple(_filter_complete_subperiods(split_into_subperiods(segment, payload["unit"]), payload.get("complete_only", False)))
            ),
        )

    if op == "select_hour":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(base, lambda segment: select_hour_range(segment, payload["hour"]))

    if op == "slice_hours":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(base, lambda segment: slice_hours_range(segment, payload["mode"], payload["count"]))

    if op == "slice_subperiods":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(
            base,
            lambda segment: slice_subperiods_range(
                segment,
                payload["mode"],
                payload["unit"],
                payload["count"],
                payload.get("complete_only", False),
            ),
        )

    if op == "select_subperiod":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(
            base,
            lambda segment: select_subperiod_range(
                segment,
                payload["unit"],
                payload["index"],
                payload.get("complete_only", False),
            ),
        )

    if op == "select_weekday":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        def _select_weekday(segment: TimeRange) -> TimeRange:
            _require_week_base(segment, "select_weekday")
            natural_week = _natural_period_containing(segment.start, "week")
            target = natural_week.start + timedelta(days=payload["weekday"] - 1)
            if not (segment.start <= target <= segment.end):
                raise ValueError("select_weekday could not find the requested weekday within the clipped week.")
            return TimeRange(start_of_day(target), end_of_day(target), grain="day")

        return _map_over_members(base, _select_weekday)

    if op == "select_weekend":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        def _select_weekend(segment: TimeRange) -> TimeRange:
            _require_week_base(segment, "select_weekend")
            natural_week = _natural_period_containing(segment.start, "week")
            natural_start = start_of_day(natural_week.start + timedelta(days=5))
            natural_end = end_of_day(natural_week.start + timedelta(days=6))
            start = max(natural_start, segment.start)
            end = min(natural_end, segment.end)
            if start > end:
                raise ValueError("select_weekend could not find a weekend within the clipped week.")
            is_partial = start != natural_start or end != natural_end
            return TimeRange(
                start,
                end,
                grain=None if is_partial else "weekend",
                is_partial=is_partial if is_partial else None,
                natural_grain="weekend" if is_partial else None,
            )

        return _map_over_members(base, _select_weekend)

    if op == "select_occurrence":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_over_members(
            base,
            lambda segment: select_occurrence_range(
                segment,
                payload["kind"],
                payload["index"] if payload["ordinal"] == "nth_from_end" else payload["ordinal"],
                payload.get("weekday"),
                from_end=payload["ordinal"] == "nth_from_end",
            ),
        )

    if op == "select_month":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_year_valued_base(
            base,
            "select_month",
            lambda segment: _natural_month_range(segment.start.year, payload["month"], segment.start.tzinfo),
        )

    if op == "select_quarter":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_year_valued_base(
            base,
            "select_quarter",
            lambda segment: _natural_quarter_range(segment.start.year, payload["quarter"], segment.start.tzinfo),
        )

    if op == "select_half_year":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _map_year_valued_base(
            base,
            "select_half_year",
            lambda segment: _natural_half_year_range(segment.start.year, payload["half"], segment.start.tzinfo),
        )

    if op == "select_segment":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _apply_within_groups(base, lambda value: _select_segment_value(value, payload["mode"], payload.get("index")))

    if op == "segments_bounds":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _apply_within_groups(base, _segments_bounds_value)

    if op == "slice_segments":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
            reference_resolver=reference_resolver,
        )
        return _apply_within_groups(base, lambda value: _slice_segments_value(value, payload["mode"], payload["count"]))

    if op == "reference":
        if context is not None and payload["ref"] in context:
            return context[payload["ref"]]
        if reference_resolver is not None:
            return reference_resolver(payload["ref"])
        raise ValueError(f"Unknown reference: {payload['ref']}")

    raise ValueError(f"Unsupported op: {op}")


def _iter_dates(start: date, end: date) -> list[date]:
    dates: list[date] = []
    cursor = start
    while cursor <= end:
        dates.append(cursor)
        cursor = cursor + timedelta(days=1)
    return dates


def _filter_calendar_dates(
    *,
    base: TimeRange,
    region: str,
    day_kind: str,
    business_calendar: BusinessCalendarPort,
    calendar_versions: set[str] | None = None,
) -> list[date]:
    dates = _iter_dates(base.start.date(), base.end.date())
    matched: list[date] = []
    for d in dates:
        status = business_calendar.get_day_status(region=region, d=d)
        if calendar_versions is not None:
            calendar_versions.update(status.calendar_versions)
        if day_kind == "workday" and status.is_workday:
            matched.append(d)
        elif day_kind == "restday" and not status.is_workday:
            matched.append(d)
        elif day_kind == "holiday" and status.is_holiday:
            matched.append(d)
    if day_kind in {"workday", "restday", "holiday"}:
        return matched
    raise ValueError(f"Unsupported calendar day kind: {day_kind}")


def _date_to_day_range(d: date, tzinfo) -> TimeRange:
    target = datetime.combine(d, datetime.min.time(), tzinfo=tzinfo)
    return TimeRange(start_of_day(target), end_of_day(target), grain="day")


def _dates_to_atomic_ranges(dates: list[date], tzinfo) -> TimeSegmentSet:
    return TimeSegmentSet(tuple(_date_to_day_range(d, tzinfo) for d in dates))


def _merge_dates_to_ranges(dates: list[date], tzinfo) -> list[TimeRange]:
    if not dates:
        return []

    ranges: list[TimeRange] = []
    segment_start = dates[0]
    segment_end = dates[0]

    for current in dates[1:]:
        if current == segment_end + timedelta(days=1):
            segment_end = current
            continue

        start_dt = datetime.combine(segment_start, datetime.min.time(), tzinfo=tzinfo)
        end_dt = datetime.combine(segment_end, datetime.min.time(), tzinfo=tzinfo)
        ranges.append(TimeRange(start_of_day(start_dt), end_of_day(end_dt), grain=None))
        segment_start = current
        segment_end = current

    start_dt = datetime.combine(segment_start, datetime.min.time(), tzinfo=tzinfo)
    end_dt = datetime.combine(segment_end, datetime.min.time(), tzinfo=tzinfo)
    ranges.append(TimeRange(start_of_day(start_dt), end_of_day(end_dt), grain=None))
    return ranges


def _build_resolved_item_payload(
    *,
    item_id: str,
    text: str,
    timezone: str,
    resolved: TimeRange,
    source_id: str | None,
    source_text: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": item_id,
        "text": text,
        "source_id": source_id,
        "source_text": source_text,
        "start_time": resolved.start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": resolved.end.strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": timezone,
    }
    if resolved.is_partial is not None:
        payload["is_partial"] = resolved.is_partial
    return payload


def _build_group_payload(
    *,
    item_id: str,
    text: str,
    timezone: str,
    resolved: TimeRange,
    source_id: str | None,
    source_text: str | None,
    children: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = _build_resolved_item_payload(
        item_id=item_id,
        text=text,
        timezone=timezone,
        resolved=resolved,
        source_id=source_id,
        source_text=source_text,
    )
    payload["children"] = children
    return payload


def _temporal_value_bounds(value: TemporalValue) -> TimeRange:
    if isinstance(value, TimeGroupedSet):
        members = [group.parent for group in value.groups]
    else:
        members = _flatten_temporal_value(value)
    if not members:
        raise ValueError("Temporal value has no members.")
    if len(members) == 1:
        return members[0]
    return TimeRange(
        members[0].start,
        members[-1].end,
        grain=None,
        is_partial=_aggregate_partial_flag(members),
    )


def _serialize_group_children(
    *,
    item_id: str,
    text: str,
    timezone: str,
    value: TemporalValue,
    path: str = "",
) -> list[dict[str, Any]]:
    if isinstance(value, TimeRange):
        return [
            _build_group_payload(
                item_id=f"{item_id}{path}__seg_01",
                text=text,
                timezone=timezone,
                resolved=value,
                source_id=item_id,
                source_text=text,
                children=[],
            )
        ]
    if isinstance(value, TimeSegmentSet):
        return [
            _build_group_payload(
                item_id=f"{item_id}{path}__seg_{index:02d}",
                text=text,
                timezone=timezone,
                resolved=segment,
                source_id=item_id,
                source_text=text,
                children=[],
            )
            for index, segment in enumerate(value.members, start=1)
        ]
    children: list[dict[str, Any]] = []
    for index, group in enumerate(value.groups, start=1):
        child_path = f"{path}__grp_{index:02d}"
        children.append(
            _build_group_payload(
                item_id=f"{item_id}{child_path}",
                text=text,
                timezone=timezone,
                resolved=group.parent,
                source_id=item_id,
                source_text=text,
                children=_serialize_group_children(
                    item_id=item_id,
                    text=text,
                    timezone=timezone,
                    value=group.value,
                    path=child_path,
                ),
            )
        )
    return children


def _serialize_grouped_value(
    *,
    item_id: str,
    text: str,
    value: TemporalValue,
    timezone: str,
) -> dict[str, Any]:
    return _build_group_payload(
        item_id=item_id,
        text=text,
        timezone=timezone,
        resolved=_temporal_value_bounds(value),
        source_id=None,
        source_text=None,
        children=_serialize_group_children(
            item_id=item_id,
            text=text,
            timezone=timezone,
            value=value,
        )
        if not isinstance(value, TimeRange)
        else [],
    )


def _drop_none_partials_from_group(node: dict[str, Any]) -> None:
    if node.get("is_partial") is None:
        node.pop("is_partial", None)
    for child in node.get("children", []):
        _drop_none_partials_from_group(child)


def _resolve_system_context(
    *,
    system_date: str | None,
    system_datetime: str | None,
    timezone: str,
) -> tuple[datetime, bool]:
    if system_datetime is not None:
        parsed_datetime = datetime.strptime(system_datetime, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo(timezone))
        if system_date is not None and system_date != parsed_datetime.strftime("%Y-%m-%d"):
            raise ValueError("system_date must match the date portion of system_datetime")
        return parsed_datetime, True

    if system_date is None:
        raise ValueError("system_date is required when system_datetime is omitted")

    return datetime.strptime(system_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo(timezone)), False


def _serialize_top_level_value(
    *,
    item_id: str,
    text: str,
    expr_payload: Mapping[str, Any],
    value: TemporalValue,
    timezone: str,
) -> list[dict[str, Any]]:
    if isinstance(value, TimeRange):
        return [
            _build_resolved_item_payload(
                item_id=item_id,
                text=text,
                timezone=timezone,
                resolved=value,
                source_id=None,
                source_text=None,
            )
        ]

    segments = _flatten_temporal_value(value)
    if expr_payload["op"] in {"enumerate_calendar_days", "enumerate_makeup_workdays"} and segments:
        segments = _merge_dates_to_ranges([segment.start.date() for segment in segments], segments[0].start.tzinfo)

    return [
        _build_resolved_item_payload(
            item_id=f"{item_id}__seg_{index:02d}",
            text=text,
            timezone=timezone,
            resolved=segment,
            source_id=item_id,
            source_text=text,
        )
        for index, segment in enumerate(segments, start=1)
    ]


def resolve_query(
    parsed_time_expressions: dict[str, Any] | ParsedTimeExpressions,
    system_date: str | None = None,
    system_datetime: str | None = None,
    timezone: str = "Asia/Shanghai",
    business_calendar: BusinessCalendarPort | None = None,
    business_calendar_root: Path | None = None,
) -> dict[str, Any]:
    parsed = ParsedTimeExpressions.model_validate(parsed_time_expressions)
    system_dt, system_has_time = _resolve_system_context(
        system_date=system_date,
        system_datetime=system_datetime,
        timezone=timezone,
    )
    parsed_payload = parsed.model_dump(mode="python")
    parsed_payload["time_expressions"] = [
        {
            **item,
            "expr": _normalize_legacy_rolling_expr(item["expr"], parsed.rolling_includes_today),
        }
        for item in parsed_payload["time_expressions"]
    ]
    parsed = ParsedTimeExpressions.model_validate(parsed_payload)
    rolling_anchor_dt = system_dt if parsed.rolling_includes_today else system_dt - timedelta(days=1)
    if business_calendar is None:
        needs_business_calendar = any(_expr_uses_business_calendar(item.expr) for item in parsed.time_expressions)
        if needs_business_calendar:
            business_calendar = JsonBusinessCalendar.from_root(root=business_calendar_root or get_business_calendar_root())
    items_by_id = {item.id: item for item in parsed.time_expressions}
    resolved_map: dict[str, TemporalValue] = {}
    resolving_stack: list[str] = []
    resolved_items: list[dict[str, Any]] = []
    resolved_groups: list[dict[str, Any]] = []
    calendar_versions: set[str] = set()
    enumerated_counts: dict[str, int] = {}
    rewrite_hints: dict[str, dict[str, Any]] = {}
    no_match_results: list[dict[str, Any]] = []

    def _resolve_item(item_id: str) -> TemporalValue:
        if item_id in resolved_map:
            return resolved_map[item_id]
        if item_id not in items_by_id:
            raise ValueError(f"Unknown reference: {item_id}")
        if item_id in resolving_stack:
            cycle = " -> ".join([*resolving_stack, item_id])
            raise ValueError(f"Circular reference: {cycle}")

        resolving_stack.append(item_id)
        item = items_by_id[item_id]
        try:
            resolved_value = eval_expr(
                item.expr,
                system_dt,
                resolved_map,
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
                rolling_anchor_dt=rolling_anchor_dt,
                system_has_time=system_has_time,
                reference_resolver=_resolve_item,
            )
        finally:
            resolving_stack.pop()
        resolved_map[item_id] = resolved_value
        return resolved_value

    for item in parsed.time_expressions:
        expr_payload = _normalize_expr(item.expr)
        try:
            resolved = _resolve_item(item.id)
        except ValueError as exc:
            if _should_convert_empty_selection_error_to_no_match(expr_payload, exc):
                no_match = _build_no_match_result(item.id, item.text, expr_payload)
                if no_match is not None:
                    no_match_results.append(no_match)
                    continue
            raise
        if expr_payload["op"] in {"enumerate_calendar_days", "enumerate_makeup_workdays", "enumerate_hours"}:
            enumerated_counts[item.id] = len(_flatten_temporal_value(resolved))
        if not _flatten_temporal_value(resolved):
            no_match = _build_no_match_result(item.id, item.text, expr_payload)
            if no_match is not None:
                no_match_results.append(no_match)
        rewrite_hint = _build_rewrite_hint(resolved)
        if rewrite_hint is not None:
            rewrite_hints[item.id] = rewrite_hint
        resolved_items.extend(
            _serialize_top_level_value(
                item_id=item.id,
                text=item.text,
                expr_payload=expr_payload,
                value=resolved,
                timezone=timezone,
            )
        )
        if _has_temporal_members(resolved):
            resolved_groups.append(
                _serialize_grouped_value(
                    item_id=item.id,
                    text=item.text,
                    value=resolved,
                    timezone=timezone,
                )
            )

    metadata = None
    if calendar_versions or enumerated_counts or rewrite_hints or no_match_results:
        metadata = {
            "calendar_version": ",".join(sorted(calendar_versions)) if calendar_versions else None,
            "enumerated_counts": enumerated_counts or None,
            "rewrite_hints": rewrite_hints or None,
            "no_match_results": no_match_results or None,
        }

    payload = ResolvedTimeExpressions(
        resolved_time_expressions=resolved_items,
        resolved_time_expression_groups=resolved_groups,
        metadata=metadata,
    ).model_dump(mode="python", exclude_none=False)
    for item in payload["resolved_time_expressions"]:
        if item.get("is_partial") is None:
            item.pop("is_partial", None)
    if not payload.get("resolved_time_expression_groups"):
        payload.pop("resolved_time_expression_groups", None)
    else:
        for item in payload["resolved_time_expression_groups"]:
            _drop_none_partials_from_group(item)
    if payload.get("metadata") is None:
        payload.pop("metadata", None)
    else:
        if payload["metadata"].get("calendar_version") is None:
            payload["metadata"].pop("calendar_version", None)
        if payload["metadata"].get("enumerated_counts") is None:
            payload["metadata"].pop("enumerated_counts", None)
        if payload["metadata"].get("rewrite_hints") is None:
            payload["metadata"].pop("rewrite_hints", None)
        if payload["metadata"].get("no_match_results") is None:
            payload["metadata"].pop("no_match_results", None)
        if not payload["metadata"]:
            payload.pop("metadata", None)
    return payload
