from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
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

    @property
    def subperiod_parent_grain(self) -> str | None:
        return self.slicing_grain or self.grain


def start_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=23, minute=59, second=59, microsecond=0)


def start_of_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def end_of_hour(dt: datetime) -> datetime:
    return dt.replace(minute=59, second=59, microsecond=0)


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


def _normalize_expr(expr: Any) -> dict[str, Any]:
    if hasattr(expr, "model_dump"):
        return expr.model_dump(mode="python")
    return dict(expr)


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


def _validate_subperiod_request(base: TimeRange, unit: str, count: int) -> str:
    base_grain = base.subperiod_parent_grain
    if base_grain is None:
        raise ValueError("Unsupported subperiod slicing: base range does not have a natural grain.")

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
                )
            )
        current = _natural_period_containing(_next_natural_period_start(current.start, unit), unit)
    return parts


def split_into_subperiods(base: TimeRange, unit: str) -> list[TimeRange]:
    _validate_subperiod_request(base, unit, 1)

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


def _aggregate_partial_flag(parts: list[TimeRange]) -> bool | None:
    partial_flags = [part.is_partial for part in parts if part.is_partial is not None]
    if not partial_flags:
        return None
    return any(partial_flags)


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


def slice_subperiods_range(base: TimeRange, mode: str, unit: str, count: int) -> TimeRange:
    base_grain = _validate_subperiod_request(base, unit, count)
    parts = split_into_subperiods(base, unit)

    if count > len(parts):
        raise ValueError(
            f"Requested {count} {unit} subperiods from {base_grain}, but only {len(parts)} are available."
        )

    selected = parts[:count] if mode == "first" else parts[-count:]
    if not selected:
        raise ValueError("No subperiods selected.")

    return TimeRange(
        selected[0].start,
        selected[-1].end,
        grain=None,
        is_partial=_aggregate_partial_flag(selected),
    )


def select_subperiod_range(base: TimeRange, unit: str, index: int) -> TimeRange:
    base_grain = _validate_subperiod_request(base, unit, index)
    parts = split_into_subperiods(base, unit)

    if index > len(parts):
        raise ValueError(
            f"Requested subperiod index {index} for {base_grain} -> {unit}, "
            f"but only {len(parts)} subperiods are available."
        )

    return parts[index - 1]


def _require_week_base(base: TimeRange, op_name: str) -> None:
    if base.grain != "week":
        raise ValueError(f"{op_name} requires a week base, got {base.grain!r}.")


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


def select_occurrence_range(base: TimeRange, kind: str, ordinal: int | str, weekday: int | None = None) -> TimeRange:
    if base.grain not in {"month", "quarter", "half_year", "year"}:
        raise ValueError(
            "select_occurrence requires a parent period with grain month, quarter, half_year, or year."
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
            f"Requested occurrence {ordinal} for {kind} within {base.grain}, "
            f"but only {len(occurrences)} are available."
        )

    return occurrences[ordinal - 1]


def _ensure_single_day(base: TimeRange, op_name: str) -> date:
    if base.start.date() != base.end.date():
        raise ValueError(f"{op_name} requires a single-day base.")
    return base.start.date()


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
    checked_years: set[int] = set()

    while True:
        if cursor.year not in checked_years:
            version = business_calendar.calendar_version_for_year(region=region, year=cursor.year)
            if version is None:
                raise ValueError(f"Missing business calendar data for region={region}, year={cursor.year}")
            if calendar_versions is not None:
                calendar_versions.add(version)
            checked_years.add(cursor.year)
        if business_calendar.is_workday(region=region, d=cursor):
            remaining -= 1
            if remaining == 0:
                target = datetime.combine(cursor, datetime.min.time(), tzinfo=base.start.tzinfo)
                return TimeRange(start_of_day(target), end_of_day(target), grain="day")
        cursor = cursor + timedelta(days=step)


def _expr_uses_business_calendar(expr: Mapping[str, Any] | Any) -> bool:
    payload = _normalize_expr(expr)
    if payload.get("op") in {
        "calendar_event_range",
        "business_day_offset",
        "enumerate_calendar_days",
        "enumerate_makeup_workdays",
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


def eval_expr(
    expr: Mapping[str, Any] | Any,
    system_dt: datetime,
    context: Mapping[str, TimeRange] | None = None,
    business_calendar: BusinessCalendarPort | None = None,
    calendar_versions: set[str] | None = None,
    rolling_anchor_dt: datetime | None = None,
    system_has_time: bool = False,
) -> TimeRange:
    payload = _normalize_expr(expr)
    op = payload["op"]

    if op == "anchor":
        return TimeRange(start_of_day(system_dt), end_of_day(system_dt), grain="day")

    if op == "current_period":
        return current_period(system_dt, payload["unit"])

    if op == "current_hour":
        if not system_has_time:
            raise ValueError("current_hour requires system_datetime.")
        return current_hour(system_dt)

    if op == "rolling_hours":
        if not system_has_time:
            raise ValueError("rolling_hours requires system_datetime.")
        return rolling_hours_range(system_dt, payload["value"])

    if op == "shift":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return shift_range(base, payload["unit"], payload["value"])

    if op == "rolling":
        return rolling_range(rolling_anchor_dt or system_dt, payload["unit"], payload["value"])

    if op == "bounded_range":
        start = eval_expr(
            payload["start"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        end = eval_expr(
            payload["end"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return bounded_range(start, end)

    if op == "calendar_event_range":
        if business_calendar is None:
            raise ValueError("Business calendar is required for calendar_event_range.")
        version = business_calendar.calendar_version_for_year(region=payload["region"], year=payload["year"])
        if version is None:
            raise ValueError(f"Missing business calendar data for region={payload['region']}, year={payload['year']}")
        span = business_calendar.get_event_span(
            region=payload["region"],
            event_key=payload["event_key"],
            year=payload["year"],
            scope=payload["scope"],
        )
        if span is None:
            raise ValueError(
                f"Missing business calendar data for region={payload['region']}, "
                f"event_key={payload['event_key']}, year={payload['year']}, scope={payload['scope']}"
            )
        if calendar_versions is not None:
            calendar_versions.add(version)
        start, end = span
        start_dt = datetime.combine(start, datetime.min.time(), tzinfo=system_dt.tzinfo)
        end_dt = datetime.combine(end, datetime.min.time(), tzinfo=system_dt.tzinfo)
        return TimeRange(start_of_day(start_dt), end_of_day(end_dt), grain=None)

    if op == "range_edge":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return range_edge(base, payload["edge"])

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
        )
        return business_day_offset_range(
            base=base,
            value=payload["value"],
            region=payload["region"],
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
        )

    if op == "enumerate_calendar_days":
        if business_calendar is None:
            raise ValueError("Business calendar is required for enumerate_calendar_days.")
        return eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )

    if op == "enumerate_makeup_workdays":
        raise ValueError("enumerate_makeup_workdays must be resolved via resolve_query.")

    if op == "enumerate_hours":
        raise ValueError("enumerate_hours must be resolved via resolve_query.")

    if op == "enumerate_subperiods":
        raise ValueError("enumerate_subperiods must be resolved via resolve_query.")

    if op == "select_hour":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return select_hour_range(base, payload["hour"])

    if op == "slice_hours":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return slice_hours_range(base, payload["mode"], payload["count"])

    if op == "slice_subperiods":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return slice_subperiods_range(base, payload["mode"], payload["unit"], payload["count"])

    if op == "select_subperiod":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return select_subperiod_range(base, payload["unit"], payload["index"])

    if op == "select_weekday":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        _require_week_base(base, "select_weekday")
        target = base.start + timedelta(days=payload["weekday"] - 1)
        return TimeRange(start_of_day(target), end_of_day(target), grain="day")

    if op == "select_weekend":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        _require_week_base(base, "select_weekend")
        start = start_of_day(base.start + timedelta(days=5))
        end = end_of_day(base.start + timedelta(days=6))
        return TimeRange(start, end, grain=None)

    if op == "select_occurrence":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return select_occurrence_range(
            base,
            payload["kind"],
            payload["ordinal"],
            payload.get("weekday"),
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
        )
        month = payload["month"]
        year = base.start.year
        return _natural_month_range(year, month, base.start.tzinfo)

    if op == "select_quarter":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        quarter = payload["quarter"]
        year = base.start.year
        return _natural_quarter_range(year, quarter, base.start.tzinfo)

    if op == "select_half_year":
        base = eval_expr(
            payload["base"],
            system_dt,
            context,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        return _natural_half_year_range(base.start.year, payload["half"], base.start.tzinfo)

    if op == "reference":
        if context is None or payload["ref"] not in context:
            raise ValueError(f"Unknown reference: {payload['ref']}")
        return context[payload["ref"]]

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
    years = sorted({d.year for d in dates})
    missing_years = [
        year for year in years if business_calendar.calendar_version_for_year(region=region, year=year) is None
    ]
    if missing_years:
        raise ValueError(f"Missing business calendar data for region={region}, year={missing_years[0]}")
    if calendar_versions is not None:
        for year in years:
            version = business_calendar.calendar_version_for_year(region=region, year=year)
            if version is not None:
                calendar_versions.add(version)

    if day_kind == "workday":
        return [d for d in dates if business_calendar.is_workday(region=region, d=d)]
    if day_kind == "restday":
        return [d for d in dates if not business_calendar.is_workday(region=region, d=d)]
    if day_kind == "holiday":
        return [d for d in dates if business_calendar.is_holiday(region=region, d=d)]
    raise ValueError(f"Unsupported calendar day kind: {day_kind}")


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
    rolling_anchor_dt = system_dt if parsed.rolling_includes_today else system_dt - timedelta(days=1)
    if business_calendar is None:
        needs_business_calendar = any(_expr_uses_business_calendar(item.expr) for item in parsed.time_expressions)
        if needs_business_calendar:
            business_calendar = JsonBusinessCalendar.from_root(root=business_calendar_root or get_business_calendar_root())
    resolved_map: dict[str, TimeRange] = {}
    resolved_items = []
    calendar_versions: set[str] = set()
    enumerated_counts: dict[str, int] = {}
    for item in parsed.time_expressions:
        expr_payload = _normalize_expr(item.expr)
        if expr_payload["op"] == "enumerate_calendar_days":
            if business_calendar is None:
                raise ValueError("Business calendar is required for enumerate_calendar_days.")
            base = eval_expr(
                expr_payload["base"],
                system_dt,
                resolved_map,
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
                rolling_anchor_dt=rolling_anchor_dt,
                system_has_time=system_has_time,
            )
            resolved_map[item.id] = base
            matched_dates = _filter_calendar_dates(
                base=base,
                region=expr_payload["region"],
                day_kind=expr_payload["day_kind"],
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
            )
            enumerated_counts[item.id] = len(matched_dates)
            segments = _merge_dates_to_ranges(matched_dates, base.start.tzinfo)
            for index, segment in enumerate(segments, start=1):
                resolved_items.append(
                    {
                        "id": f"{item.id}__seg_{index:02d}",
                        "text": item.text,
                        "source_id": item.id,
                        "source_text": item.text,
                        "start_time": segment.start.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": segment.end.strftime("%Y-%m-%d %H:%M:%S"),
                        "timezone": timezone,
                    }
                )
            continue

        if expr_payload["op"] == "enumerate_makeup_workdays":
            if business_calendar is None:
                raise ValueError("Business calendar is required for enumerate_makeup_workdays.")
            version = business_calendar.calendar_version_for_year(
                region=expr_payload["region"],
                year=expr_payload["year"],
            )
            if version is None:
                raise ValueError(
                    f"Missing business calendar data for region={expr_payload['region']}, "
                    f"year={expr_payload['year']}"
                )
            calendar_versions.add(version)
            matched_dates = business_calendar.list_makeup_workdays(
                region=expr_payload["region"],
                event_key=expr_payload["event_key"],
                year=expr_payload["year"],
            )
            enumerated_counts[item.id] = len(matched_dates)
            segments = _merge_dates_to_ranges(matched_dates, system_dt.tzinfo)
            for index, segment in enumerate(segments, start=1):
                resolved_items.append(
                    {
                        "id": f"{item.id}__seg_{index:02d}",
                        "text": item.text,
                        "source_id": item.id,
                        "source_text": item.text,
                        "start_time": segment.start.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": segment.end.strftime("%Y-%m-%d %H:%M:%S"),
                        "timezone": timezone,
                    }
                )
            continue

        if expr_payload["op"] == "enumerate_subperiods":
            base = eval_expr(
                expr_payload["base"],
                system_dt,
                resolved_map,
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
                rolling_anchor_dt=rolling_anchor_dt,
                system_has_time=system_has_time,
            )
            resolved_map[item.id] = base
            segments = split_into_subperiods(base, expr_payload["unit"])
            for index, segment in enumerate(segments, start=1):
                resolved_items.append(
                    _build_resolved_item_payload(
                        item_id=f"{item.id}__seg_{index:02d}",
                        text=item.text,
                        timezone=timezone,
                        resolved=segment,
                        source_id=item.id,
                        source_text=item.text,
                    )
                )
            continue

        if expr_payload["op"] == "enumerate_hours":
            base = eval_expr(
                expr_payload["base"],
                system_dt,
                resolved_map,
                business_calendar=business_calendar,
                calendar_versions=calendar_versions,
                rolling_anchor_dt=rolling_anchor_dt,
                system_has_time=system_has_time,
            )
            resolved_map[item.id] = base
            segments = enumerate_hours_segments(base)
            enumerated_counts[item.id] = len(segments)
            for index, segment in enumerate(segments, start=1):
                resolved_items.append(
                    _build_resolved_item_payload(
                        item_id=f"{item.id}__seg_{index:02d}",
                        text=item.text,
                        timezone=timezone,
                        resolved=segment,
                        source_id=item.id,
                        source_text=item.text,
                    )
                )
            continue

        resolved = eval_expr(
            item.expr,
            system_dt,
            resolved_map,
            business_calendar=business_calendar,
            calendar_versions=calendar_versions,
            rolling_anchor_dt=rolling_anchor_dt,
            system_has_time=system_has_time,
        )
        resolved_map[item.id] = resolved
        resolved_items.append(
            _build_resolved_item_payload(
                item_id=item.id,
                text=item.text,
                timezone=timezone,
                resolved=resolved,
                source_id=None,
                source_text=None,
            )
        )

    metadata = None
    if calendar_versions or enumerated_counts:
        metadata = {
            "calendar_version": ",".join(sorted(calendar_versions)) if calendar_versions else None,
            "enumerated_counts": enumerated_counts or None,
        }

    payload = ResolvedTimeExpressions(
        resolved_time_expressions=resolved_items,
        metadata=metadata,
    ).model_dump(mode="python", exclude_none=False)
    for item in payload["resolved_time_expressions"]:
        if item.get("is_partial") is None:
            item.pop("is_partial", None)
    if payload.get("metadata") is None:
        payload.pop("metadata", None)
    else:
        if payload["metadata"].get("calendar_version") is None:
            payload["metadata"].pop("calendar_version", None)
        if payload["metadata"].get("enumerated_counts") is None:
            payload["metadata"].pop("enumerated_counts", None)
        if not payload["metadata"]:
            payload.pop("metadata", None)
    return payload
