from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from time_query_service.config import get_slice_subperiod_max_counts
from time_query_service.schemas import ParsedTimeExpressions, ResolvedTimeExpressions


@dataclass(frozen=True)
class TimeRange:
    start: datetime
    end: datetime
    grain: str | None = None


def start_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=23, minute=59, second=59, microsecond=0)


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
    return TimeRange(start, end, grain=base.grain)


def shift_year_like(base: TimeRange, years: int) -> TimeRange:
    start = add_years(base.start, years)
    end = add_years(base.end, years)
    return TimeRange(start, end, grain=base.grain)


def shift_range(base: TimeRange, unit: str, value: int) -> TimeRange:
    if unit == "day":
        delta = timedelta(days=value)
        return TimeRange(base.start + delta, base.end + delta, grain=base.grain)

    if unit == "week":
        delta = timedelta(weeks=value)
        return TimeRange(base.start + delta, base.end + delta, grain=base.grain)

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
        return TimeRange(start_of_day(start_date), end, grain=None)

    if unit == "week":
        start_date = add_days(system_dt, -(value * 7) + 1)
        return TimeRange(start_of_day(start_date), end, grain=None)

    if unit == "month":
        start_date = add_days(add_months(system_dt, -value), 1)
        return TimeRange(start_of_day(start_date), end, grain=None)

    if unit == "quarter":
        start_date = add_days(add_months(system_dt, -(value * 3)), 1)
        return TimeRange(start_of_day(start_date), end, grain=None)

    if unit == "half_year":
        start_date = add_days(add_months(system_dt, -(value * 6)), 1)
        return TimeRange(start_of_day(start_date), end, grain=None)

    if unit == "year":
        start_date = add_days(add_years(system_dt, -value), 1)
        return TimeRange(start_of_day(start_date), end, grain=None)

    raise ValueError(f"Unsupported rolling unit: {unit}")


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


def _validate_subperiod_request(base_grain: str | None, unit: str, count: int) -> None:
    if base_grain is None:
        raise ValueError("Unsupported subperiod slicing: base range does not have a natural grain.")

    max_counts = get_slice_subperiod_max_counts()
    child_limits = max_counts.get(base_grain)
    if child_limits is None or unit not in child_limits:
        raise ValueError(f"Unsupported subperiod slicing: {base_grain} -> {unit}")

    max_allowed = child_limits[unit]
    if count > max_allowed:
        raise ValueError(
            f"Requested {count} {unit} subperiods from {base_grain}, "
            f"which exceeds configured maximum {max_allowed}."
        )


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


def split_into_subperiods(base: TimeRange, unit: str) -> list[TimeRange]:
    _validate_subperiod_request(base.grain, unit, 1)

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


def slice_subperiods_range(base: TimeRange, mode: str, unit: str, count: int) -> TimeRange:
    _validate_subperiod_request(base.grain, unit, count)
    parts = split_into_subperiods(base, unit)

    if count > len(parts):
        raise ValueError(
            f"Requested {count} {unit} subperiods from {base.grain}, but only {len(parts)} are available."
        )

    selected = parts[:count] if mode == "first" else parts[-count:]
    if not selected:
        raise ValueError("No subperiods selected.")

    return TimeRange(selected[0].start, selected[-1].end, grain=None)


def select_subperiod_range(base: TimeRange, unit: str, index: int) -> TimeRange:
    _validate_subperiod_request(base.grain, unit, index)
    parts = split_into_subperiods(base, unit)

    if index > len(parts):
        raise ValueError(
            f"Requested subperiod index {index} for {base.grain} -> {unit}, "
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


def eval_expr(
    expr: Mapping[str, Any] | Any,
    system_dt: datetime,
    context: Mapping[str, TimeRange] | None = None,
) -> TimeRange:
    payload = _normalize_expr(expr)
    op = payload["op"]

    if op == "anchor":
        return TimeRange(start_of_day(system_dt), end_of_day(system_dt), grain="day")

    if op == "current_period":
        return current_period(system_dt, payload["unit"])

    if op == "shift":
        base = eval_expr(payload["base"], system_dt, context)
        return shift_range(base, payload["unit"], payload["value"])

    if op == "rolling":
        return rolling_range(system_dt, payload["unit"], payload["value"])

    if op == "slice_subperiods":
        base = eval_expr(payload["base"], system_dt, context)
        return slice_subperiods_range(base, payload["mode"], payload["unit"], payload["count"])

    if op == "select_subperiod":
        base = eval_expr(payload["base"], system_dt, context)
        return select_subperiod_range(base, payload["unit"], payload["index"])

    if op == "select_weekday":
        base = eval_expr(payload["base"], system_dt, context)
        _require_week_base(base, "select_weekday")
        target = base.start + timedelta(days=payload["weekday"] - 1)
        return TimeRange(start_of_day(target), end_of_day(target), grain="day")

    if op == "select_weekend":
        base = eval_expr(payload["base"], system_dt, context)
        _require_week_base(base, "select_weekend")
        start = start_of_day(base.start + timedelta(days=5))
        end = end_of_day(base.start + timedelta(days=6))
        return TimeRange(start, end, grain=None)

    if op == "select_occurrence":
        base = eval_expr(payload["base"], system_dt, context)
        return select_occurrence_range(
            base,
            payload["kind"],
            payload["ordinal"],
            payload.get("weekday"),
        )

    if op == "select_month":
        base = eval_expr(payload["base"], system_dt, context)
        month = payload["month"]
        year = base.start.year
        return _natural_month_range(year, month, base.start.tzinfo)

    if op == "select_quarter":
        base = eval_expr(payload["base"], system_dt, context)
        quarter = payload["quarter"]
        year = base.start.year
        return _natural_quarter_range(year, quarter, base.start.tzinfo)

    if op == "select_half_year":
        base = eval_expr(payload["base"], system_dt, context)
        return _natural_half_year_range(base.start.year, payload["half"], base.start.tzinfo)

    if op == "reference":
        if context is None or payload["ref"] not in context:
            raise ValueError(f"Unknown reference: {payload['ref']}")
        return context[payload["ref"]]

    raise ValueError(f"Unsupported op: {op}")


def resolve_query(
    parsed_time_expressions: dict[str, Any] | ParsedTimeExpressions,
    system_date: str,
    timezone: str,
) -> dict[str, Any]:
    parsed = ParsedTimeExpressions.model_validate(parsed_time_expressions)
    system_dt = datetime.strptime(system_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo(timezone))
    resolved_map: dict[str, TimeRange] = {}
    resolved_items = []
    for item in parsed.time_expressions:
        resolved = eval_expr(item.expr, system_dt, resolved_map)
        resolved_map[item.id] = resolved
        resolved_items.append(
            {
                "id": item.id,
                "text": item.text,
                "start_time": resolved.start.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": resolved.end.strftime("%Y-%m-%d %H:%M:%S"),
                "timezone": timezone,
            }
        )

    return ResolvedTimeExpressions(
        resolved_time_expressions=resolved_items,
    ).model_dump(mode="python")
