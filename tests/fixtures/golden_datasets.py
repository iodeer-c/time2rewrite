from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.post_processor import StageAOutput, StageBOutput
from time_query_service.resolved_plan import Interval, IntervalTree, ResolvedComparison, ResolvedComparisonPair, ResolvedNode, ResolvedPlan, TreeLabels
from time_query_service.time_plan import (
    Carrier,
    CalendarFilter,
    CalendarEvent,
    Comparison,
    ComparisonPair,
    DerivationSource,
    EnumerationSet,
    GroupedTemporalValue,
    MappedRange,
    NamedPeriod,
    RollingByCalendarUnit,
    RollingWindow,
    ScheduleYearRef,
    StandaloneContent,
    SurfaceFragment,
    TimePlan,
    Unit,
)


def _calendar() -> JsonBusinessCalendar:
    return JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))


def _month_interval(year: int, month: int) -> Interval:
    return Interval(start=date(year, month, 1), end=date(year, month, monthrange(year, month)[1]), end_inclusive=True)


def _quarter_interval(year: int, quarter: int) -> Interval:
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    return Interval(start=date(year, start_month, 1), end=date(year, end_month, monthrange(year, end_month)[1]), end_inclusive=True)


def _half_interval(year: int, half: int) -> Interval:
    start_month = 1 if half == 1 else 7
    end_month = 6 if half == 1 else 12
    return Interval(start=date(year, start_month, 1), end=date(year, end_month, monthrange(year, end_month)[1]), end_inclusive=True)


def _year_interval(year: int) -> Interval:
    return Interval(start=date(year, 1, 1), end=date(year, 12, 31), end_inclusive=True)


def _add_months(anchor: date, months: int) -> date:
    total = anchor.year * 12 + (anchor.month - 1) + months
    year = total // 12
    month = total % 12 + 1
    day = min(anchor.day, monthrange(year, month)[1])
    return date(year, month, day)


def _rolling_interval(system_date: date, *, length: int, unit: str) -> Interval:
    if unit == "day":
        start = system_date - timedelta(days=length - 1)
    elif unit == "week":
        start = system_date - timedelta(days=7 * length - 1)
    elif unit == "month":
        start = _add_months(system_date, -(length))
        start = start + timedelta(days=1)
    elif unit == "quarter":
        start = _add_months(system_date, -(3 * length))
        start = start + timedelta(days=1)
    elif unit == "half_year":
        start = _add_months(system_date, -(6 * length))
        start = start + timedelta(days=1)
    elif unit == "year":
        start = _add_months(system_date, -(12 * length))
        start = start + timedelta(days=1)
    else:
        raise ValueError(unit)
    return Interval(start=start, end=system_date, end_inclusive=True)


def _atom_tree(interval: Interval) -> IntervalTree:
    return IntervalTree(role="atom", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval))


def _standalone_named_period_case(*, query: str, system_date: date, unit_id: str, render_text: str, carrier: Carrier, interval: Interval, tier: int, tags: list[str]) -> dict:
    time_plan = TimePlan(
        query=query,
        system_date=system_date,
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id=unit_id,
                render_text=render_text,
                surface_fragments=[SurfaceFragment(start=0, end=len(render_text))],
                content=StandaloneContent(content_kind="standalone", carrier=carrier),
            )
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(nodes={unit_id: ResolvedNode(tree=_atom_tree(interval), derived_from=[])}, comparisons=[])
    return {
        "query": query,
        "system_date": system_date.isoformat(),
        "tier": tier,
        "expected_time_plan": time_plan,
        "expected_resolved_plan": resolved_plan,
        "capability_tags": tags,
    }


def _standalone_tree_case(
    *,
    query: str,
    system_date: date,
    unit_id: str,
    render_text: str,
    carrier: Carrier,
    tree: IntervalTree,
    tier: int,
    tags: list[str],
) -> dict:
    time_plan = TimePlan(
        query=query,
        system_date=system_date,
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id=unit_id,
                render_text=render_text,
                surface_fragments=[SurfaceFragment(start=0, end=len(render_text))],
                content=StandaloneContent(content_kind="standalone", carrier=carrier),
            )
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(nodes={unit_id: ResolvedNode(tree=tree, derived_from=[])}, comparisons=[])
    return {
        "query": query,
        "system_date": system_date.isoformat(),
        "tier": tier,
        "expected_time_plan": time_plan,
        "expected_resolved_plan": resolved_plan,
        "capability_tags": tags,
    }


def _matches_day_class(day: date, day_class: str) -> bool:
    status = _calendar().get_day_status(region="CN", d=day)
    if day_class == "workday":
        return bool(status.is_workday)
    if day_class == "holiday":
        return bool(status.is_holiday)
    if day_class == "makeup_workday":
        return bool(status.is_makeup_workday)
    if day_class == "weekend":
        return not bool(status.is_workday) and not bool(status.is_holiday)
    raise ValueError(day_class)


def _counted_day_intervals(*, system_date: date, length: int, day_class: str) -> list[Interval]:
    matched_days: list[date] = []
    cursor = system_date
    while len(matched_days) < length:
        if _matches_day_class(cursor, day_class):
            matched_days.append(cursor)
        cursor = cursor - timedelta(days=1)
    matched_days.reverse()
    return [Interval(start=day, end=day, end_inclusive=True) for day in matched_days]


def _filtered_collection_tree(*, aggregate: Interval, members: list[Interval]) -> IntervalTree:
    children = [_atom_tree(member) for member in members]
    return IntervalTree(
        role="filtered_collection",
        intervals=[aggregate],
        children=children,
        labels=TreeLabels(absolute_core_time=aggregate),
    )


def _grouped_tree(*, aggregate: Interval, buckets: list[Interval]) -> IntervalTree:
    children = [_atom_tree(bucket) for bucket in buckets]
    return IntervalTree(
        role="grouped_member",
        intervals=buckets,
        children=children,
        labels=TreeLabels(absolute_core_time=aggregate),
    )


def _union_tree(intervals: list[Interval]) -> IntervalTree:
    children = [_atom_tree(interval) for interval in intervals]
    aggregate = Interval(start=intervals[0].start, end=intervals[-1].end, end_inclusive=True)
    return IntervalTree(
        role="union",
        intervals=intervals,
        children=children,
        labels=TreeLabels(absolute_core_time=aggregate),
    )


def _calendar_event_interval(*, event_key: str, year: int, scope: str = "consecutive_rest") -> Interval:
    span = _calendar().get_event_span(region="CN", event_key=event_key, schedule_year=year, scope=scope)
    if span is None:
        raise ValueError(f"missing event span for {event_key}/{year}/{scope}")
    return Interval(start=span[0], end=span[1], end_inclusive=True)


def _comparison_case(
    *,
    query: str,
    system_date: date,
    left_render_text: str,
    left_carrier: Carrier,
    left_tree: IntervalTree,
    right_render_text: str,
    right_carrier: Carrier,
    right_tree: IntervalTree,
    tier: int,
    tags: list[str],
) -> dict:
    anchor_index = query.index("对比")
    right_start = query.index(right_render_text)
    time_plan = TimePlan(
        query=query,
        system_date=system_date,
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u1",
                render_text=left_render_text,
                surface_fragments=[SurfaceFragment(start=0, end=len(left_render_text))],
                content=StandaloneContent(content_kind="standalone", carrier=left_carrier),
            ),
            Unit(
                unit_id="u2",
                render_text=right_render_text,
                surface_fragments=[SurfaceFragment(start=right_start, end=right_start + len(right_render_text))],
                content=StandaloneContent(content_kind="standalone", carrier=right_carrier),
            ),
        ],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[ComparisonPair(subject_unit_id="u1", reference_unit_id="u2")],
            )
        ],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(tree=left_tree, derived_from=[]),
            "u2": ResolvedNode(tree=right_tree, derived_from=[]),
        },
        comparisons=[
            ResolvedComparison(
                comparison_id="c1",
                pairs=[
                    ResolvedComparisonPair(
                        subject_unit_id="u1",
                        reference_unit_id="u2",
                        degraded=False,
                        subject_absolute_core_time=left_tree.labels.absolute_core_time,
                        reference_absolute_core_time=right_tree.labels.absolute_core_time,
                    )
                ],
            )
        ],
    )
    return {
        "query": query,
        "system_date": system_date.isoformat(),
        "tier": tier,
        "expected_time_plan": time_plan,
        "expected_resolved_plan": resolved_plan,
        "capability_tags": tags,
    }


def _stage_a_case(query: str, units: list[dict], comparisons: list[dict] | None = None) -> dict:
    return {
        "query": query,
        "expected": StageAOutput.model_validate(
            {
                "query": query,
                "system_date": "2026-04-17",
                "timezone": "Asia/Shanghai",
                "units": units,
                "comparisons": comparisons or [],
            }
        ),
    }


def _stage_b_case(text: str, payload: dict) -> dict:
    return {"text": text, "expected": StageBOutput.model_validate(payload)}


STAGE_A_GOLDEN_CASES: list[dict] = []
STAGE_B_GOLDEN_CASES: list[dict] = []
LAYER1_GOLDEN_CASES: list[dict] = []


for month in range(1, 13):
    STAGE_A_GOLDEN_CASES.append(
        _stage_a_case(
            f"2025年{month}月收益",
            [
                {
                    "unit_id": "u1",
                    "render_text": f"2025年{month}月",
                    "surface_fragments": [{"start": 0, "end": len(f'2025年{month}月')}],
                    "content_kind": "standalone",
                    "self_contained_text": f"2025年{month}月",
                    "sources": [],
                }
            ],
        )
    )
    STAGE_B_GOLDEN_CASES.append(
        _stage_b_case(
            f"2025年{month}月",
            {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": month},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        )
    )
    LAYER1_GOLDEN_CASES.append(
        _standalone_named_period_case(
            query=f"2025年{month}月收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text=f"2025年{month}月",
            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=month), modifiers=[]),
            interval=_month_interval(2025, month),
            tier=1,
            tags=["literal-period-expressions", "time-plan-schema", "resolved-plan-schema"],
        )
    )

for quarter in range(1, 5):
    STAGE_A_GOLDEN_CASES.append(
        _stage_a_case(
            f"2025年Q{quarter}收益",
            [
                {
                    "unit_id": "u1",
                    "render_text": f"2025年Q{quarter}",
                    "surface_fragments": [{"start": 0, "end": len(f'2025年Q{quarter}')}],
                    "content_kind": "standalone",
                    "self_contained_text": f"2025年Q{quarter}",
                    "sources": [],
                }
            ],
        )
    )
    STAGE_B_GOLDEN_CASES.append(
        _stage_b_case(
            f"2025年Q{quarter}",
            {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "quarter", "year": 2025, "quarter": quarter},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        )
    )
    LAYER1_GOLDEN_CASES.append(
        _standalone_named_period_case(
            query=f"2025年Q{quarter}收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text=f"2025年Q{quarter}",
            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="quarter", year=2025, quarter=quarter), modifiers=[]),
            interval=_quarter_interval(2025, quarter),
            tier=1,
            tags=["grouped-temporal-values", "time-plan-schema"],
        )
    )

for half in (1, 2):
    STAGE_B_GOLDEN_CASES.append(
        _stage_b_case(
            f"2025年上半年" if half == 1 else "2025年下半年",
            {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "half_year", "year": 2025, "half": half},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        )
    )
    LAYER1_GOLDEN_CASES.append(
        _standalone_named_period_case(
            query=f"2025年{'上' if half == 1 else '下'}半年收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text=f"2025年{'上' if half == 1 else '下'}半年",
            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="half_year", year=2025, half=half), modifiers=[]),
            interval=_half_interval(2025, half),
            tier=1,
            tags=["time-plan-schema"],
        )
    )

for unit in ("day", "week", "month", "quarter", "half_year", "year"):
    text = {
        "day": "最近7天",
        "week": "最近一周",
        "month": "最近一个月",
        "quarter": "最近一季度",
        "half_year": "最近半年",
        "year": "最近一年",
    }[unit]
    STAGE_A_GOLDEN_CASES.append(
        _stage_a_case(
            f"{text}收益",
            [
                {
                    "unit_id": "u1",
                    "render_text": text,
                    "surface_fragments": [{"start": 0, "end": len(text)}],
                    "content_kind": "standalone",
                    "self_contained_text": text,
                    "sources": [],
                }
            ],
        )
    )
    STAGE_B_GOLDEN_CASES.append(
        _stage_b_case(
            text,
            {
                "carrier": {
                    "anchor": {"kind": "rolling_window", "length": 1 if unit != "day" else 7, "unit": unit, "endpoint": "today", "include_endpoint": True},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        )
    )
    LAYER1_GOLDEN_CASES.append(
        _standalone_named_period_case(
            query=f"{text}收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text=text,
            carrier=Carrier(anchor=RollingWindow(kind="rolling_window", length=1 if unit != "day" else 7, unit=unit, endpoint="today", include_endpoint=True), modifiers=[]),
            interval=_rolling_interval(date(2026, 4, 17), length=1 if unit != "day" else 7, unit=unit),
            tier=1,
            tags=["rolling-windows", "time-plan-schema"],
        )
    )

for text, day_class in [("最近5个工作日", "workday"), ("最近3个节假日", "holiday"), ("最近1个补班日", "makeup_workday")]:
    STAGE_A_GOLDEN_CASES.append(
        _stage_a_case(
            f"{text}收益",
            [
                {
                    "unit_id": "u1",
                    "render_text": text,
                    "surface_fragments": [{"start": 0, "end": len(text)}],
                    "content_kind": "standalone",
                    "self_contained_text": text,
                    "sources": [],
                    "surface_hint": "calendar_grain_rolling",
                }
            ],
        )
    )
    STAGE_B_GOLDEN_CASES.append(
        _stage_b_case(
            text,
            {
                "carrier": {
                    "anchor": {"kind": "rolling_by_calendar_unit", "length": int(text[2]), "day_class": day_class, "endpoint": "today", "include_endpoint": True},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        )
    )

STAGE_A_GOLDEN_CASES.extend(
    [
        _stage_a_case(
            "2025年3月和5月收益",
            [
                {"unit_id": "u1", "render_text": "2025年3月", "surface_fragments": [{"start": 0, "end": 7}], "content_kind": "standalone", "self_contained_text": "2025年3月", "sources": []},
                {"unit_id": "u2", "render_text": "5月", "surface_fragments": [{"start": 8, "end": 10}], "content_kind": "standalone", "self_contained_text": "2025年5月", "sources": []},
            ],
        ),
        _stage_a_case(
            "2025年3月和2025年3月对比",
            [
                {"unit_id": "u1", "render_text": "2025年3月", "surface_fragments": [{"start": 0, "end": 7}], "content_kind": "standalone", "self_contained_text": "2025年3月", "sources": []},
                {"unit_id": "u2", "render_text": "2025年3月", "surface_fragments": [{"start": 8, "end": 15}], "content_kind": "standalone", "self_contained_text": "2025年3月", "sources": []},
            ],
            comparisons=[{"comparison_id": "c1", "anchor_text": "对比", "pairs": [{"subject_unit_id": "u1", "reference_unit_id": "u2"}]}],
        ),
        _stage_a_case(
            "今年3月和5月，去年同期",
            [
                {"unit_id": "u1", "render_text": "今年3月", "surface_fragments": [{"start": 0, "end": 4}], "content_kind": "standalone", "self_contained_text": "今年3月", "sources": []},
                {"unit_id": "u2", "render_text": "5月", "surface_fragments": [{"start": 5, "end": 7}], "content_kind": "standalone", "self_contained_text": "今年5月", "sources": []},
                {"unit_id": "u3", "render_text": "去年同期", "surface_fragments": [{"start": 8, "end": 12}], "content_kind": "derived", "self_contained_text": None, "sources": [{"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}}, {"source_unit_id": "u2", "transform": {"kind": "shift_year", "offset": -1}}]},
            ],
        ),
        _stage_a_case(
            "2025年3月对比2024年3月",
            [
                {"unit_id": "u1", "render_text": "2025年3月", "surface_fragments": [{"start": 0, "end": 7}], "content_kind": "standalone", "self_contained_text": "2025年3月", "sources": []},
                {"unit_id": "u2", "render_text": "2024年3月", "surface_fragments": [{"start": 9, "end": 16}], "content_kind": "standalone", "self_contained_text": "2024年3月", "sources": []},
            ],
            comparisons=[{"comparison_id": "c1", "anchor_text": "对比", "pairs": [{"subject_unit_id": "u1", "reference_unit_id": "u2"}]}],
        ),
        _stage_a_case(
            "2025年每天收益",
            [
                {"unit_id": "u1", "render_text": "2025年每天", "surface_fragments": [{"start": 0, "end": 7}], "content_kind": "standalone", "self_contained_text": "2025年每天", "sources": []},
            ],
        ),
        _stage_a_case(
            "最近5天中的工作日收益",
            [
                {"unit_id": "u1", "render_text": "最近5天中的工作日", "surface_fragments": [{"start": 0, "end": 9}], "content_kind": "standalone", "self_contained_text": "最近5天中的工作日", "sources": []},
            ],
        ),
        _stage_a_case(
            "最近半年每季度收益",
            [
                {"unit_id": "u1", "render_text": "最近半年每季度", "surface_fragments": [{"start": 0, "end": 7}], "content_kind": "standalone", "self_contained_text": "最近半年每季度", "sources": []},
            ],
        ),
        _stage_a_case(
            "最近一年每半年收益",
            [
                {"unit_id": "u1", "render_text": "最近一年每半年", "surface_fragments": [{"start": 0, "end": 7}], "content_kind": "standalone", "self_contained_text": "最近一年每半年", "sources": []},
            ],
        ),
        _stage_a_case(
            "2025年3月的工作日对比2024年3月的工作日",
            [
                {"unit_id": "u1", "render_text": "2025年3月的工作日", "surface_fragments": [{"start": 0, "end": 11}], "content_kind": "standalone", "self_contained_text": "2025年3月的工作日", "sources": []},
                {"unit_id": "u2", "render_text": "2024年3月的工作日", "surface_fragments": [{"start": 13, "end": 24}], "content_kind": "standalone", "self_contained_text": "2024年3月的工作日", "sources": []},
            ],
            comparisons=[{"comparison_id": "c1", "anchor_text": "对比", "pairs": [{"subject_unit_id": "u1", "reference_unit_id": "u2"}]}],
        ),
        _stage_a_case(
            "2025年中秋假期和国庆假期收益",
            [
                {"unit_id": "u1", "render_text": "2025年中秋假期", "surface_fragments": [{"start": 0, "end": 9}], "content_kind": "standalone", "self_contained_text": "2025年中秋假期", "sources": []},
                {"unit_id": "u2", "render_text": "国庆假期", "surface_fragments": [{"start": 10, "end": 14}], "content_kind": "standalone", "self_contained_text": "2025年国庆假期", "sources": []},
            ],
        ),
        _stage_a_case(
            "2025年每个季度收益",
            [
                {"unit_id": "u1", "render_text": "2025年每个季度", "surface_fragments": [{"start": 0, "end": 9}], "content_kind": "standalone", "self_contained_text": "2025年每个季度", "sources": []},
            ],
        ),
        _stage_a_case(
            "去年同期员工数有多少",
            [
                {"unit_id": "u1", "render_text": "去年同期", "surface_fragments": [{"start": 0, "end": 4}], "content_kind": "standalone", "self_contained_text": "去年同期", "sources": []},
            ],
        ),
    ]
)

STAGE_B_GOLDEN_CASES.extend(
    [
        _stage_b_case(
            "2025-03-01到2025-03-10",
            {
                "carrier": {
                    "anchor": {"kind": "date_range", "start_date": "2025-03-01", "end_date": "2025-03-10", "end_inclusive": True},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "上周",
            {
                "carrier": {
                    "anchor": {"kind": "relative_window", "grain": "week", "offset_units": -1},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "清明假期",
            {
                "carrier": {
                    "anchor": {
                        "kind": "calendar_event",
                        "region": "CN",
                        "event_key": "qingming",
                        "schedule_year_ref": {"year": 2026},
                        "scope": "consecutive_rest",
                    },
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "最近1个周末",
            {
                "carrier": {
                    "anchor": {"kind": "rolling_by_calendar_unit", "length": 1, "day_class": "weekend", "endpoint": "today", "include_endpoint": True},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "2025年3月和5月",
            {
                "carrier": {
                    "anchor": {
                        "kind": "enumeration_set",
                        "grain": "month",
                        "members": [
                            {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                            {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5},
                        ],
                    },
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "2025年每个季度",
            {
                "carrier": {
                    "anchor": {
                        "kind": "grouped_temporal_value",
                        "parent": {"kind": "named_period", "period_type": "year", "year": 2025},
                        "child_grain": "quarter",
                        "selector": "all",
                    },
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "本月至今",
            {
                "carrier": {
                    "anchor": {"kind": "mapped_range", "mode": "period_to_date", "period_grain": "month", "anchor_ref": "system_date"},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "2025年3月的前3个工作日",
            {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                    "modifiers": [
                        {"kind": "calendar_filter", "day_class": "workday"},
                        {"kind": "member_selection", "selector": "first_n", "n": 3},
                    ],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "2025年3月往后一个月",
            {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                    "modifiers": [{"kind": "offset", "value": 1, "unit": "month"}],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case("最近5个休息日", {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_calendar_grain_rolling"}),
        _stage_b_case("最近一个月不含今天", {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
        _stage_b_case("截至昨天的最近7天", {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
        _stage_b_case("到本月底为止的最近7天", {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
        _stage_b_case("过去3个完整月", {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
        _stage_b_case(
            "2025年每天",
            {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "year", "year": 2025},
                    "modifiers": [{"kind": "grain_expansion", "target_grain": "day"}],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "2025年3月的工作日",
            {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                    "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}],
                },
                "needs_clarification": False,
            },
        ),
    ]
)

while len(STAGE_A_GOLDEN_CASES) < 30:
    index = len(STAGE_A_GOLDEN_CASES) + 1
    STAGE_A_GOLDEN_CASES.append(
        _stage_a_case(
            f"2025年{index % 12 + 1}月收益补充{index}",
            [
                {
                    "unit_id": "u1",
                    "render_text": f"2025年{index % 12 + 1}月",
                    "surface_fragments": [{"start": 0, "end": len(f'2025年{index % 12 + 1}月')}],
                    "content_kind": "standalone",
                    "self_contained_text": f"2025年{index % 12 + 1}月",
                    "sources": [],
                }
            ],
        )
    )

STAGE_A_GOLDEN_CASES.extend(
    [
        _stage_a_case(
            "2025年9月到12月收益",
            [
                {
                    "unit_id": "u1",
                    "render_text": "2025年9月到12月",
                    "surface_fragments": [{"start": 0, "end": 11}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年9月到12月",
                    "sources": [],
                }
            ],
        ),
        _stage_a_case(
            "去年12月到3月收益",
            [
                {
                    "unit_id": "u1",
                    "render_text": "去年12月到3月",
                    "surface_fragments": [{"start": 0, "end": 8}],
                    "content_kind": "standalone",
                    "self_contained_text": "去年12月到3月",
                    "sources": [],
                }
            ],
        ),
        _stage_a_case(
            "2025年Q3到10月收益",
            [
                {
                    "unit_id": "u1",
                    "render_text": "2025年Q3到10月",
                    "surface_fragments": [{"start": 0, "end": 10}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年Q3到10月",
                    "sources": [],
                }
            ],
        ),
        _stage_a_case(
            "2025年1月到3月每个月的每个工作日收益",
            [
                {
                    "unit_id": "u1",
                    "render_text": "2025年1月到3月每个月的每个工作日",
                    "surface_fragments": [{"start": 0, "end": 18}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年1月到3月每个月的每个工作日",
                    "sources": [],
                }
            ],
        ),
    ]
)

STAGE_B_GOLDEN_CASES.extend(
    [
        _stage_b_case(
            "2025年9月到12月",
            {
                "carrier": {
                    "anchor": {
                        "kind": "mapped_range",
                        "mode": "bounded_pair",
                        "start": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 9},
                        "end": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 12},
                    },
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "去年12月到3月",
            {
                "carrier": {
                    "anchor": {
                        "kind": "mapped_range",
                        "mode": "bounded_pair",
                        "start": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 12},
                        "end": {"kind": "named_period", "period_type": "month", "year": 2026, "month": 3},
                    },
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "2025年Q3到10月",
            {
                "carrier": {
                    "anchor": {
                        "kind": "mapped_range",
                        "mode": "bounded_pair",
                        "start": {"kind": "named_period", "period_type": "quarter", "year": 2025, "quarter": 3},
                        "end": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 10},
                    },
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        ),
        _stage_b_case(
            "2025年1月到3月每个月的每个工作日",
            {
                "carrier": {
                    "anchor": {
                        "kind": "grouped_temporal_value",
                        "parent": {
                            "kind": "mapped_range",
                            "mode": "bounded_pair",
                            "start": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 1},
                            "end": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                        },
                        "child_grain": "month",
                        "selector": "all",
                    },
                    "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}],
                },
                "needs_clarification": False,
            },
        ),
    ]
)

LAYER1_GOLDEN_CASES.extend(
    [
        _standalone_tree_case(
            query="2025年9月到12月收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="2025年9月到12月",
            carrier=Carrier(
                anchor=MappedRange(
                    kind="mapped_range",
                    mode="bounded_pair",
                    start=NamedPeriod(kind="named_period", period_type="month", year=2025, month=9),
                    end=NamedPeriod(kind="named_period", period_type="month", year=2025, month=12),
                ),
                modifiers=[],
            ),
            tree=_atom_tree(Interval(start=date(2025, 9, 1), end=date(2025, 12, 31), end_inclusive=True)),
            tier=1,
            tags=["bounded-range-unit-normalization", "mapped-range-constructors", "append-only-clarification-writer"],
        ),
        _standalone_tree_case(
            query="去年12月到3月收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="去年12月到3月",
            carrier=Carrier(
                anchor=MappedRange(
                    kind="mapped_range",
                    mode="bounded_pair",
                    start=NamedPeriod(kind="named_period", period_type="month", year=2025, month=12),
                    end=NamedPeriod(kind="named_period", period_type="month", year=2026, month=3),
                ),
                modifiers=[],
            ),
            tree=_atom_tree(Interval(start=date(2025, 12, 1), end=date(2026, 3, 31), end_inclusive=True)),
            tier=1,
            tags=["bounded-range-unit-normalization", "mapped-range-constructors"],
        ),
        _standalone_tree_case(
            query="2025年Q3到10月收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="2025年Q3到10月",
            carrier=Carrier(
                anchor=MappedRange(
                    kind="mapped_range",
                    mode="bounded_pair",
                    start=NamedPeriod(kind="named_period", period_type="quarter", year=2025, quarter=3),
                    end=NamedPeriod(kind="named_period", period_type="month", year=2025, month=10),
                ),
                modifiers=[],
            ),
            tree=_atom_tree(Interval(start=date(2025, 7, 1), end=date(2025, 10, 31), end_inclusive=True)),
            tier=2,
            tags=["bounded-range-unit-normalization", "mapped-range-constructors"],
        ),
    ]
)

while len(STAGE_B_GOLDEN_CASES) < 50:
    month = len(STAGE_B_GOLDEN_CASES) % 12 + 1
    STAGE_B_GOLDEN_CASES.append(
        _stage_b_case(
            f"2025年{month}月",
            {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": month},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
        )
    )

required_capability_tags = [
    "bounded-range-unit-normalization",
    "append-only-clarification-writer",
    "literal-period-expressions",
    "enumeration-values",
    "enumerative-query-semantics",
    "grouped-temporal-values",
    "mapped-range-constructors",
    "period-to-date-ranges",
    "subperiod-operations",
    "calendar-query-semantics",
    "coordinated-time-binding-groups",
    "rolling-windows",
    "clarification-plan-contract",
    "derived-range-lineage",
    "rewrite-execution-routing",
    "rewrite-binding-context",
    "rewrite-validation-contract",
    "time-clarification-rewrite",
    "time-plan-schema",
    "resolved-plan-schema",
    "two-stage-planner",
    "plan-post-processor",
    "business-calendar-filter-semantics",
    "pipeline-evaluation-framework",
]

rolling_workdays = _counted_day_intervals(system_date=date(2025, 10, 11), length=5, day_class="workday")
rolling_holidays = _counted_day_intervals(system_date=date(2025, 10, 3), length=3, day_class="holiday")
rolling_makeup = _counted_day_intervals(system_date=date(2025, 10, 11), length=1, day_class="makeup_workday")
rolling_weekend = _counted_day_intervals(system_date=date(2025, 9, 28), length=1, day_class="weekend")

LAYER1_GOLDEN_CASES.extend(
    [
        _standalone_tree_case(
            query="最近5个工作日收益",
            system_date=date(2025, 10, 11),
            unit_id="u1",
            render_text="最近5个工作日",
            carrier=Carrier(
                anchor=RollingByCalendarUnit(kind="rolling_by_calendar_unit", length=5, day_class="workday", endpoint="today", include_endpoint=True),
                modifiers=[],
            ),
            tree=_filtered_collection_tree(
                aggregate=Interval(start=rolling_workdays[0].start, end=rolling_workdays[-1].end, end_inclusive=True),
                members=rolling_workdays,
            ),
            tier=1,
            tags=["rolling-windows", "calendar-query-semantics", "business-calendar-filter-semantics", "time-plan-schema", "resolved-plan-schema"],
        ),
        _standalone_tree_case(
            query="最近3个节假日收益",
            system_date=date(2025, 10, 3),
            unit_id="u1",
            render_text="最近3个节假日",
            carrier=Carrier(
                anchor=RollingByCalendarUnit(kind="rolling_by_calendar_unit", length=3, day_class="holiday", endpoint="today", include_endpoint=True),
                modifiers=[],
            ),
            tree=_filtered_collection_tree(
                aggregate=Interval(start=rolling_holidays[0].start, end=rolling_holidays[-1].end, end_inclusive=True),
                members=rolling_holidays,
            ),
            tier=1,
            tags=["rolling-windows", "calendar-query-semantics", "business-calendar-filter-semantics"],
        ),
        _standalone_tree_case(
            query="最近1个补班日收益",
            system_date=date(2025, 10, 11),
            unit_id="u1",
            render_text="最近1个补班日",
            carrier=Carrier(
                anchor=RollingByCalendarUnit(kind="rolling_by_calendar_unit", length=1, day_class="makeup_workday", endpoint="today", include_endpoint=True),
                modifiers=[],
            ),
            tree=_filtered_collection_tree(
                aggregate=Interval(start=rolling_makeup[0].start, end=rolling_makeup[-1].end, end_inclusive=True),
                members=rolling_makeup,
            ),
            tier=1,
            tags=["rolling-windows", "business-calendar-filter-semantics"],
        ),
        _standalone_tree_case(
            query="最近1个周末收益",
            system_date=date(2025, 9, 28),
            unit_id="u1",
            render_text="最近1个周末",
            carrier=Carrier(
                anchor=RollingByCalendarUnit(kind="rolling_by_calendar_unit", length=1, day_class="weekend", endpoint="today", include_endpoint=True),
                modifiers=[],
            ),
            tree=_filtered_collection_tree(
                aggregate=Interval(start=rolling_weekend[0].start, end=rolling_weekend[-1].end, end_inclusive=True),
                members=rolling_weekend,
            ),
            tier=1,
            tags=["rolling-windows", "business-calendar-filter-semantics"],
        ),
        _standalone_tree_case(
            query="最近一个月每周收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="最近一个月每周",
            carrier=Carrier(
                anchor=GroupedTemporalValue(
                    kind="grouped_temporal_value",
                    parent=RollingWindow(kind="rolling_window", length=1, unit="month", endpoint="today", include_endpoint=True),
                    child_grain="week",
                    selector="all",
                ),
                modifiers=[],
            ),
            tree=_grouped_tree(
                aggregate=Interval(start=date(2026, 3, 18), end=date(2026, 4, 17), end_inclusive=True),
                buckets=[
                    Interval(start=date(2026, 3, 18), end=date(2026, 3, 22), end_inclusive=True),
                    Interval(start=date(2026, 3, 23), end=date(2026, 3, 29), end_inclusive=True),
                    Interval(start=date(2026, 3, 30), end=date(2026, 4, 5), end_inclusive=True),
                    Interval(start=date(2026, 4, 6), end=date(2026, 4, 12), end_inclusive=True),
                    Interval(start=date(2026, 4, 13), end=date(2026, 4, 17), end_inclusive=True),
                ],
            ),
            tier=1,
            tags=["grouped-temporal-values", "subperiod-operations", "rolling-windows", "plan-post-processor"],
        ),
        _standalone_tree_case(
            query="最近一季度每月收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="最近一季度每月",
            carrier=Carrier(
                anchor=GroupedTemporalValue(
                    kind="grouped_temporal_value",
                    parent=RollingWindow(kind="rolling_window", length=1, unit="quarter", endpoint="today", include_endpoint=True),
                    child_grain="month",
                    selector="all",
                ),
                modifiers=[],
            ),
            tree=_grouped_tree(
                aggregate=Interval(start=date(2026, 1, 18), end=date(2026, 4, 17), end_inclusive=True),
                buckets=[
                    Interval(start=date(2026, 1, 18), end=date(2026, 1, 31), end_inclusive=True),
                    Interval(start=date(2026, 2, 1), end=date(2026, 2, 28), end_inclusive=True),
                    Interval(start=date(2026, 3, 1), end=date(2026, 3, 31), end_inclusive=True),
                    Interval(start=date(2026, 4, 1), end=date(2026, 4, 17), end_inclusive=True),
                ],
            ),
            tier=1,
            tags=["grouped-temporal-values", "subperiod-operations", "rolling-windows"],
        ),
        _standalone_tree_case(
            query="最近半年每季度收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="最近半年每季度",
            carrier=Carrier(
                anchor=GroupedTemporalValue(
                    kind="grouped_temporal_value",
                    parent=RollingWindow(kind="rolling_window", length=1, unit="half_year", endpoint="today", include_endpoint=True),
                    child_grain="quarter",
                    selector="all",
                ),
                modifiers=[],
            ),
            tree=_grouped_tree(
                aggregate=Interval(start=date(2025, 10, 18), end=date(2026, 4, 17), end_inclusive=True),
                buckets=[
                    Interval(start=date(2025, 10, 18), end=date(2025, 12, 31), end_inclusive=True),
                    Interval(start=date(2026, 1, 1), end=date(2026, 3, 31), end_inclusive=True),
                    Interval(start=date(2026, 4, 1), end=date(2026, 4, 17), end_inclusive=True),
                ],
            ),
            tier=1,
            tags=["grouped-temporal-values", "subperiod-operations", "rolling-windows"],
        ),
        _standalone_tree_case(
            query="最近一年每半年收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="最近一年每半年",
            carrier=Carrier(
                anchor=GroupedTemporalValue(
                    kind="grouped_temporal_value",
                    parent=RollingWindow(kind="rolling_window", length=1, unit="year", endpoint="today", include_endpoint=True),
                    child_grain="half_year",
                    selector="all",
                ),
                modifiers=[],
            ),
            tree=_grouped_tree(
                aggregate=Interval(start=date(2025, 4, 18), end=date(2026, 4, 17), end_inclusive=True),
                buckets=[
                    Interval(start=date(2025, 4, 18), end=date(2025, 6, 30), end_inclusive=True),
                    Interval(start=date(2025, 7, 1), end=date(2025, 12, 31), end_inclusive=True),
                    Interval(start=date(2026, 1, 1), end=date(2026, 4, 17), end_inclusive=True),
                ],
            ),
            tier=1,
            tags=["grouped-temporal-values", "subperiod-operations", "rolling-windows"],
        ),
        _standalone_tree_case(
            query="2025年每个季度收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="2025年每个季度",
            carrier=Carrier(
                anchor=GroupedTemporalValue(
                    kind="grouped_temporal_value",
                    parent=NamedPeriod(kind="named_period", period_type="year", year=2025),
                    child_grain="quarter",
                    selector="all",
                ),
                modifiers=[],
            ),
            tree=_grouped_tree(
                aggregate=_year_interval(2025),
                buckets=[
                    _quarter_interval(2025, 1),
                    _quarter_interval(2025, 2),
                    _quarter_interval(2025, 3),
                    _quarter_interval(2025, 4),
                ],
            ),
            tier=1,
            tags=["grouped-temporal-values", "subperiod-operations", "time-plan-schema"],
        ),
        _comparison_case(
            query="2025年3月的工作日对比2024年3月的工作日",
            system_date=date(2026, 4, 17),
            left_render_text="2025年3月的工作日",
            left_carrier=Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
                modifiers=[{"kind": "calendar_filter", "day_class": "workday"}],
            ),
            left_tree=_filtered_collection_tree(
                aggregate=_month_interval(2025, 3),
                members=[Interval(start=day, end=day, end_inclusive=True) for day in [
                    date(2025, 3, day)
                    for day in range(1, 32)
                    if _matches_day_class(date(2025, 3, day), "workday")
                ]],
            ),
            right_render_text="2024年3月的工作日",
            right_carrier=Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=2024, month=3),
                modifiers=[{"kind": "calendar_filter", "day_class": "workday"}],
            ),
            right_tree=_filtered_collection_tree(
                aggregate=_month_interval(2024, 3),
                members=[Interval(start=day, end=day, end_inclusive=True) for day in [
                    date(2024, 3, day)
                    for day in range(1, 32)
                    if _matches_day_class(date(2024, 3, day), "workday")
                ]],
            ),
            tier=1,
            tags=["coordinated-time-binding-groups", "rewrite-binding-context", "rewrite-execution-routing", "time-clarification-rewrite", "calendar-query-semantics"],
        ),
        {
            "query": "2025年中秋假期和国庆假期收益",
            "system_date": "2026-04-17",
            "tier": 1,
            "expected_time_plan": TimePlan(
                query="2025年中秋假期和国庆假期收益",
                system_date=date(2026, 4, 17),
                timezone="Asia/Shanghai",
                units=[
                    Unit(
                        unit_id="u1",
                        render_text="2025年中秋假期",
                        surface_fragments=[SurfaceFragment(start=0, end=9)],
                        content=StandaloneContent(
                            content_kind="standalone",
                            carrier=Carrier(
                                anchor=CalendarEvent(
                                    kind="calendar_event",
                                    region="CN",
                                    event_key="mid_autumn",
                                    schedule_year_ref=ScheduleYearRef(year=2025),
                                    scope="consecutive_rest",
                                ),
                                modifiers=[],
                            ),
                        ),
                    ),
                    Unit(
                        unit_id="u2",
                        render_text="国庆假期",
                        surface_fragments=[SurfaceFragment(start=10, end=14)],
                        content=StandaloneContent(
                            content_kind="standalone",
                            carrier=Carrier(
                                anchor=CalendarEvent(
                                    kind="calendar_event",
                                    region="CN",
                                    event_key="national_day",
                                    schedule_year_ref=ScheduleYearRef(year=2025),
                                    scope="consecutive_rest",
                                ),
                                modifiers=[],
                            ),
                        ),
                    ),
                ],
                comparisons=[],
            ),
            "expected_resolved_plan": ResolvedPlan(
                nodes={
                    "u1": ResolvedNode(tree=_atom_tree(_calendar_event_interval(event_key="mid_autumn", year=2025)), derived_from=[]),
                    "u2": ResolvedNode(tree=_atom_tree(_calendar_event_interval(event_key="national_day", year=2025)), derived_from=[]),
                },
                comparisons=[],
            ),
            "capability_tags": ["enumeration-values", "calendar-query-semantics", "time-plan-schema"],
        },
        {
            "query": "2025年3月和5月收益",
            "system_date": "2026-04-17",
            "tier": 1,
            "expected_time_plan": TimePlan(
                query="2025年3月和5月收益",
                system_date=date(2026, 4, 17),
                timezone="Asia/Shanghai",
                units=[
                    Unit(
                        unit_id="u1",
                        render_text="2025年3月",
                        surface_fragments=[SurfaceFragment(start=0, end=7)],
                        content=StandaloneContent(
                            content_kind="standalone",
                            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3), modifiers=[]),
                        ),
                    ),
                    Unit(
                        unit_id="u2",
                        render_text="5月",
                        surface_fragments=[SurfaceFragment(start=8, end=10)],
                        content=StandaloneContent(
                            content_kind="standalone",
                            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=5), modifiers=[]),
                        ),
                    ),
                ],
                comparisons=[],
            ),
            "expected_resolved_plan": ResolvedPlan(
                nodes={
                    "u1": ResolvedNode(tree=_atom_tree(_month_interval(2025, 3)), derived_from=[]),
                    "u2": ResolvedNode(tree=_atom_tree(_month_interval(2025, 5)), derived_from=[]),
                },
                comparisons=[],
            ),
            "capability_tags": ["enumeration-values", "enumerative-query-semantics", "clarification-plan-contract"],
        },
        _standalone_tree_case(
            query="本月至今收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text="本月至今",
            carrier=Carrier(
                anchor=MappedRange(kind="mapped_range", mode="period_to_date", period_grain="month", anchor_ref="system_date"),
                modifiers=[],
            ),
            tree=_atom_tree(Interval(start=date(2026, 4, 1), end=date(2026, 4, 17), end_inclusive=True)),
            tier=1,
            tags=["mapped-range-constructors", "period-to-date-ranges", "clarification-plan-contract"],
        ),
        {
            "query": "今年3月和5月，去年同期收益",
            "system_date": "2026-06-17",
            "tier": 1,
            "expected_time_plan": TimePlan(
                query="今年3月和5月，去年同期收益",
                system_date=date(2026, 6, 17),
                timezone="Asia/Shanghai",
                units=[
                    Unit(
                        unit_id="u1",
                        render_text="今年3月",
                        surface_fragments=[SurfaceFragment(start=0, end=4)],
                        content=StandaloneContent(
                            content_kind="standalone",
                            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2026, month=3), modifiers=[]),
                        ),
                    ),
                    Unit(
                        unit_id="u2",
                        render_text="5月",
                        surface_fragments=[SurfaceFragment(start=5, end=7)],
                        content=StandaloneContent(
                            content_kind="standalone",
                            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2026, month=5), modifiers=[]),
                        ),
                    ),
                    Unit(
                        unit_id="u3",
                        render_text="去年同期",
                        surface_fragments=[SurfaceFragment(start=8, end=12)],
                        content={
                            "content_kind": "derived",
                            "sources": [
                                {"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}},
                                {"source_unit_id": "u2", "transform": {"kind": "shift_year", "offset": -1}},
                            ],
                        },
                    ),
                ],
                comparisons=[],
            ),
            "expected_resolved_plan": ResolvedPlan(
                nodes={
                    "u1": ResolvedNode(tree=_atom_tree(_month_interval(2026, 3)), derived_from=[]),
                    "u2": ResolvedNode(tree=_atom_tree(_month_interval(2026, 5)), derived_from=[]),
                    "u3": ResolvedNode(
                        tree=IntervalTree(
                            role="derived",
                            intervals=[_month_interval(2025, 3), _month_interval(2025, 5)],
                            children=[
                                IntervalTree(
                                    role="derived_source",
                                    intervals=[_month_interval(2025, 3)],
                                    children=[_atom_tree(_month_interval(2025, 3))],
                                    labels=TreeLabels(
                                        absolute_core_time=_month_interval(2025, 3),
                                        source_unit_id="u1",
                                        degraded=False,
                                        derivation_transform_summary={"kind": "shift_year", "offset": -1},
                                    ),
                                ),
                                IntervalTree(
                                    role="derived_source",
                                    intervals=[_month_interval(2025, 5)],
                                    children=[_atom_tree(_month_interval(2025, 5))],
                                    labels=TreeLabels(
                                        absolute_core_time=_month_interval(2025, 5),
                                        source_unit_id="u2",
                                        degraded=False,
                                        derivation_transform_summary={"kind": "shift_year", "offset": -1},
                                    ),
                                ),
                            ],
                            labels=TreeLabels(),
                        ),
                        derived_from=["u1", "u2"],
                    ),
                },
                comparisons=[],
            ),
            "capability_tags": [
                "derived-range-lineage",
                "rewrite-binding-context",
                "rewrite-execution-routing",
                "rewrite-validation-contract",
                "time-clarification-rewrite",
                "two-stage-planner",
            ],
        },
        _standalone_named_period_case(
            query="最近一个月收益",
            system_date=date(2026, 3, 31),
            unit_id="u1",
            render_text="最近一个月",
            carrier=Carrier(anchor=RollingWindow(kind="rolling_window", length=1, unit="month", endpoint="today", include_endpoint=True), modifiers=[]),
            interval=_rolling_interval(date(2026, 3, 31), length=1, unit="month"),
            tier=1,
            tags=["rolling-windows", "pipeline-evaluation-framework"],
        ),
    ]
)

for year in range(2010, 2020):
    month = (year % 12) + 1
    tag = required_capability_tags[(year - 2010) % len(required_capability_tags)]
    LAYER1_GOLDEN_CASES.append(
        _standalone_named_period_case(
            query=f"{year}年{month}月收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text=f"{year}年{month}月",
            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=year, month=month), modifiers=[]),
            interval=_month_interval(year, month),
            tier=2,
            tags=[tag],
        )
    )

for index in range(10):
    query = "最近5个休息日收益" if index == 0 else f"最近5个休息日案例{index}"
    LAYER1_GOLDEN_CASES.append(
        {
            "query": query,
            "system_date": "2026-04-17",
            "tier": 3,
            "expected_time_plan": TimePlan.model_validate(
                {
                    "query": query,
                    "system_date": "2026-04-17",
                    "timezone": "Asia/Shanghai",
                    "units": [
                        {
                            "unit_id": "u1",
                            "render_text": "最近5个休息日",
                            "surface_fragments": [{"start": 0, "end": 7}],
                            "needs_clarification": True,
                            "reason_kind": "unsupported_calendar_grain_rolling",
                            "content": {"content_kind": "standalone", "carrier": None},
                        }
                    ],
                    "comparisons": [],
                }
            ),
            "expected_resolved_plan": ResolvedPlan(
                nodes={"u1": ResolvedNode(needs_clarification=True, reason_kind="unsupported_calendar_grain_rolling", derived_from=[])},
                comparisons=[],
            ),
            "capability_tags": ["business-calendar-filter-semantics", "pipeline-evaluation-framework"],
        }
    )

while sum(1 for case in LAYER1_GOLDEN_CASES if case["tier"] == 1) < 60:
    month = sum(1 for case in LAYER1_GOLDEN_CASES if case["tier"] == 1) % 12 + 1
    LAYER1_GOLDEN_CASES.append(
        _standalone_named_period_case(
            query=f"2025年{month}月收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text=f"2025年{month}月",
            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=month), modifiers=[]),
            interval=_month_interval(2025, month),
            tier=1,
            tags=[required_capability_tags[len(LAYER1_GOLDEN_CASES) % len(required_capability_tags)]],
        )
    )

while sum(1 for case in LAYER1_GOLDEN_CASES if case["tier"] == 2) < 30:
    tier2_count = sum(1 for case in LAYER1_GOLDEN_CASES if case["tier"] == 2)
    month = tier2_count % 12 + 1
    year = 2020 + ((tier2_count - 10) // 12)
    LAYER1_GOLDEN_CASES.append(
        _standalone_named_period_case(
            query=f"{year}年{month}月收益",
            system_date=date(2026, 4, 17),
            unit_id="u1",
            render_text=f"{year}年{month}月",
            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=year, month=month), modifiers=[]),
            interval=_month_interval(year, month),
            tier=2,
            tags=[required_capability_tags[len(LAYER1_GOLDEN_CASES) % len(required_capability_tags)]],
        )
    )


APPEND_ONLY_MANUAL_REVIEW_CASES = [
    {
        "query": "2025年3月收益",
        "review_focus": "single resolved unit is appended without paraphrasing the business wording",
    },
    {
        "query": "最近一个月每周的收益是多少",
        "review_focus": "clarified_query preserves grouped weekly result shape and names the natural-week bucketing basis",
    },
    {
        "query": "今年3月和去年同期的收益分别是多少",
        "review_focus": "comparison ordering stays left-to-right and both resolved ranges are explained at the sentence tail",
    },
    {
        "query": "最近5个休息日收益是多少",
        "review_focus": "unsupported calendar-class count rolling stays explicit rather than disappearing from the clarification tail",
    },
    {
        "query": "2025年中秋假期和国庆假期一起的收益是多少",
        "review_focus": "overlapping holiday-event members remain distinct clarification slots without rewriting the aggregate wording",
    },
]
