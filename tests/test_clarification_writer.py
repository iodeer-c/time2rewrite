from __future__ import annotations

from datetime import date
from dataclasses import dataclass

from time_query_service.clarification_writer import build_clarification_facts, render_clarified_query
from time_query_service.resolved_plan import Interval, IntervalTree, ResolvedNode, ResolvedPlan, TreeLabels
from time_query_service.time_plan import (
    CalendarFilter,
    Carrier,
    Comparison,
    ComparisonPair,
    GroupedTemporalValue,
    MappedRange,
    NamedPeriod,
    StandaloneContent,
    TimePlan,
    Unit,
)


@dataclass
class _Response:
    content: str


class _StaticRunner:
    def __init__(self, content: str) -> None:
        self.content = content
        self.calls = 0

    def invoke(self, messages):
        self.calls += 1
        return _Response(self.content)


def _month_unit(unit_id: str, render_text: str, *, year: int, month: int) -> Unit:
    return Unit(
        unit_id=unit_id,
        render_text=render_text,
        surface_fragments=[],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=year, month=month),
                modifiers=[],
            ),
        ),
    )


def _bounded_month_range_unit(unit_id: str, render_text: str, *, start_month: int, end_month: int) -> Unit:
    return Unit(
        unit_id=unit_id,
        render_text=render_text,
        surface_fragments=[],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=MappedRange(
                    kind="mapped_range",
                    mode="bounded_pair",
                    start=NamedPeriod(kind="named_period", period_type="month", year=2025, month=start_month),
                    end=NamedPeriod(kind="named_period", period_type="month", year=2025, month=end_month),
                ),
                modifiers=[],
            ),
        ),
    )


def test_build_clarification_facts_preserves_stage_a_order_for_comparison_units() -> None:
    march_2026 = Interval(start=date(2026, 3, 1), end=date(2026, 3, 31), end_inclusive=True)
    march_2025 = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="今年3月和去年同期的收益分别是多少",
        system_date=date(2026, 4, 19),
        timezone="Asia/Shanghai",
        units=[
            _month_unit("u1", "今年3月", year=2026, month=3),
            _month_unit("u2", "去年同期", year=2025, month=3),
        ],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="和",
                pairs=[ComparisonPair(subject_unit_id="u1", reference_unit_id="u2")],
            )
        ],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2026], children=[], labels=TreeLabels(absolute_core_time=march_2026)),
                derived_from=[],
            ),
            "u2": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2025], children=[], labels=TreeLabels(absolute_core_time=march_2025)),
                derived_from=[],
            ),
        },
        comparisons=[],
    )

    facts = build_clarification_facts(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert [fact.unit_id for fact in facts] == ["u1", "u2"]
    assert facts[0].label == "今年3月"
    assert facts[1].label == "去年同期"
    assert facts[0].resolved_text == "2026年3月1日至2026年3月31日"
    assert facts[1].resolved_text == "2025年3月1日至2025年3月31日"


def test_render_clarified_query_appends_resolved_and_unresolved_facts() -> None:
    march_2025 = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="2025年3月和最近5个休息日收益是多少",
        system_date=date(2026, 4, 19),
        timezone="Asia/Shanghai",
        units=[
            _month_unit("u1", "2025年3月", year=2025, month=3),
            _month_unit("u2", "最近5个休息日", year=2025, month=3),
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2025], children=[], labels=TreeLabels(absolute_core_time=march_2025)),
                derived_from=[],
            ),
            "u2": ResolvedNode(needs_clarification=True, reason_kind="unsupported_calendar_grain_rolling", derived_from=[]),
        },
        comparisons=[],
    )

    clarified_query = render_clarified_query(
        original_query=time_plan.query,
        clarification_facts=build_clarification_facts(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan),
    )

    assert clarified_query == (
        "2025年3月和最近5个休息日收益是多少"
        "（2025年3月指2025年3月1日至2025年3月31日；最近5个休息日当前无法确定）"
    )


def test_build_clarification_facts_keeps_repeated_render_text_in_unit_order() -> None:
    march_2025 = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    march_2024 = Interval(start=date(2024, 3, 1), end=date(2024, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="2025年3月和2025年3月对比去年同期",
        system_date=date(2026, 4, 19),
        timezone="Asia/Shanghai",
        units=[
            _month_unit("u1", "2025年3月", year=2025, month=3),
            _month_unit("u2", "2025年3月", year=2025, month=3),
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2025], children=[], labels=TreeLabels(absolute_core_time=march_2025)),
                derived_from=[],
            ),
            "u2": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2024], children=[], labels=TreeLabels(absolute_core_time=march_2024)),
                derived_from=[],
            ),
        },
        comparisons=[],
    )

    facts = build_clarification_facts(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert [fact.unit_id for fact in facts] == ["u1", "u2"]
    assert [fact.label for fact in facts] == ["2025年3月", "2025年3月"]
    assert [fact.resolved_text for fact in facts] == ["2025年3月1日至2025年3月31日", "2024年3月1日至2024年3月31日"]


def test_render_clarified_query_explains_grouping_basis() -> None:
    trailing = Interval(start=date(2026, 3, 18), end=date(2026, 4, 17), end_inclusive=True)
    time_plan = TimePlan(
        query="最近一个月每周的收益是多少",
        system_date=date(2026, 4, 19),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u1",
                render_text="最近一个月每周",
                surface_fragments=[],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor=GroupedTemporalValue(
                            kind="grouped_temporal_value",
                            parent=NamedPeriod(kind="named_period", period_type="month", year=2026, month=4),
                            child_grain="week",
                            selector="all",
                        ),
                        modifiers=[],
                    ),
                ),
            )
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="grouped_member", intervals=[trailing], children=[], labels=TreeLabels(absolute_core_time=trailing)),
                derived_from=[],
            )
        },
        comparisons=[],
    )

    clarified_query = render_clarified_query(
        original_query=time_plan.query,
        clarification_facts=build_clarification_facts(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan),
    )

    assert clarified_query == "最近一个月每周的收益是多少（最近一个月每周指2026年3月18日至2026年4月17日，按自然周分组）"


def test_build_clarification_facts_returns_one_fact_for_canonical_bounded_range_unit() -> None:
    bounded = Interval(start=date(2025, 9, 1), end=date(2025, 12, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="2025年9月到12月的收益是多少",
        system_date=date(2026, 4, 19),
        timezone="Asia/Shanghai",
        units=[_bounded_month_range_unit("u1", "2025年9月到12月", start_month=9, end_month=12)],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[bounded], children=[], labels=TreeLabels(absolute_core_time=bounded)),
                derived_from=[],
            )
        },
        comparisons=[],
    )

    facts = build_clarification_facts(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert len(facts) == 1
    assert facts[0].unit_id == "u1"
    assert facts[0].label == "2025年9月到12月"
    assert facts[0].resolved_text == "2025年9月1日至2025年12月31日"


def test_build_clarification_facts_keeps_grouped_bounded_range_parent_ownership() -> None:
    aggregate = Interval(start=date(2025, 1, 1), end=date(2025, 3, 31), end_inclusive=True)
    january = Interval(start=date(2025, 1, 1), end=date(2025, 1, 31), end_inclusive=True)
    february = Interval(start=date(2025, 2, 1), end=date(2025, 2, 28), end_inclusive=True)
    march = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="2025年1月到3月每个月的每个工作日的收益是多少",
        system_date=date(2026, 4, 19),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u1",
                render_text="2025年1月到3月每个月的每个工作日",
                surface_fragments=[],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor=GroupedTemporalValue(
                            kind="grouped_temporal_value",
                            parent=MappedRange(
                                kind="mapped_range",
                                mode="bounded_pair",
                                start=NamedPeriod(kind="named_period", period_type="month", year=2025, month=1),
                                end=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
                            ),
                            child_grain="month",
                            selector="all",
                        ),
                        modifiers=[CalendarFilter(kind="calendar_filter", day_class="workday")],
                    ),
                ),
            )
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(
                    role="grouped_member",
                    intervals=[january, february, march],
                    children=[
                        IntervalTree(role="atom", intervals=[january], children=[], labels=TreeLabels(absolute_core_time=january)),
                        IntervalTree(role="atom", intervals=[february], children=[], labels=TreeLabels(absolute_core_time=february)),
                        IntervalTree(role="atom", intervals=[march], children=[], labels=TreeLabels(absolute_core_time=march)),
                    ],
                    labels=TreeLabels(absolute_core_time=aggregate),
                ),
                derived_from=[],
            )
        },
        comparisons=[],
    )

    facts = build_clarification_facts(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert len(facts) == 1
    assert facts[0].unit_id == "u1"
    assert facts[0].label == "2025年1月到3月每个月的每个工作日"
    assert facts[0].resolved_text == "2025年1月1日至2025年3月31日"
    assert facts[0].grouping_grain == "month"


def test_render_clarified_query_explains_canonical_bounded_range_as_one_interval() -> None:
    clarified_query = render_clarified_query(
        original_query="2025年9月到12月的收益是多少",
        clarification_facts=[
            build_clarification_facts(
                original_query="2025年9月到12月的收益是多少",
                time_plan=TimePlan(
                    query="2025年9月到12月的收益是多少",
                    system_date=date(2026, 4, 19),
                    timezone="Asia/Shanghai",
                    units=[_bounded_month_range_unit("u1", "2025年9月到12月", start_month=9, end_month=12)],
                    comparisons=[],
                ),
                resolved_plan=ResolvedPlan(
                    nodes={
                        "u1": ResolvedNode(
                            tree=IntervalTree(
                                role="atom",
                                intervals=[Interval(start=date(2025, 9, 1), end=date(2025, 12, 31), end_inclusive=True)],
                                children=[],
                                labels=TreeLabels(
                                    absolute_core_time=Interval(
                                        start=date(2025, 9, 1),
                                        end=date(2025, 12, 31),
                                        end_inclusive=True,
                                    )
                                ),
                            ),
                            derived_from=[],
                        )
                    },
                    comparisons=[],
                ),
            )[0]
        ],
    )

    assert clarified_query == "2025年9月到12月的收益是多少（2025年9月到12月指2025年9月1日至2025年12月31日）"


def test_render_clarified_query_keeps_split_endpoint_coalescing_only_as_fallback() -> None:
    clarified_query = render_clarified_query(
        original_query="2025年9月到12月的收益是多少",
        clarification_facts=[
            build_clarification_facts(
                original_query="2025年9月到12月的收益是多少",
                time_plan=TimePlan(
                    query="2025年9月到12月的收益是多少",
                    system_date=date(2026, 4, 19),
                    timezone="Asia/Shanghai",
                    units=[
                        _month_unit("u1", "2025年9月", year=2025, month=9),
                        _month_unit("u2", "12月", year=2025, month=12),
                    ],
                    comparisons=[],
                ),
                resolved_plan=ResolvedPlan(
                    nodes={
                        "u1": ResolvedNode(
                            tree=IntervalTree(
                                role="atom",
                                intervals=[Interval(start=date(2025, 9, 1), end=date(2025, 9, 30), end_inclusive=True)],
                                children=[],
                                labels=TreeLabels(
                                    absolute_core_time=Interval(
                                        start=date(2025, 9, 1),
                                        end=date(2025, 9, 30),
                                        end_inclusive=True,
                                    )
                                ),
                            ),
                            derived_from=[],
                        ),
                        "u2": ResolvedNode(
                            tree=IntervalTree(
                                role="atom",
                                intervals=[Interval(start=date(2025, 12, 1), end=date(2025, 12, 31), end_inclusive=True)],
                                children=[],
                                labels=TreeLabels(
                                    absolute_core_time=Interval(
                                        start=date(2025, 12, 1),
                                        end=date(2025, 12, 31),
                                        end_inclusive=True,
                                    )
                                ),
                            ),
                            derived_from=[],
                        ),
                    },
                    comparisons=[],
                ),
            )[0],
            build_clarification_facts(
                original_query="2025年9月到12月的收益是多少",
                time_plan=TimePlan(
                    query="2025年9月到12月的收益是多少",
                    system_date=date(2026, 4, 19),
                    timezone="Asia/Shanghai",
                    units=[
                        _month_unit("u1", "2025年9月", year=2025, month=9),
                        _month_unit("u2", "12月", year=2025, month=12),
                    ],
                    comparisons=[],
                ),
                resolved_plan=ResolvedPlan(
                    nodes={
                        "u1": ResolvedNode(
                            tree=IntervalTree(
                                role="atom",
                                intervals=[Interval(start=date(2025, 9, 1), end=date(2025, 9, 30), end_inclusive=True)],
                                children=[],
                                labels=TreeLabels(
                                    absolute_core_time=Interval(
                                        start=date(2025, 9, 1),
                                        end=date(2025, 9, 30),
                                        end_inclusive=True,
                                    )
                                ),
                            ),
                            derived_from=[],
                        ),
                        "u2": ResolvedNode(
                            tree=IntervalTree(
                                role="atom",
                                intervals=[Interval(start=date(2025, 12, 1), end=date(2025, 12, 31), end_inclusive=True)],
                                children=[],
                                labels=TreeLabels(
                                    absolute_core_time=Interval(
                                        start=date(2025, 12, 1),
                                        end=date(2025, 12, 31),
                                        end_inclusive=True,
                                    )
                                ),
                            ),
                            derived_from=[],
                        ),
                    },
                    comparisons=[],
                ),
            )[1],
        ],
    )

    assert clarified_query == "2025年9月到12月的收益是多少（2025年9月到12月指2025年9月1日至2025年12月31日）"


def test_render_clarified_query_uses_llm_for_complex_multi_fact_case() -> None:
    march_2026 = Interval(start=date(2026, 3, 1), end=date(2026, 3, 31), end_inclusive=True)
    march_2025 = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="今年3月和去年同期的收益分别是多少",
        system_date=date(2026, 4, 19),
        timezone="Asia/Shanghai",
        units=[
            _month_unit("u1", "今年3月", year=2026, month=3),
            _month_unit("u2", "去年同期", year=2025, month=3),
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2026], children=[], labels=TreeLabels(absolute_core_time=march_2026)),
                derived_from=[],
            ),
            "u2": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2025], children=[], labels=TreeLabels(absolute_core_time=march_2025)),
                derived_from=[],
            ),
        },
        comparisons=[],
    )
    runner = _StaticRunner(
        "今年3月和去年同期的收益分别是多少（今年3月为2026年3月1日至2026年3月31日；去年同期为2025年3月1日至2025年3月31日）"
    )

    clarified_query = render_clarified_query(
        original_query=time_plan.query,
        clarification_facts=build_clarification_facts(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan),
        text_runner=runner,
    )

    assert runner.calls == 1
    assert clarified_query == "今年3月和去年同期的收益分别是多少（今年3月为2026年3月1日至2026年3月31日；去年同期为2025年3月1日至2025年3月31日）"


def test_render_clarified_query_falls_back_when_llm_output_is_invalid() -> None:
    march_2026 = Interval(start=date(2026, 3, 1), end=date(2026, 3, 31), end_inclusive=True)
    march_2025 = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="今年3月和去年同期的收益分别是多少",
        system_date=date(2026, 4, 19),
        timezone="Asia/Shanghai",
        units=[
            _month_unit("u1", "今年3月", year=2026, month=3),
            _month_unit("u2", "去年同期", year=2025, month=3),
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2026], children=[], labels=TreeLabels(absolute_core_time=march_2026)),
                derived_from=[],
            ),
            "u2": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march_2025], children=[], labels=TreeLabels(absolute_core_time=march_2025)),
                derived_from=[],
            ),
        },
        comparisons=[],
    )
    runner = _StaticRunner("请计算今年3月和去年同期")

    clarified_query = render_clarified_query(
        original_query=time_plan.query,
        clarification_facts=build_clarification_facts(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan),
        text_runner=runner,
    )

    assert runner.calls == 1
    assert clarified_query == (
        "今年3月和去年同期的收益分别是多少"
        "（今年3月指2026年3月1日至2026年3月31日；去年同期指2025年3月1日至2025年3月31日）"
    )
