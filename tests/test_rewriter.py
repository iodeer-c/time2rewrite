from __future__ import annotations

import json
from datetime import date

from time_query_service.resolved_plan import Interval, IntervalTree, ResolvedComparison, ResolvedComparisonPair, ResolvedNode, ResolvedPlan, TreeLabels
from time_query_service.rewriter import build_rewriter_messages, build_rewriter_payload, build_time_bindings, rewrite_query
from time_query_service.time_plan import (
    Carrier,
    Comparison,
    ComparisonPair,
    NamedPeriod,
    PairExpansion,
    RollingByCalendarUnit,
    StandaloneContent,
    SurfaceFragment,
    TimePlan,
    Unit,
)


def _month_unit(unit_id: str, render_text: str, start: int, end: int, *, year: int, month: int) -> Unit:
    return Unit(
        unit_id=unit_id,
        render_text=render_text,
        surface_fragments=[SurfaceFragment(start=start, end=end)],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=year, month=month),
                modifiers=[],
            ),
        ),
    )


def test_build_time_bindings_preserves_per_source_attribution_and_partial_no_match() -> None:
    healthy = Interval(start=date(2024, 3, 1), end=date(2024, 3, 31), end_inclusive=True)
    derived_tree = IntervalTree(
        role="derived",
        intervals=[healthy],
        children=[
            IntervalTree(
                role="derived_source",
                intervals=[healthy],
                children=[],
                labels=TreeLabels(source_unit_id="u1", absolute_core_time=healthy, degraded=False),
            ),
            IntervalTree(
                role="derived_source",
                intervals=[],
                children=[],
                labels=TreeLabels(source_unit_id="u2", degraded=True, degraded_source_reason_kind="calendar_data_missing"),
            ),
        ],
        labels=TreeLabels(absolute_core_time=healthy),
    )
    time_plan = TimePlan.model_validate(
        {
            "query": "今年3月和5月，去年同期",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "今年3月",
                    "surface_fragments": [{"start": 0, "end": 4}],
                    "content": {"content_kind": "standalone", "carrier": {"anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, "modifiers": []}},
                },
                {
                    "unit_id": "u2",
                    "render_text": "今年5月",
                    "surface_fragments": [{"start": 5, "end": 8}],
                    "content": {"content_kind": "standalone", "carrier": {"anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5}, "modifiers": []}},
                },
                {
                    "unit_id": "u3",
                    "render_text": "去年同期",
                    "surface_fragments": [{"start": 9, "end": 13}],
                    "content": {
                        "content_kind": "derived",
                        "sources": [
                            {"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}},
                            {"source_unit_id": "u2", "transform": {"kind": "shift_year", "offset": -1}},
                        ],
                    },
                },
            ],
            "comparisons": [],
        }
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[healthy], children=[], labels=TreeLabels(absolute_core_time=healthy)),
                derived_from=[],
            ),
            "u2": ResolvedNode(needs_clarification=True, reason_kind="calendar_data_missing", derived_from=[]),
            "u3": ResolvedNode(tree=derived_tree, derived_from=["u1", "u2"]),
        },
        comparisons=[],
    )

    bindings = build_time_bindings(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    derived_binding = next(binding for binding in bindings if binding.unit_id == "u3")
    assert derived_binding.route_state == "partial_no_match"
    assert [source.source_unit_id for source in derived_binding.source_bindings] == ["u1", "u2"]
    assert derived_binding.source_bindings[1].degraded is True


def test_build_rewriter_payload_regroups_expanded_comparisons_by_source_pair_index() -> None:
    interval_a = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    interval_b = Interval(start=date(2025, 5, 1), end=date(2025, 5, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="今年3月和5月对比去年同期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _month_unit("u1", "今年3月", 0, 4, year=2025, month=3),
            _month_unit("u2", "今年5月", 5, 8, year=2025, month=5),
        ],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[
                    ComparisonPair(
                        subject_unit_id="u1",
                        reference_unit_id="u2",
                        expansion=PairExpansion(
                            source_pair_index=0,
                            expansion_index=0,
                            expansion_cardinality=2,
                            subject_core_index=0,
                            reference_core_index=0,
                        ),
                    ),
                    ComparisonPair(
                        subject_unit_id="u1",
                        reference_unit_id="u2",
                        expansion=PairExpansion(
                            source_pair_index=0,
                            expansion_index=1,
                            expansion_cardinality=2,
                            subject_core_index=1,
                            reference_core_index=1,
                        ),
                    ),
                ],
            )
        ],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(tree=IntervalTree(role="atom", intervals=[interval_a], children=[], labels=TreeLabels(absolute_core_time=interval_a)), derived_from=[]),
            "u2": ResolvedNode(tree=IntervalTree(role="atom", intervals=[interval_b], children=[], labels=TreeLabels(absolute_core_time=interval_b)), derived_from=[]),
        },
        comparisons=[
            ResolvedComparison(
                comparison_id="c1",
                pairs=[
                    ResolvedComparisonPair(
                        subject_unit_id="u1",
                        reference_unit_id="u2",
                        degraded=False,
                        subject_absolute_core_time=interval_a,
                        reference_absolute_core_time=interval_b,
                        expansion={"source_pair_index": 0, "expansion_index": 0, "expansion_cardinality": 2, "subject_core_index": 0, "reference_core_index": 0},
                    ),
                    ResolvedComparisonPair(
                        subject_unit_id="u1",
                        reference_unit_id="u2",
                        degraded=False,
                        subject_absolute_core_time=interval_b,
                        reference_absolute_core_time=interval_a,
                        expansion={"source_pair_index": 0, "expansion_index": 1, "expansion_cardinality": 2, "subject_core_index": 1, "reference_core_index": 1},
                    ),
                ],
            )
        ],
    )

    payload = build_rewriter_payload(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert len(payload["comparisons"]) == 1
    assert payload["comparisons"][0]["source_pair_index"] == 0
    assert [pair["expansion"]["expansion_index"] for pair in payload["comparisons"][0]["pairs"]] == [0, 1]


def test_build_time_bindings_preserves_filtered_collection_and_count_scaffolds() -> None:
    march = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    workday = Interval(start=date(2025, 3, 3), end=date(2025, 3, 3), end_inclusive=True)
    rolling = Interval(start=date(2025, 10, 10), end=date(2025, 10, 11), end_inclusive=True)
    time_plan = TimePlan(
        query="2025年3月的工作日和最近5个工作日收益",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u_workdays",
                render_text="2025年3月的工作日",
                surface_fragments=[SurfaceFragment(start=0, end=10)],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3), modifiers=[]),
                ),
            ),
            Unit(
                unit_id="u_recent",
                render_text="最近5个工作日",
                surface_fragments=[SurfaceFragment(start=11, end=18)],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor=RollingByCalendarUnit(
                            kind="rolling_by_calendar_unit",
                            length=5,
                            day_class="workday",
                            endpoint="today",
                            include_endpoint=True,
                        ),
                        modifiers=[],
                    ),
                ),
            ),
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u_workdays": ResolvedNode(
                tree=IntervalTree(
                    role="filtered_collection",
                    intervals=[march],
                    children=[IntervalTree(role="atom", intervals=[workday], children=[], labels=TreeLabels(absolute_core_time=workday))],
                    labels=TreeLabels(absolute_core_time=march),
                ),
                derived_from=[],
            ),
            "u_recent": ResolvedNode(
                tree=IntervalTree(
                    role="filtered_collection",
                    intervals=[rolling],
                    children=[IntervalTree(role="atom", intervals=[rolling], children=[], labels=TreeLabels(absolute_core_time=rolling))],
                    labels=TreeLabels(absolute_core_time=rolling),
                ),
                derived_from=[],
            ),
        },
        comparisons=[],
    )

    bindings = build_time_bindings(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert next(binding for binding in bindings if binding.unit_id == "u_workdays").scaffold_tokens_to_preserve == ["工作日"]
    assert next(binding for binding in bindings if binding.unit_id == "u_recent").scaffold_tokens_to_preserve == ["5个工作日", "工作日"]


def test_build_rewriter_messages_include_new_contract_surfaces_and_omit_legacy_fields() -> None:
    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="2025年3月收益",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_month_unit("u1", "2025年3月", 0, 7, year=2025, month=3)],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(tree=IntervalTree(role="atom", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval)), derived_from=[]),
        },
        comparisons=[],
    )

    messages = build_rewriter_messages(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)
    payload = json.loads(messages[-1].content)

    binding = payload["bindings"][0]
    assert binding["unit_id"] == "u1"
    assert "surface_fragments" in binding
    assert "source_bindings" in binding
    assert "edit_mode" in binding
    assert "clarification_plan" not in messages[-1].content
    assert "inheritance_mode" not in messages[-1].content
    assert "rebind_target_path" not in messages[-1].content


def test_rewrite_query_uses_append_only_annotations_over_new_bindings() -> None:
    interval = Interval(start=date(2025, 3, 18), end=date(2025, 4, 17), end_inclusive=True)
    time_plan = TimePlan(
        query="最近一个月收益",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u1",
                render_text="最近一个月",
                surface_fragments=[SurfaceFragment(start=0, end=5)],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3), modifiers=[]),
                ),
            )
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={"u1": ResolvedNode(tree=IntervalTree(role="atom", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval)), derived_from=[])},
        comparisons=[],
    )

    rewritten = rewrite_query(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert rewritten == "最近一个月（2025年3月18日至2025年4月17日）收益"
