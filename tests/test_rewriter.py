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
    StandaloneContent,
    SurfaceFragment,
    TimePlan,
    Unit,
)


class _StaticRunner:
    def __init__(self, content: str) -> None:
        self._content = content

    def invoke(self, messages):  # noqa: ANN001 - simple test double
        class _Response:
            def __init__(self, content: str) -> None:
                self.content = content

        return _Response(self._content)


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


def test_build_time_bindings_emit_clarification_facts_in_unit_order() -> None:
    march = Interval(start=date(2026, 3, 1), end=date(2026, 3, 31), end_inclusive=True)
    prior_march = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="今年3月和去年同期的收益分别是多少",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _month_unit("u1", "今年3月", 0, 4, year=2026, month=3),
            Unit(
                unit_id="u2",
                render_text="去年同期",
                surface_fragments=[SurfaceFragment(start=5, end=9)],
                content={
                    "content_kind": "derived",
                    "sources": [{"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}}],
                },
            ),
        ],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="分别",
                pairs=[ComparisonPair(subject_unit_id="u1", reference_unit_id="u2")],
            )
        ],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(
                tree=IntervalTree(role="atom", intervals=[march], children=[], labels=TreeLabels(absolute_core_time=march)),
                derived_from=[],
            ),
            "u2": ResolvedNode(
                tree=IntervalTree(
                    role="derived",
                    intervals=[prior_march],
                    children=[
                        IntervalTree(
                            role="derived_source",
                            intervals=[prior_march],
                            children=[],
                            labels=TreeLabels(source_unit_id="u1", absolute_core_time=prior_march, degraded=False),
                        )
                    ],
                    labels=TreeLabels(absolute_core_time=prior_march),
                ),
                derived_from=["u1"],
            ),
        },
        comparisons=[
            ResolvedComparison(
                comparison_id="c1",
                pairs=[
                    ResolvedComparisonPair(
                        subject_unit_id="u1",
                        reference_unit_id="u2",
                        degraded=False,
                        subject_absolute_core_time=march,
                        reference_absolute_core_time=prior_march,
                    )
                ],
            )
        ],
    )

    bindings = build_time_bindings(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert [binding.unit_id for binding in bindings] == ["u1", "u2"]
    assert bindings[0].resolved_text == "2026年3月1日至2026年3月31日"
    assert bindings[1].resolved_text == "2025年3月1日至2025年3月31日"
    assert bindings[1].derived_from == ["u1"]
    assert bindings[1].comparison_peers == ["u1"]


def test_build_rewriter_payload_uses_append_only_clarification_contract() -> None:
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

    payload = build_rewriter_payload(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert payload["original_query"] == "2025年3月收益"
    assert list(payload) == ["original_query", "clarification_facts"]
    assert payload["clarification_facts"][0]["unit_id"] == "u1"
    assert payload["clarification_facts"][0]["resolved_text"] == "2025年3月1日至2025年3月31日"


def test_build_rewriter_messages_follow_append_only_prompt_contract() -> None:
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

    assert payload["clarification_facts"][0]["label"] == "2025年3月"
    assert "surface_fragments" not in messages[-1].content
    assert "edit_mode" not in messages[-1].content
    assert "scaffold_tokens_to_preserve" not in messages[-1].content


def test_rewrite_query_returns_append_only_clarified_query() -> None:
    interval = Interval(start=date(2026, 3, 18), end=date(2026, 4, 17), end_inclusive=True)
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

    clarified = rewrite_query(original_query=time_plan.query, time_plan=time_plan, resolved_plan=resolved_plan)

    assert clarified == "最近一个月收益（最近一个月指2026年3月18日至2026年4月17日）"


def test_rewrite_query_uses_bounded_llm_writer_for_complex_case() -> None:
    march = Interval(start=date(2026, 3, 1), end=date(2026, 3, 31), end_inclusive=True)
    prior_march = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    time_plan = TimePlan(
        query="今年3月和去年同期的收益分别是多少",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _month_unit("u1", "今年3月", 0, 4, year=2026, month=3),
            Unit(
                unit_id="u2",
                render_text="去年同期",
                surface_fragments=[SurfaceFragment(start=5, end=9)],
                content={
                    "content_kind": "derived",
                    "sources": [{"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}}],
                },
            ),
        ],
        comparisons=[],
    )
    resolved_plan = ResolvedPlan(
        nodes={
            "u1": ResolvedNode(tree=IntervalTree(role="atom", intervals=[march], children=[], labels=TreeLabels(absolute_core_time=march)), derived_from=[]),
            "u2": ResolvedNode(
                tree=IntervalTree(
                    role="derived",
                    intervals=[prior_march],
                    children=[
                        IntervalTree(
                            role="derived_source",
                            intervals=[prior_march],
                            children=[],
                            labels=TreeLabels(source_unit_id="u1", absolute_core_time=prior_march, degraded=False),
                        )
                    ],
                    labels=TreeLabels(absolute_core_time=prior_march),
                ),
                derived_from=["u1"],
            ),
        },
        comparisons=[],
    )

    clarified = rewrite_query(
        original_query=time_plan.query,
        time_plan=time_plan,
        resolved_plan=resolved_plan,
        text_runner=_StaticRunner(
            "今年3月和去年同期的收益分别是多少（今年3月为2026年3月1日至2026年3月31日；去年同期为2025年3月1日至2025年3月31日）"
        ),
    )

    assert clarified == "今年3月和去年同期的收益分别是多少（今年3月为2026年3月1日至2026年3月31日；去年同期为2025年3月1日至2025年3月31日）"
