from __future__ import annotations

import json
from collections import deque
from datetime import date
from pathlib import Path

import pytest

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.post_processor import (
    StageAComparisonOutput,
    StageAComparisonPairOutput,
    StageAOutput,
    StageAUnitOutput,
    StageBOutput,
)
from time_query_service.resolved_plan import Interval, IntervalTree, ResolvedComparison, ResolvedComparisonPair, ResolvedNode, ResolvedPlan, TreeLabels
from time_query_service.time_plan import Carrier, GroupedTemporalValue, NamedPeriod, StandaloneContent, TimePlan, Unit


def _atom_node(start: date, end: date, *, unit_id: str = "u1") -> tuple[str, ResolvedNode]:
    interval = Interval(start=start, end=end, end_inclusive=True)
    return (
        unit_id,
        ResolvedNode(
            needs_clarification=False,
            tree=IntervalTree(role="atom", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval)),
            derived_from=[],
        ),
    )


def _resolved_plan(*nodes: tuple[str, ResolvedNode], comparisons: list[ResolvedComparison] | None = None) -> ResolvedPlan:
    return ResolvedPlan(nodes=dict(nodes), comparisons=comparisons or [])


def _stage_a_output() -> StageAOutput:
    return StageAOutput(
        query="2025年3月和去年同期对比",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            StageAUnitOutput(
                unit_id="u1",
                render_text="2025年3月",
                surface_fragments=[{"start": 0, "end": 7}],
                content_kind="standalone",
                self_contained_text="2025年3月",
                sources=[],
            ),
            StageAUnitOutput(
                unit_id="u2",
                render_text="去年同期",
                surface_fragments=[{"start": 8, "end": 12}],
                content_kind="derived",
                self_contained_text=None,
                sources=[{"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}}],
            ),
        ],
        comparisons=[
            StageAComparisonOutput(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[StageAComparisonPairOutput(subject_unit_id="u1", reference_unit_id="u2")],
            )
        ],
    )


def _calendar() -> JsonBusinessCalendar:
    return JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))


class _StringRunner:
    def __init__(self, payloads: list[str]) -> None:
        self._payloads = deque(payloads)

    def invoke(self, _messages):
        if not self._payloads:
            raise AssertionError("no payload queued")
        return self._payloads.popleft()


def test_resolved_plan_equals_passes_on_identical_plan() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    plan = _resolved_plan(_atom_node(date(2025, 3, 1), date(2025, 3, 31)))

    result = resolved_plan_equals(plan, plan)

    assert result.passed is True
    assert result.diffs == []


def test_stage_a_match_passes_on_identical_payload() -> None:
    from time_query_service.evaluator import stage_a_match

    payload = _stage_a_output()

    result = stage_a_match(payload, payload)

    assert result.passed is True


def test_stage_a_match_reports_source_edge_mismatch() -> None:
    from time_query_service.evaluator import stage_a_match

    expected = _stage_a_output()
    actual = StageAOutput.model_validate(
        {
            **expected.model_dump(mode="python"),
            "units": [
                expected.units[0].model_dump(mode="python"),
                {
                    **expected.units[1].model_dump(mode="python"),
                    "sources": [{"source_unit_id": "u_missing", "transform": {"kind": "shift_year", "offset": -1}}],
                },
            ],
        }
    )

    result = stage_a_match(expected, actual)

    assert any(diff.path == "sources" for diff in result.diffs)


def test_stage_a_match_reports_surface_hint_mismatch_when_present() -> None:
    from time_query_service.evaluator import stage_a_match

    expected = _stage_a_output()
    actual = StageAOutput.model_validate(
        {
            **expected.model_dump(mode="python"),
            "units": [
                {
                    **expected.units[0].model_dump(mode="python"),
                    "surface_hint": "calendar_grain_rolling",
                },
                expected.units[1].model_dump(mode="python"),
            ],
        }
    )

    result = stage_a_match(expected, actual)

    assert any(diff.path == "units[u1].surface_hint" for diff in result.diffs)


def test_stage_b_match_passes_on_identical_payload() -> None:
    from time_query_service.evaluator import stage_b_match

    payload = StageBOutput.model_validate(
        {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}],
            },
            "needs_clarification": False,
        }
    )

    result = stage_b_match(payload, payload)

    assert result.passed is True


def test_stage_b_match_reports_modifier_order_and_numeric_mismatch() -> None:
    from time_query_service.evaluator import stage_b_match

    expected = StageBOutput.model_validate(
        {
            "carrier": {
                "anchor": {"kind": "rolling_window", "length": 1, "unit": "month", "endpoint": "today", "include_endpoint": True},
                "modifiers": [
                    {"kind": "calendar_filter", "day_class": "workday"},
                    {"kind": "member_selection", "selector": "first_n", "n": 3},
                ],
            },
            "needs_clarification": False,
        }
    )
    actual = StageBOutput.model_validate(
        {
            "carrier": {
                "anchor": {"kind": "rolling_window", "length": 2, "unit": "month", "endpoint": "today", "include_endpoint": True},
                "modifiers": [
                    {"kind": "member_selection", "selector": "first_n", "n": 2},
                    {"kind": "calendar_filter", "day_class": "workday"},
                ],
            },
            "needs_clarification": False,
        }
    )

    result = stage_b_match(expected, actual)

    assert any(diff.path == "carrier.modifiers[*].kind" for diff in result.diffs)
    assert any(diff.path == "carrier.anchor.length" for diff in result.diffs)
    assert any(diff.path == "carrier.modifiers[1].n" for diff in result.diffs)


def test_stage_b_match_reports_reason_kind_mismatch_for_degraded_output() -> None:
    from time_query_service.evaluator import stage_b_match

    expected = StageBOutput(carrier=None, needs_clarification=True, reason_kind="unsupported_anchor_semantics")
    actual = StageBOutput(carrier=None, needs_clarification=True, reason_kind="semantic_conflict")

    result = stage_b_match(expected, actual)

    assert any(diff.path == "reason_kind" for diff in result.diffs)


def test_resolved_plan_equals_reports_unit_keyset_mismatch() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    expected = _resolved_plan(_atom_node(date(2025, 3, 1), date(2025, 3, 31), unit_id="u1"))
    actual = _resolved_plan(
        _atom_node(date(2025, 3, 1), date(2025, 3, 31), unit_id="u1"),
        _atom_node(date(2025, 5, 1), date(2025, 5, 31), unit_id="u2"),
    )

    result = resolved_plan_equals(expected, actual)

    assert result.passed is False
    assert any(diff.path == "nodes" for diff in result.diffs)


def test_resolved_plan_equals_reports_reason_kind_mismatch() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    expected = _resolved_plan(
        (
            "u1",
            ResolvedNode(needs_clarification=True, reason_kind="unsupported_anchor_semantics", tree=None, derived_from=[]),
        )
    )
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(needs_clarification=True, reason_kind="semantic_conflict", tree=None, derived_from=[]),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].reason_kind" for diff in result.diffs)


def test_resolved_plan_equals_reports_needs_clarification_mismatch() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    expected = _resolved_plan(
        (
            "u1",
            ResolvedNode(needs_clarification=True, reason_kind="semantic_conflict", tree=None, derived_from=[]),
        )
    )
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(needs_clarification=False, tree=None, derived_from=[]),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].needs_clarification" for diff in result.diffs)


def test_resolved_plan_equals_reports_derived_from_order_mismatch() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    child_a = IntervalTree(role="derived_source", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval, source_unit_id="u_a"))
    child_b = IntervalTree(role="derived_source", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval, source_unit_id="u_b"))
    expected = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(role="derived", intervals=[interval, interval], children=[child_a, child_b], labels=TreeLabels(absolute_core_time=interval)),
                derived_from=["u_a", "u_b"],
            ),
        )
    )
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(role="derived", intervals=[interval, interval], children=[child_b, child_a], labels=TreeLabels(absolute_core_time=interval)),
                derived_from=["u_b", "u_a"],
            ),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].derived_from" for diff in result.diffs)


def test_resolved_plan_equals_reports_derived_child_order_violation() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    interval_a = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    interval_b = Interval(start=date(2025, 5, 1), end=date(2025, 5, 31), end_inclusive=True)
    expected = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(
                    role="derived",
                    intervals=[interval_a, interval_b],
                    children=[
                        IntervalTree(role="derived_source", intervals=[interval_a], children=[], labels=TreeLabels(absolute_core_time=interval_a, source_unit_id="u_march")),
                        IntervalTree(role="derived_source", intervals=[interval_b], children=[], labels=TreeLabels(absolute_core_time=interval_b, source_unit_id="u_may")),
                    ],
                    labels=TreeLabels(absolute_core_time=interval_a),
                ),
                derived_from=["u_march", "u_may"],
            ),
        )
    )
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(
                    role="derived",
                    intervals=[interval_a, interval_b],
                    children=[
                        IntervalTree(role="derived_source", intervals=[interval_b], children=[], labels=TreeLabels(absolute_core_time=interval_b, source_unit_id="u_may")),
                        IntervalTree(role="derived_source", intervals=[interval_a], children=[], labels=TreeLabels(absolute_core_time=interval_a, source_unit_id="u_march")),
                    ],
                    labels=TreeLabels(absolute_core_time=interval_a),
                ),
                derived_from=["u_may", "u_march"],
            ),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].tree.children[0].labels.source_unit_id" for diff in result.diffs)


def test_resolved_plan_equals_reports_tree_interval_mismatch_even_when_labels_match() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    expected_interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    actual_interval = Interval(start=date(2025, 3, 2), end=date(2025, 3, 31), end_inclusive=True)
    expected = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(role="atom", intervals=[expected_interval], children=[], labels=TreeLabels(absolute_core_time=expected_interval)),
                derived_from=[],
            ),
        )
    )
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(role="atom", intervals=[actual_interval], children=[], labels=TreeLabels(absolute_core_time=expected_interval)),
                derived_from=[],
            ),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].tree.intervals[0].start" for diff in result.diffs)


def test_resolved_plan_equals_reports_role_mismatch() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    expected = _resolved_plan(
        ("u1", ResolvedNode(needs_clarification=False, tree=IntervalTree(role="union", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval)), derived_from=[]))
    )
    actual = _resolved_plan(
        ("u1", ResolvedNode(needs_clarification=False, tree=IntervalTree(role="grouped_member", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval)), derived_from=[]))
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].tree.role" for diff in result.diffs)


def test_resolved_plan_equals_reports_grouped_member_order_violation() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    q1 = Interval(start=date(2025, 1, 1), end=date(2025, 3, 31), end_inclusive=True)
    q2 = Interval(start=date(2025, 4, 1), end=date(2025, 6, 30), end_inclusive=True)
    expected = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(
                    role="grouped_member",
                    intervals=[q1, q2],
                    children=[
                        IntervalTree(role="atom", intervals=[q1], children=[], labels=TreeLabels(absolute_core_time=q1)),
                        IntervalTree(role="atom", intervals=[q2], children=[], labels=TreeLabels(absolute_core_time=q2)),
                    ],
                    labels=TreeLabels(absolute_core_time=Interval(start=date(2025, 1, 1), end=date(2025, 6, 30), end_inclusive=True)),
                ),
                derived_from=[],
            ),
        )
    )
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(
                    role="grouped_member",
                    intervals=[q2, q1],
                    children=[
                        IntervalTree(role="atom", intervals=[q2], children=[], labels=TreeLabels(absolute_core_time=q2)),
                        IntervalTree(role="atom", intervals=[q1], children=[], labels=TreeLabels(absolute_core_time=q1)),
                    ],
                    labels=TreeLabels(absolute_core_time=Interval(start=date(2025, 1, 1), end=date(2025, 6, 30), end_inclusive=True)),
                ),
                derived_from=[],
            ),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path.startswith("nodes[u1].tree.intervals[0]") for diff in result.diffs)


@pytest.mark.parametrize(
    ("field_name", "override"),
    [
        ("source_pair_index", {"source_pair_index": 1}),
        ("expansion_index", {"expansion_index": 1}),
        ("expansion_cardinality", {"expansion_cardinality": 3}),
        ("subject_core_index", {"subject_core_index": 1}),
        ("reference_core_index", {"reference_core_index": 0}),
    ],
)
def test_resolved_plan_equals_reports_comparison_expansion_mismatch(field_name: str, override: dict[str, int | None]) -> None:
    from time_query_service.evaluator import resolved_plan_equals

    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    expected_expansion = {
        "source_pair_index": 0,
        "expansion_index": 0,
        "expansion_cardinality": 2,
        "subject_core_index": 0,
        "reference_core_index": None,
    }
    comparison_expected = ResolvedComparison(
        comparison_id="c1",
        pairs=[
            ResolvedComparisonPair(
                subject_unit_id="u1",
                reference_unit_id="u2",
                degraded=False,
                degraded_reason=None,
                subject_absolute_core_time=interval,
                reference_absolute_core_time=interval,
                expansion=expected_expansion,
            )
        ],
    )
    comparison_actual = ResolvedComparison.model_validate(
        {
            "comparison_id": "c1",
            "pairs": [
                {
                    "subject_unit_id": "u1",
                    "reference_unit_id": "u2",
                    "degraded": False,
                    "subject_absolute_core_time": interval,
                    "reference_absolute_core_time": interval,
                    "expansion": expected_expansion | override,
                }
            ],
        }
    )

    result = resolved_plan_equals(
        _resolved_plan(_atom_node(date(2025, 3, 1), date(2025, 3, 31)), comparisons=[comparison_expected]),
        _resolved_plan(_atom_node(date(2025, 3, 1), date(2025, 3, 31)), comparisons=[comparison_actual]),
    )

    assert any(diff.path == f"comparisons[0].pairs[0].expansion.{field_name}" for diff in result.diffs)


def test_resolved_plan_equals_reports_end_inclusive_flip() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    expected = _resolved_plan(_atom_node(date(2025, 3, 1), date(2025, 3, 31)))
    actual_interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=False)
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(role="atom", intervals=[actual_interval], children=[], labels=TreeLabels(absolute_core_time=actual_interval)),
                derived_from=[],
            ),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].tree.intervals[0].end_inclusive" for diff in result.diffs)


def test_resolved_plan_equals_reports_derived_source_degraded_label_mismatch() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    expected_child = IntervalTree(
        role="derived_source",
        intervals=[],
        children=[],
        labels=TreeLabels(source_unit_id="u1", degraded=True, degraded_source_reason_kind="semantic_conflict"),
    )
    actual_child = IntervalTree(
        role="derived_source",
        intervals=[],
        children=[],
        labels=TreeLabels(source_unit_id="u1", degraded=False, degraded_source_reason_kind=None),
    )
    expected = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(role="derived", intervals=[interval], children=[expected_child], labels=TreeLabels(absolute_core_time=interval)),
                derived_from=["u1"],
            ),
        )
    )
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(role="derived", intervals=[interval], children=[actual_child], labels=TreeLabels(absolute_core_time=interval)),
                derived_from=["u1"],
            ),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].tree.children[0].labels.degraded" for diff in result.diffs)


def test_resolved_plan_equals_reports_structured_derivation_transform_summary_mismatch() -> None:
    from time_query_service.evaluator import resolved_plan_equals

    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    expected = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(
                    role="atom",
                    intervals=[interval],
                    children=[],
                    labels=TreeLabels(
                        absolute_core_time=interval,
                        derivation_transform_summary={"kind": "shift_year", "offset": -1},
                    ),
                ),
                derived_from=[],
            ),
        )
    )
    actual = _resolved_plan(
        (
            "u1",
            ResolvedNode(
                needs_clarification=False,
                tree=IntervalTree(
                    role="atom",
                    intervals=[interval],
                    children=[],
                    labels=TreeLabels(
                        absolute_core_time=interval,
                        derivation_transform_summary={"kind": "shift_year", "offset": -2},
                    ),
                ),
                derived_from=[],
            ),
        )
    )

    result = resolved_plan_equals(expected, actual)

    assert any(diff.path == "nodes[u1].tree.labels.derivation_transform_summary" for diff in result.diffs)


def test_lint_layer1_case_rejects_transient_non_day_grain_expansion_in_expected_time_plan() -> None:
    from time_query_service.evaluator import GoldenCaseAuthoringError, lint_layer1_case

    case = {
        "query": "2025年每个季度",
        "system_date": "2026-04-17",
        "tier": 1,
        "expected_time_plan": TimePlan.model_validate(
            {
                "query": "2025年每个季度",
                "system_date": "2026-04-17",
                "timezone": "Asia/Shanghai",
                "units": [
                    {
                        "unit_id": "u1",
                        "render_text": "2025年每个季度",
                        "surface_fragments": [{"start": 0, "end": 9}],
                        "content": {
                            "content_kind": "standalone",
                            "carrier": {
                                "anchor": {"kind": "named_period", "period_type": "year", "year": 2025},
                                "modifiers": [{"kind": "grain_expansion", "target_grain": "quarter"}],
                            },
                        },
                    }
                ],
                "comparisons": [],
            }
        ),
        "expected_resolved_plan": _resolved_plan(_atom_node(date(2025, 3, 1), date(2025, 3, 31))),
    }

    with pytest.raises(GoldenCaseAuthoringError) as excinfo:
        lint_layer1_case(case)

    assert "expected_time_plan" in str(excinfo.value)


def test_lint_layer1_case_rejects_bad_filtered_collection_intervals_shape() -> None:
    from time_query_service.evaluator import GoldenCaseAuthoringError, lint_layer1_case

    day1 = Interval(start=date(2025, 3, 3), end=date(2025, 3, 3), end_inclusive=True)
    day2 = Interval(start=date(2025, 3, 4), end=date(2025, 3, 4), end_inclusive=True)
    aggregate = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    case = {
        "query": "2025年3月的工作日",
        "system_date": "2026-04-17",
        "tier": 1,
        "expected_time_plan": TimePlan(
            query="2025年3月的工作日",
            system_date=date(2026, 4, 17),
            timezone="Asia/Shanghai",
            units=[
                Unit(
                    unit_id="u1",
                    render_text="2025年3月的工作日",
                    surface_fragments=[{"start": 0, "end": 10}],
                    content=StandaloneContent(
                        content_kind="standalone",
                        carrier=Carrier(
                            anchor=GroupedTemporalValue(
                                kind="grouped_temporal_value",
                                parent=NamedPeriod(kind="named_period", period_type="year", year=2025),
                                child_grain="quarter",
                                selector="all",
                            ),
                            modifiers=[],
                        ),
                    ),
                )
            ],
            comparisons=[],
        ),
        "expected_resolved_plan": _resolved_plan(
            (
                "u1",
                ResolvedNode(
                    needs_clarification=False,
                    tree=IntervalTree(
                        role="filtered_collection",
                        intervals=[day1, day2],
                        children=[
                            IntervalTree(role="atom", intervals=[day1], children=[], labels=TreeLabels(absolute_core_time=day1)),
                            IntervalTree(role="atom", intervals=[day2], children=[], labels=TreeLabels(absolute_core_time=day2)),
                        ],
                        labels=TreeLabels(absolute_core_time=aggregate),
                    ),
                    derived_from=[],
                ),
            )
        ),
    }

    with pytest.raises(GoldenCaseAuthoringError) as excinfo:
        lint_layer1_case(case)

    assert "filtered_collection" in str(excinfo.value)


def test_lint_layer1_case_rejects_missing_expected_time_plan() -> None:
    from time_query_service.evaluator import GoldenCaseAuthoringError, lint_layer1_case

    with pytest.raises(GoldenCaseAuthoringError) as excinfo:
        lint_layer1_case({"expected_resolved_plan": _resolved_plan(_atom_node(date(2025, 3, 1), date(2025, 3, 31)))})

    assert "expected_time_plan" in str(excinfo.value)


def test_lint_layer1_case_rejects_derived_intervals_with_placeholder_entry() -> None:
    from time_query_service.evaluator import GoldenCaseAuthoringError, lint_layer1_case

    healthy = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    degraded_child = IntervalTree(
        role="derived_source",
        intervals=[],
        children=[],
        labels=TreeLabels(source_unit_id="u2", degraded=True, degraded_source_reason_kind="semantic_conflict"),
    )
    healthy_child = IntervalTree(
        role="derived_source",
        intervals=[healthy],
        children=[],
        labels=TreeLabels(absolute_core_time=healthy, source_unit_id="u1"),
    )
    bad_derived = IntervalTree(
        role="derived",
        intervals=[healthy, Interval(start=date(1970, 1, 1), end=date(1970, 1, 1), end_inclusive=True)],
        children=[healthy_child, degraded_child],
        labels=TreeLabels(absolute_core_time=healthy),
    )
    case = {
        "expected_time_plan": TimePlan(
            query="A",
            system_date=date(2026, 4, 17),
            timezone="Asia/Shanghai",
            units=[
                Unit(
                    unit_id="u1",
                    render_text="A",
                    surface_fragments=[{"start": 0, "end": 1}],
                    content=StandaloneContent(
                        content_kind="standalone",
                        carrier=Carrier(
                            anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
                            modifiers=[],
                        ),
                    ),
                )
            ],
            comparisons=[],
        ),
        "expected_resolved_plan": _resolved_plan(("u1", ResolvedNode(needs_clarification=False, tree=bad_derived, derived_from=["u1", "u2"]))),
    }

    with pytest.raises(GoldenCaseAuthoringError) as excinfo:
        lint_layer1_case(case)

    assert "derived" in str(excinfo.value)


def test_case_passes_is_case_level_pass_fail_without_partial_credit() -> None:
    from time_query_service.evaluator import case_passes, resolved_plan_equals

    expected = _resolved_plan(_atom_node(date(2025, 3, 1), date(2025, 3, 31)))
    actual = _resolved_plan(_atom_node(date(2025, 3, 2), date(2025, 3, 31)))

    result = resolved_plan_equals(expected, actual)

    assert case_passes(result) is False


def test_evaluate_stage_a_golden_summarizes_accuracy_and_diffs() -> None:
    from time_query_service.evaluator import evaluate_stage_a_golden

    expected = _stage_a_output()
    mismatched = StageAOutput.model_validate(
        {
            **expected.model_dump(mode="python"),
            "units": [expected.units[0].model_dump(mode="python")],
        }
    )
    cases = [
        {"query": "case-1", "expected": expected},
        {"query": "case-2", "expected": expected},
    ]
    runner = _StringRunner(
        [
            json.dumps(expected.model_dump(mode="json"), ensure_ascii=False),
            json.dumps(mismatched.model_dump(mode="json"), ensure_ascii=False),
        ]
    )

    report = evaluate_stage_a_golden(cases=cases, text_runner=runner, system_date="2026-04-17", timezone="Asia/Shanghai")

    assert report["summary"] == {"total_cases": 2, "passed_cases": 1, "failed_cases": 1, "accuracy": 0.5}
    assert report["results"][0]["passed"] is True
    assert report["results"][1]["passed"] is False
    assert any(diff["path"] == "units" for diff in report["results"][1]["diffs"])


def test_evaluate_stage_b_golden_summarizes_accuracy_and_diffs() -> None:
    from time_query_service.evaluator import evaluate_stage_b_golden

    expected = StageBOutput.model_validate(
        {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}],
            },
            "needs_clarification": False,
        }
    )
    mismatched = StageBOutput.model_validate(
        {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [{"kind": "calendar_filter", "day_class": "holiday"}],
            },
            "needs_clarification": False,
        }
    )
    runner = _StringRunner(
        [
            json.dumps(expected.model_dump(mode="json"), ensure_ascii=False),
            json.dumps(mismatched.model_dump(mode="json"), ensure_ascii=False),
        ]
    )

    report = evaluate_stage_b_golden(
        cases=[{"text": "2025年3月的工作日", "expected": expected}, {"text": "2025年3月的工作日", "expected": expected}],
        text_runner=runner,
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    assert report["summary"] == {"total_cases": 2, "passed_cases": 1, "failed_cases": 1, "accuracy": 0.5}
    assert report["results"][1]["passed"] is False
    assert any(diff["path"] == "carrier.modifiers[0].day_class" for diff in report["results"][1]["diffs"])


def test_evaluate_layer1_golden_runs_pipeline_and_summarizes_by_tier() -> None:
    from time_query_service.evaluator import evaluate_layer1_golden

    expected_stage_a = StageAOutput.model_validate(
        {
            "query": "2025年3月收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "2025年3月",
                    "surface_fragments": [{"start": 0, "end": 7}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年3月",
                    "sources": [],
                }
            ],
            "comparisons": [],
        }
    )
    expected_stage_b = StageBOutput.model_validate(
        {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [],
            },
            "needs_clarification": False,
        }
    )
    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    layer1_case = {
        "query": "2025年3月收益",
        "system_date": "2026-04-17",
        "tier": 1,
        "expected_time_plan": TimePlan(
            query="2025年3月收益",
            system_date=date(2026, 4, 17),
            timezone="Asia/Shanghai",
            units=[
                Unit(
                    unit_id="u1",
                    render_text="2025年3月",
                    surface_fragments=[{"start": 0, "end": 7}],
                    content=StandaloneContent(
                        content_kind="standalone",
                        carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3), modifiers=[]),
                    ),
                )
            ],
            comparisons=[],
        ),
        "expected_resolved_plan": _resolved_plan(
            (
                "u1",
                ResolvedNode(
                    tree=IntervalTree(role="atom", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval)),
                    derived_from=[],
                ),
            )
        ),
        "capability_tags": ["time-plan-schema"],
    }
    stage_a_runner = _StringRunner([json.dumps(expected_stage_a.model_dump(mode="json"), ensure_ascii=False)])
    stage_b_runner = _StringRunner([json.dumps(expected_stage_b.model_dump(mode="json"), ensure_ascii=False)])

    report = evaluate_layer1_golden(
        cases=[layer1_case],
        stage_a_runner=stage_a_runner,
        stage_b_runner=stage_b_runner,
        business_calendar=_calendar(),
        max_stage_b_concurrent=1,
    )

    assert report["summary"] == {"total_cases": 1, "passed_cases": 1, "failed_cases": 0, "accuracy": 1.0}
    assert report["tier_summary"] == {
        1: {"total_cases": 1, "passed_cases": 1, "failed_cases": 0, "pass_rate": 1.0}
    }
    assert report["results"][0]["passed"] is True


def test_evaluate_layer1_golden_records_pipeline_exception_as_case_failure() -> None:
    from time_query_service.evaluator import evaluate_layer1_golden

    expected_stage_a = StageAOutput.model_validate(
        {
            "query": "2025年3月收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
                "units": [
                    {
                        "unit_id": "u1",
                        "render_text": "2025年3月",
                        "surface_fragments": [{"start": 0, "end": 9}],
                        "content_kind": "standalone",
                        "self_contained_text": "2025年3月",
                        "sources": [],
                    }
            ],
            "comparisons": [],
        }
    )
    expected_stage_b = StageBOutput.model_validate(
        {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [],
            },
            "needs_clarification": False,
        }
    )
    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    layer1_case = {
        "query": "2025年3月收益",
        "system_date": "2026-04-17",
        "tier": 1,
        "expected_time_plan": TimePlan(
            query="2025年3月收益",
            system_date=date(2026, 4, 17),
            timezone="Asia/Shanghai",
            units=[
                Unit(
                    unit_id="u1",
                    render_text="2025年3月",
                    surface_fragments=[{"start": 0, "end": 7}],
                    content=StandaloneContent(
                        content_kind="standalone",
                        carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3), modifiers=[]),
                    ),
                )
            ],
            comparisons=[],
        ),
        "expected_resolved_plan": _resolved_plan(
            (
                "u1",
                ResolvedNode(
                    tree=IntervalTree(role="atom", intervals=[interval], children=[], labels=TreeLabels(absolute_core_time=interval)),
                    derived_from=[],
                ),
            )
        ),
        "capability_tags": ["time-plan-schema"],
    }
    stage_a_runner = _StringRunner([json.dumps(expected_stage_a.model_dump(mode="json"), ensure_ascii=False)])
    stage_b_runner = _StringRunner([json.dumps(expected_stage_b.model_dump(mode="json"), ensure_ascii=False)])

    report = evaluate_layer1_golden(
        cases=[layer1_case],
        stage_a_runner=stage_a_runner,
        stage_b_runner=stage_b_runner,
        business_calendar=_calendar(),
        max_stage_b_concurrent=1,
    )

    assert report["summary"] == {"total_cases": 1, "passed_cases": 0, "failed_cases": 1, "accuracy": 0.0}
    assert report["tier_summary"] == {
        1: {"total_cases": 1, "passed_cases": 0, "failed_cases": 1, "pass_rate": 0.0}
    }
    assert report["results"][0]["passed"] is False
    assert report["results"][0]["actual_time_plan"] is None
    assert any(diff["path"] == "pipeline.execution" for diff in report["results"][0]["diffs"])


def test_build_cutover_gate_summary_applies_contract_thresholds() -> None:
    from time_query_service.evaluator import build_cutover_gate_summary

    summary = build_cutover_gate_summary(
        stage_a_report={"summary": {"accuracy": 0.96}},
        stage_b_report={"summary": {"accuracy": 0.97}},
        layer1_report={
            "tier_summary": {
                1: {"pass_rate": 1.0},
                2: {"pass_rate": 0.85},
            },
            "results": [
                {"tier": 3, "query": "坏案例", "passed": False},
                {"tier": 3, "query": "好案例", "passed": True},
            ],
        },
    )

    assert summary == {
        "stage_a_accuracy": 0.96,
        "stage_b_accuracy": 0.97,
        "tier1_pass_rate": 1.0,
        "tier2_pass_rate": 0.85,
        "tier3_failed_queries": ["坏案例"],
        "passes_cutover_gate": True,
    }


def test_build_cutover_gate_summary_accepts_json_round_tripped_tier_keys() -> None:
    from time_query_service.evaluator import build_cutover_gate_summary

    summary = build_cutover_gate_summary(
        stage_a_report={"summary": {"accuracy": 1.0}},
        stage_b_report={"summary": {"accuracy": 0.96}},
        layer1_report={
            "tier_summary": {
                "1": {"pass_rate": 1.0},
                "2": {"pass_rate": 0.8},
            },
            "results": [],
        },
    )

    assert summary == {
        "stage_a_accuracy": 1.0,
        "stage_b_accuracy": 0.96,
        "tier1_pass_rate": 1.0,
        "tier2_pass_rate": 0.8,
        "tier3_failed_queries": [],
        "passes_cutover_gate": True,
    }
