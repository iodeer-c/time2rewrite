from __future__ import annotations

from collections import Counter
from datetime import date

from time_query_service.evaluator import lint_layer1_case, resolved_plan_equals, stage_a_match, stage_b_match
from time_query_service.resolved_plan import Interval
from tests.fixtures.golden_datasets import (
    LAYER1_GOLDEN_CASES,
    STAGE_A_GOLDEN_CASES,
    STAGE_B_GOLDEN_CASES,
    required_capability_tags,
)


def test_stage_a_golden_dataset_meets_size_and_required_query_coverage() -> None:
    assert len(STAGE_A_GOLDEN_CASES) >= 30

    covered_queries = {case["query"] for case in STAGE_A_GOLDEN_CASES}
    assert {
        "2025年3月和5月收益",
        "2025年3月和2025年3月对比",
        "今年3月和5月，去年同期",
        "2025年9月到12月收益",
        "去年12月到3月收益",
        "2025年1月到3月每个月的每个工作日收益",
        "最近5天中的工作日收益",
        "最近5个工作日收益",
        "最近一周收益",
        "最近一个月收益",
        "最近一季度收益",
        "最近半年收益",
        "最近一年收益",
    } <= covered_queries


def test_stage_a_golden_cases_are_self_matching() -> None:
    for case in STAGE_A_GOLDEN_CASES:
        result = stage_a_match(case["expected"], case["expected"])
        assert result.passed, case["query"]


def test_stage_b_golden_dataset_meets_size_and_required_text_coverage() -> None:
    assert len(STAGE_B_GOLDEN_CASES) >= 50

    covered_texts = {case["text"] for case in STAGE_B_GOLDEN_CASES}
    assert {
        "2025-03-01到2025-03-10",
        "上周",
        "清明假期",
        "2025年3月和5月",
        "2025年9月到12月",
        "去年12月到3月",
        "2025年Q3到10月",
        "2025年1月到3月每个月的每个工作日",
        "2025年每个季度",
        "本月至今",
        "2025年3月的前3个工作日",
        "2025年3月往后一个月",
        "最近1个周末",
        "最近5个工作日",
        "最近3个节假日",
        "最近1个补班日",
        "最近一周",
        "最近一个月",
        "最近一季度",
        "最近半年",
        "最近一年",
        "最近5个休息日",
        "过去3个完整月",
        "到本月底为止的最近7天",
    } <= covered_texts


def test_stage_b_golden_cases_are_self_matching() -> None:
    for case in STAGE_B_GOLDEN_CASES:
        result = stage_b_match(case["expected"], case["expected"])
        assert result.passed, case["text"]


def test_stage_b_golden_dataset_covers_all_anchor_kinds_and_common_modifier_kinds() -> None:
    anchor_kinds = set()
    modifier_kinds = set()
    for case in STAGE_B_GOLDEN_CASES:
        expected = case["expected"]
        if expected.carrier is None:
            continue
        anchor_kinds.add(expected.carrier.anchor.kind)
        modifier_kinds.update(modifier.kind for modifier in expected.carrier.modifiers)

    assert anchor_kinds == {
        "named_period",
        "date_range",
        "relative_window",
        "rolling_window",
        "rolling_by_calendar_unit",
        "enumeration_set",
        "grouped_temporal_value",
        "calendar_event",
        "mapped_range",
    }
    assert {"grain_expansion", "calendar_filter", "member_selection", "offset"} <= modifier_kinds


def test_lexical_rolling_forms_stay_on_their_grain_and_resolve_to_trailing_intervals() -> None:
    stage_b_cases = {case["text"]: case["expected"] for case in STAGE_B_GOLDEN_CASES}
    assert stage_b_cases["最近一周"].carrier.anchor.unit == "week"
    assert stage_b_cases["最近一个月"].carrier.anchor.unit == "month"
    assert stage_b_cases["最近一季度"].carrier.anchor.unit == "quarter"
    assert stage_b_cases["最近半年"].carrier.anchor.unit == "half_year"
    assert stage_b_cases["最近一年"].carrier.anchor.unit == "year"

    layer1_cases = {(case["query"], case["system_date"]): case for case in LAYER1_GOLDEN_CASES}
    assert (
        layer1_cases[("最近一周收益", "2026-04-17")]["expected_resolved_plan"].nodes["u1"].tree.labels.absolute_core_time
        == Interval(start=date(2026, 4, 11), end=date(2026, 4, 17), end_inclusive=True)
    )
    assert (
        layer1_cases[("最近一个月收益", "2026-04-17")]["expected_resolved_plan"].nodes["u1"].tree.labels.absolute_core_time
        == Interval(start=date(2026, 3, 18), end=date(2026, 4, 17), end_inclusive=True)
    )
    assert (
        layer1_cases[("最近一季度收益", "2026-04-17")]["expected_resolved_plan"].nodes["u1"].tree.labels.absolute_core_time
        == Interval(start=date(2026, 1, 18), end=date(2026, 4, 17), end_inclusive=True)
    )
    assert (
        layer1_cases[("最近半年收益", "2026-04-17")]["expected_resolved_plan"].nodes["u1"].tree.labels.absolute_core_time
        == Interval(start=date(2025, 10, 18), end=date(2026, 4, 17), end_inclusive=True)
    )
    assert (
        layer1_cases[("最近一年收益", "2026-04-17")]["expected_resolved_plan"].nodes["u1"].tree.labels.absolute_core_time
        == Interval(start=date(2025, 4, 18), end=date(2026, 4, 17), end_inclusive=True)
    )


def test_frozen_v1_rolling_parameters_degrade_in_stage_b_dataset() -> None:
    stage_b_cases = {case["text"]: case["expected"] for case in STAGE_B_GOLDEN_CASES}
    for text in (
        "最近一个月不含今天",
        "截至昨天的最近7天",
        "到本月底为止的最近7天",
        "过去3个完整月",
    ):
        expected = stage_b_cases[text]
        assert expected.needs_clarification is True
        assert expected.reason_kind == "unsupported_anchor_semantics"
        assert expected.carrier is None


def test_layer1_golden_dataset_meets_tier_thresholds_and_required_query_coverage() -> None:
    assert len(LAYER1_GOLDEN_CASES) >= 100

    tier_counts = Counter(case["tier"] for case in LAYER1_GOLDEN_CASES)
    assert tier_counts[1] >= 60
    assert tier_counts[2] >= 30
    assert tier_counts[3] >= 10

    covered_queries = {case["query"] for case in LAYER1_GOLDEN_CASES}
    assert {
        "最近一周收益",
        "最近一个月收益",
        "最近一季度收益",
        "最近半年收益",
        "最近一年收益",
        "2025年9月到12月收益",
        "去年12月到3月收益",
        "2025年Q3到10月收益",
        "最近5个工作日收益",
        "最近3个节假日收益",
        "最近一个月每周收益",
        "最近一季度每月收益",
        "最近半年每季度收益",
        "最近一年每半年收益",
        "2025年3月的工作日对比2024年3月的工作日",
        "2025年中秋假期和国庆假期收益",
    } <= covered_queries


def test_layer1_golden_dataset_covers_all_required_capabilities() -> None:
    covered = set()
    for case in LAYER1_GOLDEN_CASES:
        covered.update(case["capability_tags"])
    assert set(required_capability_tags) <= covered


def test_layer1_golden_dataset_has_representative_queries_for_capability_contracts() -> None:
    representatives = {
        "bounded-range-unit-normalization": "2025年9月到12月收益",
        "append-only-clarification-writer": "2025年9月到12月收益",
        "literal-period-expressions": "2025年1月收益",
        "enumeration-values": "2025年3月和5月收益",
        "enumerative-query-semantics": "2025年3月和5月收益",
        "grouped-temporal-values": "最近一个月每周收益",
        "mapped-range-constructors": "本月至今收益",
        "period-to-date-ranges": "本月至今收益",
        "subperiod-operations": "最近一季度每月收益",
        "calendar-query-semantics": "2025年3月的工作日对比2024年3月的工作日",
        "coordinated-time-binding-groups": "2025年3月的工作日对比2024年3月的工作日",
        "rolling-windows": "最近一个月收益",
        "clarification-plan-contract": "2025年3月和5月收益",
        "derived-range-lineage": "今年3月和5月，去年同期收益",
        "rewrite-execution-routing": "今年3月和5月，去年同期收益",
        "rewrite-binding-context": "今年3月和5月，去年同期收益",
        "rewrite-validation-contract": "今年3月和5月，去年同期收益",
        "time-clarification-rewrite": "今年3月和5月，去年同期收益",
        "time-plan-schema": "最近5个工作日收益",
        "resolved-plan-schema": "最近5个工作日收益",
        "two-stage-planner": "今年3月和5月，去年同期收益",
        "plan-post-processor": "最近一个月每周收益",
        "business-calendar-filter-semantics": "最近5个工作日收益",
        "pipeline-evaluation-framework": "最近5个休息日收益",
    }

    query_to_tags = {}
    for case in LAYER1_GOLDEN_CASES:
        query_to_tags.setdefault(case["query"], set()).update(case["capability_tags"])

    for capability, query in representatives.items():
        assert capability in query_to_tags[query], capability


def test_layer1_golden_cases_pass_authoring_lint_and_self_compare() -> None:
    for case in LAYER1_GOLDEN_CASES:
        lint_layer1_case(case)
        result = resolved_plan_equals(case["expected_resolved_plan"], case["expected_resolved_plan"])
        assert result.passed, case["query"]


def test_layer1_tier1_and_tier2_fillers_use_natural_queries() -> None:
    for case in LAYER1_GOLDEN_CASES:
        if case["tier"] not in {1, 2}:
            continue
        assert "补充案例" not in case["query"], case["query"]
        render_text = case["expected_time_plan"].units[0].render_text
        assert render_text in case["query"], case["query"]
