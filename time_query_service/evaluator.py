from __future__ import annotations

from dataclasses import dataclass, field
import argparse
import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.clarification_writer import ClarificationFact, build_clarification_facts, render_clarified_query
from time_query_service.config import get_business_calendar_root
from time_query_service.llm import LLMFactory, LLMRuntimeConfig, load_llm_runtime_config
from time_query_service.post_processor import (
    StageAOutput,
    StageAUnitOutput,
    StageBOutput,
    assemble_time_plan,
)
from time_query_service.resolved_plan import Interval, IntervalTree, ResolvedComparison, ResolvedComparisonPair, ResolvedNode, ResolvedPlan
from time_query_service.new_resolver import resolve_plan
from time_query_service.stage_a_planner import run_stage_a
from time_query_service.stage_b_planner import StageBRequest, run_stage_b_batch
from time_query_service.time_plan import GrainExpansion, GroupedTemporalValue, TimePlan


@dataclass(frozen=True)
class ComparatorDiff:
    path: str
    expected: Any
    actual: Any


@dataclass(frozen=True)
class ComparatorResult:
    diffs: list[ComparatorDiff] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.diffs


@dataclass
class GoldenCaseAuthoringError(Exception):
    details: str

    def __str__(self) -> str:
        return self.details


def case_passes(result: ComparatorResult) -> bool:
    return result.passed


def evaluate_stage_a_golden(
    *,
    cases: list[dict[str, Any]],
    text_runner: Any,
    system_date: str,
    timezone: str,
    pipeline_logging_enabled: bool = False,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    passed_cases = 0
    for case in cases:
        actual = run_stage_a(
            text_runner=text_runner,
            query=case["query"],
            system_date=system_date,
            timezone=timezone,
            pipeline_logging_enabled=pipeline_logging_enabled,
        )
        comparison = stage_a_match(case["expected"], actual)
        passed = case_passes(comparison)
        passed_cases += int(passed)
        results.append(
            {
                "query": case["query"],
                "passed": passed,
                "diffs": _serialize_diffs(comparison.diffs),
                "actual": actual.model_dump(mode="python"),
            }
        )
    return _evaluation_report(results)


def evaluate_stage_b_golden(
    *,
    cases: list[dict[str, Any]],
    text_runner: Any,
    system_date: str,
    timezone: str,
    pipeline_logging_enabled: bool = False,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for index, case in enumerate(cases):
        actual = run_stage_b_batch(
            text_runner=text_runner,
            requests=[StageBRequest(unit_id=f"u{index+1}", text=case["text"])],
            system_date=system_date,
            timezone=timezone,
            max_concurrent=1,
            pipeline_logging_enabled=pipeline_logging_enabled,
        )[0]
        comparison = stage_b_match(case["expected"], actual)
        results.append(
            {
                "text": case["text"],
                "passed": case_passes(comparison),
                "diffs": _serialize_diffs(comparison.diffs),
                "actual": actual.model_dump(mode="python"),
            }
        )
    return _evaluation_report(results)


def evaluate_layer1_golden(
    *,
    cases: list[dict[str, Any]],
    stage_a_runner: Any,
    stage_b_runner: Any,
    business_calendar: BusinessCalendarPort,
    max_stage_b_concurrent: int = 10,
    pipeline_logging_enabled: bool = False,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    tier_buckets: dict[int, list[bool]] = {}
    clarified_query_outcomes: list[bool] = []

    for case in cases:
        time_plan = None
        resolved_plan = None
        clarification_facts: list[ClarificationFact] | None = None
        clarified_query: str | None = None
        clarified_query_validation: ComparatorResult | None = None
        try:
            stage_a = run_stage_a(
                text_runner=stage_a_runner,
                query=case["query"],
                system_date=case["system_date"],
                timezone="Asia/Shanghai",
                pipeline_logging_enabled=pipeline_logging_enabled,
            )
            requests = [
                StageBRequest(
                    unit_id=unit.unit_id or f"u{index+1}",
                    text=unit.self_contained_text or unit.render_text,
                    surface_hint=unit.surface_hint,
                )
                for index, unit in enumerate(stage_a.units)
                if unit.content_kind == "standalone"
            ]
            stage_b_outputs = run_stage_b_batch(
                text_runner=stage_b_runner,
                requests=requests,
                system_date=case["system_date"],
                timezone="Asia/Shanghai",
                max_concurrent=max_stage_b_concurrent,
                pipeline_logging_enabled=pipeline_logging_enabled,
            )
            time_plan = assemble_time_plan(
                stage_a,
                {
                    request.unit_id: output
                    for request, output in zip(requests, stage_b_outputs, strict=True)
                },
            )
            resolved_plan = resolve_plan(
                time_plan,
                business_calendar=business_calendar,
                pipeline_logging_enabled=pipeline_logging_enabled,
            )
            clarification_facts = build_clarification_facts(
                original_query=case["query"],
                time_plan=time_plan,
                resolved_plan=resolved_plan,
            )
            clarified_query = render_clarified_query(
                original_query=case["query"],
                clarification_facts=clarification_facts,
            )
            clarified_query_validation = clarified_query_completeness(
                original_query=case["query"],
                clarification_facts=clarification_facts,
                clarified_query=clarified_query,
            )
        except Exception as exc:  # noqa: BLE001 - evaluation should record case failure and continue
            comparison = ComparatorResult(
                diffs=[
                    ComparatorDiff(
                        "pipeline.execution",
                        "successful_pipeline_execution",
                        f"{type(exc).__name__}: {exc}",
                    )
                ],
            )
        else:
            comparison = resolved_plan_equals(case["expected_resolved_plan"], resolved_plan)
        passed = case_passes(comparison)
        tier_buckets.setdefault(case["tier"], []).append(passed)
        if clarified_query_validation is not None:
            clarified_query_outcomes.append(clarified_query_validation.passed)
        results.append(
            {
                "query": case["query"],
                "tier": case["tier"],
                "passed": passed,
                "diffs": _serialize_diffs(comparison.diffs),
                "actual_time_plan": time_plan.model_dump(mode="python") if time_plan is not None else None,
                "actual_resolved_plan": resolved_plan.model_dump(mode="python") if resolved_plan is not None else None,
                "clarification_items": None
                if clarification_facts is None
                else [fact.model_dump(mode="python") for fact in clarification_facts],
                "clarified_query": clarified_query,
                "clarified_query_validation": None
                if clarified_query_validation is None
                else {
                    "passed": clarified_query_validation.passed,
                    "diffs": _serialize_diffs(clarified_query_validation.diffs),
                },
            }
        )

    report = _evaluation_report(results)
    report["tier_summary"] = {
        tier: {
            "total_cases": len(outcomes),
            "passed_cases": sum(1 for outcome in outcomes if outcome),
            "failed_cases": sum(1 for outcome in outcomes if not outcome),
            "pass_rate": (sum(1 for outcome in outcomes if outcome) / len(outcomes)) if outcomes else 0.0,
        }
        for tier, outcomes in sorted(tier_buckets.items())
    }
    report["clarified_query_summary"] = {
        "total_cases": len(clarified_query_outcomes),
        "passed_cases": sum(1 for outcome in clarified_query_outcomes if outcome),
        "failed_cases": sum(1 for outcome in clarified_query_outcomes if not outcome),
        "accuracy": (
            sum(1 for outcome in clarified_query_outcomes if outcome) / len(clarified_query_outcomes)
            if clarified_query_outcomes
            else 0.0
        ),
    }
    return report


def build_cutover_gate_summary(
    *,
    stage_a_report: dict[str, Any],
    stage_b_report: dict[str, Any],
    layer1_report: dict[str, Any],
) -> dict[str, Any]:
    raw_tier_summary = layer1_report.get("tier_summary", {})
    tier_summary: dict[int, dict[str, Any]] = {}
    for raw_key, payload in raw_tier_summary.items():
        try:
            tier_summary[int(raw_key)] = payload
        except (TypeError, ValueError):
            continue
    tier1 = tier_summary.get(1, {"pass_rate": 0.0})
    tier2 = tier_summary.get(2, {"pass_rate": 0.0})
    failing_tier3 = [
        result["query"]
        for result in layer1_report.get("results", [])
        if result.get("tier") == 3 and not result.get("passed")
    ]
    passes_cutover_gate = (
        stage_a_report["summary"]["accuracy"] >= 0.95
        and stage_b_report["summary"]["accuracy"] >= 0.95
        and tier1["pass_rate"] == 1.0
        and tier2["pass_rate"] >= 0.8
    )
    return {
        "stage_a_accuracy": stage_a_report["summary"]["accuracy"],
        "stage_b_accuracy": stage_b_report["summary"]["accuracy"],
        "tier1_pass_rate": tier1["pass_rate"],
        "tier2_pass_rate": tier2["pass_rate"],
        "tier3_failed_queries": failing_tier3,
        "passes_cutover_gate": passes_cutover_gate,
    }


def stage_a_match(
    expected: StageAOutput | dict[str, Any] | str,
    actual: StageAOutput | dict[str, Any] | str,
) -> ComparatorResult:
    expected_payload = _coerce_stage_a(expected)
    actual_payload = _coerce_stage_a(actual)
    diffs: list[ComparatorDiff] = []

    if len(expected_payload.units) != len(actual_payload.units):
        diffs.append(ComparatorDiff("units", len(expected_payload.units), len(actual_payload.units)))

    expected_order = [unit.unit_id or f"__index_{index}__" for index, unit in enumerate(expected_payload.units)]
    actual_order = [unit.unit_id or f"__index_{index}__" for index, unit in enumerate(actual_payload.units)]
    if expected_order != actual_order:
        diffs.append(ComparatorDiff("units.order", expected_order, actual_order))

    expected_units = {unit.unit_id or f"__index_{index}__": unit for index, unit in enumerate(expected_payload.units)}
    actual_units = {unit.unit_id or f"__index_{index}__": unit for index, unit in enumerate(actual_payload.units)}
    if set(expected_units) != set(actual_units):
        diffs.append(ComparatorDiff("units.keys", sorted(expected_units), sorted(actual_units)))

    for unit_id in sorted(set(expected_units) & set(actual_units)):
        _compare_stage_a_unit(expected_units[unit_id], actual_units[unit_id], path=f"units[{unit_id}]", diffs=diffs)

    expected_source_edges = {
        (unit.unit_id or f"__anon_{index}__", source.source_unit_id)
        for index, unit in enumerate(expected_payload.units)
        for source in unit.sources
    }
    actual_source_edges = {
        (unit.unit_id or f"__anon_{index}__", source.source_unit_id)
        for index, unit in enumerate(actual_payload.units)
        for source in unit.sources
    }
    if expected_source_edges != actual_source_edges:
        diffs.append(ComparatorDiff("sources", sorted(expected_source_edges), sorted(actual_source_edges)))

    expected_pairs = {
        (comparison.comparison_id, pair.subject_unit_id, pair.reference_unit_id)
        for comparison in expected_payload.comparisons
        for pair in comparison.pairs
    }
    actual_pairs = {
        (comparison.comparison_id, pair.subject_unit_id, pair.reference_unit_id)
        for comparison in actual_payload.comparisons
        for pair in comparison.pairs
    }
    if expected_pairs != actual_pairs:
        diffs.append(ComparatorDiff("comparisons", sorted(expected_pairs), sorted(actual_pairs)))

    return ComparatorResult(diffs=diffs)


def stage_b_match(
    expected: StageBOutput | dict[str, Any] | str,
    actual: StageBOutput | dict[str, Any] | str,
) -> ComparatorResult:
    expected_payload = _coerce_stage_b(expected)
    actual_payload = _coerce_stage_b(actual)
    diffs: list[ComparatorDiff] = []

    if expected_payload.needs_clarification != actual_payload.needs_clarification:
        diffs.append(
            ComparatorDiff(
                "needs_clarification",
                expected_payload.needs_clarification,
                actual_payload.needs_clarification,
            )
        )
    if expected_payload.reason_kind != actual_payload.reason_kind:
        diffs.append(ComparatorDiff("reason_kind", expected_payload.reason_kind, actual_payload.reason_kind))

    if expected_payload.carrier is None or actual_payload.carrier is None:
        if expected_payload.carrier != actual_payload.carrier:
            diffs.append(ComparatorDiff("carrier", expected_payload.carrier, actual_payload.carrier))
        return ComparatorResult(diffs=diffs)

    if expected_payload.carrier.anchor.kind != actual_payload.carrier.anchor.kind:
        diffs.append(
            ComparatorDiff(
                "carrier.anchor.kind",
                expected_payload.carrier.anchor.kind,
                actual_payload.carrier.anchor.kind,
            )
        )

    expected_modifier_kinds = [modifier.kind for modifier in expected_payload.carrier.modifiers]
    actual_modifier_kinds = [modifier.kind for modifier in actual_payload.carrier.modifiers]
    if expected_modifier_kinds != actual_modifier_kinds:
        diffs.append(ComparatorDiff("carrier.modifiers[*].kind", expected_modifier_kinds, actual_modifier_kinds))

    _compare_plain_values(
        expected_payload.model_dump(mode="python"),
        actual_payload.model_dump(mode="python"),
        path="",
        diffs=diffs,
    )
    return ComparatorResult(diffs=diffs)


def interval_equals(expected: Interval, actual: Interval) -> bool:
    return (
        expected.start == actual.start
        and expected.end == actual.end
        and expected.end_inclusive == actual.end_inclusive
    )


def resolved_plan_equals(expected: ResolvedPlan, actual: ResolvedPlan) -> ComparatorResult:
    diffs: list[ComparatorDiff] = []
    _compare_node_keyset(expected.nodes, actual.nodes, diffs)
    for unit_id in sorted(set(expected.nodes) & set(actual.nodes)):
        _compare_node(
            expected.nodes[unit_id],
            actual.nodes[unit_id],
            path=f"nodes[{unit_id}]",
            diffs=diffs,
        )
    _compare_comparisons(expected.comparisons, actual.comparisons, diffs)
    return ComparatorResult(diffs=diffs)


def clarified_query_completeness(
    *,
    original_query: str,
    clarification_facts: list[ClarificationFact],
    clarified_query: str,
) -> ComparatorResult:
    diffs: list[ComparatorDiff] = []
    if not clarified_query.startswith(original_query):
        diffs.append(ComparatorDiff("clarified_query.prefix", original_query, clarified_query))

    for index, fact in enumerate(clarification_facts):
        path = f"clarification_facts[{index}]"
        if fact.label not in clarified_query:
            diffs.append(ComparatorDiff(f"{path}.label", fact.label, clarified_query))
        if fact.status == "resolved":
            if fact.resolved_text is None or fact.resolved_text not in clarified_query:
                diffs.append(ComparatorDiff(f"{path}.resolved_text", fact.resolved_text, clarified_query))
            if fact.grouping_grain is not None:
                expected_phrase = _expected_grouping_phrase(fact.grouping_grain)
                if expected_phrase not in clarified_query:
                    diffs.append(ComparatorDiff(f"{path}.grouping_grain", expected_phrase, clarified_query))
        elif "无法确定" not in clarified_query:
            diffs.append(ComparatorDiff(f"{path}.status", "无法确定", clarified_query))
    return ComparatorResult(diffs=diffs)


def lint_layer1_case(case: dict[str, Any]) -> None:
    expected_time_plan = case.get("expected_time_plan")
    if expected_time_plan is None:
        raise GoldenCaseAuthoringError("expected_time_plan is required for Layer 1 authoring lint")
    expected_resolved_plan = case.get("expected_resolved_plan")
    if expected_resolved_plan is None:
        raise GoldenCaseAuthoringError("expected_resolved_plan is required for Layer 1 authoring lint")

    if not isinstance(expected_time_plan, TimePlan):
        expected_time_plan = TimePlan.model_validate(expected_time_plan)
    if not isinstance(expected_resolved_plan, ResolvedPlan):
        expected_resolved_plan = ResolvedPlan.model_validate(expected_resolved_plan)

    for unit in expected_time_plan.units:
        if unit.content.content_kind != "standalone" or unit.content.carrier is None:
            continue
        if any(isinstance(modifier, GrainExpansion) and modifier.target_grain != "day" for modifier in unit.content.carrier.modifiers):
            raise GoldenCaseAuthoringError(
                f"expected_time_plan contains transient non-day GrainExpansion on unit {unit.unit_id}"
            )
        if isinstance(unit.content.carrier.anchor, GroupedTemporalValue) and unit.content.carrier.anchor.selector != "all":
            raise GoldenCaseAuthoringError(
                f'expected_time_plan unit {unit.unit_id} must keep GroupedTemporalValue.selector="all"'
            )

    for unit_id, node in expected_resolved_plan.nodes.items():
        if node.tree is not None:
            _lint_tree_intervals(node.tree, path=f"nodes[{unit_id}].tree")


def _coerce_stage_a(payload: StageAOutput | dict[str, Any] | str) -> StageAOutput:
    try:
        if isinstance(payload, StageAOutput):
            return payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        return StageAOutput.model_validate(payload)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise GoldenCaseAuthoringError(f"invalid Stage A payload: {exc}") from exc


def _coerce_stage_b(payload: StageBOutput | dict[str, Any] | str) -> StageBOutput:
    try:
        if isinstance(payload, StageBOutput):
            return payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        return StageBOutput.model_validate(payload)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise GoldenCaseAuthoringError(f"invalid Stage B payload: {exc}") from exc


def _compare_stage_a_unit(
    expected: StageAUnitOutput,
    actual: StageAUnitOutput,
    *,
    path: str,
    diffs: list[ComparatorDiff],
) -> None:
    if expected.content_kind != actual.content_kind:
        diffs.append(ComparatorDiff(f"{path}.content_kind", expected.content_kind, actual.content_kind))
    if expected.render_text != actual.render_text:
        diffs.append(ComparatorDiff(f"{path}.render_text", expected.render_text, actual.render_text))
    if expected.self_contained_text != actual.self_contained_text:
        diffs.append(ComparatorDiff(f"{path}.self_contained_text", expected.self_contained_text, actual.self_contained_text))
    if expected.surface_hint != actual.surface_hint:
        diffs.append(ComparatorDiff(f"{path}.surface_hint", expected.surface_hint, actual.surface_hint))


def _compare_plain_values(expected: Any, actual: Any, *, path: str, diffs: list[ComparatorDiff]) -> None:
    if isinstance(expected, dict) and isinstance(actual, dict):
        keys = sorted(set(expected) | set(actual))
        for key in keys:
            next_path = f"{path}.{key}" if path else str(key)
            _compare_plain_values(expected.get(key), actual.get(key), path=next_path, diffs=diffs)
        return
    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            diffs.append(ComparatorDiff(path, len(expected), len(actual)))
            return
        for index, (expected_value, actual_value) in enumerate(zip(expected, actual, strict=True)):
            next_path = f"{path}[{index}]"
            _compare_plain_values(expected_value, actual_value, path=next_path, diffs=diffs)
        return
    if expected != actual:
        diffs.append(ComparatorDiff(path, expected, actual))


def _compare_node_keyset(expected: dict[str, ResolvedNode], actual: dict[str, ResolvedNode], diffs: list[ComparatorDiff]) -> None:
    if set(expected) != set(actual):
        diffs.append(
            ComparatorDiff(
                path="nodes",
                expected=sorted(expected),
                actual=sorted(actual),
            )
        )


def _compare_node(expected: ResolvedNode, actual: ResolvedNode, *, path: str, diffs: list[ComparatorDiff]) -> None:
    if expected.needs_clarification != actual.needs_clarification:
        diffs.append(ComparatorDiff(f"{path}.needs_clarification", expected.needs_clarification, actual.needs_clarification))
    if expected.reason_kind != actual.reason_kind:
        diffs.append(ComparatorDiff(f"{path}.reason_kind", expected.reason_kind, actual.reason_kind))
    if expected.derived_from != actual.derived_from:
        diffs.append(ComparatorDiff(f"{path}.derived_from", expected.derived_from, actual.derived_from))
    if expected.tree is None or actual.tree is None:
        if expected.tree != actual.tree:
            diffs.append(ComparatorDiff(f"{path}.tree", expected.tree, actual.tree))
        return
    _compare_tree(expected.tree, actual.tree, path=f"{path}.tree", diffs=diffs)


def _compare_tree(expected: IntervalTree, actual: IntervalTree, *, path: str, diffs: list[ComparatorDiff]) -> None:
    if expected.role != actual.role:
        diffs.append(ComparatorDiff(f"{path}.role", expected.role, actual.role))
    _compare_interval_list(expected.intervals, actual.intervals, path=f"{path}.intervals", diffs=diffs)
    _compare_labels(expected.labels.model_dump(mode="python"), actual.labels.model_dump(mode="python"), path=f"{path}.labels", diffs=diffs)
    if len(expected.children) != len(actual.children):
        diffs.append(ComparatorDiff(f"{path}.children", len(expected.children), len(actual.children)))
        return
    for index, (expected_child, actual_child) in enumerate(zip(expected.children, actual.children, strict=True)):
        _compare_tree(expected_child, actual_child, path=f"{path}.children[{index}]", diffs=diffs)


def _compare_interval_list(expected: list[Interval], actual: list[Interval], *, path: str, diffs: list[ComparatorDiff]) -> None:
    if len(expected) != len(actual):
        diffs.append(ComparatorDiff(path, len(expected), len(actual)))
        return
    for index, (expected_interval, actual_interval) in enumerate(zip(expected, actual, strict=True)):
        _compare_interval(expected_interval, actual_interval, path=f"{path}[{index}]", diffs=diffs)


def _compare_interval(expected: Interval, actual: Interval, *, path: str, diffs: list[ComparatorDiff]) -> None:
    if expected.start != actual.start:
        diffs.append(ComparatorDiff(f"{path}.start", expected.start, actual.start))
    if expected.end != actual.end:
        diffs.append(ComparatorDiff(f"{path}.end", expected.end, actual.end))
    if expected.end_inclusive != actual.end_inclusive:
        diffs.append(ComparatorDiff(f"{path}.end_inclusive", expected.end_inclusive, actual.end_inclusive))


def _compare_labels(expected: dict[str, Any], actual: dict[str, Any], *, path: str, diffs: list[ComparatorDiff]) -> None:
    keys = sorted(set(expected) | set(actual))
    for key in keys:
        if expected.get(key) != actual.get(key):
            diffs.append(ComparatorDiff(f"{path}.{key}", expected.get(key), actual.get(key)))


def _compare_comparisons(expected: list[ResolvedComparison], actual: list[ResolvedComparison], diffs: list[ComparatorDiff]) -> None:
    if len(expected) != len(actual):
        diffs.append(ComparatorDiff("comparisons", len(expected), len(actual)))
        return
    for index, (expected_comparison, actual_comparison) in enumerate(zip(expected, actual, strict=True)):
        path = f"comparisons[{index}]"
        if expected_comparison.comparison_id != actual_comparison.comparison_id:
            diffs.append(ComparatorDiff(f"{path}.comparison_id", expected_comparison.comparison_id, actual_comparison.comparison_id))
        _compare_comparison_pairs(expected_comparison.pairs, actual_comparison.pairs, path=f"{path}.pairs", diffs=diffs)


def _compare_comparison_pairs(
    expected: list[ResolvedComparisonPair],
    actual: list[ResolvedComparisonPair],
    *,
    path: str,
    diffs: list[ComparatorDiff],
) -> None:
    if len(expected) != len(actual):
        diffs.append(ComparatorDiff(path, len(expected), len(actual)))
        return
    for index, (expected_pair, actual_pair) in enumerate(zip(expected, actual, strict=True)):
        pair_path = f"{path}[{index}]"
        if expected_pair.subject_unit_id != actual_pair.subject_unit_id:
            diffs.append(ComparatorDiff(f"{pair_path}.subject_unit_id", expected_pair.subject_unit_id, actual_pair.subject_unit_id))
        if expected_pair.reference_unit_id != actual_pair.reference_unit_id:
            diffs.append(ComparatorDiff(f"{pair_path}.reference_unit_id", expected_pair.reference_unit_id, actual_pair.reference_unit_id))
        if expected_pair.degraded != actual_pair.degraded:
            diffs.append(ComparatorDiff(f"{pair_path}.degraded", expected_pair.degraded, actual_pair.degraded))
        if expected_pair.degraded_reason != actual_pair.degraded_reason:
            diffs.append(ComparatorDiff(f"{pair_path}.degraded_reason", expected_pair.degraded_reason, actual_pair.degraded_reason))
        _compare_optional_interval(
            expected_pair.subject_absolute_core_time,
            actual_pair.subject_absolute_core_time,
            path=f"{pair_path}.subject_absolute_core_time",
            diffs=diffs,
        )
        _compare_optional_interval(
            expected_pair.reference_absolute_core_time,
            actual_pair.reference_absolute_core_time,
            path=f"{pair_path}.reference_absolute_core_time",
            diffs=diffs,
        )
        _compare_expansion(expected_pair.expansion, actual_pair.expansion, path=f"{pair_path}.expansion", diffs=diffs)


def _compare_optional_interval(expected: Interval | None, actual: Interval | None, *, path: str, diffs: list[ComparatorDiff]) -> None:
    if expected is None or actual is None:
        if expected != actual:
            diffs.append(ComparatorDiff(path, expected, actual))
        return
    _compare_interval(expected, actual, path=path, diffs=diffs)


def _compare_expansion(expected: Any, actual: Any, *, path: str, diffs: list[ComparatorDiff]) -> None:
    if expected is None or actual is None:
        if expected != actual:
            diffs.append(ComparatorDiff(path, expected, actual))
        return
    expected_payload = expected.model_dump(mode="python") if hasattr(expected, "model_dump") else dict(expected)
    actual_payload = actual.model_dump(mode="python") if hasattr(actual, "model_dump") else dict(actual)
    for key in sorted(set(expected_payload) | set(actual_payload)):
        if expected_payload.get(key) != actual_payload.get(key):
            diffs.append(ComparatorDiff(f"{path}.{key}", expected_payload.get(key), actual_payload.get(key)))


def _lint_tree_intervals(tree: IntervalTree, *, path: str) -> None:
    expected_intervals = _expected_intervals_for_role(tree)
    if len(tree.intervals) != len(expected_intervals):
        raise GoldenCaseAuthoringError(
            f"{path} violates per-role intervals construction rule for role={tree.role}"
        )
    for actual, expected in zip(tree.intervals, expected_intervals, strict=True):
        if not interval_equals(expected, actual):
            raise GoldenCaseAuthoringError(
                f"{path} violates per-role intervals construction rule for role={tree.role}"
            )
    if tree.role == "grouped_member":
        starts = [child.labels.absolute_core_time.start for child in tree.children if child.labels.absolute_core_time is not None]
        if starts != sorted(starts):
            raise GoldenCaseAuthoringError(f"{path} grouped_member children must be ascending by start")
    for index, child in enumerate(tree.children):
        _lint_tree_intervals(child, path=f"{path}.children[{index}]")


def _expected_intervals_for_role(tree: IntervalTree) -> list[Interval]:
    if tree.role == "atom":
        return [] if tree.labels.absolute_core_time is None else [tree.labels.absolute_core_time]
    if tree.role == "union":
        return [child.labels.absolute_core_time for child in tree.children if child.labels.absolute_core_time is not None]
    if tree.role == "grouped_member":
        return [child.labels.absolute_core_time for child in tree.children if child.labels.absolute_core_time is not None]
    if tree.role == "filtered_collection":
        return [] if tree.labels.absolute_core_time is None else [tree.labels.absolute_core_time]
    if tree.role == "derived":
        return [child.labels.absolute_core_time for child in tree.children if child.labels.absolute_core_time is not None]
    if tree.role == "derived_source":
        return [] if tree.labels.absolute_core_time is None else [tree.labels.absolute_core_time]
    raise GoldenCaseAuthoringError(f"unsupported tree role for authoring lint: {tree.role}")


def _serialize_diffs(diffs: list[ComparatorDiff]) -> list[dict[str, Any]]:
    return [
        {
            "path": diff.path,
            "expected": diff.expected,
            "actual": diff.actual,
        }
        for diff in diffs
    ]


def _evaluation_report(results: list[dict[str, Any]]) -> dict[str, Any]:
    passed_cases = sum(1 for result in results if result["passed"])
    total_cases = len(results)
    failed_cases = total_cases - passed_cases
    accuracy = (passed_cases / total_cases) if total_cases else 0.0
    return {
        "summary": {
            "total_cases": total_cases,
            "passed_cases": passed_cases,
            "failed_cases": failed_cases,
            "accuracy": accuracy,
        },
        "results": results,
    }


def _expected_grouping_phrase(grain: str) -> str:
    mapping = {
        "day": "自然日",
        "week": "自然周",
        "month": "自然月",
        "quarter": "自然季度",
        "half_year": "自然半年",
        "year": "自然年",
    }
    return mapping.get(grain, grain)


def _create_optional_role_llm(runtime_config: LLMRuntimeConfig, *roles: str) -> Any | None:
    for role in roles:
        config = runtime_config.roles.get(role)
        if config is None:
            continue
        return LLMFactory.create_llm(config)
    return None


def _write_report(payload: dict[str, Any], output_path: Path) -> None:
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run real-model evaluation against the redesign-time-query-two-stage-pipeline golden datasets.")
    parser.add_argument("--suite", choices=["stage_a", "stage_b", "layer1", "all"], default="all")
    parser.add_argument("--llm-config", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--max-stage-b-concurrent", type=int, default=10)
    args = parser.parse_args(argv)

    from tests.fixtures.golden_datasets import LAYER1_GOLDEN_CASES, STAGE_A_GOLDEN_CASES, STAGE_B_GOLDEN_CASES

    runtime_config = load_llm_runtime_config(config_path=args.llm_config)
    stage_a_runner = _create_optional_role_llm(runtime_config, "stage_a", "planner")
    stage_b_runner = _create_optional_role_llm(runtime_config, "stage_b", "planner")
    if stage_a_runner is None or stage_b_runner is None:
        raise RuntimeError("real-model evaluation requires configured stage_a/planner and stage_b/planner roles")
    business_calendar = JsonBusinessCalendar.from_root(root=get_business_calendar_root())

    report: dict[str, Any] = {}
    if args.suite in {"stage_a", "all"}:
        report["stage_a"] = evaluate_stage_a_golden(
            cases=STAGE_A_GOLDEN_CASES,
            text_runner=stage_a_runner,
            system_date="2026-04-17",
            timezone="Asia/Shanghai",
            pipeline_logging_enabled=runtime_config.pipeline_logging.enabled,
        )
    if args.suite in {"stage_b", "all"}:
        report["stage_b"] = evaluate_stage_b_golden(
            cases=STAGE_B_GOLDEN_CASES,
            text_runner=stage_b_runner,
            system_date="2026-04-17",
            timezone="Asia/Shanghai",
            pipeline_logging_enabled=runtime_config.pipeline_logging.enabled,
        )
    if args.suite in {"layer1", "all"}:
        report["layer1"] = evaluate_layer1_golden(
            cases=LAYER1_GOLDEN_CASES,
            stage_a_runner=stage_a_runner,
            stage_b_runner=stage_b_runner,
            business_calendar=business_calendar,
            max_stage_b_concurrent=args.max_stage_b_concurrent,
            pipeline_logging_enabled=runtime_config.pipeline_logging.enabled,
        )
    if args.suite == "all":
        report["cutover_gate"] = build_cutover_gate_summary(
            stage_a_report=report["stage_a"],
            stage_b_report=report["stage_b"],
            layer1_report=report["layer1"],
        )

    if args.output is not None:
        _write_report(report, args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
