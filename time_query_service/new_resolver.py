from __future__ import annotations

import time
from typing import Any

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.carrier_materializer import materialize_carrier
from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.resolved_plan import (
    ComparisonDegradedReason,
    Interval,
    IntervalTree,
    ResolvedComparison,
    ResolvedComparisonPair,
    ResolvedNode,
    ResolvedPlan,
    TreeLabels,
)
from time_query_service.time_plan import Comparison, ComparisonPair, DerivedContent, StandaloneContent, TimePlan, Unit
from time_query_service.tree_ops import shift_tree


def resolve_plan(
    plan: TimePlan,
    *,
    business_calendar: BusinessCalendarPort,
    pipeline_logging_enabled: bool = False,
) -> ResolvedPlan:
    if not isinstance(plan, TimePlan):
        raise TypeError("resolve_plan expects TimePlan")

    nodes: dict[str, ResolvedNode] = {}
    units_by_id = {unit.unit_id: unit for unit in plan.units}
    ordered_units = _toposort_units(plan.units)

    for unit in ordered_units:
        started = time.perf_counter()
        if unit.needs_clarification:
            nodes[unit.unit_id] = ResolvedNode(needs_clarification=True, reason_kind=unit.reason_kind)
        elif isinstance(unit.content, StandaloneContent):
            try:
                tree = materialize_carrier(
                    unit.content.carrier,
                    system_date=plan.system_date,
                    business_calendar=business_calendar,
                )
            except ValueError as exc:
                if _is_calendar_data_missing_error(exc):
                    nodes[unit.unit_id] = ResolvedNode(
                        needs_clarification=True,
                        reason_kind="calendar_data_missing",
                    )
                else:
                    raise
            else:
                nodes[unit.unit_id] = ResolvedNode(tree=tree, derived_from=[])
        elif isinstance(unit.content, DerivedContent):
            nodes[unit.unit_id] = _resolve_derived_unit(unit, nodes)
        else:
            raise TypeError(f"Unsupported unit content: {type(unit.content)!r}")

        log_pipeline_event(
            "resolver",
            "resolver_step",
            {
                "unit_id": unit.unit_id,
                "anchor_kind": _anchor_kind_for_unit(unit),
                "modifier_chain_len": _modifier_len_for_unit(unit),
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            },
            enabled=pipeline_logging_enabled,
        )

    comparisons = [_resolve_comparison(comparison, nodes, units_by_id) for comparison in plan.comparisons]
    return ResolvedPlan(nodes=nodes, comparisons=comparisons)


def _toposort_units(units: list[Unit]) -> list[Unit]:
    units_by_id = {unit.unit_id: unit for unit in units}
    temporary: set[str] = set()
    permanent: set[str] = set()
    order: list[Unit] = []

    def visit(unit_id: str) -> None:
        if unit_id in permanent:
            return
        if unit_id in temporary:
            raise ValueError(f"Derivation cycle detected at {unit_id}")
        temporary.add(unit_id)
        unit = units_by_id[unit_id]
        if isinstance(unit.content, DerivedContent):
            for source in unit.content.sources:
                if source.source_unit_id not in units_by_id:
                    raise ValueError(f"Unknown source unit_id: {source.source_unit_id}")
                visit(source.source_unit_id)
        temporary.remove(unit_id)
        permanent.add(unit_id)
        order.append(unit)

    for unit in units:
        visit(unit.unit_id)
    return order


def _resolve_derived_unit(unit: Unit, nodes: dict[str, ResolvedNode]) -> ResolvedNode:
    assert isinstance(unit.content, DerivedContent)
    children: list[IntervalTree] = []

    for source in unit.content.sources:
        source_node = nodes[source.source_unit_id]
        if source_node.needs_clarification or source_node.tree is None:
            child = IntervalTree(
                role="derived_source",
                intervals=[],
                children=[],
                labels=TreeLabels(
                    source_unit_id=source.source_unit_id,
                    degraded=True,
                    degraded_source_reason_kind=source_node.reason_kind,
                    derivation_transform_summary=source.transform.model_dump(mode="python"),
                ),
            )
        else:
            shifted = shift_tree(source_node.tree, source.transform.model_dump(mode="python"))
            child = IntervalTree(
                role="derived_source",
                intervals=[shifted.labels.absolute_core_time] if shifted.labels.absolute_core_time is not None else [],
                children=[shifted],
                labels=TreeLabels(
                    source_unit_id=source.source_unit_id,
                    absolute_core_time=shifted.labels.absolute_core_time,
                    degraded=False,
                    derivation_transform_summary=source.transform.model_dump(mode="python"),
                ),
            )
        children.append(child)

    healthy_intervals = [child.labels.absolute_core_time for child in children if child.labels.absolute_core_time is not None]
    tree = IntervalTree(
        role="derived",
        intervals=healthy_intervals,
        children=children,
        labels=TreeLabels(),
    )
    if healthy_intervals:
        return ResolvedNode(tree=tree, derived_from=[source.source_unit_id for source in unit.content.sources])
    return ResolvedNode(
        tree=tree,
        needs_clarification=True,
        reason_kind="all_sources_degraded",
        derived_from=[source.source_unit_id for source in unit.content.sources],
    )


def _resolve_comparison(
    comparison: Comparison,
    nodes: dict[str, ResolvedNode],
    units_by_id: dict[str, Unit],
) -> ResolvedComparison:
    return ResolvedComparison(
        comparison_id=comparison.comparison_id,
        pairs=[
            _resolve_comparison_pair(pair, nodes, units_by_id[pair.subject_unit_id], units_by_id[pair.reference_unit_id])
            for pair in comparison.pairs
        ],
    )


def _resolve_comparison_pair(
    pair: ComparisonPair,
    nodes: dict[str, ResolvedNode],
    subject_unit: Unit,
    reference_unit: Unit,
) -> ResolvedComparisonPair:
    subject_node = nodes[pair.subject_unit_id]
    reference_node = nodes[pair.reference_unit_id]

    subject_degraded = _endpoint_is_degraded(subject_node, pair.expansion.subject_core_index if pair.expansion else None)
    reference_degraded = _endpoint_is_degraded(reference_node, pair.expansion.reference_core_index if pair.expansion else None)
    degraded = subject_degraded or reference_degraded

    degraded_reason: ComparisonDegradedReason | None = None
    if subject_degraded and reference_degraded:
        degraded_reason = "both_need_clarification"
    elif subject_degraded:
        degraded_reason = "subject_needs_clarification"
    elif reference_degraded:
        degraded_reason = "reference_needs_clarification"

    subject_interval = None if subject_degraded else _comparison_interval_for_node(
        subject_node, subject_unit, pair.expansion.subject_core_index if pair.expansion else None
    )
    reference_interval = None if reference_degraded else _comparison_interval_for_node(
        reference_node, reference_unit, pair.expansion.reference_core_index if pair.expansion else None
    )

    return ResolvedComparisonPair(
        subject_unit_id=pair.subject_unit_id,
        reference_unit_id=pair.reference_unit_id,
        expansion=pair.expansion,
        degraded=degraded,
        degraded_reason=degraded_reason,
        subject_absolute_core_time=subject_interval,
        reference_absolute_core_time=reference_interval,
    )


def _endpoint_is_degraded(node: ResolvedNode, selected_core_index: int | None) -> bool:
    if node.needs_clarification:
        return True
    if node.tree is None:
        return True
    if selected_core_index is not None and node.tree.role == "derived":
        child = node.tree.children[selected_core_index]
        return bool(child.labels.degraded) or child.labels.absolute_core_time is None
    return False


def _comparison_interval_for_node(node: ResolvedNode, unit: Unit, selected_core_index: int | None) -> Interval:
    if node.tree is None:
        raise ValueError("Comparison endpoint tree is missing")
    if selected_core_index is not None:
        return node.tree.children[selected_core_index].labels.absolute_core_time
    return node.tree.labels.absolute_core_time


def _anchor_kind_for_unit(unit: Unit) -> str | None:
    if isinstance(unit.content, StandaloneContent) and unit.content.carrier is not None:
        return unit.content.carrier.anchor.kind
    if isinstance(unit.content, DerivedContent):
        return "derived"
    return None


def _modifier_len_for_unit(unit: Unit) -> int:
    if isinstance(unit.content, StandaloneContent) and unit.content.carrier is not None:
        return len(unit.content.carrier.modifiers)
    return 0


def _is_calendar_data_missing_error(exc: ValueError) -> bool:
    message = str(exc)
    return message.startswith("Missing calendar event span") or message.startswith("Missing business calendar data")
