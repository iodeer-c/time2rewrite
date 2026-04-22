from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.carrier_materializer import materialize_carrier
from time_query_service.resolved_plan import Interval, IntervalTree, ResolvedNode, ResolvedReasonKind
from time_query_service.time_plan import (
    DerivedContent,
    EnumerationSet,
    GroupedTemporalValue,
    MappedRange,
    RollingByCalendarUnit,
    StandaloneContent,
    Unit,
)


class ComparisonCoreInvariantError(ValueError):
    pass


@dataclass(frozen=True)
class UnitComparisonCore:
    ordinal: int
    source_unit_id: str | None = None


@dataclass(frozen=True)
class UnitComparisonProjection:
    ordered_cores: tuple[UnitComparisonCore, ...]

    @property
    def comparison_cardinality(self) -> int:
        return len(self.ordered_cores)


@dataclass(frozen=True)
class ComparisonCoreDescriptor:
    interval: Interval | None
    degraded: bool
    endpoint_reason_kind: ResolvedReasonKind | None
    source_unit_id: str | None = None


@dataclass(frozen=True)
class ResolvedComparisonProjection:
    comparison_cardinality: int
    aggregate_core: ComparisonCoreDescriptor
    ordered_cores: tuple[ComparisonCoreDescriptor, ...]


def project_unit_comparison_cores(
    unit: Unit,
    *,
    unit_map: dict[str, Unit],
    system_datetime: datetime,
    business_calendar: BusinessCalendarPort,
) -> UnitComparisonProjection:
    if unit.needs_clarification:
        return _unit_single_core_projection()

    if isinstance(unit.content, DerivedContent):
        if len(unit.content.sources) > 1:
            return UnitComparisonProjection(
                tuple(
                    UnitComparisonCore(ordinal=index, source_unit_id=source.source_unit_id)
                    for index, source in enumerate(unit.content.sources)
                )
            )
        source = unit.content.sources[0]
        nested = project_unit_comparison_cores(
            unit_map[source.source_unit_id],
            unit_map=unit_map,
            system_datetime=system_datetime,
            business_calendar=business_calendar,
        )
        return UnitComparisonProjection(
            tuple(
                UnitComparisonCore(ordinal=index, source_unit_id=core.source_unit_id or source.source_unit_id)
                for index, core in enumerate(nested.ordered_cores)
            )
        )

    if not isinstance(unit.content, StandaloneContent) or unit.content.carrier is None:
        return _unit_single_core_projection()

    tree = materialize_carrier(
        unit.content.carrier,
        system_datetime=system_datetime,
        business_calendar=business_calendar,
    )
    if _is_zero_core_tree(tree):
        raise ComparisonCoreInvariantError(f"zero-core comparison endpoint for unit {unit.unit_id}")

    anchor = unit.content.carrier.anchor
    if isinstance(anchor, RollingByCalendarUnit) or tree.role == "filtered_collection":
        return _unit_single_core_projection()
    if isinstance(anchor, (EnumerationSet, GroupedTemporalValue, MappedRange)):
        count = _ordered_surface_count(tree)
        if count <= 0:
            raise ComparisonCoreInvariantError(f"zero-core comparison endpoint for unit {unit.unit_id}")
        return UnitComparisonProjection(tuple(UnitComparisonCore(ordinal=index) for index in range(count)))
    return _unit_single_core_projection()


def project_resolved_comparison_cores(node: ResolvedNode) -> ResolvedComparisonProjection:
    if node.needs_clarification or node.tree is None:
        raise ComparisonCoreInvariantError("runtime comparison projection requires a healthy resolved node")
    return _project_interval_tree(node.tree)


def _project_interval_tree(tree: IntervalTree, inherited_source_unit_id: str | None = None) -> ResolvedComparisonProjection:
    if tree.role == "derived":
        return _project_derived_tree(tree)
    if _is_zero_core_tree(tree):
        raise ComparisonCoreInvariantError(f"zero-core runtime projection for healthy tree role={tree.role}")

    if tree.role == "filtered_collection":
        return _single_core_runtime_projection(_descriptor_from_single_tree(tree, inherited_source_unit_id))

    if tree.role in {"union", "grouped_member"}:
        ordered = _ordered_core_descriptors(tree, inherited_source_unit_id)
        if len(ordered) == 1:
            return _single_core_runtime_projection(ordered[0])
        aggregate = ComparisonCoreDescriptor(
            interval=_structural_aggregate_interval(tree),
            degraded=False,
            endpoint_reason_kind=None,
            source_unit_id=inherited_source_unit_id,
        )
        return _resolved_projection(aggregate, ordered)

    return _single_core_runtime_projection(_descriptor_from_single_tree(tree, inherited_source_unit_id))


def _project_derived_tree(tree: IntervalTree) -> ResolvedComparisonProjection:
    if not tree.children:
        raise ComparisonCoreInvariantError("derived tree must contain derived_source children")

    if len(tree.children) > 1:
        ordered = tuple(_descriptor_from_child(child) for child in tree.children)
        if len(ordered) == 1:
            return _single_core_runtime_projection(ordered[0])
        aggregate = ComparisonCoreDescriptor(
            interval=_structural_aggregate_interval(tree),
            degraded=False,
            endpoint_reason_kind=None,
            source_unit_id=None,
        )
        return _resolved_projection(aggregate, ordered)

    source_child = tree.children[0]
    source_unit_id = source_child.labels.source_unit_id
    if source_unit_id is None:
        raise ComparisonCoreInvariantError("single-source derived child must declare labels.source_unit_id")
    if not source_child.children:
        raise ComparisonCoreInvariantError("single-source derived child must wrap one transformed subtree")

    nested = _project_interval_tree(source_child.children[0], inherited_source_unit_id=source_unit_id)
    ordered = tuple(
        ComparisonCoreDescriptor(
            interval=core.interval,
            degraded=core.degraded,
            endpoint_reason_kind=core.endpoint_reason_kind,
            source_unit_id=core.source_unit_id or source_unit_id,
        )
        for core in nested.ordered_cores
    )
    if len(ordered) == 1:
        return _single_core_runtime_projection(ordered[0])

    aggregate = ComparisonCoreDescriptor(
        interval=_structural_aggregate_interval(tree),
        degraded=False,
        endpoint_reason_kind=None,
        source_unit_id=source_unit_id,
    )
    return _resolved_projection(aggregate, ordered)


def _ordered_surface_count(tree: IntervalTree) -> int:
    if tree.children:
        return len(tree.children)
    return len(tree.intervals)


def _ordered_core_descriptors(
    tree: IntervalTree,
    inherited_source_unit_id: str | None = None,
) -> tuple[ComparisonCoreDescriptor, ...]:
    if tree.children:
        return tuple(_descriptor_from_child(child, inherited_source_unit_id) for child in tree.children)
    if tree.intervals:
        return tuple(
            ComparisonCoreDescriptor(
                interval=interval,
                degraded=False,
                endpoint_reason_kind=None,
                source_unit_id=inherited_source_unit_id,
            )
            for interval in tree.intervals
        )
    raise ComparisonCoreInvariantError(f"healthy tree role={tree.role} projected no ordered comparison cores")


def _descriptor_from_single_tree(tree: IntervalTree, inherited_source_unit_id: str | None = None) -> ComparisonCoreDescriptor:
    interval = tree.labels.absolute_core_time
    if interval is None:
        if len(tree.intervals) == 1:
            interval = tree.intervals[0]
        else:
            raise ComparisonCoreInvariantError(f"single-core tree role={tree.role} is missing structural aggregate interval")
    return ComparisonCoreDescriptor(
        interval=interval,
        degraded=False,
        endpoint_reason_kind=None,
        source_unit_id=inherited_source_unit_id,
    )


def _descriptor_from_child(
    child: IntervalTree,
    inherited_source_unit_id: str | None = None,
) -> ComparisonCoreDescriptor:
    interval = child.labels.absolute_core_time
    degraded = bool(child.labels.degraded) or interval is None
    return ComparisonCoreDescriptor(
        interval=interval,
        degraded=degraded,
        endpoint_reason_kind=child.labels.degraded_source_reason_kind if degraded else None,
        source_unit_id=child.labels.source_unit_id or inherited_source_unit_id,
    )


def _resolved_projection(
    aggregate_core: ComparisonCoreDescriptor,
    ordered_cores: Iterable[ComparisonCoreDescriptor],
) -> ResolvedComparisonProjection:
    ordered = tuple(ordered_cores)
    if not ordered:
        raise ComparisonCoreInvariantError("resolved comparison projection requires at least one ordered core")
    projection = ResolvedComparisonProjection(
        comparison_cardinality=len(ordered),
        aggregate_core=aggregate_core,
        ordered_cores=ordered,
    )
    if projection.comparison_cardinality != len(projection.ordered_cores):
        raise ComparisonCoreInvariantError("comparison_cardinality must equal len(ordered_cores)")
    return projection


def _single_core_runtime_projection(core: ComparisonCoreDescriptor) -> ResolvedComparisonProjection:
    return _resolved_projection(core, (core,))


def _unit_single_core_projection() -> UnitComparisonProjection:
    return UnitComparisonProjection((UnitComparisonCore(ordinal=0),))


def _is_zero_core_tree(tree: IntervalTree) -> bool:
    return tree.labels.absolute_core_time is None and not tree.children and not tree.intervals


def _structural_aggregate_interval(tree: IntervalTree) -> Interval:
    if tree.labels.absolute_core_time is not None:
        return tree.labels.absolute_core_time
    if tree.intervals:
        return _bounding_interval(tree.intervals)
    raise ComparisonCoreInvariantError(f"healthy tree role={tree.role} has no structural aggregate interval")


def _bounding_interval(intervals: Iterable[Interval]) -> Interval:
    interval_list = list(intervals)
    if not interval_list:
        raise ComparisonCoreInvariantError("cannot compute bounding interval from empty interval list")
    start = min(interval.start for interval in interval_list)
    end = max(interval.end for interval in interval_list)
    end_inclusive = any(interval.end == end and interval.end_inclusive for interval in interval_list)
    return Interval(start=start, end=end, end_inclusive=end_inclusive)
