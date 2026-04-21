from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError, model_validator

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.carrier_materializer import materialize_carrier
from time_query_service.config import get_business_calendar_root
from time_query_service.derivation_registry import get_derivation_transform_spec
from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.tree_ops import structural_grain
from time_query_service.time_plan import (
    Anchor,
    CalendarEvent,
    CalendarFilter,
    Carrier,
    Comparison,
    ComparisonPair,
    Content,
    DateRange,
    DatetimeRange,
    DerivationSource,
    EnumerationSet,
    GrainExpansion,
    GroupedTemporalValue,
    HolidayEventCollection,
    MemberSelection,
    MappedRange,
    NamedPeriod,
    Offset,
    PreResolverReasonKind,
    RelativeWindow,
    RollingByCalendarUnit,
    RollingWindow,
    StandaloneContent,
    SurfaceFragment,
    TimePlan,
    Unit,
    PairExpansion,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


SurfaceHint = Literal["calendar_grain_rolling"]


class StageAUnitOutput(StrictModel):
    unit_id: str | None = None
    render_text: str
    surface_fragments: list[SurfaceFragment] = Field(default_factory=list)
    content_kind: Literal["standalone", "derived"]
    self_contained_text: str | None = None
    sources: list[DerivationSource] = Field(default_factory=list)
    surface_hint: SurfaceHint | None = None


class StageAComparisonPairOutput(StrictModel):
    subject_unit_id: str
    reference_unit_id: str


class StageAComparisonOutput(StrictModel):
    comparison_id: str
    anchor_text: str
    pairs: list[StageAComparisonPairOutput]


class StageAOutput(StrictModel):
    query: str
    system_datetime: datetime
    timezone: str
    units: list[StageAUnitOutput]
    comparisons: list[StageAComparisonOutput] = Field(default_factory=list)


class StageBOutput(StrictModel):
    carrier: Carrier | None = None
    needs_clarification: bool = False
    reason_kind: PreResolverReasonKind | None = None

    @model_validator(mode="after")
    def validate_xor_contract(self) -> "StageBOutput":
        if self.needs_clarification:
            if self.carrier is not None:
                raise ValueError("degraded StageBOutput MUST NOT carry carrier")
            if self.reason_kind is None:
                raise ValueError("degraded StageBOutput requires reason_kind")
        else:
            if self.carrier is None:
                raise ValueError("healthy StageBOutput requires carrier")
            if self.reason_kind is not None:
                raise ValueError("healthy StageBOutput MUST NOT carry reason_kind")
        return self


@dataclass
class PostProcessorValidationError(Exception):
    layer: int
    stage: str
    details: str
    unit_id: str | None = None

    def __str__(self) -> str:
        unit_part = f", unit={self.unit_id}" if self.unit_id is not None else ""
        return f"Layer {self.layer} validation failed [{self.stage}{unit_part}]: {self.details}"


def assemble_time_plan(
    stage_a_output: StageAOutput | dict[str, Any] | str,
    stage_b_outputs_by_unit: dict[str, StageBOutput | dict[str, Any] | str],
) -> TimePlan:
    try:
        stage_a = _parse_stage_a(stage_a_output)
        stage_b = {key: _parse_stage_b(key, value) for key, value in stage_b_outputs_by_unit.items()}

        unit_ids, lookup_by_original = _allocate_unit_ids(stage_a.units)
        _validate_layer3(stage_a.query, stage_a.units, stage_b, system_datetime=stage_a.system_datetime)
        canonical_stage_b = _canonicalize_stage_b(stage_b)
        units = [
            _assemble_unit(
                stage_a.query,
                unit_ids[index],
                unit,
                canonical_stage_b.get(_stage_b_lookup_key(index, unit)),
                lookup_by_original,
            )
            for index, unit in enumerate(stage_a.units)
        ]
        comparisons = _assemble_comparisons(stage_a.comparisons, lookup_by_original)
        comparisons = _validate_layer4(units, comparisons, system_datetime=stage_a.system_datetime)

        plan = TimePlan(
            query=stage_a.query,
            system_datetime=stage_a.system_datetime,
            timezone=stage_a.timezone,
            units=units,
            comparisons=comparisons,
        )
        from time_query_service.new_plan_validator import validate_time_plan

        validate_time_plan(plan)
        log_pipeline_event(
            "post_processor",
            "post_processor_validation",
            {"layer": 4, "outcome": "success", "details": "assembled_time_plan"},
            enabled=True,
        )
        return plan
    except PostProcessorValidationError as exc:
        log_pipeline_event(
            "post_processor",
            "post_processor_validation",
            {"layer": exc.layer, "outcome": "failure", "details": exc.details, "unit_id": exc.unit_id, "stage": exc.stage},
            enabled=True,
        )
        raise


def _parse_stage_a(payload: StageAOutput | dict[str, Any] | str) -> StageAOutput:
    try:
        if isinstance(payload, StageAOutput):
            return payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        return StageAOutput.model_validate(payload)
    except json.JSONDecodeError as exc:
        raise PostProcessorValidationError(layer=1, stage="stage_a", details=str(exc)) from exc
    except ValidationError as exc:
        raise PostProcessorValidationError(layer=2, stage="stage_a", details=str(exc)) from exc


def _parse_stage_b(unit_id: str, payload: StageBOutput | dict[str, Any] | str) -> StageBOutput:
    try:
        if isinstance(payload, StageBOutput):
            return payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        return StageBOutput.model_validate(payload)
    except json.JSONDecodeError as exc:
        raise PostProcessorValidationError(layer=1, stage="stage_b", unit_id=unit_id, details=str(exc)) from exc
    except ValidationError as exc:
        raise PostProcessorValidationError(layer=2, stage="stage_b", unit_id=unit_id, details=str(exc)) from exc


def _allocate_unit_ids(units: list[StageAUnitOutput]) -> tuple[list[str], dict[str, str]]:
    allocated: list[str] = []
    used: set[str] = set()
    original_to_final: dict[str, str] = {}
    next_index = 1
    for index, unit in enumerate(units):
        original = unit.unit_id
        if original and re.fullmatch(r"u\d+", original) and original not in used:
            final = original
        else:
            while f"u{next_index}" in used:
                next_index += 1
            final = f"u{next_index}"
            next_index += 1
        allocated.append(final)
        used.add(final)
        if original and original not in original_to_final:
            original_to_final[original] = final
        original_to_final[f"__index_{index}__"] = final
    return allocated, original_to_final


def _assemble_unit(
    query: str,
    unit_id: str,
    stage_a_unit: StageAUnitOutput,
    stage_b_output: StageBOutput | None,
    lookup_by_original: dict[str, str],
) -> Unit:
    if stage_a_unit.content_kind == "standalone":
        if stage_b_output is None:
            raise PostProcessorValidationError(
                layer=4,
                stage="post_processor",
                unit_id=unit_id,
                details="missing Stage B output for standalone unit",
            )
        content = StandaloneContent(content_kind="standalone", carrier=stage_b_output.carrier)
        return Unit(
            unit_id=unit_id,
            render_text=stage_a_unit.render_text,
            surface_fragments=stage_a_unit.surface_fragments,
            content=content,
            needs_clarification=stage_b_output.needs_clarification,
            reason_kind=stage_b_output.reason_kind,
        )

    remapped_sources = [
        DerivationSource(
            source_unit_id=_lookup_required_source(lookup_by_original, source.source_unit_id, unit_id),
            transform=source.transform,
        )
        for source in stage_a_unit.sources
    ]
    return Unit(
        unit_id=unit_id,
        render_text=stage_a_unit.render_text,
        surface_fragments=stage_a_unit.surface_fragments,
        content={"content_kind": "derived", "sources": [source.model_dump(mode="python") for source in remapped_sources]},
    )


def _assemble_comparisons(
    comparisons: list[StageAComparisonOutput],
    lookup_by_original: dict[str, str],
) -> list[Comparison]:
    assembled: list[Comparison] = []
    for comparison in comparisons:
        pairs = [
            ComparisonPair(
                subject_unit_id=_lookup_required(lookup_by_original, pair.subject_unit_id, comparison.comparison_id),
                reference_unit_id=_lookup_required(lookup_by_original, pair.reference_unit_id, comparison.comparison_id),
            )
            for pair in comparison.pairs
        ]
        assembled.append(
            Comparison(
                comparison_id=comparison.comparison_id,
                anchor_text=comparison.anchor_text,
                pairs=pairs,
            )
        )
    return assembled


def _validate_layer3(
    query: str,
    units: list[StageAUnitOutput],
    stage_b: dict[str, StageBOutput],
    *,
    system_datetime: datetime,
) -> None:
    for index, unit in enumerate(units):
        if unit.content_kind == "standalone":
            stage_b_output = stage_b.get(_stage_b_lookup_key(index, unit))
            if stage_b_output is None:
                raise PostProcessorValidationError(
                    layer=4,
                    stage="post_processor",
                    unit_id=unit.unit_id or f"__index_{index}__",
                    details="missing Stage B output for standalone unit",
                )
            if stage_b_output.needs_clarification is False and stage_b_output.carrier is None:
                raise PostProcessorValidationError(
                    layer=3,
                    stage="post_processor",
                    unit_id=unit.unit_id or f"__index_{index}__",
                    details="healthy standalone unit requires carrier",
                )
            if stage_b_output.carrier is not None:
                _validate_carrier_semantics(
                    unit,
                    stage_b_output.carrier,
                    unit_id=unit.unit_id or f"__index_{index}__",
                    system_datetime=system_datetime,
                )
        elif unit.content_kind == "derived" and not unit.sources:
            raise PostProcessorValidationError(
                layer=3,
                stage="post_processor",
                unit_id=unit.unit_id or f"__index_{index}__",
                details="derived unit requires non-empty sources",
            )
    _validate_bounded_range_single_unit(query, units, stage_b)


def _validate_layer4(
    units: list[Unit],
    comparisons: list[Comparison],
    *,
    system_datetime: datetime,
) -> list[Comparison]:
    unit_map = {unit.unit_id: unit for unit in units}
    for comparison in comparisons:
        for pair in comparison.pairs:
            if pair.subject_unit_id not in unit_map:
                raise PostProcessorValidationError(
                    layer=4,
                    stage="post_processor",
                    details=f"comparison {comparison.comparison_id} references missing unit_id {pair.subject_unit_id}",
                )
            if pair.reference_unit_id not in unit_map:
                raise PostProcessorValidationError(
                    layer=4,
                    stage="post_processor",
                    details=f"comparison {comparison.comparison_id} references missing unit_id {pair.reference_unit_id}",
                )

    graph = {
        unit.unit_id: [source.source_unit_id for source in unit.content.sources]
        for unit in units
        if unit.content.content_kind == "derived"
    }
    _validate_derivation_transforms(units)
    _assert_acyclic(graph)
    return _expand_comparisons(comparisons, unit_map=unit_map, system_datetime=system_datetime)


def _assert_acyclic(graph: dict[str, list[str]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise PostProcessorValidationError(layer=4, stage="post_processor", details=f"derivation cycle detected at {node}")
        visiting.add(node)
        for dep in graph.get(node, []):
            if dep in graph:
                visit(dep)
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        visit(node)


def _lookup_required(lookup_by_original: dict[str, str], source_id: str, comparison_id: str) -> str:
    if source_id not in lookup_by_original:
        raise PostProcessorValidationError(
            layer=4,
            stage="post_processor",
            details=f"comparison {comparison_id} references missing unit_id {source_id}",
        )
    return lookup_by_original[source_id]


def _lookup_required_source(lookup_by_original: dict[str, str], source_id: str, unit_id: str) -> str:
    if source_id not in lookup_by_original:
        raise PostProcessorValidationError(
            layer=4,
            stage="post_processor",
            unit_id=unit_id,
            details=f"derived unit references missing source_unit_id {source_id}",
        )
    return lookup_by_original[source_id]


def _stage_b_lookup_key(index: int, unit: StageAUnitOutput) -> str:
    return unit.unit_id or f"__index_{index}__"


def _surface_text(query: str, fragments: list[SurfaceFragment]) -> str:
    return "".join(query[fragment.start:fragment.end] for fragment in fragments)


def _validate_bounded_range_single_unit(
    query: str,
    units: list[StageAUnitOutput],
    stage_b: dict[str, StageBOutput],
) -> None:
    for index in range(len(units) - 1):
        left = units[index]
        right = units[index + 1]
        if left.content_kind != "standalone" or right.content_kind != "standalone":
            continue
        if not left.surface_fragments or not right.surface_fragments:
            continue

        left_output = stage_b.get(_stage_b_lookup_key(index, left))
        right_output = stage_b.get(_stage_b_lookup_key(index + 1, right))
        if left_output is None or right_output is None:
            continue
        if left_output.needs_clarification or right_output.needs_clarification:
            continue
        if left_output.carrier is None or right_output.carrier is None:
            continue
        if not _is_bounded_range_endpoint_anchor(left_output.carrier.anchor):
            continue
        if not _is_bounded_range_endpoint_anchor(right_output.carrier.anchor):
            continue

        left_end = max(fragment.end for fragment in left.surface_fragments)
        right_start = min(fragment.start for fragment in right.surface_fragments)
        if left_end >= right_start:
            continue
        connector = query[left_end:right_start].strip()
        if connector not in {"到", "至", "-", "~", "～"}:
            continue
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=left.unit_id or f"__index_{index}__",
            details="bounded range must be emitted as one unit rather than split endpoint units",
        )


def _is_bounded_range_endpoint_anchor(anchor: object) -> bool:
    return isinstance(anchor, (NamedPeriod, DateRange, DatetimeRange))


def _validate_carrier_semantics(
    unit: StageAUnitOutput,
    carrier: Carrier,
    *,
    unit_id: str,
    system_datetime: datetime,
) -> None:
    _validate_non_head_non_day_grain_expansion(carrier, unit_id=unit_id)
    _validate_hour_modifier_topology(carrier, unit_id=unit_id)
    if unit.surface_hint == "calendar_grain_rolling" and _is_calendar_grain_rolling_approximation(carrier):
        calendar_filter = next(
            modifier for modifier in carrier.modifiers if isinstance(modifier, CalendarFilter)
        )
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details=(
                "calendar-class count rolling MUST be emitted as "
                f'RollingByCalendarUnit(day_class="{calendar_filter.day_class}") rather than '
                "RollingWindow(unit=\"day\") + CalendarFilter(...)"
            ),
        )

    anchor = carrier.anchor
    if isinstance(anchor, GroupedTemporalValue):
        if anchor.selector != "all":
            raise PostProcessorValidationError(
                layer=3,
                stage="post_processor",
                unit_id=unit_id,
                details='GroupedTemporalValue selector must stay selector="all"; express subsetting via MemberSelection',
            )
        if not _grouped_parent_is_coarser(anchor):
            raise PostProcessorValidationError(
                layer=3,
                stage="post_processor",
                unit_id=unit_id,
                details="GroupedTemporalValue parent must be strictly coarser than child_grain",
            )
    elif isinstance(anchor, EnumerationSet):
        _validate_enumeration_set(anchor, unit_id=unit_id)
    elif isinstance(anchor, RollingWindow):
        _validate_frozen_rolling_window(anchor, unit_id=unit_id)
    elif isinstance(anchor, RollingByCalendarUnit):
        _validate_frozen_rolling_by_calendar(anchor, unit_id=unit_id)
    elif isinstance(anchor, MappedRange):
        _validate_mapped_range_semantics(anchor, unit_id=unit_id, system_datetime=system_datetime)


def _validate_non_head_non_day_grain_expansion(carrier: Carrier, *, unit_id: str) -> None:
    non_day_indexes = [
        index
        for index, modifier in enumerate(carrier.modifiers)
        if isinstance(modifier, GrainExpansion) and modifier.target_grain not in {"day", "hour"}
    ]
    if not non_day_indexes:
        return
    if non_day_indexes[0] != 0 or len(non_day_indexes) > 1:
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details="non-day GrainExpansion must appear at the head of the chain for post-processor canonicalization",
        )


def _validate_hour_modifier_topology(carrier: Carrier, *, unit_id: str) -> None:
    current_grain = structural_grain(carrier.anchor)
    for modifier in carrier.modifiers:
        if isinstance(modifier, CalendarFilter) and current_grain == "hour":
            raise PostProcessorValidationError(
                layer=3,
                stage="post_processor",
                unit_id=unit_id,
                details="CalendarFilter does not support hour-native carriers",
            )
        if isinstance(modifier, GrainExpansion) and modifier.target_grain == "hour":
            if current_grain != "day":
                raise PostProcessorValidationError(
                    layer=3,
                    stage="post_processor",
                    unit_id=unit_id,
                    details="GrainExpansion(target_grain=\"hour\") requires a day-native parent",
                )
            current_grain = "hour"
            continue
        if isinstance(modifier, Offset) and modifier.unit == "hour":
            if current_grain != "hour":
                raise PostProcessorValidationError(
                    layer=3,
                    stage="post_processor",
                    unit_id=unit_id,
                    details="Offset(unit=\"hour\") only applies to hour-native carriers",
                )
        if isinstance(modifier, GrainExpansion):
            current_grain = modifier.target_grain


def _validate_mapped_range_semantics(anchor: MappedRange, *, unit_id: str, system_datetime: datetime) -> None:
    if anchor.mode != "bounded_pair":
        return
    _validate_bounded_pair_endpoint(anchor.start, unit_id=unit_id, side="start")
    _validate_bounded_pair_endpoint(anchor.end, unit_id=unit_id, side="end")
    start_precision = _bounded_pair_endpoint_precision(anchor.start, unit_id=unit_id, side="start")
    if _is_current_time_bounded_pair_endpoint(anchor.end):
        end_precision = start_precision
    else:
        end_precision = _bounded_pair_endpoint_precision(anchor.end, unit_id=unit_id, side="end")
    if start_precision != end_precision:
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details="mapped_range bounded_pair requires single-precision endpoints; mixed precision is unsupported",
        )
    _validate_shifted_day_bounded_pair_order(anchor, unit_id=unit_id, system_datetime=system_datetime)


def _validate_bounded_pair_endpoint(expr: Any, *, unit_id: str, side: str) -> None:
    if expr is None:
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details=f"mapped_range bounded_pair requires {side} endpoint",
        )
    if expr == "system_datetime":
        return

    anchor = _coerce_bounded_pair_endpoint(expr, unit_id=unit_id, side=side)
    if isinstance(anchor, (NamedPeriod, DateRange, DatetimeRange)):
        return
    if isinstance(anchor, RelativeWindow):
        if anchor.grain == "day":
            if anchor.offset_units == 0:
                return
            if anchor.offset_units < 0:
                if side == "start":
                    raise PostProcessorValidationError(
                        layer=3,
                        stage="post_processor",
                        unit_id=unit_id,
                        details="mapped_range bounded_pair does not support start relative_window(day,<0) endpoint",
                    )
                return
            raise PostProcessorValidationError(
                layer=3,
                stage="post_processor",
                unit_id=unit_id,
                details="mapped_range bounded_pair does not support future day-offset endpoints",
            )
        if anchor.grain == "hour" and anchor.offset_units == 0:
            return
        if anchor.grain == "hour":
            raise PostProcessorValidationError(
                layer=3,
                stage="post_processor",
                unit_id=unit_id,
                details="mapped_range bounded_pair only supports current-time hour relative endpoints",
            )
    if isinstance(anchor, EnumerationSet):
        for member in anchor.members:
            _validate_bounded_pair_endpoint(member, unit_id=unit_id, side=side)
        return
    raise PostProcessorValidationError(
        layer=3,
        stage="post_processor",
        unit_id=unit_id,
        details=f"mapped_range bounded_pair does not support {side} endpoint type {type(anchor).__name__}",
    )


def _coerce_bounded_pair_endpoint(expr: Any, *, unit_id: str, side: str) -> object:
    if isinstance(
        expr,
        (
            NamedPeriod,
            DateRange,
            DatetimeRange,
            RelativeWindow,
            RollingWindow,
            RollingByCalendarUnit,
            EnumerationSet,
            GroupedTemporalValue,
            CalendarEvent,
            HolidayEventCollection,
            MappedRange,
        ),
    ):
        return expr
    try:
        return TypeAdapter(Anchor).validate_python(expr)
    except ValidationError as exc:
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details=f"mapped_range bounded_pair {side} endpoint is invalid: {exc}",
        ) from exc


def _bounded_pair_endpoint_precision(expr: Any, *, unit_id: str, side: str) -> str:
    if expr == "system_datetime":
        return "day"
    anchor = _coerce_bounded_pair_endpoint(expr, unit_id=unit_id, side=side)
    if isinstance(anchor, DatetimeRange):
        return "hour"
    if isinstance(anchor, RelativeWindow):
        return "hour" if anchor.grain == "hour" else "day"
    if isinstance(anchor, RollingWindow):
        return "hour" if anchor.unit == "hour" else "day"
    if isinstance(anchor, EnumerationSet):
        precisions = {
            _bounded_pair_endpoint_precision(member, unit_id=unit_id, side=side)
            for member in anchor.members
        }
        if len(precisions) != 1:
            raise PostProcessorValidationError(
                layer=3,
                stage="post_processor",
                unit_id=unit_id,
                details=f"mapped_range bounded_pair {side} enumeration endpoint must use a single precision",
            )
        return precisions.pop()
    return "day"


def _is_current_time_bounded_pair_endpoint(expr: Any) -> bool:
    if expr == "system_datetime":
        return True
    try:
        anchor = _coerce_bounded_pair_endpoint(expr, unit_id="__system__", side="end")
    except PostProcessorValidationError:
        return False
    return isinstance(anchor, RelativeWindow) and anchor.grain in {"day", "hour"} and anchor.offset_units == 0


def _is_shifted_day_bounded_pair_endpoint(expr: Any) -> bool:
    try:
        anchor = _coerce_bounded_pair_endpoint(expr, unit_id="__system__", side="end")
    except PostProcessorValidationError:
        return False
    return isinstance(anchor, RelativeWindow) and anchor.grain == "day" and anchor.offset_units < 0


def _validate_shifted_day_bounded_pair_order(anchor: MappedRange, *, unit_id: str, system_datetime: datetime) -> None:
    if not _is_shifted_day_bounded_pair_endpoint(anchor.end):
        return
    try:
        materialize_carrier(
            Carrier(anchor=anchor, modifiers=[]),
            system_datetime=system_datetime,
            business_calendar=_load_business_calendar(),
        )
    except ValueError as exc:
        if "semantic_conflict" not in str(exc):
            raise
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details=f"semantic_conflict: {exc}",
        ) from exc


def _is_calendar_grain_rolling_approximation(carrier: Carrier) -> bool:
    if not isinstance(carrier.anchor, RollingWindow) or carrier.anchor.unit != "day":
        return False
    if len(carrier.modifiers) != 1:
        return False
    return isinstance(carrier.modifiers[0], CalendarFilter)


def _grouped_parent_is_coarser(anchor: GroupedTemporalValue) -> bool:
    if not isinstance(anchor.parent, NamedPeriod):
        return True
    parent_rank = {
        "year": 6,
        "half_year": 5,
        "quarter": 4,
        "month": 3,
        "week": 2,
        "day": 1,
    }[anchor.parent.period_type]
    child_rank = {
        "quarter": 4,
        "half_year": 5,
        "month": 3,
        "week": 2,
        "day": 1,
    }[anchor.child_grain]
    return parent_rank > child_rank


def _validate_enumeration_set(anchor: EnumerationSet, *, unit_id: str) -> None:
    if anchor.grain == "calendar_event":
        seen_event_keys: set[tuple[object, ...]] = set()
        for member in anchor.members:
            if not isinstance(member, CalendarEvent):
                raise PostProcessorValidationError(
                    layer=3,
                    stage="post_processor",
                    unit_id=unit_id,
                    details='EnumerationSet(grain="calendar_event") requires CalendarEvent members',
                )
            identity = _calendar_event_identity(member)
            if identity in seen_event_keys:
                raise PostProcessorValidationError(
                    layer=3,
                    stage="post_processor",
                    unit_id=unit_id,
                    details="duplicate calendar_event identity inside EnumerationSet",
                )
            seen_event_keys.add(identity)
        return

    seen_member_keys: set[tuple[object, ...]] = set()
    intervals: list[tuple[date, date]] = []
    for member in anchor.members:
        key = _natural_member_key(member)
        if key in seen_member_keys:
            raise PostProcessorValidationError(
                layer=3,
                stage="post_processor",
                unit_id=unit_id,
                details="duplicate EnumerationSet member",
            )
        seen_member_keys.add(key)
        intervals.append(_natural_member_interval(member))
    for left_index in range(len(intervals)):
        for right_index in range(left_index + 1, len(intervals)):
            if _interval_ranges_overlap(intervals[left_index], intervals[right_index]):
                raise PostProcessorValidationError(
                    layer=3,
                    stage="post_processor",
                    unit_id=unit_id,
                    details=f"overlap error between EnumerationSet members {left_index} and {right_index}",
                )


def _calendar_event_identity(anchor: CalendarEvent) -> tuple[object, ...]:
    return (
        anchor.region,
        anchor.event_key,
        anchor.schedule_year_ref.year,
        anchor.schedule_year_ref.source_unit_id,
        anchor.scope,
    )


def _natural_member_key(member: object) -> tuple[object, ...]:
    if isinstance(member, NamedPeriod):
        return ("named_period", member.period_type, member.year, member.quarter, member.half, member.month, member.iso_week, member.date)
    if hasattr(member, "start_date") and hasattr(member, "end_date"):
        return ("date_range", member.start_date, member.end_date, member.end_inclusive)
    raise ValueError(f"Unsupported natural EnumerationSet member: {type(member)!r}")


def _natural_member_interval(member: object) -> tuple[date, date]:
    if isinstance(member, NamedPeriod):
        interval = materialize_carrier(
            Carrier(anchor=member, modifiers=[]),
            system_datetime=datetime.combine(date.today(), datetime.min.time()),
            business_calendar=_load_business_calendar(),
        ).labels.absolute_core_time
        if interval is None:
            raise ValueError("NamedPeriod materialization produced no interval")
        return (interval.start.date(), interval.end.date())
    if hasattr(member, "start_date") and hasattr(member, "end_date"):
        return (member.start_date, member.end_date)
    raise ValueError(f"Unsupported natural EnumerationSet member: {type(member)!r}")


def _interval_ranges_overlap(left: tuple[date, date], right: tuple[date, date]) -> bool:
    return left[0] <= right[1] and right[0] <= left[1]


def _validate_frozen_rolling_window(anchor: RollingWindow, *, unit_id: str) -> None:
    if anchor.endpoint != "today":
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details="frozen v1 rolling parameter endpoint must stay today",
        )
    if anchor.include_endpoint is not True:
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details="frozen v1 rolling parameter include_endpoint must stay true",
        )


def _validate_frozen_rolling_by_calendar(anchor: RollingByCalendarUnit, *, unit_id: str) -> None:
    if anchor.endpoint != "today":
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details="frozen v1 rolling parameter endpoint must stay today",
        )
    if anchor.include_endpoint is not True:
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            unit_id=unit_id,
            details="frozen v1 rolling parameter include_endpoint must stay true",
        )


def _canonicalize_stage_b(stage_b: dict[str, StageBOutput]) -> dict[str, StageBOutput]:
    canonical: dict[str, StageBOutput] = {}
    for unit_id, output in stage_b.items():
        if output.carrier is None:
            canonical[unit_id] = output
            continue
        canonical_output = output.model_copy(deep=True)
        canonical_output.carrier = _canonicalize_carrier(canonical_output.carrier)
        canonical[unit_id] = canonical_output
    return canonical


def _canonicalize_carrier(carrier: Carrier) -> Carrier:
    if not carrier.modifiers:
        return carrier
    first_modifier = carrier.modifiers[0]
    if (
        isinstance(first_modifier, GrainExpansion)
        and first_modifier.target_grain not in {"day", "hour"}
        and carrier.anchor.kind in {"named_period", "date_range", "relative_window", "rolling_window", "calendar_event"}
    ):
        return Carrier(
            anchor=GroupedTemporalValue(
                kind="grouped_temporal_value",
                parent=carrier.anchor,
                child_grain=first_modifier.target_grain,
                selector="all",
            ),
            modifiers=carrier.modifiers[1:],
        )
    return carrier


def _validate_derivation_transforms(units: list[Unit]) -> None:
    for unit in units:
        if unit.content.content_kind != "derived":
            continue
        for source in unit.content.sources:
            transform_kind = source.transform.kind
            spec = get_derivation_transform_spec(transform_kind)
            if spec is None or not spec["distributive"]:
                raise PostProcessorValidationError(
                    layer=4,
                    stage="post_processor",
                    unit_id=unit.unit_id,
                    details=f"non-distributive derivation transform is not admitted: {transform_kind}",
                )


def _expand_comparisons(
    comparisons: list[Comparison],
    *,
    unit_map: dict[str, Unit],
    system_datetime: datetime,
) -> list[Comparison]:
    expanded_comparisons: list[Comparison] = []
    for comparison in comparisons:
        expanded_pairs: list[ComparisonPair] = []
        for source_pair_index, pair in enumerate(comparison.pairs):
            subject_cardinality = _comparison_endpoint_cardinality(
                unit_map[pair.subject_unit_id],
                unit_map=unit_map,
                system_datetime=system_datetime,
            )
            reference_cardinality = _comparison_endpoint_cardinality(
                unit_map[pair.reference_unit_id],
                unit_map=unit_map,
                system_datetime=system_datetime,
            )
            if subject_cardinality == 1 and reference_cardinality == 1:
                expanded_pairs.append(pair.model_copy(update={"expansion": None}))
                continue
            if subject_cardinality > 1 and reference_cardinality > 1 and subject_cardinality != reference_cardinality:
                raise PostProcessorValidationError(
                    layer=4,
                    stage="post_processor",
                    details=(
                        f"comparison cardinality mismatch: {pair.subject_unit_id}={subject_cardinality}, "
                        f"{pair.reference_unit_id}={reference_cardinality}"
                    ),
                )
            expansion_cardinality = max(subject_cardinality, reference_cardinality)
            for expansion_index in range(expansion_cardinality):
                expanded_pairs.append(
                    ComparisonPair(
                        subject_unit_id=pair.subject_unit_id,
                        reference_unit_id=pair.reference_unit_id,
                        expansion=PairExpansion(
                            source_pair_index=source_pair_index,
                            expansion_index=expansion_index,
                            expansion_cardinality=expansion_cardinality,
                            subject_core_index=expansion_index if subject_cardinality > 1 else None,
                            reference_core_index=expansion_index if reference_cardinality > 1 else None,
                        ),
                    )
                )
        expanded_comparisons.append(
            Comparison(
                comparison_id=comparison.comparison_id,
                anchor_text=comparison.anchor_text,
                pairs=expanded_pairs,
            )
        )
    return expanded_comparisons


def _comparison_endpoint_cardinality(
    unit: Unit,
    *,
    unit_map: dict[str, Unit],
    system_datetime: datetime,
) -> int:
    if unit.content.content_kind == "derived":
        if len(unit.content.sources) > 1:
            return len(unit.content.sources)
        if not unit.content.sources:
            return 1
        return _comparison_endpoint_cardinality(
            unit_map[unit.content.sources[0].source_unit_id],
            unit_map=unit_map,
            system_datetime=system_datetime,
        )

    carrier = unit.content.carrier
    if carrier is None:
        return 1
    anchor = carrier.anchor
    if isinstance(anchor, RollingByCalendarUnit):
        return 1
    if anchor.kind in {"named_period", "date_range", "datetime_range", "relative_window", "rolling_window", "calendar_event"}:
        return 1
    if isinstance(anchor, (EnumerationSet, GroupedTemporalValue, MappedRange)):
        selection = next((modifier for modifier in carrier.modifiers if isinstance(modifier, MemberSelection)), None)
        base_cardinality = _explicit_sibling_cardinality(carrier, system_datetime=system_datetime)
        if selection is None:
            return max(base_cardinality, 1)
        if selection.selector in {"first", "last", "nth"}:
            return 1
        if selection.n is None:
            return base_cardinality
        return min(selection.n, base_cardinality)
    return 1


def _explicit_sibling_cardinality(carrier: Carrier, *, system_datetime: datetime) -> int:
    if isinstance(carrier.anchor, EnumerationSet):
        return len(carrier.anchor.members)
    stripped_modifiers = [modifier for modifier in carrier.modifiers if not isinstance(modifier, MemberSelection)]
    tree = materialize_carrier(
        Carrier(anchor=carrier.anchor, modifiers=stripped_modifiers),
        system_datetime=system_datetime,
        business_calendar=_load_business_calendar(),
    )
    return len(tree.children) if tree.children else len(tree.intervals)


@lru_cache(maxsize=1)
def _load_business_calendar() -> JsonBusinessCalendar:
    return JsonBusinessCalendar.from_root(root=get_business_calendar_root())
