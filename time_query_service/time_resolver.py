from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from pydantic import Field

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.contracts import (
    CarrierSpec,
    CalendarSelectorSpec,
    ClarificationItem,
    ClarificationNode,
    ExplicitWindowResolutionSpec,
    HolidayWindowResolutionSpec,
    Interval,
    NestedWindowSpec,
    OffsetWindowResolutionSpec,
    RelativeWindowResolutionSpec,
    StrictModel,
    YearRef,
    WindowWithRegularGrainResolutionSpec,
    WindowWithCalendarSelectorResolutionSpec,
    WindowWithMemberSelectionResolutionSpec,
)
from time_query_service.materialization_models import (
    MaterializationContext,
    MaterializedBlock,
    MaterializedMember,
    MaterializedNestedParentBlock,
    MaterializedPairingFamily,
    MaterializedRenderTarget,
)
from time_query_service.normalized_plan_models import (
    DerivationEdge,
    NormalizedEntity,
    NormalizedPlan,
    SemanticFamily,
    SemanticFamilyMember,
)
from time_query_service.plan_validator import validate_comparison_group_boundaries


class ResolutionResult(StrictModel):
    items: list[ClarificationItem] = Field(default_factory=list)


class ResolutionAdmissibilityError(ValueError):
    pass


def resolve_plan(
    *,
    plan: NormalizedPlan,
    system_date: str | None = None,
    system_datetime: str | None = None,
    timezone: str = "Asia/Shanghai",
    business_calendar: BusinessCalendarPort | None = None,
) -> ResolutionResult:
    if not isinstance(plan, NormalizedPlan):
        raise TypeError("resolve_plan expects NormalizedPlan")
    anchor_date = _resolve_anchor_date(system_date=system_date, system_datetime=system_datetime, timezone=timezone)
    entities = plan.normalized_entities
    node_lookup = {entity.entity_id: _entity_to_node(entity) for entity in entities}
    resolved_by_node_id: dict[str, list[Interval]] = {}

    items: list[ClarificationItem] = []
    for entity in entities:
        if not entity.needs_clarification:
            continue
        node = node_lookup[entity.entity_id]
        intervals = _resolve_node_intervals(
            node=node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        items.append(_build_clarification_item_from_entity(entity=entity, intervals=intervals))
    return ResolutionResult(items=items)


def resolve_materialization_context(
    *,
    plan: NormalizedPlan,
    system_date: str | None = None,
    system_datetime: str | None = None,
    timezone: str = "Asia/Shanghai",
    business_calendar: BusinessCalendarPort | None = None,
) -> MaterializationContext | None:
    if not isinstance(plan, NormalizedPlan):
        raise TypeError("resolve_materialization_context expects NormalizedPlan")

    try:
        if not _is_materialization_candidate(plan):
            return None
        anchor_date = _resolve_anchor_date(system_date=system_date, system_datetime=system_datetime, timezone=timezone)
        entities = {entity.entity_id: entity for entity in plan.normalized_entities}
        edge_lookup = {edge.target_entity_id: edge for edge in plan.derivation_edges}
        node_lookup = {entity_id: _entity_to_node(entity) for entity_id, entity in entities.items()}
        resolved_by_node_id: dict[str, list[Interval]] = {}

        pairing_families = [
            family
            for family in plan.semantic_families
            if family.family_kind in {"flat_pairing_family", "nested_pairing_family"}
        ]
        if pairing_families:
            return _build_pairing_materialization_context(
                families=pairing_families,
                entities=entities,
                edge_lookup=edge_lookup,
                anchor_date=anchor_date,
                business_calendar=business_calendar,
                node_lookup=node_lookup,
                resolved_by_node_id=resolved_by_node_id,
            )

        standalone_families = [
            family for family in plan.semantic_families if family.family_kind == "standalone_family"
        ]
        if len(standalone_families) != 1:
            return None

        family = standalone_families[0]
        if len(family.members) != 1:
            return None
        if not _is_materialization_entity(entities[family.members[0].entity_id]):
            return None
        mode, blocks = _materialize_family_member(
            member=family.members[0],
            entities=entities,
            edge_lookup=edge_lookup,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        entity = entities[family.members[0].entity_id]
        return MaterializationContext(
            mode=mode,
            node_id=family.family_id,
            render_text=entity.render_text or family.family_id,
            blocks=blocks,
            render_targets=[_render_target_from_entity(entity)],
        )
    except ResolutionAdmissibilityError:
        raise
    except ValueError as exc:
        raise ResolutionAdmissibilityError(str(exc)) from exc


def _resolve_node_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> list[Interval]:
    cached = resolved_by_node_id.get(node.node_id)
    if cached is not None:
        return cached

    if node.node_kind == "relative_window":
        intervals = _resolve_relative_window_intervals(node=node, anchor_date=anchor_date)
    elif node.node_kind == "holiday_window":
        intervals = _resolve_holiday_window_intervals(
            node=node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
        )
    elif node.node_kind == "explicit_window":
        intervals = _resolve_explicit_window_intervals(node=node, anchor_date=anchor_date)
    elif node.node_kind == "offset_window":
        intervals = _resolve_offset_window_intervals(
            node=node,
            anchor_date=anchor_date,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
            business_calendar=business_calendar,
        )
    elif node.node_kind == "window_with_regular_grain":
        spec = WindowWithRegularGrainResolutionSpec.model_validate(node.resolution_spec)
        intervals = _resolve_inline_window_intervals(
            window=spec.window,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
    elif node.node_kind == "window_with_calendar_selector":
        spec = WindowWithCalendarSelectorResolutionSpec.model_validate(node.resolution_spec)
        base_intervals = _resolve_inline_window_intervals(
            window=spec.window,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        if len(base_intervals) != 1:
            raise ValueError("Current resolver slice only supports single-interval calendar selector windows.")
        start_date = base_intervals[0].start_date
        end_date = base_intervals[0].end_date
        matched_dates = _filter_calendar_dates(
            start_date=start_date,
            end_date=end_date,
            selector=spec.selector,
            business_calendar=business_calendar,
        )
        intervals = _compress_dates_to_intervals(matched_dates)
    elif node.node_kind == "window_with_member_selection":
        mode, blocks = _build_materialization_for_node(
            node=node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        if mode == "flat_enumeration":
            source_members = blocks[0].members
        else:
            source_members = [member for block in blocks for member in block.members]
        intervals = [
            Interval(start_date=member.start_date, end_date=member.end_date)
            for member in source_members
        ]
    else:
        raise ValueError(f"Unsupported node_kind for current resolver slice: {node.node_kind}")

    resolved_by_node_id[node.node_id] = intervals
    return intervals

def _build_clarification_item_from_entity(*, entity: NormalizedEntity, intervals: list[Interval]) -> ClarificationItem:
    return ClarificationItem(
        node_id=entity.entity_id,
        render_text=entity.render_text or entity.entity_id,
        ordinal=entity.ordinal or 0,
        display_exact_time=_render_intervals(intervals),
        surface_fragments=entity.surface_fragments,
        intervals=intervals,
    )

def _entity_to_node(entity: NormalizedEntity) -> ClarificationNode:
    if entity.node_kind is None or entity.reason_code is None or entity.resolution_spec is None:
        raise ValueError(f"Normalized entity {entity.entity_id} is missing node payload required for resolution.")
    return ClarificationNode(
        node_id=entity.entity_id,
        render_text=entity.render_text or entity.entity_id,
        ordinal=entity.ordinal or 0,
        needs_clarification=entity.needs_clarification,
        reason_code=entity.reason_code,
        carrier=CarrierSpec(kind=entity.node_kind, value=entity.resolution_spec),
        derivation=entity.derivation,
        surface_fragments=entity.surface_fragments,
    )


def _is_materialization_candidate(plan: NormalizedPlan) -> bool:
    entity_lookup = {entity.entity_id: entity for entity in plan.normalized_entities}
    pairing_families = [
        family
        for family in plan.semantic_families
        if family.family_kind in {"flat_pairing_family", "nested_pairing_family"}
    ]
    if pairing_families:
        return True

    standalone_families = [
        family for family in plan.semantic_families if family.family_kind == "standalone_family"
    ]
    if len(standalone_families) != 1:
        return False

    family = standalone_families[0]
    return (
        len(family.members) == 1
        and _is_materialization_entity(
            _require_materialization_entity(entity_lookup, family.members[0].entity_id)
        )
    )


def _require_materialization_entity(
    entity_lookup: dict[str, NormalizedEntity],
    entity_id: str,
) -> NormalizedEntity:
    entity = entity_lookup.get(entity_id)
    if entity is None:
        raise ValueError(f"Materialization family references unknown entity: {entity_id}")
    return entity


def _is_materialization_node(node: ClarificationNode) -> bool:
    if not node.needs_clarification:
        return False
    if node.node_kind == "window_with_member_selection":
        return True
    if node.node_kind == "window_with_calendar_selector":
        return True
    if node.node_kind != "window_with_regular_grain":
        return False
    spec = WindowWithRegularGrainResolutionSpec.model_validate(node.resolution_spec)
    return spec.grain != "day"


def _is_materialization_entity(entity: NormalizedEntity) -> bool:
    return _is_materialization_node(_entity_to_node(entity))


def _build_materialization_for_node(
    *,
    node: ClarificationNode,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> tuple[str, list[MaterializedBlock]]:
    if node.node_kind == "window_with_regular_grain":
        spec = WindowWithRegularGrainResolutionSpec.model_validate(node.resolution_spec)
        if spec.window.kind == "window_with_regular_grain":
            parent_spec = WindowWithRegularGrainResolutionSpec.model_validate(spec.window.value)
            parent_members = _enumerate_regular_grain_members(
                window=parent_spec.window,
                grain=parent_spec.grain,
                anchor_date=anchor_date,
                business_calendar=business_calendar,
                node_lookup=node_lookup,
                resolved_by_node_id=resolved_by_node_id,
            )
            blocks: list[MaterializedBlock] = []
            for parent_member in parent_members:
                child_members = _split_interval_by_grain(
                    interval=Interval(start_date=parent_member.start_date, end_date=parent_member.end_date),
                    grain=spec.grain,
                )
                if not child_members:
                    raise ValueError("Phase1 grouped regular-grain materialization requires child members.")
                blocks.append(
                    MaterializedBlock(
                        label=parent_member.label or node.render_text,
                        members=child_members,
                        group_member=parent_member,
                    )
                )
            return ("grouped_enumeration", blocks)
        members = _enumerate_regular_grain_members(
            window=spec.window,
            grain=spec.grain,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        return (
            "flat_enumeration",
            [MaterializedBlock(label=node.render_text, members=members)],
        )

    if node.node_kind == "window_with_member_selection":
        spec = WindowWithMemberSelectionResolutionSpec.model_validate(node.resolution_spec)
        mode, blocks = _build_materialization_from_nested_window(
            window=spec.window,
            render_text=node.render_text,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        return _apply_member_selection(
            mode=mode,
            blocks=blocks,
            selection=spec.selection,
        )

    if node.node_kind != "window_with_calendar_selector":
        raise ValueError(f"Unsupported phase1 materialization node_kind: {node.node_kind}")

    spec = WindowWithCalendarSelectorResolutionSpec.model_validate(node.resolution_spec)
    if spec.window.kind == "window_with_regular_grain":
        parent_spec = WindowWithRegularGrainResolutionSpec.model_validate(spec.window.value)
        parent_members = _enumerate_regular_grain_members(
            window=parent_spec.window,
            grain=parent_spec.grain,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        blocks: list[MaterializedBlock] = []
        for parent_member in parent_members:
            child_members = _enumerate_calendar_selector_members(
                start_date=parent_member.start_date,
                end_date=parent_member.end_date,
                selector=spec.selector,
                business_calendar=business_calendar,
            )
            if not child_members:
                raise ValueError("Phase1 grouped enumeration does not support empty child buckets.")
            blocks.append(
                MaterializedBlock(
                    label=parent_member.label or node.render_text,
                    members=child_members,
                    group_member=parent_member,
                )
            )
        return ("grouped_enumeration", blocks)

    base_intervals = _resolve_inline_window_intervals(
        window=spec.window,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )
    if len(base_intervals) != 1:
        raise ValueError("Phase1 materialization only supports single-interval calendar selector windows.")

    members = _enumerate_calendar_selector_members(
        start_date=base_intervals[0].start_date,
        end_date=base_intervals[0].end_date,
        selector=spec.selector,
        business_calendar=business_calendar,
    )
    return (
        "flat_enumeration",
        [MaterializedBlock(label=node.render_text, members=members)],
    )


def _build_materialization_from_nested_window(
    *,
    window: NestedWindowSpec,
    render_text: str,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> tuple[str, list[MaterializedBlock]]:
    nested_node = ClarificationNode(
        node_id=f"__materialize__::{len(resolved_by_node_id)}",
        render_text=render_text,
        ordinal=0,
        needs_clarification=True,
        node_kind=window.kind,
        reason_code="structural_enumeration",
        resolution_spec=window.value,
        surface_fragments=[],
    )
    return _build_materialization_for_node(
        node=nested_node,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )


def _apply_member_selection(
    *,
    mode: str,
    blocks: list[MaterializedBlock],
    selection,
) -> tuple[str, list[MaterializedBlock]]:
    if mode == "flat_enumeration":
        if len(blocks) != 1:
            raise ValueError("Flat member selection requires exactly one source block.")
        return (
            "flat_enumeration",
            [
                MaterializedBlock(
                    label=blocks[0].label,
                    members=_select_members(blocks[0].members, selection=selection),
                    group_member=blocks[0].group_member,
                )
            ],
        )
    if mode != "grouped_enumeration":
        raise ValueError(f"Unsupported member-selection mode: {mode}")
    selected_blocks: list[MaterializedBlock] = []
    for block in blocks:
        selected_blocks.append(
            MaterializedBlock(
                label=block.label,
                members=_select_members(block.members, selection=selection),
                group_member=block.group_member,
            )
        )
    return ("grouped_enumeration", selected_blocks)


def _select_members(members: list[MaterializedMember], *, selection) -> list[MaterializedMember]:
    if not members:
        raise ValueError("Phase1 materialization found no matching calendar members.")
    if selection.mode == "first":
        count = selection.count or 1
        if count > len(members):
            raise ValueError("Requested first-N members exceed the available phase1 materialization members.")
        return members[:count]
    if selection.mode == "last":
        count = selection.count or 1
        if count > len(members):
            raise ValueError("Requested last-N members exceed the available phase1 materialization members.")
        return members[-count:]
    if selection.mode == "nth":
        if selection.index is None or selection.index > len(members):
            raise ValueError("Requested nth member is outside the available phase1 materialization members.")
        return [members[selection.index - 1]]
    if selection.index is None or selection.index > len(members):
        raise ValueError("Requested nth-from-end member is outside the available phase1 materialization members.")
    return [members[-selection.index]]


def _build_pairing_materialization_context(
    *,
    families: list[SemanticFamily],
    entities: dict[str, NormalizedEntity],
    edge_lookup: dict[str, DerivationEdge],
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> MaterializationContext:
    group_materializations = [
        _build_pairing_family_materialization(
            family=family,
            entities=entities,
            edge_lookup=edge_lookup,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        for family in families
    ]
    render_targets = _collect_pairing_render_targets_from_families(families=families, entities=entities)

    nested_materializations = [result for result in group_materializations if result["kind"] == "nested"]
    if nested_materializations:
        if len(group_materializations) != 1:
            raise ValueError("Nested pairing cannot be materialized losslessly across multiple comparison families.")
        nested = nested_materializations[0]
        return MaterializationContext(
            mode="nested_pairing",
            node_id=nested["group_id"],
            render_text=nested["anchor_text"],
            nested_blocks=nested["nested_blocks"],
            nested_child_mode=nested["nested_child_mode"],
            render_targets=render_targets,
            anchor_text=nested["anchor_text"],
            anchor_ordinal=nested["anchor_ordinal"],
        )

    pairing_families = [result["family"] for result in group_materializations]
    if len(pairing_families) == 1:
        family = pairing_families[0]
        return MaterializationContext(
            mode="single_level_pairing",
            node_id=family.group_id,
            render_text=family.anchor_text,
            blocks=family.blocks,
            render_targets=render_targets,
            anchor_text=family.anchor_text,
            anchor_ordinal=family.anchor_ordinal,
        )

    return MaterializationContext(
        mode="multi_sibling_pairing",
        node_id="|".join(family.group_id for family in pairing_families),
        render_text=pairing_families[0].anchor_text,
        pairing_families=pairing_families,
        render_targets=render_targets,
    )


def _build_pairing_family_materialization(
    *,
    family: SemanticFamily,
    entities: dict[str, NormalizedEntity],
    edge_lookup: dict[str, DerivationEdge],
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> dict[str, object]:
    if len(family.members) != 2:
        raise ValueError("Phase1 materialization only supports two-member comparison families.")

    side_materializations: list[tuple[str, list[MaterializedBlock]]] = []
    for member in family.members:
        mode, blocks = _materialize_family_member(
            member=member,
            entities=entities,
            edge_lookup=edge_lookup,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
        side_materializations.append((mode, blocks))

    side_modes = {mode for mode, _ in side_materializations}
    if side_modes == {"flat_enumeration"}:
        return {
            "kind": "flat",
            "family": _build_flat_pairing_family(
                family=family,
                left_blocks=side_materializations[0][1],
                right_blocks=side_materializations[1][1],
            ),
        }
    if side_modes == {"grouped_enumeration"}:
        nested_child_mode, nested_blocks = _build_nested_pairing_blocks(
            left_blocks=side_materializations[0][1],
            right_blocks=side_materializations[1][1],
        )
        return {
            "kind": "nested",
            "group_id": family.family_id,
            "anchor_text": family.anchor_text or family.family_id,
            "anchor_ordinal": family.anchor_ordinal or 1,
            "nested_child_mode": nested_child_mode,
            "nested_blocks": nested_blocks,
        }
    raise ValueError("Nested pairing requires both comparison members to materialize to the same one-level structure.")


def _materialize_family_member(
    *,
    member: SemanticFamilyMember,
    entities: dict[str, NormalizedEntity],
    edge_lookup: dict[str, DerivationEdge],
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> tuple[str, list[MaterializedBlock]]:
    entity = entities.get(member.entity_id)
    if entity is None:
        raise ValueError(f"Missing normalized entity for family member entity_id={member.entity_id}")
    derivation_edge = edge_lookup.get(member.entity_id)
    if derivation_edge is None:
        node = node_lookup[member.entity_id]
        return _build_materialization_for_node(
            node=node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
    if derivation_edge.inheritance_mode == "rebind_nested_base":
        node = node_lookup[member.entity_id]
        return _build_materialization_for_node(
            node=node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )
    source_entity_id = member.source_entity_id or derivation_edge.source_entity_id
    source_member = SemanticFamilyMember(
        entity_id=source_entity_id,
        role=member.role,
        canonical_structure=member.canonical_structure,
    )
    mode, blocks = _materialize_family_member(
        member=source_member,
        entities=entities,
        edge_lookup=edge_lookup,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )
    return (
        mode,
        _shift_materialized_blocks(
            blocks=blocks,
            unit=derivation_edge.shift_unit,
            value=derivation_edge.shift_value,
        ),
    )


def _shift_materialized_blocks(
    *,
    blocks: list[MaterializedBlock],
    unit: str | None,
    value: int | None,
) -> list[MaterializedBlock]:
    if unit is None or value is None:
        raise ValueError("Reference-derived materialization requires an explicit shift unit and value.")
    return [
        MaterializedBlock(
            label=shifted_group_member.label if shifted_group_member is not None else block.label,
            members=[_shift_materialized_member(member=member, unit=unit, value=value) for member in block.members],
            group_member=shifted_group_member,
        )
        for block in blocks
        for shifted_group_member in [
            _shift_materialized_member(member=block.group_member, unit=unit, value=value)
            if block.group_member is not None
            else None
        ]
    ]


def _shift_materialized_member(
    *,
    member: MaterializedMember | None,
    unit: str,
    value: int,
) -> MaterializedMember | None:
    if member is None:
        return None
    shifted_interval = _shift_interval(
        Interval(start_date=member.start_date, end_date=member.end_date),
        unit=unit,
        value=value,
    )
    return MaterializedMember(
        label=_shift_materialized_label(member=member, shifted_interval=shifted_interval),
        start_date=shifted_interval.start_date,
        end_date=shifted_interval.end_date,
    )


def _shift_materialized_label(*, member: MaterializedMember, shifted_interval: Interval) -> str | None:
    if member.label is None:
        return None
    if member.label.endswith("年") and "季度" not in member.label and "月" not in member.label:
        return f"{shifted_interval.start_date.year}年"
    if member.label.endswith("月") and "年" in member.label:
        return f"{shifted_interval.start_date.year}年{shifted_interval.start_date.month}月"
    if member.label.endswith("季度") and "年第" in member.label:
        quarter = ((shifted_interval.start_date.month - 1) // 3) + 1
        return f"{shifted_interval.start_date.year}年第{quarter}季度"
    return member.label


def _build_flat_pairing_family(
    *,
    family: SemanticFamily,
    left_blocks: list[MaterializedBlock],
    right_blocks: list[MaterializedBlock],
) -> MaterializedPairingFamily:
    left_members = left_blocks[0].members
    right_members = right_blocks[0].members
    pair_count = max(len(left_members), len(right_members))
    if pair_count == 0:
        raise ValueError("Phase1 pairing requires at least one aligned bucket.")

    blocks: list[MaterializedBlock] = []
    for index in range(pair_count):
        left_member = left_members[index] if index < len(left_members) else None
        right_member = right_members[index] if index < len(right_members) else None
        if left_member is None and right_member is None:
            continue
        blocks.append(
            MaterializedBlock(
                label=(left_member.label if left_member is not None else right_member.label) or f"第{index + 1}组",
                left_label=_render_materialized_member(left_member) if left_member is not None else None,
                right_label=_render_materialized_member(right_member) if right_member is not None else None,
                left_missing=left_member is None,
                right_missing=right_member is None,
            )
        )

    return MaterializedPairingFamily(
        group_id=family.family_id,
        anchor_text=family.anchor_text or family.family_id,
        anchor_ordinal=family.anchor_ordinal or 1,
        blocks=blocks,
    )


def _build_nested_pairing_blocks(
    *,
    left_blocks: list[MaterializedBlock],
    right_blocks: list[MaterializedBlock],
) -> tuple[str, list[MaterializedNestedParentBlock]]:
    if len(left_blocks) != len(right_blocks):
        raise ValueError("Nested pairing cannot be materialized losslessly because parent groups do not align.")
    if not left_blocks:
        raise ValueError("Nested pairing requires at least one aligned parent group.")

    nested_child_mode: str | None = None
    nested_blocks: list[MaterializedNestedParentBlock] = []
    for left_block, right_block in zip(left_blocks, right_blocks):
        if left_block.group_member is None or right_block.group_member is None:
            raise ValueError("Nested pairing cannot be materialized losslessly without explicit parent labels.")
        block_child_mode = _infer_nested_child_mode(left_block=left_block, right_block=right_block)
        if nested_child_mode is None:
            nested_child_mode = block_child_mode
        elif nested_child_mode != block_child_mode:
            raise ValueError("Nested pairing cannot mix child enumeration and child pairing in one comparison group.")

        nested_block = MaterializedNestedParentBlock(
            label=left_block.label or right_block.label,
            left_parent_label=_render_materialized_member(left_block.group_member),
            right_parent_label=_render_materialized_member(right_block.group_member),
        )
        if block_child_mode == "enumeration":
            nested_block.left_members = left_block.members
            nested_block.right_members = right_block.members
        else:
            nested_block.child_pairs = _build_nested_child_pairs(
                left_members=left_block.members,
                right_members=right_block.members,
            )
        nested_blocks.append(nested_block)

    assert nested_child_mode is not None
    return nested_child_mode, nested_blocks


def _infer_nested_child_mode(
    *,
    left_block: MaterializedBlock,
    right_block: MaterializedBlock,
) -> str:
    if not left_block.members or not right_block.members:
        raise ValueError("Nested pairing cannot be materialized losslessly because a parent group lost child members.")

    left_has_labels = all(member.label is not None for member in left_block.members)
    right_has_labels = all(member.label is not None for member in right_block.members)
    if left_has_labels != right_has_labels:
        raise ValueError("Nested pairing cannot be materialized losslessly because child labels are inconsistent.")
    if left_has_labels:
        return "pairing"
    if any(member.label is not None for member in [*left_block.members, *right_block.members]):
        raise ValueError("Nested pairing cannot be materialized losslessly because child labels are incomplete.")
    return "enumeration"


def _build_nested_child_pairs(
    *,
    left_members: list[MaterializedMember],
    right_members: list[MaterializedMember],
) -> list[MaterializedBlock]:
    if len(left_members) != len(right_members):
        raise ValueError("Nested pairing cannot be materialized losslessly because child pairs do not align.")

    pairs: list[MaterializedBlock] = []
    for left_member, right_member in zip(left_members, right_members):
        if left_member.label is None or right_member.label is None:
            raise ValueError("Nested child pairing requires explicit child labels.")
        if _nested_pairing_label_key(left_member.label) != _nested_pairing_label_key(right_member.label):
            raise ValueError("Nested pairing cannot be materialized losslessly because child labels do not align.")
        pairs.append(
            MaterializedBlock(
                label=left_member.label,
                left_label=_render_materialized_member(left_member),
                right_label=_render_materialized_member(right_member),
            )
        )
    return pairs


def _nested_pairing_label_key(label: str) -> str:
    if "年" in label:
        return label.split("年", 1)[1]
    return label

def _collect_pairing_render_targets_from_families(
    *,
    families: list[SemanticFamily],
    entities: dict[str, NormalizedEntity],
) -> list[MaterializedRenderTarget]:
    targets: list[MaterializedRenderTarget] = []
    seen_entity_ids: set[str] = set()
    for family in families:
        for member in family.members:
            if member.entity_id in seen_entity_ids:
                continue
            entity = entities.get(member.entity_id)
            if entity is None:
                raise ValueError(f"Materialization family references unknown entity: {member.entity_id}")
            targets.append(_render_target_from_entity(entity))
            seen_entity_ids.add(member.entity_id)
    return targets


def _enumerate_regular_grain_members(
    *,
    window: NestedWindowSpec,
    grain: str,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> list[MaterializedMember]:
    base_intervals = _resolve_inline_window_intervals(
        window=window,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )
    if len(base_intervals) != 1:
        raise ValueError("Phase1 materialization only supports single-interval regular-grain windows.")

    interval = base_intervals[0]
    members = _split_interval_by_grain(interval=interval, grain=grain)
    if not members:
        raise ValueError("Phase1 materialization requires at least one derived bucket.")
    return members


def _enumerate_calendar_selector_members(
    *,
    start_date: date,
    end_date: date,
    selector: CalendarSelectorSpec,
    business_calendar: BusinessCalendarPort | None,
) -> list[MaterializedMember]:
    matched_dates = _filter_calendar_dates(
        start_date=start_date,
        end_date=end_date,
        selector=selector,
        business_calendar=business_calendar,
    )
    if not matched_dates:
        raise ValueError("Phase1 materialization found no matching calendar members.")
    return [MaterializedMember(start_date=value, end_date=value) for value in matched_dates]


def _split_interval_by_grain(*, interval: Interval, grain: str) -> list[MaterializedMember]:
    if grain == "day":
        return [
            MaterializedMember(start_date=value, end_date=value)
            for value in _iter_dates(interval.start_date, interval.end_date)
        ]
    if grain == "week":
        return _split_interval_into_weeks(interval)
    if grain == "month":
        return _split_interval_into_months(interval)
    if grain == "quarter":
        return _split_interval_into_quarters(interval)
    if grain == "year":
        return _split_interval_into_years(interval)
    raise ValueError(f"Unsupported phase1 regular grain: {grain}")


def _split_interval_into_weeks(interval: Interval) -> list[MaterializedMember]:
    cursor = interval.start_date - timedelta(days=interval.start_date.weekday())
    members: list[MaterializedMember] = []
    index = 1
    while cursor <= interval.end_date:
        bucket_start = max(interval.start_date, cursor)
        bucket_end = min(interval.end_date, cursor + timedelta(days=6))
        if bucket_start <= bucket_end:
            members.append(
                MaterializedMember(
                    label=f"第{index}周",
                    start_date=bucket_start,
                    end_date=bucket_end,
                )
            )
            index += 1
        cursor = cursor + timedelta(days=7)
    return members


def _split_interval_into_months(interval: Interval) -> list[MaterializedMember]:
    cursor = interval.start_date.replace(day=1)
    members: list[MaterializedMember] = []
    while cursor <= interval.end_date:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        month_end = date(cursor.year, cursor.month, last_day)
        bucket_start = max(interval.start_date, cursor)
        bucket_end = min(interval.end_date, month_end)
        if bucket_start <= bucket_end:
            members.append(
                MaterializedMember(
                    label=f"{cursor.year}年{cursor.month}月",
                    start_date=bucket_start,
                    end_date=bucket_end,
                )
            )
        cursor = _add_months(cursor, 1)
    return members


def _split_interval_into_quarters(interval: Interval) -> list[MaterializedMember]:
    start_month = ((interval.start_date.month - 1) // 3) * 3 + 1
    cursor = date(interval.start_date.year, start_month, 1)
    members: list[MaterializedMember] = []
    while cursor <= interval.end_date:
        quarter = ((cursor.month - 1) // 3) + 1
        quarter_end_month = quarter * 3
        quarter_end_day = calendar.monthrange(cursor.year, quarter_end_month)[1]
        quarter_end = date(cursor.year, quarter_end_month, quarter_end_day)
        bucket_start = max(interval.start_date, cursor)
        bucket_end = min(interval.end_date, quarter_end)
        if bucket_start <= bucket_end:
            members.append(
                MaterializedMember(
                    label=f"{cursor.year}年第{quarter}季度",
                    start_date=bucket_start,
                    end_date=bucket_end,
                )
            )
        cursor = _add_months(cursor, 3)
    return members


def _split_interval_into_years(interval: Interval) -> list[MaterializedMember]:
    cursor = date(interval.start_date.year, 1, 1)
    members: list[MaterializedMember] = []
    while cursor <= interval.end_date:
        year_end = date(cursor.year, 12, 31)
        bucket_start = max(interval.start_date, cursor)
        bucket_end = min(interval.end_date, year_end)
        if bucket_start <= bucket_end:
            members.append(
                MaterializedMember(
                    label=f"{cursor.year}年",
                    start_date=bucket_start,
                    end_date=bucket_end,
                )
            )
        cursor = date(cursor.year + 1, 1, 1)
    return members


def _render_target_from_node(node: ClarificationNode) -> MaterializedRenderTarget:
    return MaterializedRenderTarget(
        render_text=node.render_text,
        ordinal=node.ordinal,
        surface_fragments=node.surface_fragments,
    )


def _render_target_from_entity(entity: NormalizedEntity) -> MaterializedRenderTarget:
    return MaterializedRenderTarget(
        render_text=entity.render_text or entity.entity_id,
        ordinal=entity.ordinal or 0,
        surface_fragments=entity.surface_fragments,
    )


def _render_materialized_member(member: MaterializedMember) -> str:
    interval_text = _render_interval(Interval(start_date=member.start_date, end_date=member.end_date))
    if member.label is None:
        return interval_text
    return f"{member.label}（{interval_text}）"


def _iter_dates(start_date: date, end_date: date) -> list[date]:
    values: list[date] = []
    cursor = start_date
    while cursor <= end_date:
        values.append(cursor)
        cursor = cursor + timedelta(days=1)
    return values


def _resolve_relative_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
) -> list[Interval]:
    spec = RelativeWindowResolutionSpec.model_validate(node.resolution_spec)
    if spec.relative_type == "single_relative" and spec.unit == "day" and spec.direction == "previous":
        target_date = anchor_date - timedelta(days=spec.value)
        return [Interval(start_date=target_date, end_date=target_date)]
    if spec.relative_type == "single_relative" and spec.unit == "week" and spec.direction == "previous":
        current_week_monday = anchor_date - timedelta(days=anchor_date.weekday())
        start_date = current_week_monday - timedelta(weeks=spec.value)
        end_date = start_date + timedelta(days=6)
        return [Interval(start_date=start_date, end_date=end_date)]
    if spec.relative_type == "single_relative" and spec.unit == "month" and spec.direction == "previous":
        target_year = anchor_date.year
        target_month = anchor_date.month - spec.value
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        end_day = calendar.monthrange(target_year, target_month)[1]
        return [Interval(start_date=date(target_year, target_month, 1), end_date=date(target_year, target_month, end_day))]
    if spec.relative_type == "single_relative" and spec.unit == "quarter" and spec.direction == "previous":
        current_quarter = (anchor_date.month - 1) // 3 + 1
        target_quarter = current_quarter - spec.value
        target_year = anchor_date.year
        while target_quarter <= 0:
            target_quarter += 4
            target_year -= 1
        start_month = (target_quarter - 1) * 3 + 1
        end_month = start_month + 2
        end_day = calendar.monthrange(target_year, end_month)[1]
        return [Interval(start_date=date(target_year, start_month, 1), end_date=date(target_year, end_month, end_day))]
    if spec.relative_type == "single_relative" and spec.unit == "year" and spec.direction == "previous":
        target_year = anchor_date.year - spec.value
        return [Interval(start_date=date(target_year, 1, 1), end_date=date(target_year, 12, 31))]
    if spec.relative_type == "to_date" and spec.unit == "month" and spec.direction == "current":
        start_date = anchor_date.replace(day=1)
        end_date = anchor_date if spec.include_today else anchor_date - timedelta(days=1)
        return [Interval(start_date=start_date, end_date=end_date)]
    if spec.relative_type == "to_date" and spec.unit == "quarter" and spec.direction == "current":
        start_month = ((anchor_date.month - 1) // 3) * 3 + 1
        start_date = date(anchor_date.year, start_month, 1)
        end_date = anchor_date if spec.include_today else anchor_date - timedelta(days=1)
        return [Interval(start_date=start_date, end_date=end_date)]
    if spec.relative_type == "to_date" and spec.unit == "year" and spec.direction == "current":
        start_date = date(anchor_date.year, 1, 1)
        end_date = anchor_date if spec.include_today else anchor_date - timedelta(days=1)
        return [Interval(start_date=start_date, end_date=end_date)]
    raise ValueError(
        "Current resolver slice only supports previous single-day/week/month/quarter/year relative windows "
        "and current month/quarter/year-to-date windows."
    )


def _resolve_holiday_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
) -> list[Interval]:
    if business_calendar is None:
        raise ValueError("Business calendar is required for holiday_window resolution.")

    spec = HolidayWindowResolutionSpec.model_validate(node.resolution_spec)
    schedule_year = _resolve_year_ref(spec.year_ref, anchor_date=anchor_date)
    scope = "statutory" if spec.calendar_mode == "statutory" else "consecutive_rest"
    holiday_range = business_calendar.get_event_span(
        region="CN",
        event_key=spec.holiday_key,
        schedule_year=schedule_year,
        scope=scope,
    )
    if holiday_range is None:
        raise ValueError(
            f"Missing business calendar data for holiday={spec.holiday_key}, schedule_year={schedule_year}."
        )
    return [Interval(start_date=holiday_range[0], end_date=holiday_range[1])]


def _resolve_explicit_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
) -> list[Interval]:
    spec = ExplicitWindowResolutionSpec.model_validate(node.resolution_spec)
    if spec.window_type == "single_date":
        return [Interval(start_date=spec.start_date, end_date=spec.start_date)]
    if spec.window_type == "date_range":
        return [Interval(start_date=spec.start_date, end_date=spec.end_date)]
    if spec.window_type == "named_period_range":
        start_date = _resolve_named_period_point_start(spec=spec, anchor_date=anchor_date, point=spec.start_period)
        end_date = _resolve_named_period_point_end(spec=spec, anchor_date=anchor_date, point=spec.end_period)
        return [Interval(start_date=start_date, end_date=end_date)]
    if spec.window_type != "named_period":
        raise ValueError(f"Unsupported explicit window_type: {spec.window_type}")

    year = _resolve_year_ref(spec.year_ref or YearRef(mode="absolute", year=anchor_date.year), anchor_date=anchor_date)
    if spec.calendar_unit == "year":
        return [Interval(start_date=date(year, 1, 1), end_date=date(year, 12, 31))]
    if spec.calendar_unit == "month":
        if spec.month is None:
            raise ValueError("month explicit_window requires month")
        month_last_day = calendar.monthrange(year, spec.month)[1]
        return [Interval(start_date=date(year, spec.month, 1), end_date=date(year, spec.month, month_last_day))]
    if spec.calendar_unit == "quarter":
        if spec.quarter is None:
            raise ValueError("quarter explicit_window requires quarter")
        start_month = (spec.quarter - 1) * 3 + 1
        end_month = start_month + 2
        end_day = calendar.monthrange(year, end_month)[1]
        return [Interval(start_date=date(year, start_month, 1), end_date=date(year, end_month, end_day))]
    if spec.calendar_unit == "half":
        start_month = 1 if spec.half == 1 else 7
        end_month = 6 if spec.half == 1 else 12
        end_day = calendar.monthrange(year, end_month)[1]
        return [Interval(start_date=date(year, start_month, 1), end_date=date(year, end_month, end_day))]
    raise ValueError(f"Unsupported explicit calendar_unit for current resolver slice: {spec.calendar_unit}")

def _resolve_offset_window_intervals(
    *,
    node: ClarificationNode,
    anchor_date: date,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
    business_calendar: BusinessCalendarPort | None,
) -> list[Interval]:
    spec = OffsetWindowResolutionSpec.model_validate(node.resolution_spec)
    if spec.offset.unit != "day":
        raise ValueError("Current resolver slice only supports day-based offset windows.")

    base_intervals = _resolve_offset_base_intervals(
        spec=spec,
        anchor_date=anchor_date,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
        business_calendar=business_calendar,
    )
    if len(base_intervals) != 1:
        raise ValueError("Current resolver slice only supports single-interval offset bases.")

    base_interval = base_intervals[0]
    count = spec.offset.value
    if spec.offset.direction == "after":
        start_date = base_interval.end_date + timedelta(days=1)
        end_date = start_date + timedelta(days=count - 1)
    else:
        end_date = base_interval.start_date - timedelta(days=1)
        start_date = end_date - timedelta(days=count - 1)
    return [Interval(start_date=start_date, end_date=end_date)]


def _resolve_offset_base_intervals(
    *,
    spec: OffsetWindowResolutionSpec,
    anchor_date: date,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
    business_calendar: BusinessCalendarPort | None,
) -> list[Interval]:
    if spec.base.source == "node_ref":
        reference_node = node_lookup.get(spec.base.node_id)
        if reference_node is None:
            raise ValueError(f"Missing offset base node: {spec.base.node_id}")
        return _resolve_node_intervals(
            node=reference_node,
            anchor_date=anchor_date,
            business_calendar=business_calendar,
            node_lookup=node_lookup,
            resolved_by_node_id=resolved_by_node_id,
        )

    return _resolve_inline_window_intervals(
        window=spec.base.window,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )


def _resolve_anchor_date(
    *,
    system_date: str | None,
    system_datetime: str | None,
    timezone: str,
) -> date:
    if system_date:
        return date.fromisoformat(system_date)
    if system_datetime:
        return datetime.fromisoformat(system_datetime).date()
    return datetime.now(ZoneInfo(timezone)).date()


def _resolve_inline_window_intervals(
    *,
    window: NestedWindowSpec,
    anchor_date: date,
    business_calendar: BusinessCalendarPort | None,
    node_lookup: dict[str, ClarificationNode],
    resolved_by_node_id: dict[str, list[Interval]],
) -> list[Interval]:
    inline_node = ClarificationNode(
        node_id=f"__inline__::{len(resolved_by_node_id)}",
        render_text="__inline__",
        ordinal=0,
        needs_clarification=False,
        node_kind=window.kind,
        reason_code="structural_enumeration",
        resolution_spec=window.value,
        surface_fragments=[],
    )
    return _resolve_node_intervals(
        node=inline_node,
        anchor_date=anchor_date,
        business_calendar=business_calendar,
        node_lookup=node_lookup,
        resolved_by_node_id=resolved_by_node_id,
    )


def _resolve_year_ref(year_ref: YearRef, *, anchor_date: date) -> int:
    if year_ref.mode == "absolute":
        assert year_ref.year is not None
        return year_ref.year
    assert year_ref.offset is not None
    return anchor_date.year + year_ref.offset


def _filter_calendar_dates(
    *,
    start_date: date,
    end_date: date,
    selector: CalendarSelectorSpec,
    business_calendar: BusinessCalendarPort | None,
) -> list[date]:
    if business_calendar is None:
        raise ValueError("Business calendar is required for calendar-sensitive selectors.")

    matched: list[date] = []
    cursor = start_date
    while cursor <= end_date:
        status = business_calendar.get_day_status(region="CN", d=cursor)
        if selector.selector_type == "workday" and status.is_workday:
            matched.append(cursor)
        elif selector.selector_type == "holiday" and status.is_holiday:
            matched.append(cursor)
        cursor += timedelta(days=1)
    return matched


def _compress_dates_to_intervals(dates: list[date]) -> list[Interval]:
    if not dates:
        return []

    intervals: list[Interval] = []
    current_start = dates[0]
    current_end = dates[0]

    for current in dates[1:]:
        if current == current_end + timedelta(days=1):
            current_end = current
            continue
        intervals.append(Interval(start_date=current_start, end_date=current_end))
        current_start = current
        current_end = current

    intervals.append(Interval(start_date=current_start, end_date=current_end))
    return intervals


def _shift_interval(interval: Interval, *, unit: str, value: int) -> Interval:
    if unit == "day":
        return Interval(
            start_date=interval.start_date + timedelta(days=value),
            end_date=interval.end_date + timedelta(days=value),
        )
    if unit == "week":
        return Interval(
            start_date=interval.start_date + timedelta(weeks=value),
            end_date=interval.end_date + timedelta(weeks=value),
        )
    if unit == "month":
        return Interval(
            start_date=_add_months(interval.start_date, value),
            end_date=_add_months(interval.end_date, value),
        )
    if unit == "quarter":
        return Interval(
            start_date=_add_months(interval.start_date, value * 3),
            end_date=_add_months(interval.end_date, value * 3),
        )
    if unit == "year":
        return Interval(
            start_date=_add_years(interval.start_date, value),
            end_date=_add_years(interval.end_date, value),
        )
    raise ValueError(f"Unsupported shift unit: {unit}")


def _add_months(value: date, months: int) -> date:
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _add_years(value: date, years: int) -> date:
    target_year = value.year + years
    if value.month == 2 and value.day == 29 and not calendar.isleap(target_year):
        return date(target_year, 2, 28)
    return date(target_year, value.month, value.day)


def _resolve_named_period_point_start(
    *,
    spec: ExplicitWindowResolutionSpec,
    anchor_date: date,
    point,
) -> date:
    year = _resolve_year_ref(point.year_ref, anchor_date=anchor_date)
    if spec.calendar_unit == "year":
        return date(year, 1, 1)
    if spec.calendar_unit == "month":
        return date(year, point.month, 1)
    if spec.calendar_unit == "quarter":
        start_month = (point.quarter - 1) * 3 + 1
        return date(year, start_month, 1)
    if spec.calendar_unit == "half":
        start_month = 1 if point.half == 1 else 7
        return date(year, start_month, 1)
    raise ValueError(f"Unsupported named_period_range calendar_unit: {spec.calendar_unit}")


def _resolve_named_period_point_end(
    *,
    spec: ExplicitWindowResolutionSpec,
    anchor_date: date,
    point,
) -> date:
    year = _resolve_year_ref(point.year_ref, anchor_date=anchor_date)
    if spec.calendar_unit == "year":
        return date(year, 12, 31)
    if spec.calendar_unit == "month":
        end_day = calendar.monthrange(year, point.month)[1]
        return date(year, point.month, end_day)
    if spec.calendar_unit == "quarter":
        end_month = point.quarter * 3
        end_day = calendar.monthrange(year, end_month)[1]
        return date(year, end_month, end_day)
    if spec.calendar_unit == "half":
        end_month = 6 if point.half == 1 else 12
        end_day = calendar.monthrange(year, end_month)[1]
        return date(year, end_month, end_day)
    raise ValueError(f"Unsupported named_period_range calendar_unit: {spec.calendar_unit}")


def _render_intervals(intervals: list[Interval]) -> str:
    return "、".join(_render_interval(interval) for interval in intervals)


def _render_interval(interval: Interval) -> str:
    if interval.start_date == interval.end_date:
        return _format_date(interval.start_date)
    return f"{_format_date(interval.start_date)}至{_format_date(interval.end_date)}"


def _format_date(value: date) -> str:
    return f"{value.year}年{value.month}月{value.day}日"
