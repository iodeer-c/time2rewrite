from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from time_query_service.clarification_contract import (
    SUPPORTED_CALENDAR_SELECTOR_TYPES,
    SUPPORTED_EXPLICIT_PERIOD_UNITS,
    SUPPORTED_OFFSET_UNITS,
    SUPPORTED_REFERENCE_ALIGNMENTS,
    SUPPORTED_RELATIVE_TYPES,
    SUPPORTED_SINGLE_RELATIVE_UNITS,
    SUPPORTED_TO_DATE_UNITS,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


NodeKind = Literal[
    "explicit_window",
    "relative_window",
    "holiday_window",
    "offset_window",
    "window_with_regular_grain",
    "window_with_calendar_selector",
    "window_with_member_selection",
]

ReasonCode = Literal[
    "relative_time",
    "rolling_or_to_date",
    "holiday_or_business_calendar",
    "offset_from_anchor",
    "structural_enumeration",
    "already_explicit_natural_period",
    "shared_prefix_explicit",
    "same_period_reference",
]

RelationType = Literal[
    "year_over_year",
    "period_over_period",
    "same_period_reference",
    "generic_compare",
]

Direction = Literal["subject_to_reference", "reference_to_subject", "symmetric"]
ComparisonRole = Literal["subject", "reference", "peer"]
Alignment = Literal["same_period"]
CalendarSelectorType = Literal["holiday", "workday"]
CalendarMode = Literal["statutory", "configured"]
WindowType = Literal["named_period", "named_period_range", "date_range", "single_date"]
CalendarUnit = Literal["day", "week", "month", "quarter", "half", "year"]
RelativeType = Literal["single_relative", "to_date"]
RelativeDirection = Literal["current", "previous"]
OffsetDirection = Literal["before", "after"]
RelativeUnit = Literal["day", "week", "month", "quarter", "year"]
OffsetUnit = Literal["day"]
ShiftUnit = Literal["day", "week", "month", "quarter", "year"]
RegularGrain = Literal["day", "week", "month", "quarter", "year"]
MemberSelectionMode = Literal["first", "last", "nth", "nth_from_end"]
InheritanceMode = Literal[
    "scalar_projection",
    "preserve_flat_carrier",
    "preserve_grouped_carrier",
    "rebind_nested_base",
]
CarrierPathSlot = Literal["window", "base"]
NestedWindowKind = Literal[
    "explicit_window",
    "relative_window",
    "holiday_window",
    "offset_window",
    "window_with_regular_grain",
    "window_with_calendar_selector",
    "window_with_member_selection",
]


class YearRef(StrictModel):
    mode: Literal["absolute", "relative"]
    year: int | None = None
    offset: int | None = None

    @model_validator(mode="after")
    def validate_payload(self) -> "YearRef":
        if self.mode == "absolute" and self.year is None:
            raise ValueError("absolute year_ref requires year")
        if self.mode == "relative" and self.offset is None:
            raise ValueError("relative year_ref requires offset")
        return self


class NamedPeriodPoint(StrictModel):
    year_ref: YearRef
    month: int | None = None
    quarter: int | None = None
    half: int | None = None


class OffsetSpec(StrictModel):
    direction: OffsetDirection
    value: int
    unit: OffsetUnit

    @model_validator(mode="after")
    def validate_offset(self) -> "OffsetSpec":
        if self.unit not in SUPPORTED_OFFSET_UNITS:
            raise ValueError(f"Unsupported offset unit: {self.unit}")
        return self


class ShiftSpec(StrictModel):
    unit: ShiftUnit
    value: int


class CalendarSelectorSpec(StrictModel):
    selector_type: CalendarSelectorType
    selector_key: str | None = None

    @model_validator(mode="after")
    def validate_selector(self) -> "CalendarSelectorSpec":
        if self.selector_type not in SUPPORTED_CALENDAR_SELECTOR_TYPES:
            raise ValueError(f"Unsupported calendar selector type: {self.selector_type}")
        if self.selector_key is not None:
            raise ValueError("selector_key is not supported by the admitted ClarificationPlan contract")
        return self


class NestedWindowSpec(StrictModel):
    kind: NestedWindowKind
    value: Any

    @model_validator(mode="after")
    def validate_value(self) -> "NestedWindowSpec":
        self.value = validate_resolution_spec_for_kind(self.kind, self.value)
        return self


class ExplicitWindowResolutionSpec(StrictModel):
    window_type: WindowType
    calendar_unit: CalendarUnit
    year_ref: YearRef | None = None
    month: int | None = None
    quarter: int | None = None
    half: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    start_period: NamedPeriodPoint | None = None
    end_period: NamedPeriodPoint | None = None

    @model_validator(mode="after")
    def validate_payload(self) -> "ExplicitWindowResolutionSpec":
        if self.window_type == "single_date":
            if self.calendar_unit != "day":
                raise ValueError("single_date explicit_window requires calendar_unit=day")
            if self.start_date is None:
                raise ValueError("single_date explicit_window requires start_date")
            if any(
                value is not None
                for value in (self.end_date, self.year_ref, self.month, self.quarter, self.half, self.start_period, self.end_period)
            ):
                raise ValueError("single_date explicit_window only supports start_date")
            return self

        if self.window_type == "date_range":
            if self.calendar_unit != "day":
                raise ValueError("date_range explicit_window requires calendar_unit=day")
            if self.start_date is None or self.end_date is None:
                raise ValueError("date_range explicit_window requires start_date and end_date")
            if any(
                value is not None
                for value in (self.year_ref, self.month, self.quarter, self.half, self.start_period, self.end_period)
            ):
                raise ValueError("date_range explicit_window only supports start_date and end_date")
            return self

        if self.calendar_unit not in SUPPORTED_EXPLICIT_PERIOD_UNITS:
            raise ValueError(
                f"{self.window_type} explicit_window requires calendar_unit in {SUPPORTED_EXPLICIT_PERIOD_UNITS}"
            )

        if self.window_type == "named_period":
            if self.year_ref is None:
                raise ValueError("named_period explicit_window requires year_ref")
            if any(
                value is not None for value in (self.start_date, self.end_date, self.start_period, self.end_period)
            ):
                raise ValueError("named_period explicit_window does not support date or period range endpoints")
            self._validate_single_period_fields()
            return self

        if self.window_type != "named_period_range":
            raise ValueError(f"Unsupported explicit window_type: {self.window_type}")

        if self.start_period is None or self.end_period is None:
            raise ValueError("named_period_range explicit_window requires start_period and end_period")
        if any(
            value is not None
            for value in (self.year_ref, self.month, self.quarter, self.half, self.start_date, self.end_date)
        ):
            raise ValueError("named_period_range explicit_window only supports start_period and end_period")
        self._validate_period_point("start_period", self.start_period)
        self._validate_period_point("end_period", self.end_period)
        return self

    def _validate_single_period_fields(self) -> None:
        if self.calendar_unit == "year":
            if any(value is not None for value in (self.month, self.quarter, self.half)):
                raise ValueError("year named_period explicit_window does not support month/quarter/half fields")
            return
        if self.calendar_unit == "month":
            if self.month is None or self.quarter is not None or self.half is not None:
                raise ValueError("month named_period explicit_window requires month only")
            return
        if self.calendar_unit == "quarter":
            if self.quarter is None or self.month is not None or self.half is not None:
                raise ValueError("quarter named_period explicit_window requires quarter only")
            return
        if self.calendar_unit == "half":
            if self.half is None or self.month is not None or self.quarter is not None:
                raise ValueError("half named_period explicit_window requires half only")
            return
        raise ValueError(f"Unsupported named_period calendar_unit: {self.calendar_unit}")

    def _validate_period_point(self, label: str, point: NamedPeriodPoint) -> None:
        if self.calendar_unit == "year":
            if any(value is not None for value in (point.month, point.quarter, point.half)):
                raise ValueError(f"{label} for year range cannot include month/quarter/half")
            return
        if self.calendar_unit == "month":
            if point.month is None or point.quarter is not None or point.half is not None:
                raise ValueError(f"{label} for month range requires month only")
            return
        if self.calendar_unit == "quarter":
            if point.quarter is None or point.month is not None or point.half is not None:
                raise ValueError(f"{label} for quarter range requires quarter only")
            return
        if self.calendar_unit == "half":
            if point.half is None or point.month is not None or point.quarter is not None:
                raise ValueError(f"{label} for half range requires half only")
            return
        raise ValueError(f"Unsupported named_period_range calendar_unit: {self.calendar_unit}")


class RelativeWindowResolutionSpec(StrictModel):
    relative_type: RelativeType
    unit: RelativeUnit
    direction: RelativeDirection
    value: int
    include_today: bool = False

    @model_validator(mode="after")
    def validate_payload(self) -> "RelativeWindowResolutionSpec":
        if self.relative_type not in SUPPORTED_RELATIVE_TYPES:
            raise ValueError(f"Unsupported relative_type: {self.relative_type}")
        if self.relative_type == "single_relative":
            if self.direction != "previous" or self.unit not in SUPPORTED_SINGLE_RELATIVE_UNITS:
                raise ValueError("single_relative only supports previous day/week/month/quarter/year windows")
            return self
        if self.direction != "current" or self.unit not in SUPPORTED_TO_DATE_UNITS:
            raise ValueError("to_date only supports current month/quarter/year windows")
        return self


class HolidayWindowResolutionSpec(StrictModel):
    holiday_key: str
    year_ref: YearRef
    calendar_mode: CalendarMode = "configured"


class InlineOffsetBase(StrictModel):
    source: Literal["inline"]
    window: NestedWindowSpec


class NodeRefOffsetBase(StrictModel):
    source: Literal["node_ref"]
    node_id: str


OffsetBase = InlineOffsetBase | NodeRefOffsetBase


class OffsetWindowResolutionSpec(StrictModel):
    base: OffsetBase
    offset: OffsetSpec


class RebindTargetPathSegment(StrictModel):
    carrier_kind: NodeKind
    slot: CarrierPathSlot


class ReferenceDerivationSpec(StrictModel):
    source_node_id: str
    alignment: Alignment
    shift: ShiftSpec
    inheritance_mode: InheritanceMode
    rebind_target_path: list[RebindTargetPathSegment] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_alignment(self) -> "ReferenceDerivationSpec":
        if self.alignment not in SUPPORTED_REFERENCE_ALIGNMENTS:
            raise ValueError(f"Unsupported reference alignment: {self.alignment}")
        if self.inheritance_mode == "rebind_nested_base":
            if not self.rebind_target_path:
                raise ValueError("rebind_nested_base derivation requires rebind_target_path")
        elif self.rebind_target_path:
            raise ValueError("rebind_target_path is only supported when inheritance_mode=rebind_nested_base")
        return self


class CarrierSpec(StrictModel):
    kind: NodeKind
    value: Any

    @model_validator(mode="after")
    def validate_value(self) -> "CarrierSpec":
        self.value = validate_resolution_spec_for_kind(self.kind, self.value)
        return self


class WindowWithRegularGrainResolutionSpec(StrictModel):
    window: NestedWindowSpec
    grain: RegularGrain


class WindowWithCalendarSelectorResolutionSpec(StrictModel):
    window: NestedWindowSpec
    selector: CalendarSelectorSpec


class MemberSelectionSpec(StrictModel):
    mode: MemberSelectionMode
    index: int | None = None
    count: int | None = None

    @model_validator(mode="after")
    def validate_selection(self) -> "MemberSelectionSpec":
        if self.mode in {"first", "last"}:
            if self.index is not None:
                raise ValueError("first/last member selection does not support index")
            if self.count is not None and self.count <= 0:
                raise ValueError("first/last member selection count must be positive")
            return self
        if self.count is not None:
            raise ValueError("nth/nth_from_end member selection does not support count")
        if self.index is None or self.index <= 0:
            raise ValueError("nth/nth_from_end member selection requires a positive index")
        return self


class WindowWithMemberSelectionResolutionSpec(StrictModel):
    window: NestedWindowSpec
    selection: MemberSelectionSpec


def _nested_pairing_depth_for_window(window: NestedWindowSpec) -> int:
    return _nested_pairing_depth_for_kind(window.kind, window.value)


def _nested_pairing_depth_for_kind(kind: NestedWindowKind | NodeKind, payload: Any) -> int:
    if kind in {
        "explicit_window",
        "relative_window",
        "holiday_window",
        "offset_window",
    }:
        return 0
    if kind == "window_with_member_selection":
        spec = WindowWithMemberSelectionResolutionSpec.model_validate(payload)
        return _nested_pairing_depth_for_window(spec.window)
    if kind == "window_with_calendar_selector":
        spec = WindowWithCalendarSelectorResolutionSpec.model_validate(payload)
        depth = _nested_pairing_depth_for_window(spec.window)
        if spec.window.kind == "window_with_regular_grain":
            return depth + 1
        return depth
    if kind == "window_with_regular_grain":
        spec = WindowWithRegularGrainResolutionSpec.model_validate(payload)
        depth = _nested_pairing_depth_for_window(spec.window)
        if spec.window.kind == "window_with_regular_grain":
            return depth + 1
        return depth
    raise ValueError(f"Unsupported nested pairing depth kind: {kind}")


ResolutionSpec = (
    ExplicitWindowResolutionSpec
    | RelativeWindowResolutionSpec
    | HolidayWindowResolutionSpec
    | OffsetWindowResolutionSpec
    | WindowWithRegularGrainResolutionSpec
    | WindowWithCalendarSelectorResolutionSpec
    | WindowWithMemberSelectionResolutionSpec
)

NODE_KIND_TO_SPEC_MODEL: dict[NodeKind, type[ResolutionSpec]] = {
    "explicit_window": ExplicitWindowResolutionSpec,
    "relative_window": RelativeWindowResolutionSpec,
    "holiday_window": HolidayWindowResolutionSpec,
    "offset_window": OffsetWindowResolutionSpec,
    "window_with_regular_grain": WindowWithRegularGrainResolutionSpec,
    "window_with_calendar_selector": WindowWithCalendarSelectorResolutionSpec,
    "window_with_member_selection": WindowWithMemberSelectionResolutionSpec,
}


def validate_resolution_spec_for_kind(kind: NestedWindowKind | NodeKind, payload: Any) -> ResolutionSpec:
    spec_model = NODE_KIND_TO_SPEC_MODEL[kind]
    return spec_model.model_validate(payload)


def _carrier_structure_for_window(window: NestedWindowSpec) -> str:
    return _carrier_structure_for_kind(window.kind, window.value)


def _carrier_structure_for_kind(kind: NestedWindowKind | NodeKind, payload: Any) -> str:
    if kind in {"explicit_window", "relative_window", "holiday_window", "offset_window"}:
        return "scalar"
    if kind == "window_with_regular_grain":
        spec = WindowWithRegularGrainResolutionSpec.model_validate(payload)
        if spec.window.kind == "window_with_regular_grain":
            return "grouped_enumeration"
        return "flat_enumeration"
    if kind == "window_with_calendar_selector":
        spec = WindowWithCalendarSelectorResolutionSpec.model_validate(payload)
        if spec.window.kind == "window_with_regular_grain":
            return "grouped_enumeration"
        return "flat_enumeration"
    if kind == "window_with_member_selection":
        spec = WindowWithMemberSelectionResolutionSpec.model_validate(payload)
        return _carrier_structure_for_window(spec.window)
    raise ValueError(f"Unsupported carrier structure kind: {kind}")


def _validate_rebind_target_path(carrier: CarrierSpec, path: list[RebindTargetPathSegment]) -> None:
    current_kind = carrier.kind
    current_value: Any = carrier.value

    for segment in path:
        if segment.carrier_kind != current_kind:
            raise ValueError(
                f"rebind_target_path segment carrier_kind={segment.carrier_kind} does not match current carrier kind={current_kind}"
            )
        if current_kind in {"window_with_regular_grain", "window_with_calendar_selector", "window_with_member_selection"}:
            if segment.slot != "window":
                raise ValueError(f"carrier kind={current_kind} only supports rebind_target_path slot=window")
            nested_window = current_value.window
            current_kind = nested_window.kind
            current_value = nested_window.value
            continue
        if current_kind == "offset_window":
            if segment.slot != "base":
                raise ValueError("carrier kind=offset_window only supports rebind_target_path slot=base")
            if current_value.base.source != "inline":
                raise ValueError("rebind_target_path does not support offset base=node_ref")
            nested_window = current_value.base.window
            current_kind = nested_window.kind
            current_value = nested_window.value
            continue
        raise ValueError(f"carrier kind={current_kind} does not support rebind_target_path traversal")


class ComparisonMember(StrictModel):
    node_id: str
    role: ComparisonRole


class ComparisonGroup(StrictModel):
    group_id: str
    relation_type: RelationType
    anchor_text: str
    anchor_ordinal: int
    direction: Direction
    surface_fragments: list[str] = Field(default_factory=list)
    members: list[ComparisonMember]


class Interval(StrictModel):
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_bounds(self) -> "Interval":
        if self.start_date > self.end_date:
            raise ValueError("interval start_date must be on or before end_date")
        return self


class ClarificationItem(StrictModel):
    node_id: str
    render_text: str
    ordinal: int
    display_exact_time: str
    surface_fragments: list[str] = Field(default_factory=list)
    intervals: list[Interval] = Field(default_factory=list)


class ClarificationNode(StrictModel):
    node_id: str
    render_text: str
    ordinal: int
    needs_clarification: bool
    reason_code: ReasonCode
    carrier: CarrierSpec
    derivation: ReferenceDerivationSpec | None = None
    surface_fragments: list[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def coerce_legacy_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "carrier" in data:
            return data
        if "node_kind" not in data or "resolution_spec" not in data:
            return data
        if data["node_kind"] == "reference_window":
            return data
        coerced = dict(data)
        coerced["carrier"] = {
            "kind": coerced.pop("node_kind"),
            "value": coerced.pop("resolution_spec"),
        }
        return coerced

    @property
    def node_kind(self) -> NodeKind:
        return self.carrier.kind

    @property
    def resolution_spec(self) -> ResolutionSpec:
        return self.carrier.value

    @model_validator(mode="after")
    def validate_derivation_compatibility(self) -> "ClarificationNode":
        if self.derivation is None:
            return self

        carrier_structure = _carrier_structure_for_kind(self.carrier.kind, self.carrier.value)
        if self.derivation.inheritance_mode == "scalar_projection" and carrier_structure != "scalar":
            raise ValueError("scalar_projection derivation requires a scalar carrier")
        if self.derivation.inheritance_mode == "preserve_flat_carrier" and carrier_structure != "flat_enumeration":
            raise ValueError("preserve_flat_carrier derivation requires a flat enumeration carrier")
        if self.derivation.inheritance_mode == "preserve_grouped_carrier" and carrier_structure != "grouped_enumeration":
            raise ValueError("preserve_grouped_carrier derivation requires a grouped enumeration carrier")
        if self.derivation.inheritance_mode == "rebind_nested_base":
            if carrier_structure != "grouped_enumeration":
                raise ValueError("rebind_nested_base derivation requires a grouped enumeration carrier")
            _validate_rebind_target_path(self.carrier, self.derivation.rebind_target_path)
        return self


class ClarificationPlan(StrictModel):
    nodes: list[ClarificationNode]
    comparison_groups: list[ComparisonGroup] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_contract_boundaries(self) -> "ClarificationPlan":
        node_lookup = {node.node_id: node for node in self.nodes}
        nested_group_count = 0

        for node in self.nodes:
            if node.derivation is None:
                continue
            if node.derivation.source_node_id not in node_lookup:
                raise ValueError(f"Missing derivation source node_id={node.derivation.source_node_id} for {node.node_id}")
            source_node = node_lookup[node.derivation.source_node_id]
            source_structure = _carrier_structure_for_kind(source_node.node_kind, source_node.resolution_spec)
            target_structure = _carrier_structure_for_kind(node.node_kind, node.resolution_spec)
            if node.derivation.inheritance_mode == "scalar_projection" and source_structure != "scalar":
                raise ValueError(
                    f"scalar_projection derivation requires a scalar source carrier for {node.node_id}"
                )
            if node.derivation.inheritance_mode == "preserve_flat_carrier" and source_structure != "flat_enumeration":
                raise ValueError(
                    f"preserve_flat_carrier derivation requires a flat source carrier for {node.node_id}"
                )
            if (
                node.derivation.inheritance_mode in {"preserve_grouped_carrier", "rebind_nested_base"}
                and source_structure != "grouped_enumeration"
            ):
                raise ValueError(
                    f"{node.derivation.inheritance_mode} derivation requires a grouped source carrier for {node.node_id}"
                )
            if node.derivation.inheritance_mode == "scalar_projection" and target_structure != "scalar":
                raise ValueError(
                    f"scalar_projection derivation requires a scalar target carrier for {node.node_id}"
                )
            if node.derivation.inheritance_mode == "preserve_flat_carrier" and target_structure != "flat_enumeration":
                raise ValueError(
                    f"preserve_flat_carrier derivation requires a flat target carrier for {node.node_id}"
                )
            if (
                node.derivation.inheritance_mode in {"preserve_grouped_carrier", "rebind_nested_base"}
                and target_structure != "grouped_enumeration"
            ):
                raise ValueError(
                    f"{node.derivation.inheritance_mode} derivation requires a grouped target carrier for {node.node_id}"
                )

        for group in self.comparison_groups:
            member_ids = {member.node_id for member in group.members}
            depths: list[int] = []
            for member in group.members:
                node = node_lookup.get(member.node_id)
                if node is None:
                    continue
                if node.derivation is not None and node.derivation.source_node_id not in member_ids:
                    raise ValueError(
                        f"comparison_group={group.group_id} requires derived member {node.node_id} to reference a family-local source node"
                    )
                depths.append(_nested_pairing_depth_for_kind(node.node_kind, node.resolution_spec))

            if any(depth > 1 for depth in depths):
                raise ValueError("Nested pairing only supports one-level child structure beneath a comparison parent.")

            if any(depth == 1 for depth in depths):
                if len(group.members) != 2:
                    raise ValueError("Nested pairing only supports two-member comparison groups.")
                if not all(depth == 1 for depth in depths):
                    raise ValueError(
                        "Nested pairing requires both comparison members to carry the same one-level child structure."
                    )
                nested_group_count += 1

        if nested_group_count and len(self.comparison_groups) > 1:
            raise ValueError("Nested pairing does not support multiple comparison families in the same plan.")

        return self
