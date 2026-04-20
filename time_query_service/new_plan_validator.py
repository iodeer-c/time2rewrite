from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from time_query_service.post_processor import (
    PostProcessorValidationError,
    _validate_non_head_non_day_grain_expansion,
    _validate_carrier_semantics,
    _validate_layer4,
)
from time_query_service.time_plan import GrainExpansion, TimePlan


@dataclass
class TimePlanValidationError(Exception):
    layer: int
    details: str
    unit_id: str | None = None

    def __str__(self) -> str:
        unit_part = f", unit={self.unit_id}" if self.unit_id is not None else ""
        return f"TimePlan layer {self.layer} validation failed{unit_part}: {self.details}"


class TimePlanSemanticValidationError(TimePlanValidationError):
    pass


class TimePlanTopologyValidationError(TimePlanValidationError):
    pass


def validate_time_plan(plan: TimePlan) -> None:
    try:
        _validate_semantics(plan)
    except PostProcessorValidationError as exc:
        raise TimePlanSemanticValidationError(layer=3, details=exc.details, unit_id=exc.unit_id) from exc

    try:
        _validate_layer4(plan.units, plan.comparisons, system_datetime=plan.system_datetime)
    except PostProcessorValidationError as exc:
        raise TimePlanTopologyValidationError(layer=4, details=exc.details, unit_id=exc.unit_id) from exc


def _validate_semantics(plan: TimePlan) -> None:
    for unit in plan.units:
        if unit.content.content_kind == "standalone":
            if not unit.needs_clarification and unit.content.carrier is None:
                raise PostProcessorValidationError(
                    layer=3,
                    stage="new_plan_validator",
                    unit_id=unit.unit_id,
                    details="healthy standalone unit requires carrier",
                )
            if unit.content.carrier is not None:
                _reject_transient_non_day_grain_expansion(unit)
                _validate_carrier_semantics(SimpleNamespace(surface_hint=None), unit.content.carrier, unit_id=unit.unit_id)
        elif not unit.content.sources:
            raise PostProcessorValidationError(
                layer=3,
                stage="new_plan_validator",
                unit_id=unit.unit_id,
                details="derived unit requires non-empty sources",
            )


def _reject_transient_non_day_grain_expansion(unit) -> None:
    carrier = unit.content.carrier
    if carrier is None:
        return
    _validate_non_head_non_day_grain_expansion(carrier, unit_id=unit.unit_id)
    if any(
        isinstance(modifier, GrainExpansion) and modifier.target_grain not in {"day", "hour"}
        for modifier in carrier.modifiers
    ):
        raise PostProcessorValidationError(
            layer=3,
            stage="new_plan_validator",
            unit_id=unit.unit_id,
            details="final TimePlan MUST NOT retain non-day GrainExpansion; canonicalize to GroupedTemporalValue first",
        )
