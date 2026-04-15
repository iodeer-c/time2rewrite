from __future__ import annotations

from typing import Any

from pydantic import Field

from time_query_service.contracts import ClarificationPlan, StrictModel


class PlanValidationResult(StrictModel):
    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    plan: ClarificationPlan | None = None


def validate_plan(plan_payload: ClarificationPlan | dict[str, Any]) -> PlanValidationResult:
    try:
        plan = (
            plan_payload
            if isinstance(plan_payload, ClarificationPlan)
            else ClarificationPlan.model_validate(plan_payload)
        )
    except Exception as exc:
        return PlanValidationResult(is_valid=False, errors=[str(exc)])

    node_ids = {node.node_id for node in plan.nodes}
    errors: list[str] = []

    if len(node_ids) != len(plan.nodes):
        errors.append("Duplicate node_id detected in clarification plan.")

    for group in plan.comparison_groups:
        seen_member_ids: set[str] = set()
        if len(group.members) < 2:
            errors.append(f"comparison_group={group.group_id} must contain at least two members.")
        for member in group.members:
            if member.node_id not in node_ids:
                errors.append(
                    f"comparison_group={group.group_id} references missing node_id={member.node_id}."
                )
            if member.node_id in seen_member_ids:
                errors.append(
                    f"comparison_group={group.group_id} contains duplicate node_id={member.node_id}."
                )
            seen_member_ids.add(member.node_id)

    return PlanValidationResult(
        is_valid=not errors,
        errors=errors,
        plan=plan if not errors else None,
    )
