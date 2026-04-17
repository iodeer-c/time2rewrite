from __future__ import annotations

from typing import Any

from pydantic import Field

from time_query_service.contracts import ClarificationPlan, StrictModel


class PlanValidationResult(StrictModel):
    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    plan: ClarificationPlan | None = None


COMPARE_TRIGGERS: tuple[str, ...] = (
    "比较",
    "相比",
    "对比",
    "同比",
    "环比",
    "高于",
    "低于",
    "增长",
    "下降",
    "增幅",
    "降幅",
)

PARALLEL_STANDALONE_TRIGGERS: tuple[str, ...] = (
    "和",
    "与",
    "以及",
    "分别",
    "各自",
    "各",
    "分别是",
    "分别为",
)


def validate_comparison_group_boundaries(plan: ClarificationPlan) -> list[str]:
    errors: list[str] = []

    for index, group in enumerate(plan.comparison_groups):
        member_ids = {member.node_id for member in group.members}
        for other in plan.comparison_groups[index + 1 :]:
            shared_member_ids = member_ids.intersection(member.node_id for member in other.members)
            if not shared_member_ids:
                continue
            if not group.surface_fragments or not other.surface_fragments:
                errors.append(
                    "comparison_groups="
                    f"{group.group_id},{other.group_id} have shared node_id(s) {sorted(shared_member_ids)} "
                    "but do not carry explicit surface_fragments for an independent family boundary."
                )
                continue
            if tuple(group.surface_fragments) == tuple(other.surface_fragments):
                errors.append(
                    "comparison_groups="
                    f"{group.group_id},{other.group_id} have shared node_id(s) {sorted(shared_member_ids)} "
                    "but reuse the same surface_fragments, so the family boundary is ambiguous."
                )

    return errors


def _find_trigger_hits(query: str, triggers: tuple[str, ...]) -> list[str]:
    return [trigger for trigger in triggers if trigger in query]


def validate_relation_intent(
    *,
    plan: ClarificationPlan,
    original_query: str,
) -> list[str]:
    if not plan.comparison_groups:
        return []

    compare_hits = _find_trigger_hits(original_query, COMPARE_TRIGGERS)
    if compare_hits:
        return []

    parallel_hits = _find_trigger_hits(original_query, PARALLEL_STANDALONE_TRIGGERS)
    if parallel_hits:
        return [
            "query expresses parallel standalone intent via "
            f"{parallel_hits}; do not emit comparison_groups"
        ]

    group_ids = ",".join(group.group_id for group in plan.comparison_groups)
    return [
        "comparison_group"
        f"{'s' if len(plan.comparison_groups) > 1 else ''}={group_ids} "
        "is not admitted because the query lacks explicit comparison trigger"
    ]


def validate_plan(
    plan_payload: ClarificationPlan | dict[str, Any],
    *,
    original_query: str | None = None,
) -> PlanValidationResult:
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
    errors.extend(validate_comparison_group_boundaries(plan))
    if original_query:
        errors.extend(validate_relation_intent(plan=plan, original_query=original_query))

    return PlanValidationResult(
        is_valid=not errors,
        errors=errors,
        plan=plan if not errors else None,
    )
