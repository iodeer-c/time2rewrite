from __future__ import annotations

from typing import Any

from time_query_service.clarification_artifacts import build_clarification_artifacts
from time_query_service.clarification_writer import (
    ClarificationFact,
    build_clarification_facts,
    render_clarified_query,
    render_clarified_query_from_artifacts,
)
from time_query_service.clarification_writer_prompt import (
    build_clarification_writer_messages,
    build_clarification_writer_messages_from_artifacts,
)
from time_query_service.resolved_plan import ResolvedPlan
from time_query_service.time_plan import TimePlan


def build_time_bindings(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
) -> list[ClarificationFact]:
    return build_clarification_facts(
        original_query=original_query,
        time_plan=time_plan,
        resolved_plan=resolved_plan,
    )


def build_rewriter_payload(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
) -> dict[str, Any]:
    clarification_artifacts = build_clarification_artifacts(
        original_query=original_query,
        time_plan=time_plan,
        resolved_plan=resolved_plan,
    )
    return {
        "original_query": original_query,
        "clarification_artifacts": [artifact.model_dump(mode="python") for artifact in clarification_artifacts],
    }


def build_clarification_writer_payload(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
) -> dict[str, Any]:
    return build_rewriter_payload(
        original_query=original_query,
        time_plan=time_plan,
        resolved_plan=resolved_plan,
    )


def build_rewriter_messages(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
) -> list[Any]:
    clarification_artifacts = build_clarification_artifacts(
        original_query=original_query,
        time_plan=time_plan,
        resolved_plan=resolved_plan,
    )
    return build_clarification_writer_messages_from_artifacts(
        original_query=original_query,
        clarification_artifacts=clarification_artifacts,
    )


def rewrite_query(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
    text_runner: Any | None = None,
) -> str:
    clarification_artifacts = build_clarification_artifacts(
        original_query=original_query,
        time_plan=time_plan,
        resolved_plan=resolved_plan,
    )
    return render_clarified_query_from_artifacts(
        original_query=original_query,
        clarification_artifacts=clarification_artifacts,
        text_runner=text_runner,
    )
