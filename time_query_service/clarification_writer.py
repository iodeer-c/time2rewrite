from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import Field

from time_query_service.clarification_writer_prompt import build_clarification_writer_messages
from time_query_service.resolved_plan import Interval, ResolvedPlan
from time_query_service.time_plan import GrainExpansion, GroupedTemporalValue, StrictModel, TimePlan


ClarificationStatus = Literal["resolved", "unresolved"]


class ClarificationFact(StrictModel):
    unit_id: str
    label: str
    status: ClarificationStatus
    resolved_text: str | None = None
    grouping_grain: str | None = None
    reason_kind: str | None = None
    derived_from: list[str] = Field(default_factory=list)
    comparison_peers: list[str] = Field(default_factory=list)


def build_clarification_facts(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
) -> list[ClarificationFact]:
    comparison_peers = _comparison_peer_map(resolved_plan)
    facts: list[ClarificationFact] = []
    for unit in time_plan.units:
        node = resolved_plan.nodes.get(unit.unit_id)
        grouping_grain = _grouping_grain_for_unit(unit)
        if node is None or node.needs_clarification or node.tree is None or node.tree.labels.absolute_core_time is None:
            facts.append(
                ClarificationFact(
                    unit_id=unit.unit_id,
                    label=unit.render_text,
                    status="unresolved",
                    grouping_grain=grouping_grain,
                    reason_kind=None if node is None else node.reason_kind,
                    derived_from=[] if node is None or node.derived_from is None else list(node.derived_from),
                    comparison_peers=comparison_peers.get(unit.unit_id, []),
                )
            )
            continue
        facts.append(
                ClarificationFact(
                    unit_id=unit.unit_id,
                    label=unit.render_text,
                    status="resolved",
                    resolved_text=_format_interval(node.tree.labels.absolute_core_time),
                    grouping_grain=grouping_grain,
                    derived_from=[] if node.derived_from is None else list(node.derived_from),
                    comparison_peers=comparison_peers.get(unit.unit_id, []),
                )
            )
    return facts


def render_clarified_query(
    *,
    original_query: str,
    clarification_facts: list[ClarificationFact],
    text_runner: Any | None = None,
) -> str:
    if not clarification_facts:
        return original_query
    deterministic = _render_deterministically(
        original_query=original_query,
        clarification_facts=clarification_facts,
    )
    if text_runner is None or not _requires_llm_writer(clarification_facts):
        return deterministic
    response = text_runner.invoke(
        build_clarification_writer_messages(
            original_query=original_query,
            clarification_facts=clarification_facts,
        )
    )
    content = response.content if hasattr(response, "content") else response
    if not isinstance(content, str):
        return deterministic
    rendered = content.strip()
    if not rendered:
        return deterministic
    if not _is_valid_clarified_query(
        original_query=original_query,
        clarification_facts=clarification_facts,
        clarified_query=rendered,
    ):
        return deterministic
    return rendered


def _render_deterministically(
    *,
    original_query: str,
    clarification_facts: list[ClarificationFact],
) -> str:
    parts = [_render_fact(fact) for fact in clarification_facts]
    return f"{original_query}（{'；'.join(parts)}）"


def _render_fact(fact: ClarificationFact) -> str:
    if fact.status == "resolved" and fact.resolved_text is not None:
        if fact.grouping_grain is not None:
            return f"{fact.label}指{fact.resolved_text}，按{_grouping_phrase(fact.grouping_grain)}分组"
        return f"{fact.label}指{fact.resolved_text}"
    return f"{fact.label}当前无法确定"


def _format_interval(interval: Interval) -> str:
    start = _format_date(interval.start)
    end = _format_date(interval.end)
    if interval.start == interval.end:
        return start
    return f"{start}至{end}"


def _format_date(value: date) -> str:
    return f"{value.year}年{value.month}月{value.day}日"


def _comparison_peer_map(resolved_plan: ResolvedPlan) -> dict[str, list[str]]:
    peers: dict[str, list[str]] = {}
    for comparison in resolved_plan.comparisons:
        for pair in comparison.pairs:
            peers.setdefault(pair.subject_unit_id, [])
            peers.setdefault(pair.reference_unit_id, [])
            if pair.reference_unit_id not in peers[pair.subject_unit_id]:
                peers[pair.subject_unit_id].append(pair.reference_unit_id)
            if pair.subject_unit_id not in peers[pair.reference_unit_id]:
                peers[pair.reference_unit_id].append(pair.subject_unit_id)
    return peers


def _grouping_grain_for_unit(unit) -> str | None:
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return None
    anchor = unit.content.carrier.anchor
    if isinstance(anchor, GroupedTemporalValue):
        return anchor.child_grain
    for modifier in unit.content.carrier.modifiers:
        if isinstance(modifier, GrainExpansion):
            return modifier.target_grain
    return None


def _grouping_phrase(grain: str) -> str:
    mapping = {
        "day": "自然日",
        "week": "自然周",
        "month": "自然月",
        "quarter": "自然季度",
        "half_year": "自然半年",
        "year": "自然年",
    }
    return mapping.get(grain, grain)


def _requires_llm_writer(clarification_facts: list[ClarificationFact]) -> bool:
    return len(clarification_facts) > 1 or any(
        fact.derived_from or fact.comparison_peers for fact in clarification_facts
    )


def _is_valid_clarified_query(
    *,
    original_query: str,
    clarification_facts: list[ClarificationFact],
    clarified_query: str,
) -> bool:
    if not clarified_query.startswith(original_query):
        return False
    for fact in clarification_facts:
        if fact.label not in clarified_query:
            return False
        if fact.status == "resolved":
            if fact.resolved_text is None or fact.resolved_text not in clarified_query:
                return False
        else:
            if "无法确定" not in clarified_query:
                return False
    return True
