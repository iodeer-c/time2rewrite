from __future__ import annotations

from datetime import date
from typing import Any, Literal
import re

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
    fallback_fact = _coalesce_split_bounded_range_facts(
        original_query=original_query,
        clarification_facts=clarification_facts,
    )
    if fallback_fact is not None:
        parts = [_render_fact(fallback_fact)]
    else:
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


def _coalesce_split_bounded_range_facts(
    *,
    original_query: str,
    clarification_facts: list[ClarificationFact],
) -> ClarificationFact | None:
    # Defensive fallback only. Canonical bounded-range success must come from one upstream unit/fact.
    if len(clarification_facts) != 2:
        return None
    left, right = clarification_facts
    if any(fact.status != "resolved" or fact.resolved_text is None for fact in clarification_facts):
        return None
    if any(
        fact.grouping_grain is not None or fact.derived_from or fact.comparison_peers or fact.reason_kind is not None
        for fact in clarification_facts
    ):
        return None

    left_span = _find_span_after(original_query, left.label, 0)
    if left_span is None:
        return None
    right_span = _find_span_after(original_query, right.label, left_span[1])
    if right_span is None:
        return None

    connector = original_query[left_span[1]:right_span[0]].strip()
    if connector not in {"到", "至", "-", "~", "～"}:
        return None

    left_interval = _parse_interval_text(left.resolved_text)
    right_interval = _parse_interval_text(right.resolved_text)
    if left_interval is None or right_interval is None:
        return None
    if right_interval.end < left_interval.start:
        return None

    range_label = original_query[left_span[0]:right_span[1]]
    merged = Interval(start=left_interval.start, end=right_interval.end, end_inclusive=True)
    return ClarificationFact(
        unit_id=f"{left.unit_id}+{right.unit_id}",
        label=range_label,
        status="resolved",
        resolved_text=_format_interval(merged),
    )


def _find_span_after(query: str, text: str, start_at: int) -> tuple[int, int] | None:
    start = query.find(text, start_at)
    if start < 0:
        return None
    return (start, start + len(text))


def _parse_interval_text(text: str) -> Interval | None:
    parts = text.split("至", 1)
    if len(parts) == 1:
        start = _parse_date_text(parts[0])
        if start is None:
            return None
        return Interval(start=start, end=start, end_inclusive=True)
    start = _parse_date_text(parts[0])
    end = _parse_date_text(parts[1])
    if start is None or end is None:
        return None
    return Interval(start=start, end=end, end_inclusive=True)


def _parse_date_text(text: str) -> date | None:
    match = re.fullmatch(r"\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*", text)
    if match is None:
        return None
    year, month, day = (int(group) for group in match.groups())
    return date(year, month, day)


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
