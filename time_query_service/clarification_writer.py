from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import Field

from time_query_service.clarification_writer_prompt import build_clarification_writer_messages
from time_query_service.resolved_plan import Interval, IntervalTree, ResolvedPlan
from time_query_service.time_plan import CalendarFilter, GrainExpansion, GroupedTemporalValue, NamedPeriod, RollingByCalendarUnit, StandaloneContent, StrictModel, TimePlan


ClarificationStatus = Literal["resolved", "unresolved"]


class ClarificationFact(StrictModel):
    unit_id: str
    label: str
    status: ClarificationStatus
    resolved_text: str | None = None
    detail_text: str | None = None
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
                    detail_text=_detail_text_for_unit(unit, node.tree, grouping_grain),
                    grouping_grain=grouping_grain,
                    derived_from=[] if node.derived_from is None else list(node.derived_from),
                    comparison_peers=comparison_peers.get(unit.unit_id, []),
                )
            )
    return _coalesce_split_range_facts(
        original_query=original_query,
        time_plan=time_plan,
        resolved_plan=resolved_plan,
        facts=facts,
    )


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
        if fact.detail_text is not None:
            return f"{fact.label}指{fact.resolved_text}，{fact.detail_text}"
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
            if fact.detail_text is not None and fact.detail_text not in clarified_query:
                return False
        else:
            if "无法确定" not in clarified_query:
                return False
    return True


def _detail_text_for_unit(unit, tree: IntervalTree, grouping_grain: str | None) -> str | None:
    if tree.role == "grouped_member" and grouping_grain is not None:
        member_texts = _member_interval_texts(tree)
        if member_texts:
            return f"按{_grouping_phrase(grouping_grain)}分组，依次为{'、'.join(member_texts)}"
        return f"按{_grouping_phrase(grouping_grain)}分组"

    day_class = _day_class_for_unit(unit)
    if tree.role == "filtered_collection" and day_class is not None:
        member_texts = _member_interval_texts(tree)
        if member_texts and _should_list_filtered_members(unit):
            return f"依次为{'、'.join(member_texts)}"
        if unit.content.content_kind == "standalone" and unit.content.carrier is not None:
            anchor = unit.content.carrier.anchor
            if isinstance(anchor, RollingByCalendarUnit):
                if member_texts:
                    return f"依次为{'、'.join(member_texts)}"
                return None
        return f"范围内的全部{_day_class_phrase(day_class)}"

    return None


def _member_interval_texts(tree: IntervalTree) -> list[str]:
    if tree.children:
        return [
            _format_interval(child.labels.absolute_core_time)
            for child in tree.children
            if child.labels.absolute_core_time is not None
        ]
    return [_format_interval(interval) for interval in tree.intervals]


def _day_class_for_unit(unit) -> str | None:
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return None
    anchor = unit.content.carrier.anchor
    if isinstance(anchor, RollingByCalendarUnit):
        return anchor.day_class
    for modifier in unit.content.carrier.modifiers:
        if isinstance(modifier, CalendarFilter):
            return modifier.day_class
    return None


def _day_class_phrase(day_class: str) -> str:
    mapping = {
        "workday": "工作日（含补班日）",
        "weekend": "周末",
        "holiday": "节假日",
        "makeup_workday": "补班日",
    }
    return mapping.get(day_class, day_class)


def _should_list_filtered_members(unit) -> bool:
    label = unit.render_text
    return "每个" in label or "每天" in label


def _coalesce_split_range_facts(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
    facts: list[ClarificationFact],
) -> list[ClarificationFact]:
    merged: list[ClarificationFact] = []
    index = 0
    while index < len(facts):
        if index + 1 < len(facts):
            left_unit = time_plan.units[index]
            right_unit = time_plan.units[index + 1]
            left_fact = facts[index]
            right_fact = facts[index + 1]
            coalesced = _maybe_coalesce_range_pair(
                original_query=original_query,
                resolved_plan=resolved_plan,
                left_unit=left_unit,
                right_unit=right_unit,
                left_fact=left_fact,
                right_fact=right_fact,
            )
            if coalesced is not None:
                merged.append(coalesced)
                index += 2
                continue
        merged.append(facts[index])
        index += 1
    return merged


def _maybe_coalesce_range_pair(
    *,
    original_query: str,
    resolved_plan: ResolvedPlan,
    left_unit,
    right_unit,
    left_fact: ClarificationFact,
    right_fact: ClarificationFact,
) -> ClarificationFact | None:
    if left_fact.status != "resolved" or right_fact.status != "resolved":
        return None
    if any(
        [
            left_fact.detail_text is not None,
            right_fact.detail_text is not None,
            left_fact.grouping_grain is not None,
            right_fact.grouping_grain is not None,
            left_fact.derived_from,
            right_fact.derived_from,
            left_fact.comparison_peers,
            right_fact.comparison_peers,
        ]
    ):
        return None

    connector = _surface_connector_between(left_unit, right_unit, original_query)
    if connector not in {"到", "至", "-", "—", "~", "～"}:
        connector = _semantic_month_range_connector(left_unit, right_unit, original_query)
    if connector not in {"到", "至", "-", "—", "~", "～"}:
        return None

    left_node = resolved_plan.nodes.get(left_unit.unit_id)
    right_node = resolved_plan.nodes.get(right_unit.unit_id)
    if (
        left_node is None
        or right_node is None
        or left_node.tree is None
        or right_node.tree is None
        or left_node.tree.labels.absolute_core_time is None
        or right_node.tree.labels.absolute_core_time is None
    ):
        return None

    left_interval = left_node.tree.labels.absolute_core_time
    right_interval = right_node.tree.labels.absolute_core_time
    if left_interval.start > right_interval.end:
        return None

    merged_interval = Interval(
        start=left_interval.start,
        end=right_interval.end,
        end_inclusive=right_interval.end_inclusive,
    )
    return ClarificationFact(
        unit_id=left_fact.unit_id,
        label=f"{left_fact.label}{connector}{right_fact.label}",
        status="resolved",
        resolved_text=_format_interval(merged_interval),
        detail_text=None,
        grouping_grain=None,
        reason_kind=None,
        derived_from=[],
        comparison_peers=[],
    )


def _surface_connector_between(left_unit, right_unit, original_query: str) -> str | None:
    if not left_unit.surface_fragments or not right_unit.surface_fragments:
        return None
    left_end = max(_fragment_end(fragment) for fragment in left_unit.surface_fragments)
    right_start = min(_fragment_start(fragment) for fragment in right_unit.surface_fragments)
    if left_end > right_start:
        return None
    connector = original_query[left_end:right_start].strip()
    return connector or None


def _fragment_start(fragment) -> int:
    return fragment.start if hasattr(fragment, "start") else fragment["start"]


def _fragment_end(fragment) -> int:
    return fragment.end if hasattr(fragment, "end") else fragment["end"]


def _semantic_month_range_connector(left_unit, right_unit, original_query: str) -> str | None:
    left_anchor = _standalone_named_period_anchor(left_unit)
    right_anchor = _standalone_named_period_anchor(right_unit)
    if left_anchor is None or right_anchor is None:
        return None
    if left_anchor.period_type != "month" or right_anchor.period_type != "month":
        return None
    if left_anchor.year is None or right_anchor.year is None or left_anchor.year != right_anchor.year:
        return None
    if left_anchor.month is None or right_anchor.month is None:
        return None
    for connector in ("到", "至", "-", "—", "~", "～"):
        needle = f"{left_anchor.month}月{connector}{right_anchor.month}月"
        if needle in original_query:
            return connector
    return None


def _standalone_named_period_anchor(unit) -> NamedPeriod | None:
    if not isinstance(unit.content, StandaloneContent) or unit.content.carrier is None:
        return None
    anchor = unit.content.carrier.anchor
    if isinstance(anchor, NamedPeriod):
        return anchor
    return None
