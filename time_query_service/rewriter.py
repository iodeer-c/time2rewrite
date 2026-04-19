from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any, Literal

from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import Field

from time_query_service.resolved_plan import Interval, ResolvedComparisonPair, ResolvedNode, ResolvedPlan
from time_query_service.time_plan import StrictModel, SurfaceFragment, TimePlan, Unit


EditMode = Literal["replace_core_time", "preserve_and_supplement"]
RouteState = Literal["healthy", "no_match", "partial_no_match"]


class SourceBinding(StrictModel):
    source_unit_id: str
    absolute_core_time: Interval | None = None
    degraded: bool = False
    degraded_reason_kind: str | None = None


class TimeBinding(StrictModel):
    unit_id: str
    render_text: str
    surface_fragments: list[SurfaceFragment]
    absolute_core_time: Interval | None = None
    edit_mode: EditMode
    route_state: RouteState
    scaffold_tokens_to_preserve: list[str] = Field(default_factory=list)
    needs_clarification: bool = False
    reason_kind: str | None = None
    source_bindings: list[SourceBinding] = Field(default_factory=list)


class ComparisonRenderPair(StrictModel):
    subject_unit_id: str
    reference_unit_id: str
    degraded: bool
    degraded_reason: str | None = None
    subject_absolute_core_time: Interval | None = None
    reference_absolute_core_time: Interval | None = None
    expansion: dict[str, Any] | None = None


class ComparisonRenderGroup(StrictModel):
    comparison_id: str
    source_pair_index: int | None = None
    pairs: list[ComparisonRenderPair]
    all_degraded: bool = False


REWRITER_SYSTEM_PROMPT = """
你是时间澄清 rewriter。你只能根据给定的 original_query、bindings、comparisons 做时间澄清。

要求：
- 保留原始 query 的非时间语义
- 只补充或替换时间表达
- 不得引入未授权的新业务词
- 对 needs_clarification / degraded source / degraded comparison pair 显式 abstain
""".strip()

_SCAFFOLD_PATTERNS = [
    r"\d+个工作日",
    r"\d+个周末",
    r"\d+个节假日",
    r"\d+个补班日",
    r"每天",
    r"每周",
    r"每月",
    r"每个工作日",
    r"每个季度",
    r"每季度",
    r"每半年",
    r"每年",
    r"工作日",
    r"周末",
    r"节假日",
    r"补班日",
]


def build_time_bindings(*, original_query: str, time_plan: TimePlan, resolved_plan: ResolvedPlan) -> list[TimeBinding]:
    bindings: list[TimeBinding] = []
    for unit in time_plan.units:
        node = resolved_plan.nodes.get(unit.unit_id)
        if node is None:
            continue
        bindings.append(_build_binding_for_unit(original_query=original_query, unit=unit, node=node))
    return bindings


def build_rewriter_payload(*, original_query: str, time_plan: TimePlan, resolved_plan: ResolvedPlan) -> dict[str, Any]:
    bindings = build_time_bindings(original_query=original_query, time_plan=time_plan, resolved_plan=resolved_plan)
    comparison_groups = _build_comparison_groups(resolved_plan)
    return {
        "original_query": original_query,
        "bindings": [binding.model_dump(mode="python") for binding in bindings],
        "comparisons": [group.model_dump(mode="python") for group in comparison_groups],
    }


def build_rewriter_messages(*, original_query: str, time_plan: TimePlan, resolved_plan: ResolvedPlan) -> list[Any]:
    payload = build_rewriter_payload(original_query=original_query, time_plan=time_plan, resolved_plan=resolved_plan)
    return [
        SystemMessage(content=REWRITER_SYSTEM_PROMPT),
        HumanMessage(content=json.dumps(payload, ensure_ascii=False, default=str, indent=2)),
    ]


def rewrite_query(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
    text_runner: Any | None = None,
) -> str | None:
    bindings = build_time_bindings(original_query=original_query, time_plan=time_plan, resolved_plan=resolved_plan)
    deterministic = _render_deterministically(original_query=original_query, bindings=bindings)
    if deterministic is not None:
        return deterministic
    if text_runner is None:
        return None
    response = text_runner.invoke(build_rewriter_messages(original_query=original_query, time_plan=time_plan, resolved_plan=resolved_plan))
    content = response.content if hasattr(response, "content") else response
    if not isinstance(content, str):
        return None
    rendered = content.strip()
    return rendered or None


def _build_binding_for_unit(*, original_query: str, unit: Unit, node: ResolvedNode) -> TimeBinding:
    source_bindings: list[SourceBinding] = []
    route_state: RouteState = "healthy"
    if node.tree is not None and node.tree.role == "derived":
        for child in node.tree.children:
            source_bindings.append(
                SourceBinding(
                    source_unit_id=child.labels.source_unit_id or "",
                    absolute_core_time=child.labels.absolute_core_time,
                    degraded=bool(child.labels.degraded),
                    degraded_reason_kind=child.labels.degraded_source_reason_kind,
                )
            )
        if source_bindings and any(source.degraded for source in source_bindings):
            route_state = "partial_no_match" if any(not source.degraded for source in source_bindings) else "no_match"
    if node.needs_clarification:
        route_state = "no_match"

    scaffold_tokens = _extract_scaffold_tokens(unit.render_text)
    if source_bindings or scaffold_tokens or (node.tree is not None and node.tree.role in {"filtered_collection", "grouped_member"}):
        edit_mode: EditMode = "preserve_and_supplement"
    else:
        edit_mode = "replace_core_time"

    return TimeBinding(
        unit_id=unit.unit_id,
        render_text=unit.render_text,
        surface_fragments=unit.surface_fragments,
        absolute_core_time=None if node.tree is None else node.tree.labels.absolute_core_time,
        edit_mode=edit_mode,
        route_state=route_state,
        scaffold_tokens_to_preserve=scaffold_tokens,
        needs_clarification=node.needs_clarification,
        reason_kind=node.reason_kind,
        source_bindings=source_bindings,
    )


def _build_comparison_groups(resolved_plan: ResolvedPlan) -> list[ComparisonRenderGroup]:
    groups: list[ComparisonRenderGroup] = []
    for comparison in resolved_plan.comparisons:
        buckets: dict[int | str, list[ResolvedComparisonPair]] = defaultdict(list)
        for index, pair in enumerate(comparison.pairs):
            key = pair.expansion.source_pair_index if pair.expansion is not None else f"singleton-{index}"
            buckets[key].append(pair)
        for key, pairs in buckets.items():
            ordered_pairs = sorted(
                pairs,
                key=lambda pair: -1 if pair.expansion is None else pair.expansion.expansion_index,
            )
            groups.append(
                ComparisonRenderGroup(
                    comparison_id=comparison.comparison_id,
                    source_pair_index=key if isinstance(key, int) else None,
                    pairs=[
                        ComparisonRenderPair(
                            subject_unit_id=pair.subject_unit_id,
                            reference_unit_id=pair.reference_unit_id,
                            degraded=pair.degraded,
                            degraded_reason=pair.degraded_reason,
                            subject_absolute_core_time=pair.subject_absolute_core_time,
                            reference_absolute_core_time=pair.reference_absolute_core_time,
                            expansion=None if pair.expansion is None else pair.expansion.model_dump(mode="python"),
                        )
                        for pair in ordered_pairs
                    ],
                    all_degraded=all(pair.degraded for pair in ordered_pairs),
                )
            )
    return groups


def _extract_scaffold_tokens(render_text: str) -> list[str]:
    tokens: list[str] = []
    for pattern in _SCAFFOLD_PATTERNS:
        for match in re.finditer(pattern, render_text):
            token = match.group(0)
            if token not in tokens:
                tokens.append(token)
    return tokens


def _render_deterministically(*, original_query: str, bindings: list[TimeBinding]) -> str | None:
    if not bindings:
        return original_query
    if all(binding.route_state == "no_match" for binding in bindings):
        return None

    rewritten = original_query
    insertion_points: list[tuple[int, TimeBinding]] = []
    for binding in bindings:
        if not binding.surface_fragments:
            continue
        insertion_points.append((max(fragment.end for fragment in binding.surface_fragments), binding))

    for insertion_point, binding in sorted(insertion_points, key=lambda item: item[0], reverse=True):
        annotation = _annotation_text(binding)
        if annotation is None:
            continue
        rewritten = rewritten[:insertion_point] + f"（{annotation}）" + rewritten[insertion_point:]
    return rewritten


def _annotation_text(binding: TimeBinding) -> str | None:
    if binding.route_state == "no_match":
        return "时间待澄清"
    if binding.source_bindings:
        parts: list[str] = []
        for source_binding in binding.source_bindings:
            if source_binding.degraded:
                parts.append(f"{source_binding.source_unit_id}:待澄清")
            elif source_binding.absolute_core_time is not None:
                parts.append(f"{source_binding.source_unit_id}:{_format_interval(source_binding.absolute_core_time)}")
        return "；".join(parts) if parts else "时间待澄清"
    if binding.absolute_core_time is None:
        return None
    return _format_interval(binding.absolute_core_time)


def _format_interval(interval: Interval) -> str:
    start = _format_date(interval.start)
    end = _format_date(interval.end)
    if interval.start == interval.end:
        return start
    return f"{start}至{end}"


def _format_date(value) -> str:
    return f"{value.year}年{value.month}月{value.day}日"
