from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from time_query_service.contracts import ClarificationItem, ComparisonGroup


ANNOTATOR_SYSTEM_PROMPT = """
You are the Append-only Annotation Renderer.

Only add exact-time annotations to the target time expressions.
Do not rewrite non-time text. Do not explain. Output the final question only.
""".strip()


def annotate_query(
    *,
    original_query: str,
    clarification_items: list[ClarificationItem | dict[str, Any]],
    comparison_groups: list[ComparisonGroup | dict[str, Any]],
    text_runner: Any | None = None,
) -> str | None:
    renderer = AppendOnlyAnnotationRenderer(text_runner=text_runner)
    return renderer.render(
        original_query=original_query,
        clarification_items=clarification_items,
        comparison_groups=comparison_groups,
    )


class AppendOnlyAnnotationRenderer:
    def __init__(self, *, text_runner: Any | None = None) -> None:
        self._text_runner = text_runner

    def render(
        self,
        *,
        original_query: str,
        clarification_items: list[ClarificationItem | dict[str, Any]],
        comparison_groups: list[ComparisonGroup | dict[str, Any]],
    ) -> str | None:
        items = [self._normalize_item(item) for item in clarification_items]
        groups = [self._normalize_group(group) for group in comparison_groups]

        if not items:
            return original_query

        deterministic_result = self._render_deterministically(original_query=original_query, items=items)
        if deterministic_result is not None:
            return deterministic_result

        if self._text_runner is None:
            return None

        payload = {
            "original_query": original_query,
            "clarification_items": [item.model_dump(mode="python") for item in items],
            "comparison_groups": [group.model_dump(mode="python") for group in groups],
        }
        response = self._text_runner.invoke(
            [
                SystemMessage(content=ANNOTATOR_SYSTEM_PROMPT),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False, default=str)),
            ]
        )
        content = response.content if hasattr(response, "content") else response
        if not isinstance(content, str):
            return None
        content = content.strip()
        return content if _is_valid_annotation_output(content, item_count=len(items)) else None

    def _render_deterministically(self, *, original_query: str, items: list[ClarificationItem]) -> str | None:
        located_spans = _locate_render_spans(original_query=original_query, items=items)
        if located_spans is None:
            return None

        rewritten = original_query
        for start, end, item in sorted(located_spans, key=lambda value: value[0], reverse=True):
            source_text = rewritten[start:end]
            rewritten = (
                rewritten[:start]
                + f"{source_text}（{item.display_exact_time}）"
                + rewritten[end:]
            )
        return rewritten if _is_valid_annotation_output(rewritten, item_count=len(items)) else None

    @staticmethod
    def _normalize_item(item: ClarificationItem | dict[str, Any]) -> ClarificationItem:
        return item if isinstance(item, ClarificationItem) else ClarificationItem.model_validate(item)

    @staticmethod
    def _normalize_group(group: ComparisonGroup | dict[str, Any]) -> ComparisonGroup:
        return group if isinstance(group, ComparisonGroup) else ComparisonGroup.model_validate(group)


def _locate_render_spans(
    *,
    original_query: str,
    items: list[ClarificationItem],
) -> list[tuple[int, int, ClarificationItem]] | None:
    spans: list[tuple[int, int, ClarificationItem]] = []
    search_start = 0

    for item in sorted(items, key=lambda value: value.ordinal):
        span = _locate_item_span(
            original_query=original_query,
            item=item,
            search_start=search_start,
        )
        if span is None:
            return None
        spans.append((span[0], span[1], item))
        search_start = span[1]

    return spans


def _locate_item_span(
    *,
    original_query: str,
    item: ClarificationItem,
    search_start: int,
) -> tuple[int, int] | None:
    occurrences = _find_occurrences(original_query, item.render_text)
    if len(occurrences) > 1 and item.ordinal <= len(occurrences):
        candidate = occurrences[item.ordinal - 1]
        if candidate >= search_start:
            return (candidate, candidate + len(item.render_text))
    direct_index = original_query.find(item.render_text, search_start)
    if direct_index >= 0:
        return (direct_index, direct_index + len(item.render_text))
    if item.surface_fragments:
        return _locate_surface_fragment_span(
            original_query=original_query,
            item=item,
            search_start=search_start,
        )
    return None


def _locate_surface_fragment_span(
    *,
    original_query: str,
    item: ClarificationItem,
    search_start: int,
) -> tuple[int, int] | None:
    current_search_start = search_start
    first_fragment_span: tuple[int, int] | None = None
    for fragment in item.surface_fragments:
        fragment_index = original_query.find(fragment, current_search_start)
        if fragment_index < 0:
            return None
        if first_fragment_span is None:
            first_fragment_span = (fragment_index, fragment_index + len(fragment))
        current_search_start = fragment_index + len(fragment)
    return first_fragment_span


def _find_occurrences(text: str, needle: str) -> list[int]:
    indices: list[int] = []
    search_start = 0
    while True:
        index = text.find(needle, search_start)
        if index < 0:
            return indices
        indices.append(index)
        search_start = index + len(needle)


def _is_valid_annotation_output(output: str, *, item_count: int) -> bool:
    if not output.strip():
        return False
    if item_count > 0 and "（" not in output:
        return False
    return True
