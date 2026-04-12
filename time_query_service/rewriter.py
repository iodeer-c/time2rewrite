from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from time_query_service.schemas import ResolvedTimeExpressions

HOUR_ENUMERATION_MARKERS = ("每小时", "各小时", "逐小时")


REWRITER_SYSTEM_PROMPT = """你是一个查询改写器。

你的任务是根据已经计算完成的绝对时间范围，把原始问题改写成一个用户可读的问题。你只能消费输入里的 resolved_time_expressions，不能重新推理时间，也不能猜测原始语义里没有给出的时间信息。

规则：
1. 你不能重新推理时间，只能使用输入中提供的 resolved_time_expressions
2. 保持原始问题中非时间部分尽量不变
3. 只改写时间部分
4. 如果 resolved_time_expressions 为空，直接输出原始问题
5. 只输出一行纯文本，不要输出解释，不要输出 markdown

改写规则：
- 如果某个时间表达的 start_time 和 end_time 在同一天，优先改写成“YYYY年M月D日”
- 如果某个时间表达恰好覆盖同一天内的单个完整小时，优先改写成“YYYY年M月D日14点”
- 如果某个时间表达恰好覆盖同一天内连续完整小时范围，优先改写成“YYYY年M月D日14点到15点”
- 如果某个时间表达不是整日，也不是完整整点小时范围，必须改写成带时分秒的区间，例如“YYYY年M月D日14:37:00至YYYY年M月D日15:12:00”
- 如果某个时间表达覆盖多天，优先改写成“YYYY年M月D日至YYYY年M月D日”
- 如果原问题本来是在问一个单一时间窗口，就保持单时间窗口问法
- 如果原问题本来是在问多个时间窗口分别的结果，才使用“分别”“各自”等逐项表达
- 如果原问题是在问总量、总收益、均值、汇总等聚合结果，即使 resolved_time_expressions 展开成多个成员，也必须保持聚合问法
- 多个时间字段或 grouped members 只说明时间被展开，不等于用户需要多个结果
- 你的职责是保持原问题语义不变，只把时间改写为绝对表达
- 不要把聚合问题改写成逐项问题，不要凭空补出“分别”“各自”等词
- 不要新增输入中不存在的时间信息
- 如果 rewrite_hints 标记某个来源需要 `member_list`，说明 grouped root 只是结构父节点，不要把 root 的 covering span 当成直接改写结果，必须使用 leaf members
- 如果原问题没有显式时间，但输入给了 1 个默认补出的单时间窗口，可以直接把该日期补进问题里，让结果变成自然问句

示例1
原问题：上周二的日期是多少
resolved_time_expressions:
- id: t1
- text: 上周二
- start_time: 2026-03-31 00:00:00
- end_time: 2026-03-31 23:59:59
- timezone: Asia/Shanghai
输出：2026年3月31日的日期是多少

示例2
原问题：上周二和上周三的日期分别是多少
resolved_time_expressions:
- id: t1
- text: 上周二
- start_time: 2026-03-31 00:00:00
- end_time: 2026-03-31 23:59:59
- timezone: Asia/Shanghai
- id: t2
- text: 上周三
- start_time: 2026-04-01 00:00:00
- end_time: 2026-04-01 23:59:59
- timezone: Asia/Shanghai
输出：2026年3月31日和2026年4月1日的日期分别是多少

示例3
原问题：去年前两个季度的销售额分别是多少
resolved_time_expressions:
- id: t1
- text: 去年第一季度
- start_time: 2025-01-01 00:00:00
- end_time: 2025-03-31 23:59:59
- timezone: Asia/Shanghai
- id: t2
- text: 去年第二季度
- start_time: 2025-04-01 00:00:00
- end_time: 2025-06-30 23:59:59
- timezone: Asia/Shanghai
输出：2025年第一季度和2025年第二季度的销售额分别是多少

示例4
原问题：上个月的前两周的销售额分别是多少
resolved_time_expressions:
- id: t1
- text: 上个月第一周
- start_time: 2026-03-02 00:00:00
- end_time: 2026-03-08 23:59:59
- timezone: Asia/Shanghai
- id: t2
- text: 上个月第二周
- start_time: 2026-03-09 00:00:00
- end_time: 2026-03-15 23:59:59
- timezone: Asia/Shanghai
输出：2026年3月2日至2026年3月8日和2026年3月9日至2026年3月15日的销售额分别是多少

示例5
原问题：这个月第二个周二的销售额是多少
resolved_time_expressions:
- id: t1
- text: 这个月第二个周二
- start_time: 2026-09-08 00:00:00
- end_time: 2026-09-08 23:59:59
- timezone: Asia/Shanghai
输出：2026年9月8日的销售额是多少

示例6
原问题：上个月最后一个周末的销售额是多少
resolved_time_expressions:
- id: t1
- text: 上个月最后一个周末
- start_time: 2026-01-31 00:00:00
- end_time: 2026-01-31 23:59:59
- timezone: Asia/Shanghai
输出：2026年1月31日的销售额是多少

示例7
原问题：帮我看看数据
resolved_time_expressions:
- id: t1
- text: 昨天
- start_time: 2026-04-05 00:00:00
- end_time: 2026-04-05 23:59:59
- timezone: Asia/Shanghai
输出：帮我看看2026年4月5日的数据

示例8
原问题：收益是多少
resolved_time_expressions:
- id: t1
- text: 昨天
- start_time: 2026-04-05 00:00:00
- end_time: 2026-04-05 23:59:59
- timezone: Asia/Shanghai
输出：2026年4月5日的收益是多少

示例9
原问题：今天14点的收益是多少
resolved_time_expressions:
- id: t1
- text: 今天14点
- start_time: 2026-04-10 14:00:00
- end_time: 2026-04-10 14:59:59
- timezone: Asia/Shanghai
输出：2026年4月10日14点的收益是多少

示例10
原问题：今天前2小时的收益是多少
resolved_time_expressions:
- id: t1
- text: 今天前2小时
- start_time: 2026-04-10 14:00:00
- end_time: 2026-04-10 15:59:59
- timezone: Asia/Shanghai
输出：2026年4月10日14点到15点的收益是多少

示例11
原问题：最近24小时的收益是多少
resolved_time_expressions:
- id: t1
- text: 最近24小时
- start_time: 2026-04-09 14:37:00
- end_time: 2026-04-10 14:37:00
- timezone: Asia/Shanghai
输出：2026年4月9日14:37:00至2026年4月10日14:37:00的收益是多少

示例12
原问题：最近10个工作日杭千公司的总收益是多少
resolved_time_expressions:
- id: t1__seg_01
- text: 最近10个工作日
- start_time: 2026-03-27 00:00:00
- end_time: 2026-03-27 23:59:59
- timezone: Asia/Shanghai
- id: t1__seg_02
- text: 最近10个工作日
- start_time: 2026-03-30 00:00:00
- end_time: 2026-03-30 23:59:59
- timezone: Asia/Shanghai
输出：2026年3月27日、2026年3月30日等最近10个工作日杭千公司的总收益是多少
"""


def build_rewriter_user_prompt(original_query: str, resolved_time_expressions: ResolvedTimeExpressions) -> str:
    lines = [f"original_query: {original_query}", "resolved_time_expressions:"]
    if not resolved_time_expressions.resolved_time_expressions:
        lines.append("- []")
    else:
        for item in resolved_time_expressions.resolved_time_expressions:
            lines.extend(
                [
                    f"- id: {item.id}",
                    f"- text: {item.text}",
                    f"  source_id: {item.source_id}",
                    f"  source_text: {item.source_text}",
                    f"  start_time: {item.start_time}",
                    f"  end_time: {item.end_time}",
                    f"  timezone: {item.timezone}",
                    f"  is_partial: {item.is_partial}",
                ]
            )
    if resolved_time_expressions.metadata is not None:
        lines.append("metadata:")
        lines.append(f"- calendar_version: {resolved_time_expressions.metadata.calendar_version}")
        lines.append(f"- enumerated_counts: {resolved_time_expressions.metadata.enumerated_counts}")
        lines.append(f"- rewrite_hints: {resolved_time_expressions.metadata.rewrite_hints}")
    structural_parent_ids = (
        {
            source_id
            for source_id, hint in resolved_time_expressions.metadata.rewrite_hints.items()
            if hint.preferred_rendering == "member_list"
        }
        if resolved_time_expressions.metadata is not None and resolved_time_expressions.metadata.rewrite_hints is not None
        else set()
    )
    lines.append("resolved_time_expression_groups:")
    if not resolved_time_expressions.resolved_time_expression_groups:
        lines.append("- []")
    else:
        for group in resolved_time_expressions.resolved_time_expression_groups:
            _append_group_prompt_lines(lines, group, indent=0, structural_parent_ids=structural_parent_ids)
    return "\n".join(lines)


def _append_group_prompt_lines(lines: list[str], group: Any, indent: int, structural_parent_ids: set[str]) -> None:
    prefix = "  " * indent
    lines.extend(
        [
            f"{prefix}- id: {group.id}",
            f"{prefix}- text: {group.text}",
            f"{prefix}  source_id: {group.source_id}",
            f"{prefix}  source_text: {group.source_text}",
            f"{prefix}  start_time: {group.start_time}",
            f"{prefix}  end_time: {group.end_time}",
            f"{prefix}  timezone: {group.timezone}",
            f"{prefix}  is_partial: {group.is_partial}",
        ]
    )
    if group.id in structural_parent_ids:
        lines.append(f"{prefix}  rewrite_role: structural_parent")
    if not group.children:
        return
    lines.append(f"{prefix}  children:")
    for child in group.children:
        _append_group_prompt_lines(lines, child, indent + 1, structural_parent_ids)


def _parse_resolved_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def _format_date(dt: datetime) -> str:
    return f"{dt.year}年{dt.month}月{dt.day}日"


def _format_datetime(dt: datetime) -> str:
    return f"{_format_date(dt)}{dt.strftime('%H:%M:%S')}"


def _is_full_day_range(start: datetime, end: datetime) -> bool:
    return start == start.replace(hour=0, minute=0, second=0, microsecond=0) and end == end.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=0,
    )


def _is_hour_aligned_range(start: datetime, end: datetime) -> bool:
    return (
        start.minute == 0
        and start.second == 0
        and start.microsecond == 0
        and end.minute == 59
        and end.second == 59
        and end.microsecond == 0
    )


def _format_range(start_time: str, end_time: str) -> str:
    start = _parse_resolved_time(start_time)
    end = _parse_resolved_time(end_time)
    if _is_full_day_range(start, end):
        if start.date() == end.date():
            return _format_date(start)
        return f"{_format_date(start)}至{_format_date(end)}"
    if start.date() == end.date() and _is_hour_aligned_range(start, end):
        if start.hour == end.hour:
            return f"{_format_date(start)}{start.hour}点"
        return f"{_format_date(start)}{start.hour}点到{end.hour}点"
    return f"{_format_datetime(start)}至{_format_datetime(end)}"


def _extract_calendar_day_label(source_text: str) -> str:
    if "工作日" in source_text:
        return "工作日"
    if "休息日" in source_text:
        return "休息日"
    if "节假日" in source_text:
        return "节假日"
    return source_text


def _is_hour_enumeration_source(text: str) -> bool:
    return any(marker in text for marker in HOUR_ENUMERATION_MARKERS)


def _is_hour_segment(start: datetime, end: datetime) -> bool:
    if _is_full_day_range(start, end):
        return False
    return start.date() == end.date() and (end - start).total_seconds() <= 3599


def _format_hour_segment_for_list(start: datetime, end: datetime, *, include_date: bool) -> str:
    if _is_hour_aligned_range(start, end):
        if include_date:
            return f"{_format_date(start)}{start.hour}点"
        return f"{start.hour}点"
    if start.date() == end.date():
        start_text = start.strftime("%H:%M:%S")
        end_text = end.strftime("%H:%M:%S")
        if include_date:
            return f"{_format_date(start)}{start_text}至{end_text}"
        return f"{start_text}至{end_text}"
    return f"{_format_datetime(start)}至{_format_datetime(end)}"


def _ensure_plural_hour_question(query: str) -> str:
    if "分别" in query or "各自" in query:
        return query
    for source, target in (
        ("是多少？", "分别是多少？"),
        ("是多少?", "分别是多少?"),
        ("是多少", "分别是多少"),
        ("是什么？", "分别是什么？"),
        ("是什么?", "分别是什么?"),
        ("是什么", "分别是什么"),
        ("有多少？", "分别有多少？"),
        ("有多少?", "分别有多少?"),
        ("有多少", "分别有多少"),
    ):
        if source in query:
            return query.replace(source, target, 1)
    return query


def _rewrite_enumerated_hours(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    grouped: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
    for item in resolved.resolved_time_expressions:
        if item.source_id is None:
            continue
        source_text = item.source_text or item.text
        if not (_is_hour_enumeration_source(source_text) or _is_hour_enumeration_source(original_query)):
            continue
        start = _parse_resolved_time(item.start_time)
        end = _parse_resolved_time(item.end_time)
        if not _is_hour_segment(start, end):
            return None
        group = grouped.setdefault(
            item.source_id,
            {
                "source_text": source_text,
                "segments": [],
            },
        )
        group["segments"].append((start, end))

    if not grouped:
        return None

    rewritten = original_query
    for group in grouped.values():
        labels: list[str] = []
        last_date = None
        for start, end in group["segments"]:
            include_date = start.date() != last_date
            labels.append(_format_hour_segment_for_list(start, end, include_date=include_date))
            last_date = start.date()
        source_text = group["source_text"]
        replacement = "、".join(labels)
        updated = rewritten.replace(source_text, replacement, 1)
        if updated == rewritten:
            return None
        rewritten = updated

    return _ensure_plural_hour_question(rewritten)


def _rewrite_enumerated_calendar_days(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if resolved.metadata is None or resolved.metadata.enumerated_counts is None:
        return None

    grouped: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
    has_enumerated_segments = False

    for item in resolved.resolved_time_expressions:
        if item.source_id is None:
            continue
        has_enumerated_segments = True
        group = grouped.setdefault(
            item.source_id,
            {
                "source_text": item.source_text or item.text,
                "ranges": [],
            },
        )
        group["ranges"].append(_format_range(item.start_time, item.end_time))

    if not has_enumerated_segments:
        return None

    clauses = []
    for group in grouped.values():
        label = _extract_calendar_day_label(group["source_text"])
        clauses.append(f"{label}为{'、'.join(group['ranges'])}")

    prefix = "，其中"
    return f"{original_query}{prefix}{'；其中'.join(clauses)}"


def _rewrite_non_contiguous_discrete_days(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if resolved.metadata is None or resolved.metadata.rewrite_hints is None:
        return None

    rewritten = original_query
    changed = False
    for source_id, hint in resolved.metadata.rewrite_hints.items():
        if hint.topology != "discrete_set":
            continue
        if hint.member_grain != "day" or hint.is_contiguous or hint.preferred_rendering != "member_list":
            continue

        members = [item for item in resolved.resolved_time_expressions if item.source_id == source_id]
        if not members:
            continue
        source_text = members[0].source_text or members[0].text
        replacement = "、".join(_format_range(item.start_time, item.end_time) for item in members)
        updated = rewritten.replace(source_text, replacement, 1)
        if updated == rewritten:
            return None
        rewritten = updated
        changed = True

    return rewritten if changed else None


def _rewrite_single_calendar_event_range(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if len(resolved.resolved_time_expressions) != 1:
        return None
    item = resolved.resolved_time_expressions[0]
    if item.source_id is not None:
        return None
    if item.text not in original_query:
        return None
    if "假期" not in item.text:
        return None
    replacement = _format_range(item.start_time, item.end_time)
    rewritten = original_query.replace(item.text, replacement, 1)
    if rewritten == original_query:
        return None
    return rewritten


def _rewrite_single_calendar_event_day(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if len(resolved.resolved_time_expressions) != 1:
        return None
    item = resolved.resolved_time_expressions[0]
    if item.source_id is not None:
        return None
    if item.text not in original_query:
        return None
    if not any(marker in item.text for marker in ("当天", "当日", "正日", "第一天", "最后一天")):
        return None
    replacement = _format_range(item.start_time, item.end_time)
    rewritten = original_query.replace(item.text, replacement, 1)
    if rewritten == original_query:
        return None
    return rewritten


def _rewrite_date_identification_query(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if not any(marker in original_query for marker in ("是哪天", "是哪两天", "是几号")):
        return None
    if not resolved.resolved_time_expressions:
        return None
    primary = resolved.resolved_time_expressions[0]
    if primary.source_id is not None:
        source_text = primary.source_text or primary.text
        members = [item for item in resolved.resolved_time_expressions if (item.source_id or item.id) == primary.source_id]
    else:
        source_text = primary.text
        members = resolved.resolved_time_expressions
    if not source_text:
        return None
    rendered = "和".join(_format_range(item.start_time, item.end_time) for item in members)
    return f"{source_text}对应的日期是{rendered}"


def _collect_leaf_groups(node: Any) -> list[Any]:
    if not getattr(node, "children", None):
        return [node]
    leaves: list[Any] = []
    for child in node.children:
        leaves.extend(_collect_leaf_groups(child))
    return leaves


def _rewrite_grouped_atomic_calendar_days(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if not resolved.resolved_time_expression_groups:
        return None
    if not any(marker in original_query for marker in ("每个工作日", "每个休息日", "每个节假日", "每天")):
        return None

    root = resolved.resolved_time_expression_groups[0]
    if not root.children:
        return None
    leaves = _collect_leaf_groups(root)
    labels = [_format_range(leaf.start_time, leaf.end_time) for leaf in leaves]
    replacement = "、".join(labels)
    rewritten = original_query.replace(root.text, replacement, 1)
    if rewritten == original_query:
        return None
    return _ensure_plural_hour_question(rewritten)


class QueryRewriter:
    def __init__(
        self,
        *,
        text_runner: Any | None = None,
        llm: Any | None = None,
    ) -> None:
        self._text_runner = text_runner
        self._llm = llm

    def _get_text_runner(self) -> Any:
        if self._text_runner is None:
            if self._llm is None:
                raise RuntimeError("QueryRewriter requires an injected llm or text_runner.")
            self._text_runner = self._llm
        return self._text_runner

    def rewrite_query_with_llm(
        self,
        *,
        original_query: str,
        resolved_time_expressions: dict[str, Any] | ResolvedTimeExpressions,
    ) -> str | None:
        resolved = ResolvedTimeExpressions.model_validate(resolved_time_expressions)
        if resolved.metadata is not None and resolved.metadata.no_match_results:
            return None
        if not resolved.resolved_time_expressions:
            return original_query

        enumerated_hour_rewrite = _rewrite_enumerated_hours(
            original_query=original_query,
            resolved=resolved,
        )
        if enumerated_hour_rewrite is not None:
            return enumerated_hour_rewrite

        discrete_day_rewrite = _rewrite_non_contiguous_discrete_days(
            original_query=original_query,
            resolved=resolved,
        )
        if discrete_day_rewrite is not None:
            return discrete_day_rewrite

        date_identification_rewrite = _rewrite_date_identification_query(
            original_query=original_query,
            resolved=resolved,
        )
        if date_identification_rewrite is not None:
            return date_identification_rewrite

        holiday_single_day_rewrite = _rewrite_single_calendar_event_day(
            original_query=original_query,
            resolved=resolved,
        )
        if holiday_single_day_rewrite is not None:
            return holiday_single_day_rewrite

        holiday_single_range_rewrite = _rewrite_single_calendar_event_range(
            original_query=original_query,
            resolved=resolved,
        )
        if holiday_single_range_rewrite is not None:
            return holiday_single_range_rewrite

        grouped_atomic_rewrite = _rewrite_grouped_atomic_calendar_days(
            original_query=original_query,
            resolved=resolved,
        )
        if grouped_atomic_rewrite is not None:
            return grouped_atomic_rewrite

        enumerated_rewrite = _rewrite_enumerated_calendar_days(
            original_query=original_query,
            resolved=resolved,
        )
        if enumerated_rewrite is not None:
            return enumerated_rewrite

        messages = [
            SystemMessage(content=REWRITER_SYSTEM_PROMPT),
            HumanMessage(content=build_rewriter_user_prompt(original_query, resolved)),
        ]
        result = self._get_text_runner().invoke(messages)
        return self._coerce_text(result).strip()

    @staticmethod
    def _coerce_text(result: Any) -> str:
        if isinstance(result, str):
            return result

        content = getattr(result, "content", None)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_parts = []
            for item in content:
                if isinstance(item, str):
                    text_parts.append(item)
                elif isinstance(item, dict) and item.get("type") == "text":
                    text_parts.append(item.get("text", ""))
            if text_parts:
                return "".join(text_parts)
        return str(result)


def rewrite_query_with_llm(
    original_query: str,
    resolved_time_expressions: dict[str, Any] | ResolvedTimeExpressions,
) -> str | None:
    from time_query_service.service import QueryPipelineService

    service = QueryPipelineService()
    return service.rewrite_query(
        original_query=original_query,
        resolved_time_expressions=resolved_time_expressions,
    )
