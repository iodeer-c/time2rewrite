from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from time_query_service.config import get_llm_config
from time_query_service.schemas import ResolvedTimeExpressions


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
- 如果某个时间表达覆盖多天，优先改写成“YYYY年M月D日至YYYY年M月D日”
- 如果 resolved_time_expressions 只有 1 个，把它改写成单时间窗口问题
- 如果有多个时间字段，把它们改写成多时间窗口问题，并按输入顺序依次改写
- 当输入中已有多个时间字段时，优先使用“分别”“各自”等能明确表达多窗口结果的说法
- 多个时间字段可能来自同一个原文时间短语拆分后的规范化字段，也必须按输入顺序分别改写
- 不要新增输入中不存在的时间信息

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
                ]
            )
    if resolved_time_expressions.metadata is not None:
        lines.append("metadata:")
        lines.append(f"- calendar_version: {resolved_time_expressions.metadata.calendar_version}")
        lines.append(f"- enumerated_counts: {resolved_time_expressions.metadata.enumerated_counts}")
    return "\n".join(lines)


def _parse_resolved_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def _format_date(dt: datetime) -> str:
    return f"{dt.year}年{dt.month}月{dt.day}日"


def _format_range(start_time: str, end_time: str) -> str:
    start = _parse_resolved_time(start_time)
    end = _parse_resolved_time(end_time)
    if start.date() == end.date():
        return _format_date(start)
    return f"{_format_date(start)}至{_format_date(end)}"


def _extract_calendar_day_label(source_text: str) -> str:
    if "工作日" in source_text:
        return "工作日"
    if "休息日" in source_text:
        return "休息日"
    if "节假日" in source_text:
        return "节假日"
    return source_text


def _rewrite_enumerated_calendar_days(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
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


class QueryRewriter:
    def __init__(
        self,
        *,
        model: str | None = None,
        temperature: float = 0,
        text_runner: Any | None = None,
        llm: ChatOpenAI | None = None,
    ) -> None:
        self.model = model
        self.temperature = temperature
        self._text_runner = text_runner
        self._llm = llm

    def _get_llm(self) -> ChatOpenAI:
        if self._llm is None:
            config = get_llm_config()
            if not config.api_key:
                raise RuntimeError("Missing DASHSCOPE_API_KEY. Set it in .env or your shell environment.")
            self._llm = ChatOpenAI(
                model=self.model or config.model_name,
                temperature=self.temperature,
                api_key=config.api_key,
                base_url=config.base_url,
            )
        return self._llm

    def _get_text_runner(self) -> Any:
        if self._text_runner is None:
            self._text_runner = self._get_llm()
        return self._text_runner

    def rewrite_query_with_llm(
        self,
        *,
        original_query: str,
        resolved_time_expressions: dict[str, Any] | ResolvedTimeExpressions,
    ) -> str:
        resolved = ResolvedTimeExpressions.model_validate(resolved_time_expressions)
        if not resolved.resolved_time_expressions:
            return original_query

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
) -> str:
    rewriter = QueryRewriter()
    return rewriter.rewrite_query_with_llm(
        original_query=original_query,
        resolved_time_expressions=resolved_time_expressions,
    )
