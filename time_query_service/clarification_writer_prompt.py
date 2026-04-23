from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage


CLARIFICATION_WRITER_SYSTEM_PROMPT = """
你是 append-only 时间澄清 writer。

你只能根据给定的 original_query 和 clarification_artifacts 输出一条 clarified_query。

要求：
- 必须保留 original_query 作为句子主体，不得改写、删除或替换其中的非时间业务语义
- 只能在句末追加时间澄清
- clarification_artifacts 必须按给定顺序表达
- 不能新增 facts 之外的时间解释
- 如果某个 fact 未解析，必须明确表达“当前无法确定”
- 只输出最终 clarified_query 文本，不要输出 JSON，不要解释
""".strip()


def build_clarification_writer_messages_from_artifacts(
    *,
    original_query: str,
    clarification_artifacts: list[Any],
) -> list[Any]:
    payload = {
        "original_query": original_query,
        "clarification_artifacts": [artifact.model_dump(mode="python") for artifact in clarification_artifacts],
    }
    return [
        SystemMessage(content=CLARIFICATION_WRITER_SYSTEM_PROMPT),
        HumanMessage(content=json.dumps(payload, ensure_ascii=False, default=str, indent=2)),
    ]


def build_clarification_writer_messages(
    *,
    original_query: str,
    clarification_facts: list[Any],
) -> list[Any]:
    payload = {
        "original_query": original_query,
        "clarification_facts": [fact.model_dump(mode="python") for fact in clarification_facts],
    }
    return [
        SystemMessage(content=CLARIFICATION_WRITER_SYSTEM_PROMPT),
        HumanMessage(content=json.dumps(payload, ensure_ascii=False, default=str, indent=2)),
    ]
