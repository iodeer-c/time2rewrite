from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage


STAGE_A_SYSTEM_PROMPT = """
你是 Stage A 时间分段器。

只输出一个 JSON object，结构必须满足：
- query
- system_date
- timezone
- units
- comparisons

你只负责：
- 按原句切分时间 unit
- 为每个 unit 给出 render_text、content_kind、self_contained_text
- 对 derived unit 给出 sources
- 对 comparison 给出 pair 关系
- 对 calendar-class rolling phrase 给出 surface_hint = "calendar_grain_rolling"

你绝不能输出 carrier / anchor / modifier。
如果提供了 previous_validation_errors，你必须修正这些错误，不要重复上轮非法输出。

额外硬约束：
- render_text 必须是原句里真实出现的表面文本，不能补全年份、不能改写、不能扩写共享前缀。
- self_contained_text 可以在保持语义等价前提下补足共享前缀缺失的信息。
- 如果 query 是“2025年3月和5月…”，第二个 unit 的 render_text 必须是“5月”，不是“2025年5月”；只有 self_contained_text 才能是“2025年5月”。
- 如果 query 是“今年3月和5月…”，第二个 unit 的 render_text 必须是“5月”，不是“今年5月”；只有 self_contained_text 才能是“今年5月”。
- 如果 query 是“2025年中秋假期和国庆假期…”，第二个 unit 的 render_text 必须是“国庆假期”，不是“2025年国庆假期”；只有 self_contained_text 才能是“2025年国庆假期”。
- 如果 query 是“2025年每个季度…”这类 grouped-temporal 短语，render_text 必须保留完整短语，例如“2025年每个季度”不能漏掉最后的“度”。
- `sources[].source_unit_id` 只能引用本次输出里已经出现过的真实 unit_id，绝不能捏造 `anchor_current_year`、`system_date_anchor` 之类 synthetic anchor。
- units 必须保持原句从左到右顺序；后面的 clarification writer 只按这个顺序解释时间单元。
- 如果“去年同期 / 同比 / 环比”在句中有明确前置时间 antecedent，可回指时必须输出 derived；如果 antecedent 有多个并列时间 unit，就必须把全部相关 unit_id 按声明顺序放进 `sources`，绝不能只引用第一个。
- 只有当“去年同期 / 同比 / 环比”在句中完全没有明确前置时间 antecedent 可回指时，才不要输出 derived；此时应把它当 standalone unit 留给 Stage B / downstream 决定是否能结构化。
- surface_fragments 如果提供，只能作为原句表面位置的可选 hint，不能指向扩写后的假想跨度。
- surface_fragments 如果提供，使用半开区间 [start, end)，必须精确覆盖 render_text，不能把后面的“收益 / 对比 / ，”等非时间字符卷进去。
- 如果你不能稳定给出正确的 surface_fragments，就省略这个字段，不要猜。
- surface_hint = "calendar_grain_rolling" 只允许用于“最近N个工作日 / 节假日 / 周末 / 补班日”这类 calendar-class count rolling；普通 rolling（最近7天 / 最近一周 / 最近一个月 / 最近一季度 / 最近半年 / 最近一年）绝不能带这个 hint。
""".strip()


_FEW_SHOTS: list[tuple[dict[str, Any], dict[str, Any]]] = [
    (
        {"query": "2025年3月的收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年3月的收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "2025年3月",
                    "surface_fragments": [{"start": 0, "end": 7}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年3月",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "2025年3月和5月的收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年3月和5月的收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {"unit_id": "u1", "render_text": "2025年3月", "surface_fragments": [{"start": 0, "end": 7}], "content_kind": "standalone", "self_contained_text": "2025年3月", "sources": []},
                {"unit_id": "u2", "render_text": "5月", "surface_fragments": [{"start": 8, "end": 10}], "content_kind": "standalone", "self_contained_text": "2025年5月", "sources": []},
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "今年3月和5月，去年同期", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "今年3月和5月，去年同期",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {"unit_id": "u1", "render_text": "今年3月", "surface_fragments": [{"start": 0, "end": 4}], "content_kind": "standalone", "self_contained_text": "今年3月", "sources": []},
                {"unit_id": "u2", "render_text": "5月", "surface_fragments": [{"start": 5, "end": 7}], "content_kind": "standalone", "self_contained_text": "今年5月", "sources": []},
                {
                    "unit_id": "u3",
                    "render_text": "去年同期",
                    "surface_fragments": [{"start": 8, "end": 12}],
                    "content_kind": "derived",
                    "self_contained_text": None,
                    "sources": [
                        {"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}},
                        {"source_unit_id": "u2", "transform": {"kind": "shift_year", "offset": -1}},
                    ],
                },
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "去年同期员工数有多少", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "去年同期员工数有多少",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "去年同期",
                    "surface_fragments": [{"start": 0, "end": 4}],
                    "content_kind": "standalone",
                    "self_contained_text": "去年同期",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "2025年Q1收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年Q1收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "2025年Q1",
                    "surface_fragments": [{"start": 0, "end": 7}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年Q1",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "2025年每个季度收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年每个季度收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "2025年每个季度",
                    "surface_fragments": [{"start": 0, "end": 9}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年每个季度",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "最近7天收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "最近7天收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "最近7天",
                    "surface_fragments": [{"start": 0, "end": 4}],
                    "content_kind": "standalone",
                    "self_contained_text": "最近7天",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "最近一个月收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "最近一个月收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "最近一个月",
                    "surface_fragments": [{"start": 0, "end": 5}],
                    "content_kind": "standalone",
                    "self_contained_text": "最近一个月",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "最近1个补班日收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "最近1个补班日收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "最近1个补班日",
                    "surface_fragments": [{"start": 0, "end": 7}],
                    "content_kind": "standalone",
                    "self_contained_text": "最近1个补班日",
                    "sources": [],
                    "surface_hint": "calendar_grain_rolling",
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "2025年3月对比2024年3月", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年3月对比2024年3月",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {"unit_id": "u1", "render_text": "2025年3月", "surface_fragments": [{"start": 0, "end": 7}], "content_kind": "standalone", "self_contained_text": "2025年3月", "sources": []},
                {"unit_id": "u2", "render_text": "2024年3月", "surface_fragments": [{"start": 9, "end": 16}], "content_kind": "standalone", "self_contained_text": "2024年3月", "sources": []},
            ],
            "comparisons": [{"comparison_id": "c1", "anchor_text": "对比", "pairs": [{"subject_unit_id": "u1", "reference_unit_id": "u2"}]}],
        },
    ),
    (
        {"query": "最近5个工作日的收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "最近5个工作日的收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "最近5个工作日",
                    "surface_fragments": [{"start": 0, "end": 7}],
                    "content_kind": "standalone",
                    "self_contained_text": "最近5个工作日",
                    "sources": [],
                    "surface_hint": "calendar_grain_rolling",
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "最近5天中的工作日收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "最近5天中的工作日收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "最近5天中的工作日",
                    "surface_fragments": [{"start": 0, "end": 9}],
                    "content_kind": "standalone",
                    "self_contained_text": "最近5天中的工作日",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "2025年每天收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年每天收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "2025年每天",
                    "surface_fragments": [{"start": 0, "end": 7}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年每天",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "最近半年每季度收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "最近半年每季度收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "最近半年每季度",
                    "surface_fragments": [{"start": 0, "end": 7}],
                    "content_kind": "standalone",
                    "self_contained_text": "最近半年每季度",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "最近一年每半年收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "最近一年每半年收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "最近一年每半年",
                    "surface_fragments": [{"start": 0, "end": 7}],
                    "content_kind": "standalone",
                    "self_contained_text": "最近一年每半年",
                    "sources": [],
                }
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "2025年3月的工作日对比2024年3月的工作日", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年3月的工作日对比2024年3月的工作日",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "2025年3月的工作日",
                    "surface_fragments": [{"start": 0, "end": 11}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年3月的工作日",
                    "sources": [],
                },
                {
                    "unit_id": "u2",
                    "render_text": "2024年3月的工作日",
                    "surface_fragments": [{"start": 13, "end": 24}],
                    "content_kind": "standalone",
                    "self_contained_text": "2024年3月的工作日",
                    "sources": [],
                },
            ],
            "comparisons": [
                {
                    "comparison_id": "c1",
                    "anchor_text": "对比",
                    "pairs": [{"subject_unit_id": "u1", "reference_unit_id": "u2"}],
                }
            ],
        },
    ),
    (
        {"query": "2025年中秋假期和国庆假期收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年中秋假期和国庆假期收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "2025年中秋假期",
                    "surface_fragments": [{"start": 0, "end": 9}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年中秋假期",
                    "sources": [],
                },
                {
                    "unit_id": "u2",
                    "render_text": "国庆假期",
                    "surface_fragments": [{"start": 10, "end": 14}],
                    "content_kind": "standalone",
                    "self_contained_text": "2025年国庆假期",
                    "sources": [],
                },
            ],
            "comparisons": [],
        },
    ),
    (
        {"query": "2025年和2024年3月收益", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"},
        {
            "query": "2025年和2024年3月收益",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {"unit_id": "u1", "render_text": "2025年", "surface_fragments": [{"start": 0, "end": 5}], "content_kind": "standalone", "self_contained_text": "2025年", "sources": []},
                {"unit_id": "u2", "render_text": "2024年3月", "surface_fragments": [{"start": 6, "end": 13}], "content_kind": "standalone", "self_contained_text": "2024年3月", "sources": []},
            ],
            "comparisons": [],
        },
    ),
]


def build_stage_a_messages(
    *,
    query: str,
    system_date: str,
    timezone: str,
    previous_validation_errors: list[str] | None = None,
) -> list[Any]:
    messages: list[Any] = [SystemMessage(content=STAGE_A_SYSTEM_PROMPT)]
    for request_payload, response_payload in _FEW_SHOTS:
        messages.append(HumanMessage(content=json.dumps(request_payload, ensure_ascii=False, indent=2)))
        messages.append(AIMessage(content=json.dumps(response_payload, ensure_ascii=False, indent=2)))

    payload: dict[str, Any] = {
        "query": query,
        "system_date": system_date,
        "timezone": timezone,
    }
    if previous_validation_errors:
        payload["previous_validation_errors"] = previous_validation_errors
    messages.append(HumanMessage(content=json.dumps(payload, ensure_ascii=False, indent=2)))
    return messages
