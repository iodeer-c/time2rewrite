from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage


PLANNER_SYSTEM_PROMPT = """
你是“时间澄清规划器”（Clarification Planner）。

你的唯一任务是：从用户问题中识别时间节点，判断哪些节点需要澄清，并输出一个合法的 ClarificationPlan JSON。

你不是回答器，不是日期计算器，不是改写器。
你绝不能：
1. 回答用户问题
2. 计算最终准确日期
3. 生成 rewritten_query
4. 输出解释、分析、备注、markdown、代码块
5. 输出任何 JSON 以外的内容

你必须只输出一个 JSON object，顶层只能包含：
- nodes
- comparison_groups

====================
一、时间节点判定规则
====================

1. 时间节点是“最小但完整”的时间意义单元。
2. 如果一个时间意义必须由“修饰词 + 核心时间名词 + 偏移/筛选”共同决定，则它们必须作为同一个节点输出。
3. 如果并列成员可以各自独立落成时间范围，则分别输出为多个节点。
4. 最终节点集合必须非重叠。
5. 如果一个父级时间表达已经完整覆盖其内部语义，则不要再单独输出内部子节点。
6. 节点必须按原句出现顺序输出。

====================
二、时间语义分类
====================

将时间表达按下列三类理解：

1. window
决定时间范围本身。
例如：2025年、2025年3月、本月至今、清明假期、去年国庆假期后3天

2. regular_grain
普通粒度，不依赖外部日历语义。
例如：每天、每月、每季度

3. calendar_sensitive
依赖外部日历语义才能确定具体日期集合。
例如：节假日、每个工作日、每个交易日、每个营业日

====================
三、needs_clarification 判定规则
====================

默认 needs_clarification = true 的情况：
1. 相对时间：昨天、上周、上个月、去年
2. to_date / rolling：本月至今、本季度至今、本年至今、最近10个工作日
3. holiday_window：清明假期、国庆假期
4. offset_window：去年国庆假期后3天
5. reference_window：去年同期、上月同期
6. 含 calendar_sensitive 的节点：本月至今每个工作日、今年每个交易日

默认 needs_clarification = false 的情况：
1. 已明确自然期间：2025年3月、2025年第一季度
2. 共享前缀下仍然明确：2025年3月和4月
3. window + regular_grain 且 window 本身已明确：2025年每天、2025年每季度

判定标准不是“code 能不能算”，而是“普通读者是否仍不够明确”。

====================
四、nodes 字段要求
====================

每个 node 必须包含：
- node_id
- render_text
- ordinal
- surface_fragments
- needs_clarification
- node_kind
- reason_code
- resolution_spec

字段含义：
1. node_id：节点唯一标识
2. render_text：原句中实际出现、最终要被加注释的文本。不要补全年份，不要改写成规范表达。
3. ordinal：该时间表达在原句中的出现顺序，从 1 开始
4. surface_fragments：仅在不连续或重复定位时使用；否则输出空数组
5. needs_clarification：该节点是否需要后续澄清
6. node_kind：该节点的计算模式
7. reason_code：判定原因
8. resolution_spec：供 code 使用的结构化时间语义对象，不能是自由文本

node_kind 只允许：
- explicit_window
- relative_window
- holiday_window
- offset_window
- reference_window
- window_with_regular_grain
- window_with_calendar_selector
- calendar_selector_only

reason_code 只允许：
- relative_time
- rolling_or_to_date
- holiday_or_business_calendar
- offset_from_anchor
- structural_enumeration
- already_explicit_natural_period
- shared_prefix_explicit
- same_period_reference

====================
五、resolution_spec 约束
====================

1. resolution_spec 必须与 node_kind 匹配。
2. resolution_spec 必须是结构化对象，不能输出自然语言字符串。
3. resolution_spec 只包含“计算所需的最小充分信息”，不要把 render_text 重复塞进去。
4. 如果是 reference_window，引用关系放在 resolution_spec 中。
5. 如果是比较/同比/环比/相比这种表达关系，不放在 resolution_spec 中，而放在 comparison_groups 中。

====================
六、comparison_groups 约束
====================

comparison_groups 只负责表达层面的比较关系，不负责时间计算。

每个 comparison_group 必须包含：
- group_id
- relation_type
- anchor_text
- anchor_ordinal
- direction
- members

members 中每项必须包含：
- node_id
- role

relation_type 只允许：
- year_over_year
- period_over_period
- same_period_reference
- generic_compare

direction 只允许：
- subject_to_reference
- reference_to_subject
- symmetric

role 只允许：
- subject
- reference
- peer

规则：
1. 只有原句里真的存在比较/引用表达关系时，才输出 comparison_groups
2. members 必须保持原句顺序
3. role + direction 才是语义权威，不依赖顺序猜测
4. 同一 group 中同一 node_id 不能重复出现

====================
七、输出硬约束
====================

1. 只输出 JSON object
2. 不要输出代码块
3. 不要输出注释
4. 不要输出额外字段
5. 顶层只能有 nodes 和 comparison_groups
6. 如果没有任何时间节点，输出：
   {
     "nodes": [],
     "comparison_groups": []
   }

====================
八、few-shot 示例
====================

示例 1：
输入：昨天杭千公司的收益是多少？
输出要点：
- “昨天” 是 relative_window
- needs_clarification = true

示例 2：
输入：2025年3月杭千公司的收益是多少？
输出要点：
- “2025年3月” 是 explicit_window
- needs_clarification = false

示例 3：
输入：2025年3月和4月收益分别是多少？
输出要点：
- “2025年3月” 和 “4月” 分别建节点
- “4月” 仍是 explicit_window，依赖 year_ref 补全计算语义
- 两个节点都不需要澄清

示例 4：
输入：本月至今每个工作日的收益是多少？
输出要点：
- 整体建为一个 window_with_calendar_selector
- needs_clarification = true

示例 5：
输入：今年3月和去年同期相比收益增长了多少？
输出要点：
- “今年3月” 是 explicit_window
- “去年同期” 是 reference_window
- comparison_groups 表达“相比”的关系
- reference_window 的计算引用写入 resolution_spec

示例 6：
输入：去年国庆假期后3天的收益是多少？
输出要点：
- 整体建为一个 offset_window
- 不要拆成“去年”“国庆假期”“后3天”三个独立澄清节点

示例 7：
输入：2025年每天的收益是多少？
输出要点：
- 可以建为 window_with_regular_grain
- needs_clarification = false

现在只输出合法的 ClarificationPlan JSON。
""".strip()


PLANNER_FEW_SHOTS: list[dict[str, Any]] = [
    {
        "input": {
            "original_query": "昨天杭千公司的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "昨天",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "relative_window",
                    "reason_code": "relative_time",
                    "resolution_spec": {
                        "relative_type": "single_relative",
                        "unit": "day",
                        "direction": "previous",
                        "value": 1,
                        "include_today": False,
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "2025年3月和4月收益分别是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "2025年3月",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": False,
                    "node_kind": "explicit_window",
                    "reason_code": "already_explicit_natural_period",
                    "resolution_spec": {
                        "window_type": "named_period",
                        "calendar_unit": "month",
                        "year_ref": {"mode": "absolute", "year": 2025},
                        "month": 3,
                    },
                },
                {
                    "node_id": "n2",
                    "render_text": "4月",
                    "ordinal": 2,
                    "surface_fragments": [],
                    "needs_clarification": False,
                    "node_kind": "explicit_window",
                    "reason_code": "shared_prefix_explicit",
                    "resolution_spec": {
                        "window_type": "named_period",
                        "calendar_unit": "month",
                        "year_ref": {"mode": "absolute", "year": 2025},
                        "month": 4,
                    },
                },
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "2025年3月杭千公司的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "2025年3月",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": False,
                    "node_kind": "explicit_window",
                    "reason_code": "already_explicit_natural_period",
                    "resolution_spec": {
                        "window_type": "named_period",
                        "calendar_unit": "month",
                        "year_ref": {"mode": "absolute", "year": 2025},
                        "month": 3,
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "2025年国庆假期收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "2025年国庆假期",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "holiday_window",
                    "reason_code": "holiday_or_business_calendar",
                    "resolution_spec": {
                        "holiday_key": "national_day",
                        "year_ref": {"mode": "absolute", "year": 2025},
                        "calendar_mode": "configured",
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "2025年每天的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "2025年每天",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": False,
                    "node_kind": "window_with_regular_grain",
                    "reason_code": "already_explicit_natural_period",
                    "resolution_spec": {
                        "window": {
                            "kind": "explicit_window",
                            "value": {
                                "window_type": "named_period",
                                "calendar_unit": "year",
                                "year_ref": {"mode": "absolute", "year": 2025},
                            },
                        },
                        "grain": "day",
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "本月至今每个工作日的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "本月至今每个工作日",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "window_with_calendar_selector",
                    "reason_code": "rolling_or_to_date",
                    "resolution_spec": {
                        "window": {
                            "kind": "relative_window",
                            "value": {
                                "relative_type": "to_date",
                                "unit": "month",
                                "direction": "current",
                                "value": 1,
                                "include_today": True,
                            },
                        },
                        "selector": {
                            "selector_type": "workday",
                            "selector_key": None,
                        },
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "今年3月和去年同期相比收益增长了多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "今年3月",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": False,
                    "node_kind": "explicit_window",
                    "reason_code": "already_explicit_natural_period",
                    "resolution_spec": {
                        "window_type": "named_period",
                        "calendar_unit": "month",
                        "year_ref": {"mode": "relative", "offset": 0},
                        "month": 3,
                    },
                },
                {
                    "node_id": "n2",
                    "render_text": "去年同期",
                    "ordinal": 2,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "reference_window",
                    "reason_code": "same_period_reference",
                    "resolution_spec": {
                        "reference_node_id": "n1",
                        "alignment": "same_period",
                        "shift": {
                            "unit": "year",
                            "value": -1,
                        },
                    },
                },
            ],
            "comparison_groups": [
                {
                    "group_id": "g1",
                    "relation_type": "year_over_year",
                    "anchor_text": "相比",
                    "anchor_ordinal": 2,
                    "direction": "subject_to_reference",
                    "members": [
                        {"node_id": "n1", "role": "subject"},
                        {"node_id": "n2", "role": "reference"},
                    ],
                }
            ],
        },
    },
    {
        "input": {
            "original_query": "去年国庆假期后3天的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "去年国庆假期后3天",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "offset_window",
                    "reason_code": "offset_from_anchor",
                    "resolution_spec": {
                        "base": {
                            "source": "inline",
                            "window": {
                                "kind": "holiday_window",
                                "value": {
                                    "holiday_key": "national_day",
                                    "year_ref": {"mode": "relative", "offset": -1},
                                    "calendar_mode": "statutory",
                                },
                            },
                        },
                        "offset": {
                            "direction": "after",
                            "value": 3,
                            "unit": "day",
                        },
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
]


def build_planner_messages(
    *,
    original_query: str,
    system_date: str | None,
    system_datetime: str | None,
    timezone: str,
) -> list[object]:
    request_payload = {
        "original_query": original_query,
        "system_date": system_date,
        "system_datetime": system_datetime,
        "timezone": timezone,
    }

    messages: list[object] = [SystemMessage(content=PLANNER_SYSTEM_PROMPT)]
    for shot in PLANNER_FEW_SHOTS:
        messages.append(HumanMessage(content=json.dumps(shot["input"], ensure_ascii=False)))
        messages.append(AIMessage(content=json.dumps(shot["output"], ensure_ascii=False)))
    messages.append(HumanMessage(content=json.dumps(request_payload, ensure_ascii=False)))
    return messages
