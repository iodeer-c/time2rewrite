from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from time_query_service.clarification_contract import (
    SUPPORTED_CALENDAR_SELECTOR_TYPES,
    SUPPORTED_EXPLICIT_WINDOW_TYPES,
    SUPPORTED_NODE_KINDS,
    UNSUPPORTED_PROMPT_PATTERNS,
    bullet_lines,
)


SUPPORTED_NODE_KIND_LINES = bullet_lines(SUPPORTED_NODE_KINDS)
SUPPORTED_EXPLICIT_WINDOW_TYPE_LINES = bullet_lines(SUPPORTED_EXPLICIT_WINDOW_TYPES)
SUPPORTED_SELECTOR_TYPE_LINES = bullet_lines(SUPPORTED_CALENDAR_SELECTOR_TYPES)
UNSUPPORTED_PATTERN_LINES = bullet_lines(UNSUPPORTED_PROMPT_PATTERNS)

PLANNER_SYSTEM_PROMPT = f"""
你是“时间澄清规划器”（Clarification Planner）。

你的唯一任务是：从用户问题中识别时间节点，判断哪些节点需要澄清，并输出一个合法的 ClarificationPlan JSON。

你不是回答器，不是日期计算器，不是改写器。
你绝不能：
1. 回答用户问题
2. 计算最终准确日期
3. 生成 rewritten_query
4. 输出解释、分析、备注、markdown、代码块
5. 输出任何 JSON 以外的内容
6. 忽略 `previous_validation_errors`

你必须只输出一个 JSON object，顶层只能包含：
- nodes
- comparison_groups

如果请求 payload 里带有 `previous_validation_errors`：
- 这表示你上一轮输出违反了 contract
- 你必须逐条修正这些错误
- 不要重复上一轮非法结构

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
二、时间节点的语义拆解与类型映射
====================

你必须先理解时间节点的语义组成，再决定最终输出的 node_kind。
不要先猜 node_kind，再反推字段。

每个时间节点都按下面四层理解：

1. base_window
表示时间范围本身。
base_window 只可能是以下几类之一：
- explicit_window：明确自然期间、自然期间范围或绝对时间窗口，例如 2025年3月、2025年第一季度、2025年9月到12月
- relative_window：相对时间或 to_date 窗口，例如 昨天、上周、本月至今
- holiday_window：节假日窗口，例如 清明节假期、国庆假期
- offset_window：基准窗口加偏移，例如 去年国庆假期后3天

2. modifier
表示附着在时间窗口上的额外修饰。
modifier 只可能是以下三类之一：
- none：没有额外修饰
- regular_grain：普通粒度，例如 每天、每周、每月、每个月、各月、各月份、每季度
- calendar_selector：依赖外部日历语义的筛选，例如 每个工作日、节假日

3. derivation
表示该节点是否通过引用其他时间节点得到。
例如 去年同期、上月同期、去年同期每个月，不再输出顶层 `reference_window` node_kind。
如果一个节点需要表达 ref 语义：
- 该节点仍然必须有一个正常的 carrier
- 引用关系必须写入 derivation
- derivation 只表达来源和对齐/平移，不表达结构

4. relation
表示多个时间节点之间的比较或引用关系，例如 同比、环比、相比、对比。
relation 不决定 carrier.kind，只进入 comparison_groups。

你必须按下表选择唯一合法的 carrier.kind：
1. 如果节点只有 base_window，没有 modifier：
- explicit_window -> carrier.kind = explicit_window
- relative_window -> carrier.kind = relative_window
- holiday_window -> carrier.kind = holiday_window
- offset_window -> carrier.kind = offset_window
2. 如果节点由 base_window + regular_grain 组成：
- 最终 carrier.kind 必须是 window_with_regular_grain
3. 如果节点由 base_window + calendar_selector 组成：
- 最终 carrier.kind 必须是 window_with_calendar_selector
4. 如果节点是在 `window_with_regular_grain`、`window_with_calendar_selector` 或其他可枚举集合之上，再附加“第一个 / 最后一个 / 第 N 个 / 倒数第 N 个 / 前 N 个 / 后 N 个”这类成员选择：
- 最终 carrier.kind 必须是 window_with_member_selection
- `selection.mode = first/last` 可配 `count`
- `selection.mode = nth/nth_from_end` 必须配 `index`
5. 如果节点只有 regular_grain，没有明确 window：
- regular_grain 不能单独输出为一个合法 node
- 你应当优先把它并入所属的时间窗口节点
- 如果无法并入任何明确窗口，则不要构造非法 carrier.kind

====================
三、needs_clarification 判定规则
====================

默认 needs_clarification = true 的情况：
1. 相对时间：昨天、上周、上个月、去年
2. to_date：本月至今、本季度至今、本年至今
3. holiday_window：清明假期、国庆假期
4. offset_window：去年国庆假期后3天
5. derivation ref：去年同期、上月同期
6. 含 calendar_selector 的节点：本月至今每个工作日、今年节假日
7. window + regular_grain 且成员无法直接从字面唯一读出的节点：例如 2025年上半年的每周
8. 父层比较/配对下只保留一层子结构的节点：例如 比较今年每个月的每个工作日和去年同期每个月的每个工作日
9. 过滤后的成员选择或切片：例如 2025年10月第一个工作日、今年每个月的前3个工作日
10. 子周期成员选择：例如 今年每个月的前3日、今年每个月的第3日

默认 needs_clarification = false 的情况：
1. 已明确自然期间：2025年3月、2025年第一季度
2. 共享前缀下仍然明确：2025年3月和4月
3. window + regular_grain 且成员可直接从字面唯一读出：2025年9月到12月的各月份
4. window + regular_grain 但只是笼统粒度词、并未要求系统展开具体成员时：2025年每天、2025年每季度

判定标准不是“code 能不能算”，而是“普通读者是否仍不够明确”。
如果 query 没有直接列出每个成员，而 rewrite 又必须无损物化这些成员，就应视为 needs_clarification = true。

====================
四、nodes 字段要求
====================

每个 node 必须包含：
- node_id
- render_text
- ordinal
- surface_fragments
- needs_clarification
- reason_code
- carrier

只有节点带 ref 语义时，才额外包含：
- derivation

字段含义：
1. node_id：节点唯一标识
2. render_text：原句中实际出现、最终要被加注释的文本。不要补全年份，不要改写成规范表达。
3. ordinal：该时间表达在原句中的出现顺序，从 1 开始
4. surface_fragments：仅在不连续或重复定位时使用；否则输出空数组
5. needs_clarification：该节点是否需要后续澄清
6. carrier.kind 是最终合法输出类型，不是语义标签。regular_grain 和 calendar_selector 只是语义成分，不是合法输出字段。
7. carrier.value：供 code 使用的结构化时间语义对象，必须和 carrier.kind 一一对应，不能是自由文本
8. derivation：只在 ref 语义时出现，负责表达 source_node_id、alignment、shift、inheritance_mode 和可选 rebind_target_path
9. reason_code：判定原因

carrier.kind 只允许：
{SUPPORTED_NODE_KIND_LINES}

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
五、carrier / derivation 约束
====================

1. carrier.value 必须与 carrier.kind 匹配。
2. carrier.value 必须是结构化对象，不能输出自然语言字符串。
3. carrier.value 只包含“计算所需的最小充分信息”，不要把 render_text 重复塞进去。
4. 如果是比较/同比/环比/相比这种表达关系，不放在 carrier 或 derivation 中，而放在 comparison_groups 中。
5. 如果是 window_with_regular_grain，carrier.value 必须包含 window 和 grain。
6. 如果是 window_with_calendar_selector，carrier.value 必须包含 window 和 selector。
7. 如果是 window_with_member_selection，carrier.value 必须包含 window 和 selection。
8. `selection.mode = first/last` 时，只能配可选 `count`；`selection.mode = nth/nth_from_end` 时，只能配正整数 `index`。
9. 不要把“前 N 个 / 后 N 个 / 第 N 个”塞进 `selector`，更不要自造 `selector_key`、`n` 这类字段。
10. 如果语义是“每个月的前3日 / 第3日”，必须先把“每个月”建成父层，再把“前3日 / 第3日”建成父层内部的 child-member selection；绝不能把它误写成“月份集合取前3个成员”。
11. 对 explicit_window，window_type 只允许：
{SUPPORTED_EXPLICIT_WINDOW_TYPE_LINES}
12. `named_period` 只用于一个完整自然期间；`named_period_range` 只用于月 / 季度 / 半年 / 年这种自然期间范围。
13. `named_period_range` 必须使用 `start_period` 和 `end_period`，不要自造 `range_period`、`start_month`、`end_month` 这类字段。
14. 对 calendar selector，selector_type 只允许：
{SUPPORTED_SELECTOR_TYPE_LINES}
15. 如果节点带 derivation：
- 必须同时保留正常 carrier
- derivation 目前只允许 `alignment = same_period`
- `inheritance_mode` 只允许 admitted 枚举，不能自造
- 只有 nested 重绑时才允许 `rebind_target_path`
16. comparison family 里的 derived member，`source_node_id` 必须直接指向同 family 内的 source node，不能绕到 helper node。
17. 营业日当前不进入 admitted contract，绝不能输出成新的 selector_type。

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

当多个 sibling comparison_group 共享时间成员时，还必须额外包含：
- surface_fragments

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
2. `comparison_groups` 只在原句出现明确比较触发词时才 admitted，例如：比较 / 相比 / 对比 / 同比 / 环比 / 高于 / 低于 / 增长 / 下降 / 增幅 / 降幅
3. 如果原句只有 `和 / 与 / 以及 / 分别 / 各自 / 各 / 分别是 / 分别为` 这类并列求值表达，而没有明确比较触发词：
- 默认按 parallel standalone 处理
- 必须输出 `comparison_groups = []`
- 不要因为两个节点看起来可对齐，就偷偷构造 symmetric comparison_group
4. members 必须保持原句顺序
5. role + direction 才是语义权威，不依赖顺序猜测
6. 同一 group 中同一 node_id 不能重复出现
7. 如果多个 comparison_group 共享时间成员，必须用不同的 surface_fragments 显式标出各自 family 边界
8. 如果共享时间成员但无法从原句唯一拆出独立 family，就不要输出这种 payload
9. 如果 comparison_group 成员自带父子层级，只允许一层父组 + 一层子结构；超过一层不要输出
10. standalone 普通父子枚举仍是 phase1 普通节点，不要因为有父子层级就额外构造 comparison_groups

====================
七、输出硬约束
====================

1. 只输出 JSON object
2. 不要输出代码块
3. 不要输出注释
4. 不要输出额外字段
5. 顶层只能有 nodes 和 comparison_groups
6. 如果没有任何时间节点，输出：
   {{
     "nodes": [],
     "comparison_groups": []
   }}
7. 以下输出是非法的，绝不能生成：
   - node_kind = regular_grain
   - node_kind = calendar_sensitive
   - node_kind = calendar_selector_only
   - 顶层 `node_kind = reference_window`
   - 在 window_with_calendar_selector 中，把 selector_type 写成 day、month、quarter、year 或 regular_grain
   - 把 comparison / 同比 / 环比 / 相比 关系写进 carrier.value 或 derivation
   - 先输出一个窗口节点，再单独输出一个“每天”节点，造成语义重叠
   - 只输出 derivation，不输出对应的 carrier
   - 输出任何下列 ad hoc 结构：
{UNSUPPORTED_PATTERN_LINES}

====================
八、决策顺序
====================

你必须按以下顺序思考：
1. 找到最小但完整的时间节点
2. 判断这个节点的 base_window 是什么
3. 判断这个节点是否带有 modifier
4. 根据 base_window + modifier 的组合，选择唯一合法 carrier.kind
5. 再填写对应的 carrier.value
6. 如果节点带 ref 语义，再填写 derivation
7. 最后判断 needs_clarification
8. 如果存在比较关系，再填写 comparison_groups

====================
九、few-shot 示例
====================

下面给出的完整 JSON few-shot 示例是权威示例。
你必须学习它们的结构模式、字段落位、合法枚举和节点组合方式。

现在只输出合法的 ClarificationPlan JSON。
""".strip()


PLANNER_FEW_SHOTS: list[dict[str, Any]] = [
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
            "original_query": "2025年9月到12月杭千公司的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "2025年9月到12月",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": False,
                    "node_kind": "explicit_window",
                    "reason_code": "already_explicit_natural_period",
                    "resolution_spec": {
                        "window_type": "named_period_range",
                        "calendar_unit": "month",
                        "start_period": {
                            "year_ref": {"mode": "absolute", "year": 2025},
                            "month": 9,
                        },
                        "end_period": {
                            "year_ref": {"mode": "absolute", "year": 2025},
                            "month": 12,
                        },
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "2025年杭千公司9月到12月的各月份的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "2025年9月到12月的各月份",
                    "ordinal": 1,
                    "surface_fragments": ["2025年", "9月到12月", "各月份"],
                    "needs_clarification": False,
                    "node_kind": "window_with_regular_grain",
                    "reason_code": "already_explicit_natural_period",
                    "resolution_spec": {
                        "window": {
                            "kind": "explicit_window",
                            "value": {
                                "window_type": "named_period_range",
                                "calendar_unit": "month",
                                "start_period": {
                                    "year_ref": {"mode": "absolute", "year": 2025},
                                    "month": 9,
                                },
                                "end_period": {
                                    "year_ref": {"mode": "absolute", "year": 2025},
                                    "month": 12,
                                },
                            },
                        },
                        "grain": "month",
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
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
            "original_query": "清明节假期杭千公司的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "清明节假期",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "holiday_window",
                    "reason_code": "holiday_or_business_calendar",
                    "resolution_spec": {
                        "holiday_key": "qingming",
                        "year_ref": {"mode": "relative", "offset": 0},
                        "calendar_mode": "configured",
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "2025年杭千公司每天的收益是多少？",
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
                    "surface_fragments": ["2025年", "每天"],
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
            "original_query": "2025年上半年的每周的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "2025年上半年的每周",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "window_with_regular_grain",
                    "reason_code": "structural_enumeration",
                    "resolution_spec": {
                        "window": {
                            "kind": "explicit_window",
                            "value": {
                                "window_type": "named_period",
                                "calendar_unit": "half",
                                "year_ref": {"mode": "absolute", "year": 2025},
                                "half": 1,
                            },
                        },
                        "grain": "week",
                    },
                }
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "本月至今杭千公司每个工作日的收益是多少？",
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
                    "surface_fragments": ["本月至今", "每个工作日"],
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
            "original_query": "今年3月和去年同期的收益分别是多少？",
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
                    "reason_code": "already_explicit_natural_period",
                    "carrier": {
                        "kind": "explicit_window",
                        "value": {
                            "window_type": "named_period",
                            "calendar_unit": "month",
                            "year_ref": {"mode": "relative", "offset": 0},
                            "month": 3,
                        },
                    },
                },
                {
                    "node_id": "n2",
                    "render_text": "去年同期",
                    "ordinal": 2,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "reason_code": "same_period_reference",
                    "carrier": {
                        "kind": "explicit_window",
                        "value": {
                            "window_type": "named_period",
                            "calendar_unit": "month",
                            "year_ref": {"mode": "relative", "offset": -1},
                            "month": 3,
                        },
                    },
                    "derivation": {
                        "source_node_id": "n1",
                        "alignment": "same_period",
                        "shift": {"unit": "year", "value": -1},
                        "inheritance_mode": "scalar_projection",
                    },
                },
            ],
            "comparison_groups": [],
        },
    },
    {
        "input": {
            "original_query": "前前年3个月和去年同期的收益分别是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "前前年3个月",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "reason_code": "relative_time",
                    "carrier": {
                        "kind": "window_with_member_selection",
                        "value": {
                            "window": {
                                "kind": "window_with_regular_grain",
                                "value": {
                                    "window": {
                                        "kind": "explicit_window",
                                        "value": {
                                            "window_type": "named_period",
                                            "calendar_unit": "year",
                                            "year_ref": {"mode": "relative", "offset": -2},
                                        },
                                    },
                                    "grain": "month",
                                },
                            },
                            "selection": {"mode": "first", "count": 3},
                        },
                    },
                },
                {
                    "node_id": "n2",
                    "render_text": "去年同期",
                    "ordinal": 2,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "reason_code": "same_period_reference",
                    "carrier": {
                        "kind": "window_with_member_selection",
                        "value": {
                            "window": {
                                "kind": "window_with_regular_grain",
                                "value": {
                                    "window": {
                                        "kind": "explicit_window",
                                        "value": {
                                            "window_type": "named_period",
                                            "calendar_unit": "year",
                                            "year_ref": {"mode": "relative", "offset": -1},
                                        },
                                    },
                                    "grain": "month",
                                },
                            },
                            "selection": {"mode": "first", "count": 3},
                        },
                    },
                    "derivation": {
                        "source_node_id": "n1",
                        "alignment": "same_period",
                        "shift": {"unit": "year", "value": 1},
                        "inheritance_mode": "preserve_flat_carrier",
                    },
                },
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
                    "reason_code": "already_explicit_natural_period",
                    "carrier": {
                        "kind": "explicit_window",
                        "value": {
                            "window_type": "named_period",
                            "calendar_unit": "month",
                            "year_ref": {"mode": "relative", "offset": 0},
                            "month": 3,
                        },
                    },
                },
                {
                    "node_id": "n2",
                    "render_text": "去年同期",
                    "ordinal": 2,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "reason_code": "same_period_reference",
                    "carrier": {
                        "kind": "explicit_window",
                        "value": {
                            "window_type": "named_period",
                            "calendar_unit": "month",
                            "year_ref": {"mode": "relative", "offset": -1},
                            "month": 3,
                        },
                    },
                    "derivation": {
                        "source_node_id": "n1",
                        "alignment": "same_period",
                        "shift": {
                            "unit": "year",
                            "value": -1,
                        },
                        "inheritance_mode": "scalar_projection",
                    },
                },
            ],
            "comparison_groups": [
                {
                    "group_id": "g1",
                    "relation_type": "year_over_year",
                    "anchor_text": "相比",
                    "anchor_ordinal": 1,
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
                                    "calendar_mode": "configured",
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
    {
        "input": {
            "original_query": "比较今年每个月和去年同期每个月的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "今年每个月",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "reason_code": "structural_enumeration",
                    "carrier": {
                        "kind": "window_with_regular_grain",
                        "value": {
                            "window": {
                                "kind": "explicit_window",
                                "value": {
                                    "window_type": "named_period",
                                    "calendar_unit": "year",
                                    "year_ref": {"mode": "relative", "offset": 0},
                                },
                            },
                            "grain": "month",
                        },
                    },
                },
                {
                    "node_id": "n2",
                    "render_text": "去年同期每个月",
                    "ordinal": 2,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "reason_code": "same_period_reference",
                    "carrier": {
                        "kind": "window_with_regular_grain",
                        "value": {
                            "window": {
                                "kind": "explicit_window",
                                "value": {
                                    "window_type": "named_period",
                                    "calendar_unit": "year",
                                    "year_ref": {"mode": "relative", "offset": -1},
                                },
                            },
                            "grain": "month",
                        },
                    },
                    "derivation": {
                        "source_node_id": "n1",
                        "alignment": "same_period",
                        "shift": {"unit": "year", "value": -1},
                        "inheritance_mode": "preserve_flat_carrier",
                    },
                },
            ],
            "comparison_groups": [
                {
                    "group_id": "g1",
                    "relation_type": "year_over_year",
                    "anchor_text": "比较",
                    "anchor_ordinal": 1,
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
            "original_query": "比较今年每个月的前3个工作日和去年同期每个月的前3个工作日的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "今年每个月的前3个工作日",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "window_with_member_selection",
                    "reason_code": "structural_enumeration",
                    "resolution_spec": {
                        "window": {
                            "kind": "window_with_calendar_selector",
                            "value": {
                                "window": {
                                    "kind": "window_with_regular_grain",
                                    "value": {
                                        "window": {
                                            "kind": "explicit_window",
                                            "value": {
                                                "window_type": "named_period",
                                                "calendar_unit": "year",
                                                "year_ref": {"mode": "relative", "offset": 0},
                                            },
                                        },
                                        "grain": "month",
                                    },
                                },
                                "selector": {"selector_type": "workday"},
                            },
                        },
                        "selection": {"mode": "first", "count": 3},
                    },
                },
                {
                    "node_id": "n2",
                    "render_text": "去年同期每个月的前3个工作日",
                    "ordinal": 2,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "reason_code": "same_period_reference",
                    "carrier": {
                        "kind": "window_with_member_selection",
                        "value": {
                            "window": {
                                "kind": "window_with_calendar_selector",
                                "value": {
                                    "window": {
                                        "kind": "window_with_regular_grain",
                                        "value": {
                                            "window": {
                                                "kind": "explicit_window",
                                                "value": {
                                                    "window_type": "named_period",
                                                    "calendar_unit": "year",
                                                    "year_ref": {"mode": "relative", "offset": -1},
                                                },
                                            },
                                            "grain": "month",
                                        },
                                    },
                                    "selector": {"selector_type": "workday"},
                                },
                            },
                            "selection": {"mode": "first", "count": 3},
                        },
                    },
                    "derivation": {
                        "source_node_id": "n1",
                        "alignment": "same_period",
                        "shift": {"unit": "year", "value": -1},
                        "inheritance_mode": "rebind_nested_base",
                        "rebind_target_path": [
                            {"carrier_kind": "window_with_member_selection", "slot": "window"},
                            {"carrier_kind": "window_with_calendar_selector", "slot": "window"},
                            {"carrier_kind": "window_with_regular_grain", "slot": "window"},
                        ],
                    },
                },
            ],
            "comparison_groups": [
                {
                    "group_id": "g1",
                    "relation_type": "year_over_year",
                    "anchor_text": "比较",
                    "anchor_ordinal": 1,
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
            "original_query": "比较今年每个月的前3日和去年同期每个月的前3日的收益是多少？",
            "system_date": "2026-04-15",
            "system_datetime": "2026-04-15 09:30:00",
            "timezone": "Asia/Shanghai",
        },
        "output": {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "今年每个月的前3日",
                    "ordinal": 1,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "node_kind": "window_with_member_selection",
                    "reason_code": "structural_enumeration",
                    "resolution_spec": {
                        "window": {
                            "kind": "window_with_regular_grain",
                            "value": {
                                "window": {
                                    "kind": "window_with_regular_grain",
                                    "value": {
                                        "window": {
                                            "kind": "explicit_window",
                                            "value": {
                                                "window_type": "named_period",
                                                "calendar_unit": "year",
                                                "year_ref": {"mode": "relative", "offset": 0},
                                            },
                                        },
                                        "grain": "month",
                                    },
                                },
                                "grain": "day",
                            },
                        },
                        "selection": {"mode": "first", "count": 3},
                    },
                },
                {
                    "node_id": "n2",
                    "render_text": "去年同期每个月的前3日",
                    "ordinal": 2,
                    "surface_fragments": [],
                    "needs_clarification": True,
                    "reason_code": "same_period_reference",
                    "carrier": {
                        "kind": "window_with_member_selection",
                        "value": {
                            "window": {
                                "kind": "window_with_regular_grain",
                                "value": {
                                    "window": {
                                        "kind": "window_with_regular_grain",
                                        "value": {
                                            "window": {
                                                "kind": "explicit_window",
                                                "value": {
                                                    "window_type": "named_period",
                                                    "calendar_unit": "year",
                                                    "year_ref": {"mode": "relative", "offset": -1},
                                                },
                                            },
                                            "grain": "month",
                                        },
                                    },
                                    "grain": "day",
                                },
                            },
                            "selection": {"mode": "first", "count": 3},
                        },
                    },
                    "derivation": {
                        "source_node_id": "n1",
                        "alignment": "same_period",
                        "shift": {"unit": "year", "value": -1},
                        "inheritance_mode": "preserve_grouped_carrier",
                    },
                },
            ],
            "comparison_groups": [
                {
                    "group_id": "g1",
                    "relation_type": "year_over_year",
                    "anchor_text": "比较",
                    "anchor_ordinal": 1,
                    "direction": "subject_to_reference",
                    "members": [
                        {"node_id": "n1", "role": "subject"},
                        {"node_id": "n2", "role": "reference"},
                    ],
                }
            ],
        },
    },
]


def _canonicalize_planner_node(node: dict[str, Any]) -> dict[str, Any]:
    if "carrier" in node:
        return node
    if "node_kind" not in node or "resolution_spec" not in node:
        return node
    canonical = dict(node)
    canonical["carrier"] = {
        "kind": canonical.pop("node_kind"),
        "value": canonical.pop("resolution_spec"),
    }
    return canonical


PLANNER_FEW_SHOTS = [
    {
        "input": shot["input"],
        "output": {
            "nodes": [_canonicalize_planner_node(node) for node in shot["output"]["nodes"]],
            "comparison_groups": shot["output"].get("comparison_groups", []),
        },
    }
    for shot in PLANNER_FEW_SHOTS
]


def build_planner_messages(
    *,
    original_query: str,
    system_date: str | None,
    system_datetime: str | None,
    timezone: str,
    previous_validation_errors: list[str] | None = None,
) -> list[object]:
    request_payload = {
        "original_query": original_query,
        "system_date": system_date,
        "system_datetime": system_datetime,
        "timezone": timezone,
    }
    if previous_validation_errors:
        request_payload["previous_validation_errors"] = previous_validation_errors

    messages: list[object] = [SystemMessage(content=PLANNER_SYSTEM_PROMPT)]
    for shot in PLANNER_FEW_SHOTS:
        messages.append(HumanMessage(content=json.dumps(shot["input"], ensure_ascii=False)))
        messages.append(AIMessage(content=json.dumps(shot["output"], ensure_ascii=False)))
    messages.append(HumanMessage(content=json.dumps(request_payload, ensure_ascii=False)))
    return messages
