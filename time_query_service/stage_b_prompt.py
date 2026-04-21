from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from time_query_service.config import load_business_calendar_event_aliases


_STAGE_B_SYSTEM_PROMPT_TEMPLATE = """
你是 Stage B 时间结构化器。

输入只包含一个 self_contained_text 的时间片段，以及 system_datetime / timezone / surface_hint / previous_validation_errors。
你必须只输出一个 JSON object，结构必须满足 StageBOutput：
- carrier
- needs_clarification
- reason_kind

规则：
- 成功时输出一个 carrier，needs_clarification=false
- 失败时输出 carrier=null, needs_clarification=true, reason_kind 为闭集值
- v1 所有 RollingWindow / RollingByCalendarUnit 只能输出 endpoint="today" 且 include_endpoint=true
- 遇到 unsupported calendar-class count rolling（例如 最近5个休息日）必须 degrade，不能近似
- `surface_hint="calendar_grain_rolling"` 只用于 day_class ∈ {workday, weekend, statutory_holiday, makeup_workday} 的 counted rolling；普通 rolling、to_date、offset、date_range 都不能因为这个字段缺失而 degrade
- 节假日事件如果没有显式年份，`schedule_year_ref` 必须直接使用 `{"year": system_datetime.year}`；不要输出当前实现未支持的 `source_unit_id`
- 节日相关短语必须按词义分流：
  - 裸节日名（如 `元旦`、`中秋`）-> `calendar_event(scope="statutory")`
  - 带 `假期` 或 `长假` 的节日短语 -> `calendar_event(scope="consecutive_rest")`
  - `节假日` -> `statutory_holiday`，不是 `holiday`
- `本月至今 / 本季度至今 / 本年至今` 这类 to_date 语义，必须建模为 `mapped_range(mode="period_to_date")`
- `X至今 / X到现在` 这类“显式起点到当前”的 bounded range，必须建模为 `mapped_range(mode="bounded_pair")`，不是 `period_to_date`
- `X至今 / X到现在` 的右端精度必须继承左端：左端是自然周期/日级端点时，右端使用 `"system_datetime"` 表示当前日；左端是小时端点时，右端使用 `relative_window(grain="hour", offset_units=0)` 表示当前整点小时；不要输出精确分钟时间戳
- `本小时 / 当前小时` -> `relative_window(grain="hour", offset_units=0)`；`上一小时 / 上个小时 / 3小时前 / 2小时后` 这类 standalone 相对小时短语统一建模为 `relative_window(grain="hour", offset_units=±N)`
- `最近N小时 / 过去N小时 / 近N小时` 这类滚动小时窗口统一建模为 `rolling_window(unit="hour", length=N, endpoint="today", include_endpoint=true)`，不能误写成单点 `relative_window(hour, -N)`
- `A往前/后N小时` 这类带显式基准的表达才使用 `offset(unit="hour")`
- `今天14点`、`今天14:00-18:00`、`今天23点到明天2点`、`今天23点到2点`、`昨天2点到今天2点` 这类 hour-to-hour bounded range 必须使用 `datetime_range`
- 对于只有左端显式日期词、右端是裸小时的 hour range，先继承左端日期；若 `end < start`，则把右端顺延到次日
- 如果 hour range 两端都显式写死且仍然倒退，例如 `今天23点到今天2点`，必须 degrade 为 `semantic_conflict`
- `今天下午`、`14:30`、`今天到14点`、`今天+3小时前` 这类 hour v1 不支持的表达必须 degrade 为 `unsupported_anchor_semantics`
- `最近/过去/近 + N天|周|月|季度|半年|年` 统一视为同一家 rolling family；词面本身不决定是否到今天或昨天，planner 一律输出 `endpoint="today"`，后续 endpoint policy 由下游确定性层处理
- rolling family 的 canonical shape 必须唯一：
  - 纯 rolling -> `rolling_window` 或 `rolling_by_calendar_unit`
  - rolling + `每天` -> 保持 `rolling_window(...) + grain_expansion(day)`
  - rolling + 非 day 子粒度（`每周/每月/每季度/每半年/每年`）-> `grouped_temporal_value(parent=rolling_window(...), child_grain=..., selector="all")`
  - rolling + `calendar_filter` / `member_selection` -> 作为 trailing modifiers 挂在 canonical 主链后，不要改写进 anchor
- 例如：
  - `过去半年每月的每个工作日` -> `grouped_temporal_value(parent=rolling_window(unit="half_year"), child_grain="month", selector="all") + calendar_filter(workday)`
  - `最近一个季度每天的第一个工作日` -> `rolling_window(unit="quarter") + grain_expansion(day) + calendar_filter(workday) + member_selection(first)`
- `2025年每天` 这种 day-grain child expansion，保持 `named_period + grain_expansion(day)`，不要改写成 `grouped_temporal_value`
- `上周二 / 本周五 / 下周一` 这类“相对周里的星期几”，建模为 `grouped_temporal_value(parent=relative_window(week), child_grain="day", selector="all") + member_selection(nth)`；星期一到星期日分别对应 n=1..7
- `今年第一天 / 今年第一个工作日 / 今年第一个假期 / 今年最后一个假期 / 今年第二个假期 / 今年前两个假期 / 今年第一个季度 / 今年第二个季度 / 今年前3个工作日` 这类 selector family，统一建模为“连续自然周期 parent + 过滤/展开/事件集合 + member_selection”：
  - `第一天` -> `relative_window(year) + grain_expansion(day) + member_selection(first)`
  - `第一个工作日` -> `relative_window(year) + calendar_filter(workday) + member_selection(first)`
  - `第一个/最后一个/第二个/前两个假期` -> `holiday_event_collection(parent=relative_window(year), region="CN", scope="consecutive_rest", selector="all") + member_selection(...)`
  - `第一个/第二个季度` -> `grouped_temporal_value(parent=relative_window(year), child_grain="quarter", selector="all") + member_selection(first/nth)`
  - `前3个工作日` -> `relative_window(year) + calendar_filter(workday) + member_selection(first_n, n=3)`
  - `最近3个假期`、`上个季度第一个假期` 这类假期事件 rolling / 非 year parent 选择器，这期必须 degrade 为 `unsupported_anchor_semantics`
- 显式 bounded range（例如 `2025年9月到12月`、`去年12月到3月`、`2025年Q3到10月`、`2025年9月到10月15日`）必须输出一个单 carrier，不能拆成两个 standalone endpoint carriers。
- 如果 bounded range 两端都是显式日级日期，使用 `date_range`。
- 如果 bounded range 至少一端是自然周期边界，使用 `mapped_range(mode="bounded_pair")`，`start` 和 `end` 必须直接放 canonical endpoint anchors。
- 对于右边界缺少年份的 bounded range，必须做最小非倒退补全：先继承左边界年份；如果会倒退，再把右边界滚到下一个最小自然周期。例如 `去年12月到3月` 的右边界必须是 `2026年3月`。
- `X截止到昨天 / 前天 / 大前天 / N天前 / N日前` 这类显式 day cutoff 也属于 `mapped_range(mode="bounded_pair")`；右端必须输出 `relative_window(grain="day", offset_units=-N)`。
- `0天前 / 0日前` 不是“至今”的同义词，必须 degrade 为 `unsupported_anchor_semantics`，不能映射成 `offset_units=0`。
- 本次只支持右端显式 shifted-day cutoff；不要输出 `start = relative_window(grain="day", offset_units<0)`。
- `X截止到一周前 / 一个月前`、`X截止到3小时前`、`X截止到明天`、`最近一个月截至昨天`、`截至昨天的最近7天` 仍然必须 degrade 为 `unsupported_anchor_semantics`。
- `mapped_range(mode="bounded_pair")` 在这期只允许自然周期、显式日级端点、当前端点，以及右端 day cutoff；如果端点涉及 `calendar_event`、rolling 或 calendar-class 语义，必须 degrade 为 `unsupported_anchor_semantics`。
- 如果 bounded range 还带有 grouped / filter scaffold（例如 `2025年1月到3月每个月的每个工作日`），必须先建一个单 bounded-range parent，再把 grouped/filter 语义挂在这个 parent 上；不能先拆成两个端点 carrier。
- 节假日事件必须使用当前 schema 的英文 canonical key，不能自造拼音 key。当前俗称与 canonical key 的映射如下：
__CALENDAR_EVENT_ALIAS_CATALOG__
""".strip()


_FEW_SHOTS: list[tuple[dict[str, Any], dict[str, Any]]] = [
    ({"text": "2025年3月", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年第一季度", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "quarter", "year": 2025, "quarter": 1}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年上半年", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "half_year", "year": 2025, "half": 1}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025-03-01到2025-03-10", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "date_range", "start_date": "2025-03-01", "end_date": "2025-03-10", "end_inclusive": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年3月1日到3月10日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "date_range", "start_date": "2025-03-01", "end_date": "2025-03-10", "end_inclusive": True}, "modifiers": []}, "needs_clarification": False}),
    (
        {"text": "2025年9月到12月", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 9},
                    "end": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 12},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "去年12月到3月", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 12},
                    "end": {"kind": "named_period", "period_type": "month", "year": 2026, "month": 3},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "2024年元旦", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "calendar_event",
                    "region": "CN",
                    "event_key": "new_year",
                    "schedule_year_ref": {"year": 2024},
                    "scope": "statutory",
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "2024年元旦假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "calendar_event",
                    "region": "CN",
                    "event_key": "new_year",
                    "schedule_year_ref": {"year": 2024},
                    "scope": "consecutive_rest",
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "2025年十一假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "calendar_event",
                    "region": "CN",
                    "event_key": "national_day",
                    "schedule_year_ref": {"year": 2025},
                    "scope": "consecutive_rest",
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "2025年Q3到10月", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "quarter", "year": 2025, "quarter": 3},
                    "end": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 10},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "2025年9月到10月15日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 9},
                    "end": {"kind": "named_period", "period_type": "day", "year": 2025, "date": "2025-10-15"},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "2026年1月1日至今", "system_datetime": "2026-04-19T15:24:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "day", "year": 2026, "date": "2026-01-01"},
                    "end": "system_datetime",
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "3月1日截止到昨天", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "day", "year": 2026, "date": "2026-03-01"},
                    "end": {"kind": "relative_window", "grain": "day", "offset_units": -1},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "3月1日截止到前天", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "day", "year": 2026, "date": "2026-03-01"},
                    "end": {"kind": "relative_window", "grain": "day", "offset_units": -2},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "3月1日截止到大前天", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "day", "year": 2026, "date": "2026-03-01"},
                    "end": {"kind": "relative_window", "grain": "day", "offset_units": -3},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "3月1日截止到7天前", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {"kind": "named_period", "period_type": "day", "year": 2026, "date": "2026-03-01"},
                    "end": {"kind": "relative_window", "grain": "day", "offset_units": -7},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "昨天8点至今", "system_datetime": "2026-04-19T15:24:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "mapped_range",
                    "mode": "bounded_pair",
                    "start": {
                        "kind": "datetime_range",
                        "start_datetime": "2026-04-18T08:00:00",
                        "end_datetime": "2026-04-18T08:00:00",
                        "end_inclusive": True,
                    },
                    "end": {"kind": "relative_window", "grain": "hour", "offset_units": 0},
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "2025年1月到3月每个月的每个工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "grouped_temporal_value",
                    "parent": {
                        "kind": "mapped_range",
                        "mode": "bounded_pair",
                        "start": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 1},
                        "end": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                    },
                    "child_grain": "month",
                    "selector": "all",
                },
                "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "本小时", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "relative_window", "grain": "hour", "offset_units": 0},
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "3小时前", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "relative_window", "grain": "hour", "offset_units": -3},
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "过去三小时", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "rolling_window", "length": 3, "unit": "hour", "endpoint": "today", "include_endpoint": True},
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "最近5小时", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "rolling_window", "length": 5, "unit": "hour", "endpoint": "today", "include_endpoint": True},
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今天14点", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "datetime_range",
                    "start_datetime": "2026-04-17T14:00:00",
                    "end_datetime": "2026-04-17T14:00:00",
                    "end_inclusive": True,
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今天14:00-18:00", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "datetime_range",
                    "start_datetime": "2026-04-17T14:00:00",
                    "end_datetime": "2026-04-17T18:00:00",
                    "end_inclusive": True,
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今天23点到2点", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "datetime_range",
                    "start_datetime": "2026-04-17T23:00:00",
                    "end_datetime": "2026-04-18T02:00:00",
                    "end_inclusive": True,
                },
                "modifiers": [],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今天14点往后3小时", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "datetime_range",
                    "start_datetime": "2026-04-17T14:00:00",
                    "end_datetime": "2026-04-17T14:00:00",
                    "end_inclusive": True,
                },
                "modifiers": [{"kind": "offset", "value": 3, "unit": "hour"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今天23点到今天2点", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {"carrier": None, "needs_clarification": True, "reason_kind": "semantic_conflict"},
    ),
    (
        {"text": "今天下午", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"},
    ),
    (
        {"text": "14:30", "system_datetime": "2026-04-17T14:37:00", "timezone": "Asia/Shanghai"},
        {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"},
    ),
    ({"text": "过去七天", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 7, "unit": "day", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "近7天", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 7, "unit": "day", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一周", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "week", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一个月", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "month", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一季度", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "quarter", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "过去一个季度", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "quarter", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近半年", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "half_year", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "过去半年", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "half_year", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一年", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "year", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    (
        {"text": "过去半年每月的每个工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "grouped_temporal_value",
                    "parent": {"kind": "rolling_window", "length": 1, "unit": "half_year", "endpoint": "today", "include_endpoint": True},
                    "child_grain": "month",
                    "selector": "all",
                },
                "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "最近一个季度每天", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "rolling_window", "length": 1, "unit": "quarter", "endpoint": "today", "include_endpoint": True},
                "modifiers": [{"kind": "grain_expansion", "target_grain": "day"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "最近一个季度每天的第一个工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "rolling_window", "length": 1, "unit": "quarter", "endpoint": "today", "include_endpoint": True},
                "modifiers": [
                    {"kind": "grain_expansion", "target_grain": "day"},
                    {"kind": "calendar_filter", "day_class": "workday"},
                    {"kind": "member_selection", "selector": "first"},
                ],
            },
            "needs_clarification": False,
        },
    ),
    ({"text": "最近5天中的工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 5, "unit": "day", "endpoint": "today", "include_endpoint": True}, "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}]}, "needs_clarification": False}),
    ({"text": "最近5个工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": {"anchor": {"kind": "rolling_by_calendar_unit", "length": 5, "day_class": "workday", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近3个节假日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": {"anchor": {"kind": "rolling_by_calendar_unit", "length": 3, "day_class": "statutory_holiday", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近1个周末", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": {"anchor": {"kind": "rolling_by_calendar_unit", "length": 1, "day_class": "weekend", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近1个补班日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": {"anchor": {"kind": "rolling_by_calendar_unit", "length": 1, "day_class": "makeup_workday", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "本月工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "relative_window", "grain": "month", "offset_units": 0}, "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}]}, "needs_clarification": False}),
    (
        {"text": "上周二", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "grouped_temporal_value",
                    "parent": {"kind": "relative_window", "grain": "week", "offset_units": -1},
                    "child_grain": "day",
                    "selector": "all",
                },
                "modifiers": [{"kind": "member_selection", "selector": "nth", "n": 2}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年第一天", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                "modifiers": [{"kind": "grain_expansion", "target_grain": "day"}, {"kind": "member_selection", "selector": "first"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年第一个工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}, {"kind": "member_selection", "selector": "first"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年第一个假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "holiday_event_collection",
                    "parent": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                    "region": "CN",
                    "scope": "consecutive_rest",
                    "selector": "all",
                },
                "modifiers": [{"kind": "member_selection", "selector": "first"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年最后一个假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "holiday_event_collection",
                    "parent": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                    "region": "CN",
                    "scope": "consecutive_rest",
                    "selector": "all",
                },
                "modifiers": [{"kind": "member_selection", "selector": "last"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年第二个假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "holiday_event_collection",
                    "parent": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                    "region": "CN",
                    "scope": "consecutive_rest",
                    "selector": "all",
                },
                "modifiers": [{"kind": "member_selection", "selector": "nth", "n": 2}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年前两个假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "holiday_event_collection",
                    "parent": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                    "region": "CN",
                    "scope": "consecutive_rest",
                    "selector": "all",
                },
                "modifiers": [{"kind": "member_selection", "selector": "first_n", "n": 2}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年第一个季度", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "grouped_temporal_value",
                    "parent": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                    "child_grain": "quarter",
                    "selector": "all",
                },
                "modifiers": [{"kind": "member_selection", "selector": "first"}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年第二个季度", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {
                    "kind": "grouped_temporal_value",
                    "parent": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                    "child_grain": "quarter",
                    "selector": "all",
                },
                "modifiers": [{"kind": "member_selection", "selector": "nth", "n": 2}],
            },
            "needs_clarification": False,
        },
    ),
    (
        {"text": "今年前3个工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"},
        {
            "carrier": {
                "anchor": {"kind": "relative_window", "grain": "year", "offset_units": 0},
                "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}, {"kind": "member_selection", "selector": "first_n", "n": 3}],
            },
            "needs_clarification": False,
        },
    ),
    ({"text": "清明假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "calendar_event", "region": "CN", "event_key": "qingming", "schedule_year_ref": {"year": 2026}, "scope": "consecutive_rest"}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年3月和5月", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "enumeration_set", "grain": "month", "members": [{"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5}]}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年每个季度", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "grouped_temporal_value", "parent": {"kind": "named_period", "period_type": "year", "year": 2025}, "child_grain": "quarter", "selector": "all"}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一个月每周", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "grouped_temporal_value", "parent": {"kind": "rolling_window", "length": 1, "unit": "month", "endpoint": "today", "include_endpoint": True}, "child_grain": "week", "selector": "all"}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "本月至今", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "mapped_range", "mode": "period_to_date", "period_grain": "month", "anchor_ref": "system_datetime"}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年3月的前3个工作日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}, {"kind": "member_selection", "selector": "first_n", "n": 3}]}, "needs_clarification": False}),
    ({"text": "2025年3月往后一个月", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, "modifiers": [{"kind": "offset", "value": 1, "unit": "month"}]}, "needs_clarification": False}),
    ({"text": "最近5个休息日", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_calendar_grain_rolling"}),
    ({"text": "最近3个假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "上个季度第一个假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "最近一个月不含今天", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "截至昨天的最近7天", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "最近一个月截至昨天", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "过去3个完整月", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "去年9月到国庆假期", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "最近一周到上周五", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "3月1日截止到一周前", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "3月1日截止到一个月前", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "3月1日截止到0天前", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "3月1日截止到0日前", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "3月1日截止到3小时前", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "3月1日截止到明天", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "前天截止到3月1日", "system_datetime": "2026-04-21T09:30:00", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "2025年每天", "system_datetime": "2026-04-17T00:00:00", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "year", "year": 2025}, "modifiers": [{"kind": "grain_expansion", "target_grain": "day"}]}, "needs_clarification": False}),
]


def _build_calendar_event_alias_catalog_text() -> str:
    alias_map = load_business_calendar_event_aliases(region="CN")
    lines: list[str] = []
    for canonical_key, aliases in alias_map.items():
        alias_text = " / ".join(aliases)
        lines.append(f"- {alias_text} -> {canonical_key}")
    return "\n".join(lines)


def _build_stage_b_system_prompt() -> str:
    return _STAGE_B_SYSTEM_PROMPT_TEMPLATE.replace(
        "__CALENDAR_EVENT_ALIAS_CATALOG__", _build_calendar_event_alias_catalog_text()
    )


def build_stage_b_messages(
    *,
    text: str,
    system_datetime: str,
    timezone: str,
    previous_validation_errors: list[str] | None = None,
    surface_hint: str | None = None,
) -> list[Any]:
    messages: list[Any] = [SystemMessage(content=_build_stage_b_system_prompt())]
    for request_payload, response_payload in _FEW_SHOTS:
        messages.append(HumanMessage(content=json.dumps(request_payload, ensure_ascii=False, indent=2)))
        messages.append(AIMessage(content=json.dumps(response_payload, ensure_ascii=False, indent=2)))
    payload: dict[str, Any] = {"text": text, "system_datetime": system_datetime, "timezone": timezone}
    if surface_hint is not None:
        payload["surface_hint"] = surface_hint
    if previous_validation_errors:
        payload["previous_validation_errors"] = previous_validation_errors
    messages.append(HumanMessage(content=json.dumps(payload, ensure_ascii=False, indent=2)))
    return messages
