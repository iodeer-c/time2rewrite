from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage


STAGE_B_SYSTEM_PROMPT = """
你是 Stage B 时间结构化器。

输入只包含一个 self_contained_text 的时间片段，以及 system_date / timezone / surface_hint / previous_validation_errors。
你必须只输出一个 JSON object，结构必须满足 StageBOutput：
- carrier
- needs_clarification
- reason_kind

规则：
- 成功时输出一个 carrier，needs_clarification=false
- 失败时输出 carrier=null, needs_clarification=true, reason_kind 为闭集值
- v1 所有 RollingWindow / RollingByCalendarUnit 只能输出 endpoint="today" 且 include_endpoint=true
- 遇到 unsupported calendar-class count rolling（例如 最近5个休息日）必须 degrade，不能近似
- `surface_hint="calendar_grain_rolling"` 只用于 day_class ∈ {workday, weekend, holiday, makeup_workday} 的 counted rolling；普通 rolling、to_date、offset、date_range 都不能因为这个字段缺失而 degrade
- 节假日事件如果没有显式年份，`schedule_year_ref` 必须直接使用 `{"year": system_date.year}`；不要输出当前实现未支持的 `source_unit_id`
- `本月至今 / 本季度至今 / 本年至今` 这类 to_date 语义，必须建模为 `mapped_range(mode="period_to_date")`
- `2025年每天` 这种 day-grain child expansion，保持 `named_period + grain_expansion(day)`，不要改写成 `grouped_temporal_value`
""".strip()


_FEW_SHOTS: list[tuple[dict[str, Any], dict[str, Any]]] = [
    ({"text": "2025年3月", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年第一季度", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "quarter", "year": 2025, "quarter": 1}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年上半年", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "half_year", "year": 2025, "half": 1}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025-03-01到2025-03-10", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "date_range", "start_date": "2025-03-01", "end_date": "2025-03-10", "end_inclusive": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一周", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "week", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一个月", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "month", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一季度", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "quarter", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近半年", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "half_year", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一年", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 1, "unit": "year", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近5天中的工作日", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "rolling_window", "length": 5, "unit": "day", "endpoint": "today", "include_endpoint": True}, "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}]}, "needs_clarification": False}),
    ({"text": "最近5个工作日", "system_date": "2026-04-17", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": {"anchor": {"kind": "rolling_by_calendar_unit", "length": 5, "day_class": "workday", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近3个节假日", "system_date": "2026-04-17", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": {"anchor": {"kind": "rolling_by_calendar_unit", "length": 3, "day_class": "holiday", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近1个周末", "system_date": "2026-04-17", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": {"anchor": {"kind": "rolling_by_calendar_unit", "length": 1, "day_class": "weekend", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近1个补班日", "system_date": "2026-04-17", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": {"anchor": {"kind": "rolling_by_calendar_unit", "length": 1, "day_class": "makeup_workday", "endpoint": "today", "include_endpoint": True}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "本月工作日", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "relative_window", "grain": "month", "offset_units": 0}, "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}]}, "needs_clarification": False}),
    ({"text": "清明假期", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "calendar_event", "region": "CN", "event_key": "qingming", "schedule_year_ref": {"year": 2026}, "scope": "consecutive_rest"}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年3月和5月", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "enumeration_set", "grain": "month", "members": [{"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5}]}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年每个季度", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "grouped_temporal_value", "parent": {"kind": "named_period", "period_type": "year", "year": 2025}, "child_grain": "quarter", "selector": "all"}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "最近一个月每周", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "grouped_temporal_value", "parent": {"kind": "rolling_window", "length": 1, "unit": "month", "endpoint": "today", "include_endpoint": True}, "child_grain": "week", "selector": "all"}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "本月至今", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "mapped_range", "mode": "period_to_date", "period_grain": "month", "anchor_ref": "system_date"}, "modifiers": []}, "needs_clarification": False}),
    ({"text": "2025年3月的前3个工作日", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}, {"kind": "member_selection", "selector": "first_n", "n": 3}]}, "needs_clarification": False}),
    ({"text": "2025年3月往后一个月", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, "modifiers": [{"kind": "offset", "value": 1, "unit": "month"}]}, "needs_clarification": False}),
    ({"text": "最近5个休息日", "system_date": "2026-04-17", "timezone": "Asia/Shanghai", "surface_hint": "calendar_grain_rolling"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_calendar_grain_rolling"}),
    ({"text": "最近一个月不含今天", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "截至昨天的最近7天", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "过去3个完整月", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}),
    ({"text": "2025年每天", "system_date": "2026-04-17", "timezone": "Asia/Shanghai"}, {"carrier": {"anchor": {"kind": "named_period", "period_type": "year", "year": 2025}, "modifiers": [{"kind": "grain_expansion", "target_grain": "day"}]}, "needs_clarification": False}),
]


def build_stage_b_messages(
    *,
    text: str,
    system_date: str,
    timezone: str,
    previous_validation_errors: list[str] | None = None,
    surface_hint: str | None = None,
) -> list[Any]:
    messages: list[Any] = [SystemMessage(content=STAGE_B_SYSTEM_PROMPT)]
    for request_payload, response_payload in _FEW_SHOTS:
        messages.append(HumanMessage(content=json.dumps(request_payload, ensure_ascii=False, indent=2)))
        messages.append(AIMessage(content=json.dumps(response_payload, ensure_ascii=False, indent=2)))
    payload: dict[str, Any] = {"text": text, "system_date": system_date, "timezone": timezone}
    if surface_hint is not None:
        payload["surface_hint"] = surface_hint
    if previous_validation_errors:
        payload["previous_validation_errors"] = previous_validation_errors
    messages.append(HumanMessage(content=json.dumps(payload, ensure_ascii=False, indent=2)))
    return messages
