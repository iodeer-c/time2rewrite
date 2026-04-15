from pathlib import Path

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.time_resolver import resolve_plan


def test_resolve_workday_selector_returns_compressed_intervals():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))

    result = resolve_plan(
        plan={
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "本月至今每个工作日",
                    "ordinal": 1,
                    "needs_clarification": True,
                    "node_kind": "window_with_calendar_selector",
                    "reason_code": "holiday_or_business_calendar",
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
                        "selector": {"selector_type": "workday"},
                    },
                }
            ],
            "comparison_groups": [],
        },
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        business_calendar=calendar,
    )

    assert result.items[0].display_exact_time == (
        "2026年4月1日至2026年4月3日、"
        "2026年4月7日至2026年4月10日、"
        "2026年4月13日至2026年4月15日"
    )


def test_resolve_previous_day_relative_window():
    result = resolve_plan(
        plan={
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "昨天",
                    "ordinal": 1,
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
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
    )

    assert result.items[0].display_exact_time == "2026年4月14日"


def test_resolve_current_month_to_date_relative_window():
    result = resolve_plan(
        plan={
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "本月至今",
                    "ordinal": 1,
                    "needs_clarification": True,
                    "node_kind": "relative_window",
                    "reason_code": "rolling_or_to_date",
                    "resolution_spec": {
                        "relative_type": "to_date",
                        "unit": "month",
                        "direction": "current",
                        "value": 1,
                        "include_today": True,
                    },
                }
            ],
            "comparison_groups": [],
        },
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
    )

    assert result.items[0].display_exact_time == "2026年4月1日至2026年4月15日"


def test_resolve_holiday_window_uses_business_calendar():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))

    result = resolve_plan(
        plan={
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "2026年清明假期",
                    "ordinal": 1,
                    "needs_clarification": True,
                    "node_kind": "holiday_window",
                    "reason_code": "holiday_or_business_calendar",
                    "resolution_spec": {
                        "holiday_key": "qingming",
                        "year_ref": {"mode": "absolute", "year": 2026},
                        "calendar_mode": "configured",
                    },
                }
            ],
            "comparison_groups": [],
        },
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        business_calendar=calendar,
    )

    assert result.items[0].display_exact_time == "2026年4月4日至2026年4月6日"


def test_resolve_reference_window_from_explicit_month():
    result = resolve_plan(
        plan={
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "今年3月",
                    "ordinal": 1,
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
                    "needs_clarification": True,
                    "node_kind": "reference_window",
                    "reason_code": "same_period_reference",
                    "resolution_spec": {
                        "reference_node_id": "n1",
                        "alignment": "same_period",
                        "shift": {"unit": "year", "value": -1},
                    },
                },
            ],
            "comparison_groups": [],
        },
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
    )

    assert result.items[0].display_exact_time == "2025年3月1日至2025年3月31日"


def test_resolve_offset_window_after_holiday():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))

    result = resolve_plan(
        plan={
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "去年国庆假期后3天",
                    "ordinal": 1,
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
                        "offset": {"direction": "after", "value": 3, "unit": "day"},
                    },
                }
            ],
            "comparison_groups": [],
        },
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        business_calendar=calendar,
    )

    assert result.items[0].display_exact_time == "2025年10月9日至2025年10月11日"
