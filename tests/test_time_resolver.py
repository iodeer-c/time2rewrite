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
