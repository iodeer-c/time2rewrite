from fastapi.testclient import TestClient

import main
from main import app
from pathlib import Path

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.contracts import ClarificationPlan
from time_query_service.service import QueryPipelineService
from time_query_service.time_resolver import ResolutionResult


class FakePlanner:
    def __init__(self, plan_payload: dict):
        self._plan = ClarificationPlan.model_validate(plan_payload)

    def plan_query(self, **_: object) -> ClarificationPlan:
        return self._plan


class FakeResolver:
    def __init__(self, result: ResolutionResult) -> None:
        self._result = result

    def __call__(self, **_: object) -> ResolutionResult:
        return self._result


class FailingResolver:
    def __call__(self, **_: object) -> ResolutionResult:
        raise ValueError("calendar data missing")


class FailingAnnotator:
    def render(self, **_: object) -> None:
        return None


def test_process_query_returns_original_query_when_no_clarification_needed():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
                "nodes": [
                    {
                        "node_id": "n1",
                        "render_text": "2025年每天",
                        "ordinal": 1,
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
            }
        ),
        resolver=FakeResolver(ResolutionResult(items=[])),
    )

    response = service.process_query(
        query="2025年杭千公司每天的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "2025年杭千公司每天的收益是多少？"
    assert response["clarification_items"] == []


def test_pipeline_endpoint_returns_service_payload(monkeypatch):
    class FakeService:
        def process_query(self, **_: object) -> dict:
            return {
                "clarification_plan": {"nodes": [], "comparison_groups": []},
                "clarification_items": [],
                "rewritten_query": "昨天（2026年4月14日）杭千公司的收益是多少？",
            }

    monkeypatch.setattr(main, "query_service", FakeService())
    client = TestClient(app)

    response = client.post(
        "/query/pipeline",
        json={
            "query": "昨天杭千公司的收益是多少？",
            "system_date": "2026-04-15",
            "timezone": "Asia/Shanghai",
            "rewrite": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["rewritten_query"] == "昨天（2026年4月14日）杭千公司的收益是多少？"


def test_process_query_returns_null_when_resolution_fails_for_rewrite():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
            }
        ),
        resolver=FailingResolver(),
    )

    response = service.process_query(
        query="本月至今每个工作日的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] is None
    assert response["clarification_items"] == []


def test_process_query_returns_null_when_annotation_fails():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
            }
        ),
        resolver=FakeResolver(
            ResolutionResult(
                items=[
                    {
                        "node_id": "n1",
                        "render_text": "昨天",
                        "ordinal": 1,
                        "display_exact_time": "2026年4月14日",
                    }
                ]
            )
        ),
        annotator=FailingAnnotator(),
    )

    response = service.process_query(
        query="昨天杭千公司的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] is None


def test_process_query_can_render_workday_annotation_without_llm_config():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
            }
        ),
        business_calendar=calendar,
    )

    response = service.process_query(
        query="本月至今每个工作日的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == (
        "本月至今每个工作日（2026年4月1日至2026年4月3日、"
        "2026年4月7日至2026年4月10日、2026年4月13日至2026年4月15日）的收益是多少？"
    )


def test_process_query_can_render_relative_day_annotation_without_llm_config():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
            }
        ),
    )

    response = service.process_query(
        query="昨天杭千公司的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "昨天（2026年4月14日）杭千公司的收益是多少？"


def test_process_query_can_render_month_to_date_annotation_without_llm_config():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
            }
        ),
    )

    response = service.process_query(
        query="本月至今的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "本月至今（2026年4月1日至2026年4月15日）的收益是多少？"


def test_process_query_can_render_previous_week_annotation_without_llm_config():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
                "nodes": [
                    {
                        "node_id": "n1",
                        "render_text": "上周",
                        "ordinal": 1,
                        "needs_clarification": True,
                        "node_kind": "relative_window",
                        "reason_code": "relative_time",
                        "resolution_spec": {
                            "relative_type": "single_relative",
                            "unit": "week",
                            "direction": "previous",
                            "value": 1,
                            "include_today": False,
                        },
                    }
                ],
                "comparison_groups": [],
            }
        ),
    )

    response = service.process_query(
        query="上周的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "上周（2026年4月6日至2026年4月12日）的收益是多少？"


def test_process_query_can_render_holiday_annotation_without_llm_config():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
            }
        ),
        business_calendar=calendar,
    )

    response = service.process_query(
        query="2026年清明假期杭千公司的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "2026年清明假期（2026年4月4日至2026年4月6日）杭千公司的收益是多少？"


def test_process_query_preserves_comparison_structure_for_reference_window():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
                "comparison_groups": [
                    {
                        "group_id": "g1",
                        "relation_type": "same_period_reference",
                        "anchor_text": "相比",
                        "anchor_ordinal": 1,
                        "direction": "subject_to_reference",
                        "members": [
                            {"node_id": "n1", "role": "subject"},
                            {"node_id": "n2", "role": "reference"},
                        ],
                    }
                ],
            }
        ),
    )

    response = service.process_query(
        query="今年3月和去年同期相比收益增长了多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "今年3月和去年同期（2025年3月1日至2025年3月31日）相比收益增长了多少？"


def test_process_query_preserves_comparison_structure_for_quarter_reference_window():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
                "nodes": [
                    {
                        "node_id": "n1",
                        "render_text": "今年第一季度",
                        "ordinal": 1,
                        "needs_clarification": False,
                        "node_kind": "explicit_window",
                        "reason_code": "already_explicit_natural_period",
                        "resolution_spec": {
                            "window_type": "named_period",
                            "calendar_unit": "quarter",
                            "year_ref": {"mode": "relative", "offset": 0},
                            "quarter": 1,
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
                "comparison_groups": [
                    {
                        "group_id": "g1",
                        "relation_type": "same_period_reference",
                        "anchor_text": "相比",
                        "anchor_ordinal": 1,
                        "direction": "subject_to_reference",
                        "members": [
                            {"node_id": "n1", "role": "subject"},
                            {"node_id": "n2", "role": "reference"},
                        ],
                    }
                ],
            }
        ),
    )

    response = service.process_query(
        query="今年第一季度和去年同期相比收益增长了多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "今年第一季度和去年同期（2025年1月1日至2025年3月31日）相比收益增长了多少？"


def test_process_query_can_render_offset_window_annotation_without_llm_config():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
            }
        ),
        business_calendar=calendar,
    )

    response = service.process_query(
        query="去年国庆假期后3天的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "去年国庆假期后3天（2025年10月9日至2025年10月11日）的收益是多少？"


def test_process_query_can_render_explicit_month_workday_annotation_without_llm_config():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))
    service = QueryPipelineService(
        planner=FakePlanner(
            {
                "nodes": [
                    {
                        "node_id": "n1",
                        "render_text": "2026年4月每个工作日",
                        "ordinal": 1,
                        "needs_clarification": True,
                        "node_kind": "window_with_calendar_selector",
                        "reason_code": "holiday_or_business_calendar",
                        "resolution_spec": {
                            "window": {
                                "kind": "explicit_window",
                                "value": {
                                    "window_type": "named_period",
                                    "calendar_unit": "month",
                                    "year_ref": {"mode": "absolute", "year": 2026},
                                    "month": 4,
                                },
                            },
                            "selector": {"selector_type": "workday"},
                        },
                    }
                ],
                "comparison_groups": [],
            }
        ),
        business_calendar=calendar,
    )

    response = service.process_query(
        query="2026年4月每个工作日的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == (
        "2026年4月每个工作日（2026年4月1日至2026年4月3日、"
        "2026年4月7日至2026年4月10日、2026年4月13日至2026年4月17日、"
        "2026年4月20日至2026年4月24日、2026年4月27日至2026年4月30日）的收益是多少？"
    )


def test_process_query_returns_null_when_holiday_calendar_data_is_missing():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))
    service = QueryPipelineService(
        planner=FakePlanner(
            {
                "nodes": [
                    {
                        "node_id": "n1",
                        "render_text": "2030年清明假期",
                        "ordinal": 1,
                        "needs_clarification": True,
                        "node_kind": "holiday_window",
                        "reason_code": "holiday_or_business_calendar",
                        "resolution_spec": {
                            "holiday_key": "qingming",
                            "year_ref": {"mode": "absolute", "year": 2030},
                            "calendar_mode": "configured",
                        },
                    }
                ],
                "comparison_groups": [],
            }
        ),
        business_calendar=calendar,
    )

    response = service.process_query(
        query="2030年清明假期杭千公司的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["clarification_items"] == []
    assert response["rewritten_query"] is None


def test_process_query_returns_null_when_calendar_sensitive_query_has_no_calendar():
    service = QueryPipelineService(
        planner=FakePlanner(
            {
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
            }
        ),
        business_calendar=None,
    )

    response = service.process_query(
        query="本月至今每个工作日的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["clarification_items"] == []
    assert response["rewritten_query"] is None
