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
