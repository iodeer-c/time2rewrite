from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.service import QueryPipelineService


@dataclass
class _Response:
    content: str


class _StaticStageARunner:
    def invoke(self, messages):
        payload = json.loads(messages[-1].content)
        query = payload["query"]
        stage_a_payloads = {
            "2025年3月收益": {
                "query": query,
                "system_date": payload["system_date"],
                "timezone": payload["timezone"],
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
            "最近5个休息日收益": {
                "query": query,
                "system_date": payload["system_date"],
                "timezone": payload["timezone"],
                "units": [
                    {
                        "unit_id": "u1",
                        "render_text": "最近5个休息日",
                        "surface_fragments": [{"start": 0, "end": 7}],
                        "content_kind": "standalone",
                        "self_contained_text": "最近5个休息日",
                        "sources": [],
                        "surface_hint": "calendar_grain_rolling",
                    }
                ],
                "comparisons": [],
            },
            "2025年9月到12月收益": {
                "query": query,
                "system_date": payload["system_date"],
                "timezone": payload["timezone"],
                "units": [
                    {
                        "unit_id": "u1",
                        "render_text": "2025年9月到12月",
                        "surface_fragments": [{"start": 0, "end": 11}],
                        "content_kind": "standalone",
                        "self_contained_text": "2025年9月到12月",
                        "sources": [],
                    }
                ],
                "comparisons": [],
            },
            "去年12月到3月收益": {
                "query": query,
                "system_date": payload["system_date"],
                "timezone": payload["timezone"],
                "units": [
                    {
                        "unit_id": "u1",
                        "render_text": "去年12月到3月",
                        "surface_fragments": [{"start": 0, "end": 8}],
                        "content_kind": "standalone",
                        "self_contained_text": "去年12月到3月",
                        "sources": [],
                    }
                ],
                "comparisons": [],
            },
            "2025年Q3到10月收益": {
                "query": query,
                "system_date": payload["system_date"],
                "timezone": payload["timezone"],
                "units": [
                    {
                        "unit_id": "u1",
                        "render_text": "2025年Q3到10月",
                        "surface_fragments": [{"start": 0, "end": 10}],
                        "content_kind": "standalone",
                        "self_contained_text": "2025年Q3到10月",
                        "sources": [],
                    }
                ],
                "comparisons": [],
            },
            "2025年1月到3月每个月的每个工作日收益": {
                "query": query,
                "system_date": payload["system_date"],
                "timezone": payload["timezone"],
                "units": [
                    {
                        "unit_id": "u1",
                        "render_text": "2025年1月到3月每个月的每个工作日",
                        "surface_fragments": [{"start": 0, "end": 18}],
                        "content_kind": "standalone",
                        "self_contained_text": "2025年1月到3月每个月的每个工作日",
                        "sources": [],
                    }
                ],
                "comparisons": [],
            },
        }
        if query in stage_a_payloads:
            return _Response(json.dumps(stage_a_payloads[query], ensure_ascii=False))
        raise AssertionError(f"unexpected query {query}")


class _StaticStageBRunner:
    def invoke(self, messages):
        payload = json.loads(messages[-1].content)
        text = payload["text"]
        stage_b_payloads = {
            "2025年3月": {
                "carrier": {
                    "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                    "modifiers": [],
                },
                "needs_clarification": False,
            },
            "2025年9月到12月": {
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
            "去年12月到3月": {
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
            "2025年Q3到10月": {
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
            "2025年1月到3月每个月的每个工作日": {
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
        }
        if text in stage_b_payloads:
            return _Response(json.dumps(stage_b_payloads[text], ensure_ascii=False))
        raise AssertionError(f"unexpected stage B text {text}")


def _calendar() -> JsonBusinessCalendar:
    return JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))


def test_query_pipeline_service_runs_new_pipeline_end_to_end() -> None:
    service = QueryPipelineService(
        stage_a_runner=_StaticStageARunner(),
        stage_b_runner=_StaticStageBRunner(),
        business_calendar=_calendar(),
        pipeline_logging_enabled=False,
    )

    response = service.process_query(
        query="2025年3月收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["original_query"] == "2025年3月收益"
    assert response["clarification_plan"]["units"][0]["unit_id"] == "u1"
    assert response["clarification_items"][0]["unit_id"] == "u1"
    assert response["clarified_query"] == "2025年3月收益（2025年3月指2025年3月1日至2025年3月31日）"
    assert response["rewritten_query"] == response["clarified_query"]


def test_query_pipeline_service_surfaces_degraded_slot_without_failing_whole_response() -> None:
    service = QueryPipelineService(
        stage_a_runner=_StaticStageARunner(),
        stage_b_runner=_StaticStageBRunner(),
        business_calendar=_calendar(),
        pipeline_logging_enabled=False,
    )

    response = service.process_query(
        query="最近5个休息日收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["clarification_plan"]["units"][0]["needs_clarification"] is True
    assert response["clarification_plan"]["units"][0]["reason_kind"] == "unsupported_calendar_grain_rolling"
    assert response["clarified_query"] == "最近5个休息日收益（最近5个休息日当前无法确定）"
    assert response["rewritten_query"] == response["clarified_query"]


def test_query_pipeline_service_keeps_month_bounded_range_as_one_unit_end_to_end() -> None:
    service = QueryPipelineService(
        stage_a_runner=_StaticStageARunner(),
        stage_b_runner=_StaticStageBRunner(),
        business_calendar=_calendar(),
        pipeline_logging_enabled=False,
    )

    response = service.process_query(
        query="2025年9月到12月收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert len(response["clarification_plan"]["units"]) == 1
    assert response["clarification_plan"]["units"][0]["render_text"] == "2025年9月到12月"
    assert response["clarification_plan"]["units"][0]["content"]["carrier"]["anchor"]["kind"] == "mapped_range"
    assert len(response["clarification_items"]) == 1
    assert response["clarification_items"][0]["label"] == "2025年9月到12月"
    assert response["clarification_items"][0]["resolved_text"] == "2025年9月1日至2025年12月31日"
    assert response["clarified_query"] == "2025年9月到12月收益（2025年9月到12月指2025年9月1日至2025年12月31日）"


def test_query_pipeline_service_keeps_cross_year_bounded_range_as_one_unit_end_to_end() -> None:
    service = QueryPipelineService(
        stage_a_runner=_StaticStageARunner(),
        stage_b_runner=_StaticStageBRunner(),
        business_calendar=_calendar(),
        pipeline_logging_enabled=False,
    )

    response = service.process_query(
        query="去年12月到3月收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert len(response["clarification_plan"]["units"]) == 1
    assert response["clarification_items"][0]["resolved_text"] == "2025年12月1日至2026年3月31日"
    assert response["clarified_query"] == "去年12月到3月收益（去年12月到3月指2025年12月1日至2026年3月31日）"


def test_query_pipeline_service_keeps_cross_grain_bounded_range_as_one_unit_end_to_end() -> None:
    service = QueryPipelineService(
        stage_a_runner=_StaticStageARunner(),
        stage_b_runner=_StaticStageBRunner(),
        business_calendar=_calendar(),
        pipeline_logging_enabled=False,
    )

    response = service.process_query(
        query="2025年Q3到10月收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert len(response["clarification_plan"]["units"]) == 1
    assert response["clarification_items"][0]["resolved_text"] == "2025年7月1日至2025年10月31日"
    assert response["clarified_query"] == "2025年Q3到10月收益（2025年Q3到10月指2025年7月1日至2025年10月31日）"


def test_query_pipeline_service_keeps_grouped_bounded_range_parent_as_one_unit_end_to_end() -> None:
    service = QueryPipelineService(
        stage_a_runner=_StaticStageARunner(),
        stage_b_runner=_StaticStageBRunner(),
        business_calendar=_calendar(),
        pipeline_logging_enabled=False,
    )

    response = service.process_query(
        query="2025年1月到3月每个月的每个工作日收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert len(response["clarification_plan"]["units"]) == 1
    anchor = response["clarification_plan"]["units"][0]["content"]["carrier"]["anchor"]
    assert anchor["kind"] == "grouped_temporal_value"
    assert anchor["parent"]["kind"] == "mapped_range"
    assert len(response["clarification_items"]) == 1
    assert response["clarification_items"][0]["label"] == "2025年1月到3月每个月的每个工作日"
    assert response["clarification_items"][0]["grouping_grain"] == "month"
    assert response["clarified_query"] == (
        "2025年1月到3月每个月的每个工作日收益"
        "（2025年1月到3月每个月的每个工作日指2025年1月1日至2025年3月31日，按自然月分组）"
    )
