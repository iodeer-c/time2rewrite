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
        if query == "2025年3月收益":
            return _Response(
                json.dumps(
                    {
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
                    ensure_ascii=False,
                )
            )
        if query == "最近5个休息日收益":
            return _Response(
                json.dumps(
                    {
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
                    ensure_ascii=False,
                )
            )
        raise AssertionError(f"unexpected query {query}")


class _StaticStageBRunner:
    def invoke(self, messages):
        payload = json.loads(messages[-1].content)
        text = payload["text"]
        if text == "2025年3月":
            return _Response(
                json.dumps(
                    {
                        "carrier": {
                            "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                            "modifiers": [],
                        },
                        "needs_clarification": False,
                    },
                    ensure_ascii=False,
                )
            )
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
