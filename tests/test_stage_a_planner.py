from __future__ import annotations

import json
from dataclasses import dataclass

from time_query_service.stage_a_prompt import build_stage_a_messages
from time_query_service.stage_a_planner import run_stage_a


@dataclass
class _Response:
    content: str


class _FakeRunner:
    def __init__(self, responses: list[str]) -> None:
        self._responses = list(responses)
        self.calls: list[list[object]] = []

    def invoke(self, messages):
        self.calls.append(messages)
        return _Response(self._responses.pop(0))


def test_run_stage_a_retries_with_previous_validation_errors() -> None:
    runner = _FakeRunner(
        responses=[
            "{bad-json}",
            json.dumps(
                {
                    "query": "2025年3月",
                    "system_date": "2026-04-17",
                    "timezone": "Asia/Shanghai",
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
            ),
        ]
    )

    result = run_stage_a(
        text_runner=runner,
        query="2025年3月",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    assert result.units[0].render_text == "2025年3月"
    retry_payload = json.loads(runner.calls[1][-1].content)
    assert "previous_validation_errors" in retry_payload
    assert "JSON decode error" in retry_payload["previous_validation_errors"][0]


def test_run_stage_a_short_circuits_identical_invalid_output() -> None:
    runner = _FakeRunner(responses=["{}", "{}"])

    try:
        run_stage_a(
            text_runner=runner,
            query="2025年3月",
            system_date="2026-04-17",
            timezone="Asia/Shanghai",
        )
    except ValueError as exc:
        assert "schema validation" in str(exc) or "failed" in str(exc).lower()
    else:
        raise AssertionError("expected Stage A failure")

    assert len(runner.calls) == 2


def test_stage_a_prompt_keeps_surface_text_for_shared_prefix_enumeration() -> None:
    messages = build_stage_a_messages(
        query="2025年3月和5月收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    shared_prefix_response = json.loads(messages[4].content)

    assert shared_prefix_response["query"] == "2025年3月和5月的收益"
    assert shared_prefix_response["units"][0]["render_text"] == "2025年3月"
    assert shared_prefix_response["units"][1]["render_text"] == "5月"
    assert shared_prefix_response["units"][1]["surface_fragments"] == [{"start": 8, "end": 10}]
    assert shared_prefix_response["units"][1]["self_contained_text"] == "2025年5月"


def test_stage_a_prompt_keeps_surface_text_and_offsets_for_relative_shared_prefix_derivation() -> None:
    messages = build_stage_a_messages(
        query="今年3月和5月，去年同期",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    relative_shared_prefix_response = json.loads(messages[6].content)

    assert relative_shared_prefix_response["query"] == "今年3月和5月，去年同期"
    assert relative_shared_prefix_response["units"][1]["render_text"] == "5月"
    assert relative_shared_prefix_response["units"][1]["surface_fragments"] == [{"start": 5, "end": 7}]
    assert relative_shared_prefix_response["units"][1]["self_contained_text"] == "今年5月"
    assert relative_shared_prefix_response["units"][2]["render_text"] == "去年同期"
    assert relative_shared_prefix_response["units"][2]["surface_fragments"] == [{"start": 8, "end": 12}]


def test_stage_a_prompt_limits_calendar_grain_surface_hint_to_counted_day_classes() -> None:
    messages = build_stage_a_messages(
        query="最近一个月收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    examples = [
        json.loads(message.content)
        for index, message in enumerate(messages)
        if index > 0 and index % 2 == 0
    ]
    by_query = {example["query"]: example for example in examples}

    assert by_query["最近一个月收益"]["units"][0].get("surface_hint") is None
    assert by_query["最近7天收益"]["units"][0].get("surface_hint") is None
    assert by_query["最近1个补班日收益"]["units"][0]["surface_hint"] == "calendar_grain_rolling"


def test_stage_a_prompt_has_grouped_temporal_example_with_full_surface_coverage() -> None:
    messages = build_stage_a_messages(
        query="2025年每个季度收益",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    examples = [
        json.loads(message.content)
        for index, message in enumerate(messages)
        if index > 0 and index % 2 == 0
    ]
    by_query = {example["query"]: example for example in examples}

    grouped_example = by_query["2025年每个季度收益"]

    assert grouped_example["units"][0]["render_text"] == "2025年每个季度"
    assert grouped_example["units"][0]["surface_fragments"] == [{"start": 0, "end": 9}]


def test_stage_a_prompt_has_no_implicit_anchor_example_for_standalone_relative_phrase() -> None:
    messages = build_stage_a_messages(
        query="去年同期员工数有多少",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    examples = [
        json.loads(message.content)
        for index, message in enumerate(messages)
        if index > 0 and index % 2 == 0
    ]
    by_query = {example["query"]: example for example in examples}

    standalone_relative = by_query["去年同期员工数有多少"]

    assert standalone_relative["units"][0]["render_text"] == "去年同期"
    assert standalone_relative["units"][0]["content_kind"] == "standalone"
    assert standalone_relative["units"][0]["self_contained_text"] == "去年同期"
    assert standalone_relative["units"][0]["sources"] == []
