from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass

from time_query_service.stage_b_prompt import build_stage_b_messages
from time_query_service.stage_b_planner import StageBRequest, run_stage_b, run_stage_b_batch


@dataclass
class _Response:
    content: str


class _SequentialRunner:
    def __init__(self, responses: list[str]) -> None:
        self._responses = list(responses)
        self.calls: list[list[object]] = []

    def invoke(self, messages):
        self.calls.append(messages)
        return _Response(self._responses.pop(0))


class _ConcurrentRunner:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.in_flight = 0
        self.max_in_flight = 0

    def invoke(self, messages):
        with self.lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            payload = json.loads(messages[-1].content)
            time.sleep(0.05)
            return _Response(
                json.dumps(
                    {
                        "carrier": {
                            "anchor": {
                                "kind": "named_period",
                                "period_type": "month",
                                "year": 2025,
                                "month": 3 if payload["text"] == "A" else 5,
                            },
                            "modifiers": [],
                        },
                        "needs_clarification": False,
                    }
                )
            )
        finally:
            with self.lock:
                self.in_flight -= 1


def _few_shot_examples_by_text(messages) -> dict[str, dict]:
    examples: dict[str, dict] = {}
    for index in range(1, len(messages) - 1, 2):
        request = json.loads(messages[index].content)
        response = json.loads(messages[index + 1].content)
        examples[request["text"]] = response
    return examples


def test_run_stage_b_retries_and_injects_previous_validation_errors() -> None:
    runner = _SequentialRunner(
        responses=[
            "{bad-json}",
            json.dumps(
                {
                    "carrier": {
                        "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                        "modifiers": [],
                    },
                    "needs_clarification": False,
                }
            ),
        ]
    )

    result = run_stage_b(
        text_runner=runner,
        unit_id="u1",
        text="2025年3月",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    assert result.carrier is not None
    retry_payload = json.loads(runner.calls[1][-1].content)
    assert "previous_validation_errors" in retry_payload
    assert "JSON decode error" in retry_payload["previous_validation_errors"][0]


def test_run_stage_b_degrades_on_identical_invalid_output() -> None:
    runner = _SequentialRunner(responses=["{}", "{}"])

    result = run_stage_b(
        text_runner=runner,
        unit_id="u1",
        text="2025年3月",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    assert result.needs_clarification is True
    assert result.reason_kind == "llm_hard_fail"


def test_run_stage_b_calendar_grain_rolling_unsupported_token_degrades_without_runner_call() -> None:
    runner = _SequentialRunner(responses=[])

    result = run_stage_b(
        text_runner=runner,
        unit_id="u1",
        text="最近5个休息日",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        surface_hint="calendar_grain_rolling",
    )

    assert result.needs_clarification is True
    assert result.reason_kind == "unsupported_calendar_grain_rolling"
    assert runner.calls == []


def test_run_stage_b_batch_runs_requests_concurrently() -> None:
    runner = _ConcurrentRunner()

    results = run_stage_b_batch(
        text_runner=runner,
        requests=[
            StageBRequest(unit_id="u1", text="A"),
            StageBRequest(unit_id="u2", text="B"),
            StageBRequest(unit_id="u3", text="A"),
        ],
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        max_concurrent=3,
    )

    assert len(results) == 3
    assert runner.max_in_flight >= 2


def test_stage_b_prompt_uses_supported_calendar_event_schedule_year_ref_shape() -> None:
    messages = build_stage_b_messages(
        text="清明假期",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    by_text = _few_shot_examples_by_text(messages)
    qingming = by_text["清明假期"]

    assert qingming["carrier"]["anchor"]["schedule_year_ref"] == {"year": 2026}


def test_stage_b_prompt_distinguishes_calendar_count_rolling_from_regular_rolling() -> None:
    messages = build_stage_b_messages(
        text="最近1个周末",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
        surface_hint="calendar_grain_rolling",
    )

    by_text = _few_shot_examples_by_text(messages)

    assert by_text["最近1个周末"]["carrier"]["anchor"]["kind"] == "rolling_by_calendar_unit"
    assert by_text["最近1个周末"]["carrier"]["anchor"]["day_class"] == "weekend"
    assert by_text["最近一个月"]["carrier"]["anchor"]["kind"] == "rolling_window"


def test_stage_b_prompt_covers_todate_offset_and_day_expansion_shapes() -> None:
    messages = build_stage_b_messages(
        text="本月至今",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    by_text = _few_shot_examples_by_text(messages)

    assert by_text["本月至今"]["carrier"]["anchor"]["kind"] == "mapped_range"
    assert by_text["2025年3月往后一个月"]["carrier"]["modifiers"] == [{"kind": "offset", "value": 1, "unit": "month"}]
    assert by_text["2025年每天"]["carrier"]["anchor"]["kind"] == "named_period"
    assert by_text["2025年每天"]["carrier"]["modifiers"] == [{"kind": "grain_expansion", "target_grain": "day"}]


def test_stage_b_prompt_covers_bounded_range_canonical_shapes() -> None:
    messages = build_stage_b_messages(
        text="2025年9月到12月",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    by_text = _few_shot_examples_by_text(messages)

    assert by_text["2025年9月到12月"]["carrier"]["anchor"]["kind"] == "mapped_range"
    assert by_text["2025年9月到12月"]["carrier"]["anchor"]["mode"] == "bounded_pair"
    assert by_text["2025年3月1日到3月10日"]["carrier"]["anchor"]["kind"] == "date_range"


def test_stage_b_prompt_covers_cross_year_and_cross_grain_bounded_ranges() -> None:
    messages = build_stage_b_messages(
        text="去年12月到3月",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    by_text = _few_shot_examples_by_text(messages)

    cross_year = by_text["去年12月到3月"]["carrier"]["anchor"]
    assert cross_year["kind"] == "mapped_range"
    assert cross_year["start"]["year"] == 2025
    assert cross_year["end"]["year"] == 2026

    cross_grain = by_text["2025年Q3到10月"]["carrier"]["anchor"]
    assert cross_grain["kind"] == "mapped_range"
    assert cross_grain["start"]["period_type"] == "quarter"
    assert cross_grain["end"]["period_type"] == "month"

    month_to_day = by_text["2025年9月到10月15日"]["carrier"]["anchor"]
    assert month_to_day["kind"] == "mapped_range"
    assert month_to_day["start"]["period_type"] == "month"
    assert month_to_day["end"]["period_type"] == "day"


def test_stage_b_prompt_keeps_grouped_bounded_range_on_one_parent() -> None:
    messages = build_stage_b_messages(
        text="2025年1月到3月每个月的每个工作日",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    by_text = _few_shot_examples_by_text(messages)

    grouped = by_text["2025年1月到3月每个月的每个工作日"]["carrier"]
    assert grouped["anchor"]["kind"] == "grouped_temporal_value"
    assert grouped["anchor"]["parent"]["kind"] == "mapped_range"
    assert grouped["anchor"]["parent"]["mode"] == "bounded_pair"
    assert grouped["modifiers"] == [{"kind": "calendar_filter", "day_class": "workday"}]


def test_stage_b_prompt_degrades_unsupported_bounded_range_endpoint_classes() -> None:
    messages = build_stage_b_messages(
        text="去年9月到国庆假期",
        system_date="2026-04-17",
        timezone="Asia/Shanghai",
    )

    by_text = _few_shot_examples_by_text(messages)

    assert by_text["去年9月到国庆假期"]["needs_clarification"] is True
    assert by_text["去年9月到国庆假期"]["reason_kind"] == "unsupported_anchor_semantics"
    assert by_text["最近一周到上周五"]["needs_clarification"] is True
    assert by_text["最近一周到上周五"]["reason_kind"] == "unsupported_anchor_semantics"
