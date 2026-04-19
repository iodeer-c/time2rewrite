from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Iterable

from pydantic import ValidationError

from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.post_processor import StageBOutput
from time_query_service.stage_b_prompt import build_stage_b_messages


_CALENDAR_GRAIN_PATTERN = re.compile(r"最近\s*(?P<length>\d+)\s*个(?P<token>工作日|周末|节假日|补班日|休息日)")
_SUPPORTED_DAY_CLASS = {
    "工作日": "workday",
    "周末": "weekend",
    "节假日": "holiday",
    "补班日": "makeup_workday",
}


@dataclass(frozen=True)
class StageBRequest:
    unit_id: str
    text: str
    surface_hint: str | None = None


class StageBPlanner:
    def __init__(self, *, text_runner: Any, pipeline_logging_enabled: bool = False) -> None:
        self._text_runner = text_runner
        self._pipeline_logging_enabled = pipeline_logging_enabled

    def run_stage_b(
        self,
        *,
        unit_id: str,
        text: str,
        system_date: str,
        timezone: str,
        previous_validation_errors: list[str] | None = None,
        surface_hint: str | None = None,
    ) -> StageBOutput:
        immediate = _maybe_degrade_calendar_grain_rolling(text=text, surface_hint=surface_hint)
        if immediate is not None:
            return immediate

        feedback = list(previous_validation_errors or [])
        previous_raw: str | None = None
        for attempt in range(1, 3):
            started = perf_counter()
            raw_content = self._invoke_once(
                text=text,
                system_date=system_date,
                timezone=timezone,
                previous_validation_errors=feedback or None,
                surface_hint=surface_hint,
            )
            duration_ms = int((perf_counter() - started) * 1000)
            if previous_raw is not None and raw_content == previous_raw:
                log_pipeline_event(
                    "stage_b",
                    "stage_b_attempt",
                    {
                        "unit_id": unit_id,
                        "attempt_no": attempt,
                        "duration_ms": duration_ms,
                        "success": False,
                        "error_kind": "identical_invalid_output",
                    },
                    enabled=self._pipeline_logging_enabled,
                )
                return StageBOutput(carrier=None, needs_clarification=True, reason_kind="llm_hard_fail")
            previous_raw = raw_content
            try:
                payload = json.loads(raw_content)
                parsed = StageBOutput.model_validate(payload)
            except json.JSONDecodeError as exc:
                feedback = [f"Stage B JSON decode error: {exc}"]
                error_kind = "json_decode_error"
            except ValidationError as exc:
                feedback = [f"Stage B schema validation error: {exc}"]
                error_kind = "schema_validation_error"
            else:
                log_pipeline_event(
                    "stage_b",
                    "stage_b_attempt",
                    {
                        "unit_id": unit_id,
                        "attempt_no": attempt,
                        "duration_ms": duration_ms,
                        "success": True,
                        "error_kind": None,
                    },
                    enabled=self._pipeline_logging_enabled,
                )
                return parsed

            log_pipeline_event(
                "stage_b",
                "stage_b_attempt",
                {
                    "unit_id": unit_id,
                    "attempt_no": attempt,
                    "duration_ms": duration_ms,
                    "success": False,
                    "error_kind": error_kind,
                },
                enabled=self._pipeline_logging_enabled,
            )
        return StageBOutput(carrier=None, needs_clarification=True, reason_kind="llm_hard_fail")

    def run_stage_b_batch(
        self,
        requests: Iterable[StageBRequest],
        *,
        system_date: str,
        timezone: str,
        max_concurrent: int = 10,
    ) -> list[StageBOutput]:
        request_list = list(requests)
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [
                executor.submit(
                    self.run_stage_b,
                    unit_id=request.unit_id,
                    text=request.text,
                    system_date=system_date,
                    timezone=timezone,
                    surface_hint=request.surface_hint,
                )
                for request in request_list
            ]
            return [future.result() for future in futures]

    def _invoke_once(
        self,
        *,
        text: str,
        system_date: str,
        timezone: str,
        previous_validation_errors: list[str] | None,
        surface_hint: str | None,
    ) -> str:
        response = self._text_runner.invoke(
            build_stage_b_messages(
                text=text,
                system_date=system_date,
                timezone=timezone,
                previous_validation_errors=previous_validation_errors,
                surface_hint=surface_hint,
            )
        )
        raw_content = response.content if hasattr(response, "content") else response
        if not isinstance(raw_content, str):
            raise ValueError("Stage B runner must return a string JSON payload")
        return raw_content


def run_stage_b(
    *,
    text_runner: Any,
    unit_id: str,
    text: str,
    system_date: str,
    timezone: str,
    previous_validation_errors: list[str] | None = None,
    surface_hint: str | None = None,
    pipeline_logging_enabled: bool = False,
) -> StageBOutput:
    planner = StageBPlanner(text_runner=text_runner, pipeline_logging_enabled=pipeline_logging_enabled)
    return planner.run_stage_b(
        unit_id=unit_id,
        text=text,
        system_date=system_date,
        timezone=timezone,
        previous_validation_errors=previous_validation_errors,
        surface_hint=surface_hint,
    )


def run_stage_b_batch(
    *,
    text_runner: Any,
    requests: Iterable[StageBRequest],
    system_date: str,
    timezone: str,
    max_concurrent: int = 10,
    pipeline_logging_enabled: bool = False,
) -> list[StageBOutput]:
    planner = StageBPlanner(text_runner=text_runner, pipeline_logging_enabled=pipeline_logging_enabled)
    return planner.run_stage_b_batch(
        requests,
        system_date=system_date,
        timezone=timezone,
        max_concurrent=max_concurrent,
    )


def _maybe_degrade_calendar_grain_rolling(*, text: str, surface_hint: str | None) -> StageBOutput | None:
    if surface_hint != "calendar_grain_rolling":
        return None
    match = _CALENDAR_GRAIN_PATTERN.fullmatch(text.strip())
    if match is None:
        return None
    token = match.group("token")
    if token in _SUPPORTED_DAY_CLASS:
        return None
    return StageBOutput(carrier=None, needs_clarification=True, reason_kind="unsupported_calendar_grain_rolling")
