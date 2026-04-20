from __future__ import annotations

import json
from time import perf_counter
from typing import Any

from pydantic import ValidationError

from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.post_processor import StageAOutput
from time_query_service.stage_a_prompt import build_stage_a_messages


class StageAPlanner:
    def __init__(self, *, text_runner: Any, pipeline_logging_enabled: bool = False) -> None:
        self._text_runner = text_runner
        self._pipeline_logging_enabled = pipeline_logging_enabled

    def run_stage_a(
        self,
        *,
        query: str,
        system_datetime: str,
        timezone: str,
        previous_validation_errors: list[str] | None = None,
    ) -> StageAOutput:
        feedback = list(previous_validation_errors or [])
        previous_raw: str | None = None
        last_error: str | None = None
        for attempt in range(1, 3):
            started = perf_counter()
            raw_content = self._invoke_once(
                query=query,
                system_datetime=system_datetime,
                timezone=timezone,
                previous_validation_errors=feedback or None,
            )
            duration_ms = int((perf_counter() - started) * 1000)
            if previous_raw is not None and raw_content == previous_raw:
                details = "identical_invalid_output"
                log_pipeline_event(
                    "stage_a",
                    "stage_a_attempt",
                    {
                        "attempt_no": attempt,
                        "duration_ms": duration_ms,
                        "success": False,
                        "error_kind": details,
                    },
                    enabled=self._pipeline_logging_enabled,
                )
                break
            previous_raw = raw_content
            try:
                payload = json.loads(raw_content)
                parsed = StageAOutput.model_validate(payload)
            except json.JSONDecodeError as exc:
                last_error = f"Stage A JSON decode error: {exc}"
                feedback = [last_error]
                error_kind = "json_decode_error"
            except ValidationError as exc:
                last_error = f"Stage A schema validation error: {exc}"
                feedback = [last_error]
                error_kind = "schema_validation_error"
            else:
                log_pipeline_event(
                    "stage_a",
                    "stage_a_attempt",
                    {
                        "attempt_no": attempt,
                        "duration_ms": duration_ms,
                        "success": True,
                        "error_kind": None,
                    },
                    enabled=self._pipeline_logging_enabled,
                )
                return parsed

            log_pipeline_event(
                "stage_a",
                "stage_a_attempt",
                {
                    "attempt_no": attempt,
                    "duration_ms": duration_ms,
                    "success": False,
                    "error_kind": error_kind,
                },
                enabled=self._pipeline_logging_enabled,
            )
        raise ValueError(last_error or "Stage A failed after retry budget exhausted")

    def _invoke_once(
        self,
        *,
        query: str,
        system_datetime: str,
        timezone: str,
        previous_validation_errors: list[str] | None,
    ) -> str:
        response = self._text_runner.invoke(
            build_stage_a_messages(
                query=query,
                system_datetime=system_datetime,
                timezone=timezone,
                previous_validation_errors=previous_validation_errors,
            )
        )
        raw_content = response.content if hasattr(response, "content") else response
        if not isinstance(raw_content, str):
            raise ValueError("Stage A runner must return a string JSON payload")
        return raw_content


def run_stage_a(
    *,
    text_runner: Any,
    query: str,
    system_datetime: str,
    timezone: str,
    previous_validation_errors: list[str] | None = None,
    pipeline_logging_enabled: bool = False,
) -> StageAOutput:
    planner = StageAPlanner(text_runner=text_runner, pipeline_logging_enabled=pipeline_logging_enabled)
    return planner.run_stage_a(
        query=query,
        system_datetime=system_datetime,
        timezone=timezone,
        previous_validation_errors=previous_validation_errors,
    )
