from __future__ import annotations

import json
from typing import Any

from time_query_service.contracts import ClarificationPlan
from time_query_service.plan_validator import validate_plan
from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.planner_prompt import build_planner_messages


class ClarificationPlanner:
    def __init__(
        self,
        *,
        text_runner: Any | None = None,
        llm: Any | None = None,
        pipeline_logging_enabled: bool = False,
    ) -> None:
        self._text_runner = text_runner or llm
        self._pipeline_logging_enabled = pipeline_logging_enabled

    def _get_text_runner(self) -> Any:
        if self._text_runner is None:
            raise RuntimeError("ClarificationPlanner requires a text runner.")
        return self._text_runner

    def plan_query(
        self,
        *,
        original_query: str,
        system_date: str | None = None,
        system_datetime: str | None = None,
        timezone: str = "Asia/Shanghai",
    ) -> ClarificationPlan:
        previous_validation_errors: list[str] | None = None
        for attempt in range(1, 3):
            payload = self._plan_once(
                original_query=original_query,
                system_date=system_date,
                system_datetime=system_datetime,
                timezone=timezone,
                attempt=attempt,
                previous_validation_errors=previous_validation_errors,
            )
            validation = validate_plan(payload)
            if validation.is_valid and validation.plan is not None:
                log_pipeline_event(
                    "planner",
                    f"attempt={attempt}.validated_plan",
                    validation.plan.model_dump(mode="python"),
                    enabled=self._pipeline_logging_enabled,
                )
                return validation.plan
            log_pipeline_event(
                "planner",
                f"attempt={attempt}.validation_errors",
                validation.errors,
                enabled=self._pipeline_logging_enabled,
            )
            previous_validation_errors = validation.errors
        raise ValueError("Failed to build a valid ClarificationPlan after one retry.")

    def _plan_once(
        self,
        *,
        original_query: str,
        system_date: str | None,
        system_datetime: str | None,
        timezone: str,
        attempt: int,
        previous_validation_errors: list[str] | None = None,
    ) -> dict[str, Any]:
        request_payload = {
            "original_query": original_query,
            "system_date": system_date,
            "system_datetime": system_datetime,
            "timezone": timezone,
        }
        if previous_validation_errors:
            request_payload["previous_validation_errors"] = previous_validation_errors
        log_pipeline_event(
            "planner",
            f"attempt={attempt}.request_payload",
            request_payload,
            enabled=self._pipeline_logging_enabled,
        )
        response = self._get_text_runner().invoke(
            build_planner_messages(
                original_query=request_payload["original_query"],
                system_date=request_payload["system_date"],
                system_datetime=request_payload["system_datetime"],
                timezone=request_payload["timezone"],
                previous_validation_errors=request_payload.get("previous_validation_errors"),
            )
        )
        raw_content = response.content if hasattr(response, "content") else response
        log_pipeline_event(
            "planner",
            f"attempt={attempt}.raw_response",
            raw_content,
            enabled=self._pipeline_logging_enabled,
        )
        if not isinstance(raw_content, str):
            raise ValueError("Planner runner must return a string JSON payload.")
        payload = json.loads(raw_content)
        if not isinstance(payload, dict):
            raise ValueError("Planner JSON payload must be an object.")
        return payload
