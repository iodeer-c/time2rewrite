from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Iterable

from pydantic import ValidationError

from time_query_service.config import load_business_calendar_event_aliases
from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.post_processor import StageBOutput
from time_query_service.stage_b_prompt import build_stage_b_messages


_CALENDAR_GRAIN_PATTERN = re.compile(r"最近\s*(?P<length>\d+)\s*个(?P<token>工作日|周末|节假日|补班日|休息日)")
_ZERO_DAY_CUTOFF_PATTERN = re.compile(r"0[天日]前")
_SUPPORTED_DAY_CLASS = {
    "工作日": "workday",
    "周末": "weekend",
    "节假日": "statutory_holiday",
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
        self._calendar_event_aliases = load_business_calendar_event_aliases(region="CN")
        self._canonical_calendar_event_keys = frozenset(self._calendar_event_aliases)

    def run_stage_b(
        self,
        *,
        unit_id: str,
        text: str,
        system_datetime: str,
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
                system_datetime=system_datetime,
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
                payload = _apply_semantic_payload_guards(payload, text=text)
                parsed = StageBOutput.model_validate(payload)
            except json.JSONDecodeError as exc:
                feedback = [f"Stage B JSON decode error: {exc}"]
                error_kind = "json_decode_error"
            except ValidationError as exc:
                feedback = [f"Stage B schema validation error: {exc}"]
                error_kind = "schema_validation_error"
            else:
                try:
                    _validate_calendar_event_keys(parsed, valid_keys=self._canonical_calendar_event_keys)
                    _validate_holiday_event_selectors(parsed)
                except ValueError as exc:
                    feedback = [f"Stage B semantic validation error: {exc}"]
                    error_kind = "semantic_validation_error"
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
        system_datetime: str,
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
                    system_datetime=system_datetime,
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
        system_datetime: str,
        timezone: str,
        previous_validation_errors: list[str] | None,
        surface_hint: str | None,
    ) -> str:
        response = self._text_runner.invoke(
            build_stage_b_messages(
                text=text,
                system_datetime=system_datetime,
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
    system_datetime: str,
    timezone: str,
    previous_validation_errors: list[str] | None = None,
    surface_hint: str | None = None,
    pipeline_logging_enabled: bool = False,
) -> StageBOutput:
    planner = StageBPlanner(text_runner=text_runner, pipeline_logging_enabled=pipeline_logging_enabled)
    return planner.run_stage_b(
        unit_id=unit_id,
        text=text,
        system_datetime=system_datetime,
        timezone=timezone,
        previous_validation_errors=previous_validation_errors,
        surface_hint=surface_hint,
    )


def run_stage_b_batch(
    *,
    text_runner: Any,
    requests: Iterable[StageBRequest],
    system_datetime: str,
    timezone: str,
    max_concurrent: int = 10,
    pipeline_logging_enabled: bool = False,
) -> list[StageBOutput]:
    planner = StageBPlanner(text_runner=text_runner, pipeline_logging_enabled=pipeline_logging_enabled)
    return planner.run_stage_b_batch(
        requests,
        system_datetime=system_datetime,
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


def _validate_calendar_event_keys(output: StageBOutput, *, valid_keys: frozenset[str]) -> None:
    if output.carrier is None:
        return
    for event_key in _iter_calendar_event_keys(output.carrier.anchor):
        if event_key not in valid_keys:
            raise ValueError(f"unsupported calendar event key {event_key!r}")


def _iter_calendar_event_keys(anchor: Any | None) -> list[str]:
    if anchor is None:
        return []

    kind = getattr(anchor, "kind", None)
    if kind == "calendar_event":
        return [anchor.event_key]
    if kind == "enumeration_set":
        keys: list[str] = []
        for member in anchor.members:
            keys.extend(_iter_calendar_event_keys(member))
        return keys
    if kind == "grouped_temporal_value":
        return _iter_calendar_event_keys(anchor.parent)
    if kind == "mapped_range":
        keys: list[str] = []
        keys.extend(_iter_calendar_event_keys(getattr(anchor, "start", None)))
        keys.extend(_iter_calendar_event_keys(getattr(anchor, "end", None)))
        keys.extend(_iter_calendar_event_keys(getattr(anchor, "anchor_ref", None)))
        keys.extend(_iter_calendar_event_keys(getattr(anchor, "endpoint_set", None)))
        return keys
    return []


def _validate_holiday_event_selectors(output: StageBOutput) -> None:
    if output.carrier is None:
        return
    anchor = output.carrier.anchor
    if getattr(anchor, "kind", None) != "holiday_event_collection":
        return
    if getattr(anchor.parent, "kind", None) != "relative_window" or anchor.parent.grain != "year":
        raise ValueError("holiday_event_collection only supports year-scoped relative_window parents")
    if anchor.scope != "consecutive_rest":
        raise ValueError("holiday_event_collection only supports scope='consecutive_rest'")
    if anchor.selector != "all":
        raise ValueError("holiday_event_collection selector must be 'all'")


def _apply_semantic_payload_guards(payload: dict[str, Any], *, text: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    carrier = payload.get("carrier")
    if not isinstance(carrier, dict):
        return payload
    anchor = carrier.get("anchor")
    if not isinstance(anchor, dict):
        return payload

    if anchor.get("kind") == "datetime_range":
        start = _parse_hour_aligned_datetime(anchor.get("start_datetime"))
        end = _parse_hour_aligned_datetime(anchor.get("end_datetime"))
        if start is not None and end is not None and start > end:
            return {"carrier": None, "needs_clarification": True, "reason_kind": "semantic_conflict"}

    if anchor.get("kind") == "mapped_range" and anchor.get("mode") == "bounded_pair":
        start_expr = anchor.get("start")
        end_expr = anchor.get("end")
        if not _raw_bounded_pair_endpoint_supported(start_expr) or not _raw_bounded_pair_endpoint_supported(end_expr):
            return {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}
        if _raw_shifted_day_cutoff_endpoint(start_expr):
            return {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}
        if _looks_like_zero_day_cutoff_phrase(text) and _raw_day_zero_endpoint(end_expr):
            return {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}
        start_precision = _raw_bounded_pair_precision(start_expr)
        end_precision = start_precision if _raw_current_time_endpoint(end_expr) else _raw_bounded_pair_precision(end_expr)
        if start_precision is not None and end_precision is not None and start_precision != end_precision:
            return {"carrier": None, "needs_clarification": True, "reason_kind": "unsupported_anchor_semantics"}
    return payload


def _parse_hour_aligned_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _raw_current_time_endpoint(expr: object) -> bool:
    if expr == "system_datetime":
        return True
    if not isinstance(expr, dict):
        return False
    return expr.get("kind") == "relative_window" and expr.get("grain") in {"day", "hour"} and expr.get("offset_units") == 0


def _raw_day_zero_endpoint(expr: object) -> bool:
    if not isinstance(expr, dict):
        return False
    return expr.get("kind") == "relative_window" and expr.get("grain") == "day" and expr.get("offset_units") == 0


def _raw_shifted_day_cutoff_endpoint(expr: object) -> bool:
    if not isinstance(expr, dict):
        return False
    return expr.get("kind") == "relative_window" and expr.get("grain") == "day" and int(expr.get("offset_units", 0)) < 0


def _looks_like_zero_day_cutoff_phrase(text: str) -> bool:
    return bool(_ZERO_DAY_CUTOFF_PATTERN.search(text))


def _raw_bounded_pair_precision(expr: object) -> str | None:
    if expr == "system_datetime":
        return "day"
    if not isinstance(expr, dict):
        return None
    kind = expr.get("kind")
    if kind == "datetime_range":
        return "hour"
    if kind == "relative_window":
        return "hour" if expr.get("grain") == "hour" else "day"
    if kind == "rolling_window":
        return "hour" if expr.get("unit") == "hour" else "day"
    if kind == "enumeration_set":
        member_precisions = {_raw_bounded_pair_precision(member) for member in expr.get("members", [])}
        member_precisions.discard(None)
        if len(member_precisions) == 1:
            return member_precisions.pop()
        return None
    return "day"


def _raw_bounded_pair_endpoint_supported(expr: object) -> bool:
    if expr == "system_datetime":
        return True
    if not isinstance(expr, dict):
        return False
    kind = expr.get("kind")
    if kind in {"named_period", "date_range", "datetime_range"}:
        return True
    if kind == "relative_window":
        offset_units = expr.get("offset_units")
        if not isinstance(offset_units, int):
            return False
        if expr.get("grain") == "day":
            return offset_units <= 0
        if expr.get("grain") == "hour":
            return offset_units == 0
        return False
    if kind == "enumeration_set":
        members = expr.get("members", [])
        return bool(members) and all(_raw_bounded_pair_endpoint_supported(member) for member in members)
    return False
