from __future__ import annotations

import logging
from datetime import datetime
from datetime import timezone as _utc_tz
from pathlib import Path
from typing import Any, Callable, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.clarification_artifacts import build_clarification_artifacts
from time_query_service.clarification_writer import render_clarified_query_from_artifacts
from time_query_service.llm import LLMFactory, LLMRuntimeConfig, load_llm_runtime_config
from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.post_processor import StageAOutput, assemble_time_plan
from time_query_service.stage_a_planner import run_stage_a
from time_query_service.stage_b_planner import StageBRequest, run_stage_b_batch
from time_query_service.new_resolver import resolve_plan
from time_query_service.time_plan import TimePlan


logger = logging.getLogger(__name__)
NoTimePostProcessor = Callable[[datetime], str | None]
HookOutcome = Literal[
    "success",
    "noop",
    "error",
    "invalid_return",
    "not_triggered",
    "unreachable_no_rewrite",
    "unreachable_stage_a_failed",
]


class QueryPipelineService:
    def __init__(
        self,
        *,
        stage_a_runner: Any | None = None,
        stage_b_runner: Any | None = None,
        rewriter_runner: Any | None = None,
        business_calendar: BusinessCalendarPort | None = None,
        llm_runtime_config: LLMRuntimeConfig | None = None,
        llm_config_path: Path | None = None,
        pipeline_logging_enabled: bool | None = None,
        max_stage_b_concurrent: int = 10,
    ) -> None:
        self._stage_a_runner = stage_a_runner
        self._stage_b_runner = stage_b_runner
        self._rewriter_runner = rewriter_runner
        self._business_calendar = business_calendar
        self._llm_runtime_config = llm_runtime_config
        self._llm_config_path = llm_config_path
        self._pipeline_logging_enabled = pipeline_logging_enabled
        self._max_stage_b_concurrent = max_stage_b_concurrent

    def _get_llm_runtime_config(self) -> LLMRuntimeConfig:
        if self._llm_runtime_config is None:
            self._llm_runtime_config = load_llm_runtime_config(config_path=self._llm_config_path)
        return self._llm_runtime_config

    def _create_optional_role_llm(self, *roles: str) -> Any | None:
        try:
            runtime_config = self._get_llm_runtime_config()
        except RuntimeError:
            return None
        for role in roles:
            config = runtime_config.roles.get(role)
            if config is None:
                continue
            return LLMFactory.create_llm(config)
        return None

    @property
    def stage_a_runner(self) -> Any:
        if self._stage_a_runner is None:
            self._stage_a_runner = self._create_optional_role_llm("stage_a", "planner")
        if self._stage_a_runner is None:
            raise RuntimeError("Stage A runner is not configured")
        return self._stage_a_runner

    @property
    def stage_b_runner(self) -> Any:
        if self._stage_b_runner is None:
            self._stage_b_runner = self._create_optional_role_llm("stage_b", "planner")
        if self._stage_b_runner is None:
            raise RuntimeError("Stage B runner is not configured")
        return self._stage_b_runner

    @property
    def rewriter_runner(self) -> Any | None:
        if self._rewriter_runner is None:
            self._rewriter_runner = self._create_optional_role_llm("rewriter", "annotator", "fallback")
        return self._rewriter_runner

    def _is_pipeline_logging_enabled(self) -> bool:
        if self._pipeline_logging_enabled is not None:
            return self._pipeline_logging_enabled
        try:
            return self._get_llm_runtime_config().pipeline_logging.enabled
        except RuntimeError:
            return False

    def process_query(
        self,
        *,
        query: str,
        system_datetime: str | None = None,
        timezone: str = "Asia/Shanghai",
        rewrite: bool = False,
        default_rolling_endpoint: Literal["today", "yesterday"] = "today",
        no_time_post_processor: NoTimePostProcessor | None = None,
    ) -> dict[str, Any]:
        if self._business_calendar is None:
            raise ValueError("business_calendar is required for the new pipeline")
        if system_datetime is None:
            raise ValueError("system_datetime is required for the new pipeline")
        request_local_datetime = bind_request_local_datetime(system_datetime, timezone)
        request_local_datetime_iso = request_local_datetime.isoformat()

        pipeline_logging_enabled = self._is_pipeline_logging_enabled()
        log_pipeline_event(
            "service",
            "request",
            {
                "query": query,
                "system_datetime": system_datetime,
                "timezone": timezone,
                "rewrite": rewrite,
                "default_rolling_endpoint": default_rolling_endpoint,
            },
            enabled=pipeline_logging_enabled,
        )

        try:
            stage_a = run_stage_a(
                text_runner=self.stage_a_runner,
                query=query,
                system_datetime=system_datetime,
                timezone=timezone,
                pipeline_logging_enabled=pipeline_logging_enabled,
            )
        except ValueError as exc:
            if not rewrite:
                raise
            response = {
                "original_query": query,
                "clarification_plan": None,
                "clarification_items": [],
                "clarified_query": None,
                "rewritten_query": None,
                "has_time": False,
                "request_local_datetime": request_local_datetime_iso,
            }
            log_pipeline_event(
                "service",
                "response",
                {**response, "hook_outcome": "unreachable_stage_a_failed"},
                enabled=pipeline_logging_enabled,
            )
            return response

        stage_b_requests = self._stage_b_requests(stage_a)
        stage_b_outputs = run_stage_b_batch(
            text_runner=self.stage_b_runner,
            requests=stage_b_requests,
            system_datetime=system_datetime,
            timezone=timezone,
            max_concurrent=self._max_stage_b_concurrent,
            pipeline_logging_enabled=pipeline_logging_enabled,
        )
        stage_b_by_unit = {
            request.unit_id: output
            for request, output in zip(stage_b_requests, stage_b_outputs, strict=True)
        }

        time_plan = assemble_time_plan(
            stage_a,
            stage_b_by_unit,
            default_rolling_endpoint=default_rolling_endpoint,
        )
        resolved_plan = resolve_plan(
            time_plan,
            business_calendar=self._business_calendar,
            pipeline_logging_enabled=pipeline_logging_enabled,
        )

        clarification_artifacts = build_clarification_artifacts(
            original_query=query,
            time_plan=time_plan,
            resolved_plan=resolved_plan,
        )
        clarification_items = [artifact.fact for artifact in clarification_artifacts]
        log_pipeline_event(
            "service",
            "clarification_facts",
            [item.model_dump(mode="python") for item in clarification_items],
            enabled=pipeline_logging_enabled,
        )
        has_time = derive_has_time(time_plan)
        if rewrite:
            clarified_query = render_clarified_query_from_artifacts(
                original_query=query,
                clarification_artifacts=clarification_artifacts,
                text_runner=self.rewriter_runner,
            )
            rewritten_query = clarified_query
            hook_outcome: HookOutcome = "not_triggered"
            if not has_time and no_time_post_processor is not None:
                rewritten_query, hook_outcome = _apply_no_time_post_processor(
                    clarified_query=clarified_query,
                    request_local_datetime=request_local_datetime,
                    no_time_post_processor=no_time_post_processor,
                )
        else:
            clarified_query = None
            rewritten_query = None
            hook_outcome = "unreachable_no_rewrite"

        response = {
            "original_query": query,
            "clarification_plan": time_plan.model_dump(mode="python"),
            "clarification_items": [item.model_dump(mode="python") for item in clarification_items],
            "clarified_query": clarified_query,
            "rewritten_query": rewritten_query,
            "has_time": has_time,
            "request_local_datetime": request_local_datetime_iso,
        }
        log_pipeline_event(
            "service",
            "response",
            {**response, "hook_outcome": hook_outcome},
            enabled=pipeline_logging_enabled,
        )
        return response

    @staticmethod
    def _stage_b_requests(stage_a: StageAOutput) -> list[StageBRequest]:
        requests: list[StageBRequest] = []
        for index, unit in enumerate(stage_a.units):
            if unit.content_kind != "standalone":
                continue
            requests.append(
                StageBRequest(
                    unit_id=unit.unit_id or f"__index_{index}__",
                    text=unit.self_contained_text or unit.render_text,
                    surface_hint=unit.surface_hint,
                )
            )
        return requests


def _parse_system_datetime_input(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
    except ValueError as exc:
        raise ValueError("system_datetime must use YYYY-MM-DDTHH:MM:SS") from exc


def bind_request_local_datetime(system_datetime: str, timezone: str) -> datetime:
    naive = _parse_system_datetime_input(system_datetime)

    tz_name = timezone.strip() if isinstance(timezone, str) else ""
    if not tz_name:
        raise ValueError(f"timezone must be a non-empty string: {timezone!r}")
    try:
        tz = ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown timezone: {timezone!r}") from exc

    aware = naive.replace(tzinfo=tz)
    if aware != aware.astimezone(_utc_tz.utc).astimezone(tz):
        raise ValueError(
            f"system_datetime {system_datetime!r} does not exist in timezone "
            f"{tz_name!r} (local-time gap, e.g. DST spring-forward)"
        )
    if aware.replace(fold=0).utcoffset() != aware.replace(fold=1).utcoffset():
        raise ValueError(
            f"system_datetime {system_datetime!r} is ambiguous in timezone "
            f"{tz_name!r} (local-time overlap, e.g. DST fall-back, two possible instants)"
        )
    return aware


def derive_has_time(time_plan: TimePlan) -> bool:
    return len(time_plan.units) > 0


def _apply_no_time_post_processor(
    *,
    clarified_query: str,
    request_local_datetime: datetime,
    no_time_post_processor: NoTimePostProcessor,
) -> tuple[str, HookOutcome]:
    try:
        hook_return = no_time_post_processor(request_local_datetime)
    except Exception as exc:  # noqa: BLE001 - hook failures must degrade to noop
        logger.warning("no_time_post_processor raised %s: %s", type(exc).__name__, exc)
        return clarified_query, "error"

    if hook_return is None:
        return clarified_query, "noop"
    if not isinstance(hook_return, str):
        logger.warning(
            "no_time_post_processor returned non-str: got_type=%s",
            type(hook_return).__name__,
        )
        return clarified_query, "invalid_return"

    text = hook_return.strip()
    if not text:
        logger.debug("no_time_post_processor returned blank text")
        return clarified_query, "noop"
    return f"{clarified_query}（{text}）", "success"
