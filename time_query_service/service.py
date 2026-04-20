from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.clarification_writer import build_clarification_facts, render_clarified_query
from time_query_service.llm import LLMFactory, LLMRuntimeConfig, load_llm_runtime_config
from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.post_processor import StageAOutput, assemble_time_plan
from time_query_service.stage_a_planner import run_stage_a
from time_query_service.stage_b_planner import StageBRequest, run_stage_b_batch
from time_query_service.new_resolver import resolve_plan


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
    ) -> dict[str, Any]:
        if self._business_calendar is None:
            raise ValueError("business_calendar is required for the new pipeline")
        if system_datetime is None:
            raise ValueError("system_datetime is required for the new pipeline")
        _parse_system_datetime_input(system_datetime)

        pipeline_logging_enabled = self._is_pipeline_logging_enabled()
        log_pipeline_event(
            "service",
            "request",
            {
                "query": query,
                "system_datetime": system_datetime,
                "timezone": timezone,
                "rewrite": rewrite,
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
            }
            log_pipeline_event("service", "response", response, enabled=pipeline_logging_enabled)
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

        time_plan = assemble_time_plan(stage_a, stage_b_by_unit)
        resolved_plan = resolve_plan(
            time_plan,
            business_calendar=self._business_calendar,
            pipeline_logging_enabled=pipeline_logging_enabled,
        )

        clarification_items = build_clarification_facts(
            original_query=query,
            time_plan=time_plan,
            resolved_plan=resolved_plan,
        )
        log_pipeline_event(
            "service",
            "clarification_facts",
            [item.model_dump(mode="python") for item in clarification_items],
            enabled=pipeline_logging_enabled,
        )
        if rewrite:
            clarified_query = render_clarified_query(
                original_query=query,
                clarification_facts=clarification_items,
                text_runner=self.rewriter_runner,
            )
            rewritten_query = clarified_query
        else:
            clarified_query = None
            rewritten_query = None

        response = {
            "original_query": query,
            "clarification_plan": time_plan.model_dump(mode="python"),
            "clarification_items": [item.model_dump(mode="python") for item in clarification_items],
            "clarified_query": clarified_query,
            "rewritten_query": rewritten_query,
        }
        log_pipeline_event("service", "response", response, enabled=pipeline_logging_enabled)
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
