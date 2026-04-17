from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from time_query_service.annotation import AppendOnlyAnnotationRenderer
from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.llm import LLMFactory, LLMRuntimeConfig, load_llm_runtime_config
from time_query_service.materialized_rewrite import render_materialized_query
from time_query_service.plan_semantic_normalizer import NormalizationError, normalize_plan
from time_query_service.pipeline_logging import log_pipeline_event
from time_query_service.planner import ClarificationPlanner
from time_query_service.time_resolver import resolve_materialization_context, resolve_plan


ResolverCallable = Callable[..., Any]


class QueryPipelineService:
    def __init__(
        self,
        *,
        planner: Any | None = None,
        resolver: ResolverCallable | None = None,
        annotator: Any | None = None,
        business_calendar: BusinessCalendarPort | None = None,
        llm_runtime_config: LLMRuntimeConfig | None = None,
        llm_config_path: Path | None = None,
        pipeline_logging_enabled: bool | None = None,
    ) -> None:
        self._planner = planner
        self._resolver = resolver
        self._annotator = annotator
        self._business_calendar = business_calendar
        self._llm_runtime_config = llm_runtime_config
        self._llm_config_path = llm_config_path
        self._pipeline_logging_enabled = pipeline_logging_enabled

    @property
    def planner(self) -> Any:
        if self._planner is None:
            self._planner = ClarificationPlanner(
                text_runner=self._create_role_llm("planner"),
                pipeline_logging_enabled=self._is_pipeline_logging_enabled(),
            )
        return self._planner

    @property
    def annotator(self) -> Any:
        if self._annotator is None:
            self._annotator = AppendOnlyAnnotationRenderer(
                text_runner=self._create_optional_role_llm("annotator", "fallback")
            )
        return self._annotator

    @property
    def resolver(self) -> ResolverCallable:
        return self._resolver or resolve_plan

    def _get_llm_runtime_config(self) -> LLMRuntimeConfig:
        if self._llm_runtime_config is None:
            self._llm_runtime_config = load_llm_runtime_config(config_path=self._llm_config_path)
        return self._llm_runtime_config

    def _create_role_llm(self, role: str) -> Any:
        config = self._get_llm_runtime_config().get_role_config(role)
        return LLMFactory.create_llm(config)

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
        system_date: str | None = None,
        system_datetime: str | None = None,
        timezone: str = "Asia/Shanghai",
        rewrite: bool = False,
    ) -> dict[str, Any]:
        pipeline_logging_enabled = self._is_pipeline_logging_enabled()
        log_pipeline_event(
            "service",
            "request",
            {
                "query": query,
                "system_date": system_date,
                "system_datetime": system_datetime,
                "timezone": timezone,
                "rewrite": rewrite,
            },
            enabled=pipeline_logging_enabled,
        )
        try:
            plan = self.planner.plan_query(
                original_query=query,
                system_date=system_date,
                system_datetime=system_datetime,
                timezone=timezone,
            )
        except ValueError as exc:
            log_pipeline_event(
                "planner",
                "failure",
                str(exc),
                level=40,
                enabled=pipeline_logging_enabled,
            )
            if not rewrite:
                raise
            response = {
                "clarification_plan": None,
                "clarification_items": [],
                "rewritten_query": None,
            }
            log_pipeline_event(
                "service",
                "response",
                response,
                enabled=pipeline_logging_enabled,
            )
            return response

        log_pipeline_event(
            "planner",
            "validated_plan",
            plan.model_dump(mode="python"),
            enabled=pipeline_logging_enabled,
        )

        try:
            log_pipeline_event(
                "normalizer",
                "request",
                {
                    "clarification_plan": plan.model_dump(mode="python"),
                },
                enabled=pipeline_logging_enabled,
            )
            normalized_plan = normalize_plan(plan)
        except NormalizationError as exc:
            log_pipeline_event(
                "normalizer",
                "failure",
                str(exc),
                level=40,
                enabled=pipeline_logging_enabled,
            )
            if not rewrite:
                raise
            response = {
                "clarification_plan": plan.model_dump(mode="python"),
                "clarification_items": [],
                "rewritten_query": None,
            }
            log_pipeline_event(
                "service",
                "response",
                response,
                enabled=pipeline_logging_enabled,
            )
            return response
        log_pipeline_event(
            "normalizer",
            "result",
            normalized_plan.model_dump(mode="python"),
            enabled=pipeline_logging_enabled,
        )

        try:
            log_pipeline_event(
                "resolver",
                "request",
                {
                    "clarification_plan": plan.model_dump(mode="python"),
                    "normalized_plan": normalized_plan.model_dump(mode="python"),
                    "system_date": system_date,
                    "system_datetime": system_datetime,
                    "timezone": timezone,
                },
                enabled=pipeline_logging_enabled,
            )
            resolution = self.resolver(
                plan=normalized_plan,
                system_date=system_date,
                system_datetime=system_datetime,
                timezone=timezone,
                business_calendar=self._business_calendar,
            )
        except ValueError as exc:
            log_pipeline_event(
                "resolver",
                "failure",
                str(exc),
                level=40,
                enabled=pipeline_logging_enabled,
            )
            if not rewrite:
                raise
            response = {
                "clarification_plan": plan.model_dump(mode="python"),
                "clarification_items": [],
                "rewritten_query": None,
            }
            log_pipeline_event(
                "service",
                "response",
                response,
                enabled=pipeline_logging_enabled,
            )
            return response
        log_pipeline_event(
            "resolver",
            "result",
            {
                "clarification_items": [
                    item.model_dump(mode="python") for item in resolution.items
                ]
            },
            enabled=pipeline_logging_enabled,
        )
        if not rewrite:
            rewritten_query = None
        elif not resolution.items:
            rewritten_query = query
        else:
            try:
                log_pipeline_event(
                    "materializer",
                    "request",
                    {
                        "original_query": query,
                        "clarification_plan": plan.model_dump(mode="python"),
                        "normalized_plan": normalized_plan.model_dump(mode="python"),
                        "comparison_groups": [
                            group.model_dump(mode="python") for group in plan.comparison_groups
                        ],
                    },
                    enabled=pipeline_logging_enabled,
                )
                materialization_context = resolve_materialization_context(
                    plan=normalized_plan,
                    system_date=system_date,
                    system_datetime=system_datetime,
                    timezone=timezone,
                    business_calendar=self._business_calendar,
                )
            except ValueError as exc:
                log_pipeline_event(
                    "materializer",
                    "failure",
                    str(exc),
                    level=40,
                    enabled=pipeline_logging_enabled,
                )
                rewritten_query = None
            else:
                if materialization_context is not None:
                    try:
                        rewritten_query = render_materialized_query(
                            original_query=query,
                            context=materialization_context,
                        )
                    except ValueError as exc:
                        log_pipeline_event(
                            "materializer",
                            "failure",
                            str(exc),
                            level=40,
                            enabled=pipeline_logging_enabled,
                        )
                        rewritten_query = None
                    else:
                        log_pipeline_event(
                            "materializer",
                            "result",
                            {
                                "rewritten_query": rewritten_query,
                                "context": materialization_context.model_dump(mode="python"),
                            },
                            enabled=pipeline_logging_enabled,
                        )
                else:
                    try:
                        log_pipeline_event(
                            "annotator",
                            "request",
                            {
                                "original_query": query,
                                "clarification_items": [
                                    item.model_dump(mode="python") for item in resolution.items
                                ],
                                "comparison_groups": [
                                    group.model_dump(mode="python") for group in plan.comparison_groups
                                ],
                            },
                            enabled=pipeline_logging_enabled,
                        )
                        rewritten_query = self.annotator.render(
                            original_query=query,
                            clarification_items=resolution.items,
                            comparison_groups=plan.comparison_groups,
                        )
                    except ValueError as exc:
                        log_pipeline_event(
                            "annotator",
                            "failure",
                            str(exc),
                            level=40,
                            enabled=pipeline_logging_enabled,
                        )
                        rewritten_query = None
                    else:
                        log_pipeline_event(
                            "annotator",
                            "result",
                            {"rewritten_query": rewritten_query},
                            enabled=pipeline_logging_enabled,
                        )
        response = {
            "clarification_plan": plan.model_dump(mode="python"),
            "clarification_items": [item.model_dump(mode="python") for item in resolution.items],
            "rewritten_query": rewritten_query,
        }
        log_pipeline_event(
            "service",
            "response",
            response,
            enabled=pipeline_logging_enabled,
        )
        return response
