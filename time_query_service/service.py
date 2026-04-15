from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from time_query_service.annotation import AppendOnlyAnnotationRenderer
from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.llm import LLMFactory, LLMRuntimeConfig, load_llm_runtime_config
from time_query_service.planner import ClarificationPlanner
from time_query_service.time_resolver import resolve_plan


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
    ) -> None:
        self._planner = planner
        self._resolver = resolver
        self._annotator = annotator
        self._business_calendar = business_calendar
        self._llm_runtime_config = llm_runtime_config
        self._llm_config_path = llm_config_path

    @property
    def planner(self) -> Any:
        if self._planner is None:
            self._planner = ClarificationPlanner(text_runner=self._create_role_llm("planner"))
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

    def process_query(
        self,
        *,
        query: str,
        system_date: str | None = None,
        system_datetime: str | None = None,
        timezone: str = "Asia/Shanghai",
        rewrite: bool = False,
    ) -> dict[str, Any]:
        try:
            plan = self.planner.plan_query(
                original_query=query,
                system_date=system_date,
                system_datetime=system_datetime,
                timezone=timezone,
            )
        except ValueError:
            if not rewrite:
                raise
            return {
                "clarification_plan": None,
                "clarification_items": [],
                "rewritten_query": None,
            }

        try:
            resolution = self.resolver(
                plan=plan,
                system_date=system_date,
                system_datetime=system_datetime,
                timezone=timezone,
                business_calendar=self._business_calendar,
            )
        except ValueError:
            if not rewrite:
                raise
            return {
                "clarification_plan": plan.model_dump(mode="python"),
                "clarification_items": [],
                "rewritten_query": None,
            }
        if not rewrite:
            rewritten_query = None
        elif not resolution.items:
            rewritten_query = query
        else:
            try:
                rewritten_query = self.annotator.render(
                    original_query=query,
                    clarification_items=resolution.items,
                    comparison_groups=plan.comparison_groups,
                )
            except ValueError:
                rewritten_query = None
        return {
            "clarification_plan": plan.model_dump(mode="python"),
            "clarification_items": [item.model_dump(mode="python") for item in resolution.items],
            "rewritten_query": rewritten_query,
        }
