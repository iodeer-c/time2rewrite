from __future__ import annotations

from pathlib import Path
from typing import Any

from time_query_service.business_calendar import BusinessCalendarPort
from time_query_service.llm import LLMFactory, LLMRuntimeConfig, load_llm_runtime_config
from time_query_service.parser import QueryParser
from time_query_service.rewriter import QueryRewriter
from time_query_service.schemas import ParsedTimeExpressions
from time_query_service.time_resolver import resolve_query


class QueryPipelineService:
    def __init__(
        self,
        *,
        parser: Any | None = None,
        rewriter: Any | None = None,
        business_calendar: BusinessCalendarPort | None = None,
        llm_runtime_config: LLMRuntimeConfig | None = None,
        llm_config_path: Path | None = None,
    ) -> None:
        self._parser = parser
        self._rewriter = rewriter
        self._business_calendar = business_calendar
        self._llm_runtime_config = llm_runtime_config
        self._llm_config_path = llm_config_path

    @property
    def parser(self) -> Any:
        if self._parser is None:
            self._parser = QueryParser(llm=self._create_role_llm("parser"))
        return self._parser

    @property
    def rewriter(self) -> Any:
        if self._rewriter is None:
            rewriter_runner = self._create_role_llm("rewriter")
            self._rewriter = QueryRewriter(
                text_runner=rewriter_runner,
                fallback_text_runner=rewriter_runner,
                anchor_runner=self._create_optional_role_llm("semantic-anchor", "semantic_anchor"),
            )
        return self._rewriter

    def _get_llm_runtime_config(self) -> LLMRuntimeConfig:
        if self._llm_runtime_config is None:
            self._llm_runtime_config = load_llm_runtime_config(config_path=self._llm_config_path)
        return self._llm_runtime_config

    def _create_role_llm(self, role: str) -> Any:
        config = self._get_llm_runtime_config().get_role_config(role)
        try:
            return LLMFactory.create_llm(config)
        except Exception as exc:
            raise RuntimeError(f"Failed to create LLM for role={role}: {exc}") from exc

    def _create_optional_role_llm(self, *roles: str) -> Any | None:
        runtime_config = self._get_llm_runtime_config()
        for role in roles:
            config = runtime_config.roles.get(role)
            if config is None:
                continue
            try:
                return LLMFactory.create_llm(config)
            except Exception as exc:
                raise RuntimeError(f"Failed to create LLM for role={role}: {exc}") from exc
        return None

    def parse_query(
        self,
        *,
        query: str,
        system_date: str | None = None,
        system_datetime: str | None = None,
        timezone: str = "Asia/Shanghai",
    ) -> dict[str, Any]:
        parser_kwargs: dict[str, Any] = {
            "query": query,
            "timezone": timezone,
        }
        if system_date is not None:
            parser_kwargs["system_date"] = system_date
        if system_datetime is not None:
            parser_kwargs["system_datetime"] = system_datetime
        parsed = self.parser.parse_query_with_llm(**parser_kwargs)
        return ParsedTimeExpressions.model_validate(parsed).model_dump(mode="python")

    def resolve_query(
        self,
        *,
        parsed_time_expressions: dict[str, Any],
        system_date: str | None = None,
        system_datetime: str | None = None,
        timezone: str = "Asia/Shanghai",
    ) -> dict[str, Any]:
        return resolve_query(
            parsed_time_expressions=parsed_time_expressions,
            system_date=system_date,
            system_datetime=system_datetime,
            timezone=timezone,
            business_calendar=self._business_calendar,
        )

    def rewrite_query(self, *, original_query: str, resolved_time_expressions: dict[str, Any]) -> str | None:
        return self.rewriter.rewrite_query_with_llm(
            original_query=original_query,
            resolved_time_expressions=resolved_time_expressions,
        )

    def process_query(
        self,
        *,
        query: str,
        system_date: str | None = None,
        system_datetime: str | None = None,
        timezone: str = "Asia/Shanghai",
        rewrite: bool = False,
    ) -> dict[str, Any]:
        parsed = self.parse_query(
            query=query,
            system_date=system_date,
            system_datetime=system_datetime,
            timezone=timezone,
        )
        resolved = self.resolve_query(
            parsed_time_expressions=parsed,
            system_date=system_date,
            system_datetime=system_datetime,
            timezone=timezone,
        )
        rewritten = (
            self.rewrite_query(original_query=query, resolved_time_expressions=resolved)
            if rewrite
            else None
        )
        return {
            "parsed_time_expressions": parsed,
            "resolved_time_expressions": resolved,
            "rewritten_query": rewritten,
        }
