from __future__ import annotations

from typing import Any

from time_query_service.parser import QueryParser
from time_query_service.rewriter import QueryRewriter
from time_query_service.schemas import ParsedTimeExpressions
from time_query_service.time_resolver import resolve_query


class QueryPipelineService:
    def __init__(self, *, parser: Any | None = None, rewriter: Any | None = None) -> None:
        self._parser = parser
        self._rewriter = rewriter

    @property
    def parser(self) -> Any:
        if self._parser is None:
            self._parser = QueryParser()
        return self._parser

    @property
    def rewriter(self) -> Any:
        if self._rewriter is None:
            self._rewriter = QueryRewriter()
        return self._rewriter

    def parse_query(self, *, query: str, system_date: str, timezone: str) -> dict[str, Any]:
        parsed = self.parser.parse_query_with_llm(query=query, system_date=system_date, timezone=timezone)
        return ParsedTimeExpressions.model_validate(parsed).model_dump(mode="python")

    def resolve_query(self, *, parsed_time_expressions: dict[str, Any], system_date: str, timezone: str) -> dict[str, Any]:
        return resolve_query(
            parsed_time_expressions=parsed_time_expressions,
            system_date=system_date,
            timezone=timezone,
        )

    def rewrite_query(self, *, original_query: str, resolved_time_expressions: dict[str, Any]) -> str:
        return self.rewriter.rewrite_query_with_llm(
            original_query=original_query,
            resolved_time_expressions=resolved_time_expressions,
        )

    def process_query(
        self,
        *,
        query: str,
        system_date: str,
        timezone: str,
        rewrite: bool = False,
    ) -> dict[str, Any]:
        parsed = self.parse_query(query=query, system_date=system_date, timezone=timezone)
        resolved = self.resolve_query(
            parsed_time_expressions=parsed,
            system_date=system_date,
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
