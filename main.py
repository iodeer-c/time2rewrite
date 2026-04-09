import logging

from fastapi import FastAPI

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.config import get_business_calendar_root
from time_query_service.schemas import (
    ParseQueryRequest,
    ParsedTimeExpressions,
    PipelineRequest,
    PipelineResponse,
    ResolveQueryRequest,
    ResolvedTimeExpressions,
    RewriteQueryRequest,
    RewriteQueryResponse,
)
from time_query_service.service import QueryPipelineService

logger = logging.getLogger(__name__)


def _load_business_calendar() -> JsonBusinessCalendar | None:
    root = get_business_calendar_root()
    if not root.exists():
        logger.warning(
            "Business calendar root missing: %s — named holidays will fail at resolve time.",
            root,
        )
        return None
    try:
        return JsonBusinessCalendar.from_root(root=root)
    except ValueError as exc:
        logger.warning("Business calendar not loaded: %s", exc)
        return None


app = FastAPI(title="Time Query Service")
query_service = QueryPipelineService(business_calendar=_load_business_calendar())


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "time-query-service"}


@app.post("/query/parse", response_model=ParsedTimeExpressions)
def parse_query(request: ParseQueryRequest):
    return query_service.parse_query(
        query=request.query,
        system_date=request.system_date,
        timezone=request.timezone,
    )


@app.post("/query/resolve", response_model=ResolvedTimeExpressions)
def resolve_time_expressions(request: ResolveQueryRequest):
    return query_service.resolve_query(
        parsed_time_expressions=request.parsed_time_expressions.model_dump(mode="python"),
        system_date=request.system_date,
        timezone=request.timezone,
    )


@app.post("/query/rewrite", response_model=RewriteQueryResponse)
def rewrite_query(request: RewriteQueryRequest):
    rewritten_query = query_service.rewrite_query(
        original_query=request.original_query,
        resolved_time_expressions=request.resolved_time_expressions.model_dump(mode="python"),
    )
    return {"rewritten_query": rewritten_query}


@app.post("/query/pipeline", response_model=PipelineResponse)
def pipeline_query(request: PipelineRequest):
    return query_service.process_query(
        query=request.query,
        system_date=request.system_date,
        timezone=request.timezone,
        rewrite=request.rewrite,
    )
