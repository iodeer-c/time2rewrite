from fastapi import FastAPI

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

app = FastAPI(title="Time Query Service")
query_service = QueryPipelineService()


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
