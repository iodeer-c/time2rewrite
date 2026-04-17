import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.config import get_business_calendar_root
from time_query_service.service import QueryPipelineService

app = FastAPI(title="Time Query Service")
logger = logging.getLogger(__name__)


class PipelineRequest(BaseModel):
    query: str
    system_date: str | None = None
    system_datetime: str | None = None
    timezone: str = Field(default="Asia/Shanghai")
    rewrite: bool = False


def _load_business_calendar() -> JsonBusinessCalendar | None:
    root = get_business_calendar_root()
    if not root.exists():
        logger.warning("Business calendar root missing: %s", root)
        return None
    try:
        return JsonBusinessCalendar.from_root(root=root)
    except ValueError as exc:
        logger.warning("Business calendar not loaded: %s", exc)
        return None


query_service = QueryPipelineService(business_calendar=_load_business_calendar())


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "time-query-service"}


@app.post("/query/pipeline")
def pipeline_query(request: PipelineRequest) -> dict:
    try:
        return query_service.process_query(
            query=request.query,
            system_date=request.system_date,
            system_datetime=request.system_datetime,
            timezone=request.timezone,
            rewrite=request.rewrite,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
