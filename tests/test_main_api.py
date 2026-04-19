from __future__ import annotations

from fastapi.testclient import TestClient

import main
from time_query_service.post_processor import PostProcessorValidationError


def test_pipeline_query_maps_post_processor_validation_error_to_422(monkeypatch) -> None:
    def _raise(*, query: str, system_date: str | None, system_datetime: str | None, timezone: str, rewrite: bool):
        raise PostProcessorValidationError(
            layer=3,
            stage="post_processor",
            details="surface_fragments do not cover render_text",
            unit_id="u1",
        )

    monkeypatch.setattr(main.query_service, "process_query", _raise)

    client = TestClient(main.app)
    response = client.post(
        "/query/pipeline",
        json={
            "query": "2025年杭千公司工作日的总收益是多少",
            "system_date": "2026-04-19",
            "rewrite": True,
        },
    )

    assert response.status_code == 422
    assert "Layer 3 validation failed" in response.json()["detail"]
