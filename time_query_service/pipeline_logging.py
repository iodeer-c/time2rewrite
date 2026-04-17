from __future__ import annotations

import json
import logging
from typing import Any


PIPELINE_LOGGER_NAME = "time_query_service.pipeline"
_SEPARATOR = "=" * 80


def get_pipeline_logger() -> logging.Logger:
    logger = logging.getLogger(PIPELINE_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_pipeline_event(
    layer: str,
    event: str,
    payload: Any | None = None,
    *,
    enabled: bool = True,
    level: int = logging.INFO,
    exc_info: bool = False,
) -> None:
    if not enabled:
        return
    logger = get_pipeline_logger()
    message_parts = [_SEPARATOR, f"[pipeline][{layer}][{event}]"]
    if payload is not None:
        message_parts.append(_format_payload(payload))
    message_parts.append(_SEPARATOR)
    logger.log(level, "\n".join(message_parts), exc_info=exc_info)


def _format_payload(payload: Any) -> str:
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="python")
    if isinstance(payload, str):
        return payload
    try:
        return json.dumps(payload, ensure_ascii=False, indent=2, default=str, sort_keys=True)
    except TypeError:
        return str(payload)
