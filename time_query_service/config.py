from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SLICE_SUBPERIODS_CONFIG_PATH = PROJECT_ROOT / "config" / "slice_subperiods_limits.json"
DEFAULT_BUSINESS_CALENDAR_ROOT = PROJECT_ROOT / "config" / "business_calendar"
DEFAULT_DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_DASHSCOPE_MODEL_NAME = "qwen3.6-plus"


@dataclass(frozen=True)
class LlmConfig:
    api_key: str | None
    base_url: str
    model_name: str


def load_project_dotenv(*, dotenv_path: Path | None = None) -> None:
    load_dotenv(dotenv_path=dotenv_path or PROJECT_ROOT / ".env", override=False)


def get_llm_config(*, dotenv_path: Path | None = None) -> LlmConfig:
    load_project_dotenv(dotenv_path=dotenv_path)
    return LlmConfig(
        api_key=os.getenv("DASHSCOPE_API_KEY") or None,
        base_url=os.getenv("DASHSCOPE_BASE_URL", DEFAULT_DASHSCOPE_BASE_URL),
        model_name=os.getenv("DASHSCOPE_MODEL_NAME", DEFAULT_DASHSCOPE_MODEL_NAME),
    )


def require_llm_config(*, dotenv_path: Path | None = None) -> LlmConfig:
    config = get_llm_config(dotenv_path=dotenv_path)
    if not config.api_key:
        raise RuntimeError("Missing DASHSCOPE_API_KEY. Set it in .env or your shell environment.")
    return config


def get_business_calendar_root() -> Path:
    return Path(os.getenv("BUSINESS_CALENDAR_ROOT", str(DEFAULT_BUSINESS_CALENDAR_ROOT)))


def get_slice_subperiod_max_counts(*, config_path: Path | None = None) -> dict[str, dict[str, int]]:
    path = config_path or DEFAULT_SLICE_SUBPERIODS_CONFIG_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    max_count = payload.get("max_count")
    if not isinstance(max_count, dict):
        raise ValueError(f"Invalid slice_subperiods config at {path}: missing max_count object.")
    return max_count
