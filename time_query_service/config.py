from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SLICE_SUBPERIODS_CONFIG_PATH = PROJECT_ROOT / "config" / "slice_subperiods_limits.json"
DEFAULT_BUSINESS_CALENDAR_ROOT = PROJECT_ROOT / "config" / "business_calendar"


def load_project_dotenv(*, dotenv_path: Path | None = None) -> None:
    load_dotenv(dotenv_path=dotenv_path or PROJECT_ROOT / ".env", override=False)


def get_business_calendar_root() -> Path:
    return Path(os.getenv("BUSINESS_CALENDAR_ROOT", str(DEFAULT_BUSINESS_CALENDAR_ROOT)))


def get_slice_subperiod_max_counts(*, config_path: Path | None = None) -> dict[str, dict[str, int]]:
    path = config_path or DEFAULT_SLICE_SUBPERIODS_CONFIG_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    max_count = payload.get("max_count")
    if not isinstance(max_count, dict):
        raise ValueError(f"Invalid slice_subperiods config at {path}: missing max_count object.")
    return max_count
