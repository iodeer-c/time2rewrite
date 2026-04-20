from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SLICE_SUBPERIODS_CONFIG_PATH = PROJECT_ROOT / "config" / "slice_subperiods_limits.json"
DEFAULT_BUSINESS_CALENDAR_ROOT = PROJECT_ROOT / "config" / "business_calendar"
DEFAULT_BUSINESS_CALENDAR_EVENT_ALIASES_ROOT = PROJECT_ROOT / "config" / "calendar_event_aliases"


def load_project_dotenv(*, dotenv_path: Path | None = None) -> None:
    load_dotenv(dotenv_path=dotenv_path or PROJECT_ROOT / ".env", override=False)


def get_business_calendar_root() -> Path:
    return Path(os.getenv("BUSINESS_CALENDAR_ROOT", str(DEFAULT_BUSINESS_CALENDAR_ROOT)))


def get_business_calendar_event_aliases_path(*, region: str = "CN") -> Path:
    return DEFAULT_BUSINESS_CALENDAR_EVENT_ALIASES_ROOT / f"{region}.json"


def load_business_calendar_event_aliases(*, region: str = "CN", config_path: Path | None = None) -> dict[str, tuple[str, ...]]:
    path = config_path or get_business_calendar_event_aliases_path(region=region)
    payload = json.loads(path.read_text(encoding="utf-8"))
    aliases = payload.get("canonical_event_aliases")
    if not isinstance(aliases, dict):
        raise ValueError(f"Invalid business calendar alias config at {path}: missing canonical_event_aliases object.")

    normalized: dict[str, tuple[str, ...]] = {}
    for canonical_key, raw_aliases in aliases.items():
        if not isinstance(canonical_key, str) or not canonical_key:
            raise ValueError(f"Invalid business calendar alias config at {path}: canonical key must be a non-empty string.")
        if not isinstance(raw_aliases, list) or not raw_aliases or not all(isinstance(alias, str) and alias for alias in raw_aliases):
            raise ValueError(
                f"Invalid business calendar alias config at {path}: aliases for {canonical_key!r} must be a non-empty string list."
            )
        normalized[canonical_key] = tuple(raw_aliases)
    return normalized


def get_slice_subperiod_max_counts(*, config_path: Path | None = None) -> dict[str, dict[str, int]]:
    path = config_path or DEFAULT_SLICE_SUBPERIODS_CONFIG_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    max_count = payload.get("max_count")
    if not isinstance(max_count, dict):
        raise ValueError(f"Invalid slice_subperiods config at {path}: missing max_count object.")
    return max_count
