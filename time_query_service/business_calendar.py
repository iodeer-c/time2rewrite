from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator


logger = logging.getLogger(__name__)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CalendarDay(StrictModel):
    date: date
    work_kind: Literal["work", "rest"]
    labels: list[str] = Field(default_factory=list)
    related_event_keys: list[str] = Field(default_factory=list)
    rest_group_id: str | None = None
    note: str | None = None

    @model_validator(mode="after")
    def validate_rest_group_usage(self) -> "CalendarDay":
        if self.rest_group_id and self.work_kind != "rest":
            raise ValueError("rest_group_id can only be used on rest days.")
        if "makeup_workday" in self.labels and not self.related_event_keys:
            logger.warning(
                "Calendar day %s is labeled makeup_workday but has no related_event_keys.",
                self.date.isoformat(),
            )
        return self


class EventSpan(StrictModel):
    event_key: str
    year: int
    scope: Literal["consecutive_rest", "statutory"]
    start: date
    end: date
    rest_group_id: str | None = None
    co_event_keys: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_date_order(self) -> "EventSpan":
        if self.start > self.end:
            raise ValueError("event span start must be on or before end.")
        return self


class BusinessCalendarFile(StrictModel):
    schema_version: int
    region: str
    calendar_version: str
    days: list[CalendarDay] = Field(default_factory=list)
    event_spans: list[EventSpan] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_schema_version(self) -> "BusinessCalendarFile":
        if self.schema_version != 1:
            raise ValueError(f"Unsupported business calendar schema_version: {self.schema_version}")
        return self


@dataclass(frozen=True)
class EventSpanRecord:
    region: str
    event_key: str
    year: int
    scope: Literal["consecutive_rest", "statutory"]
    start: date
    end: date
    calendar_version: str
    rest_group_id: str | None = None


@dataclass(frozen=True)
class DayOverrideRecord:
    region: str
    date: date
    work_kind: Literal["work", "rest"]
    calendar_version: str
    labels: list[str]
    related_event_keys: list[str]
    rest_group_id: str | None = None


class BusinessCalendarPort(Protocol):
    def get_event_span(
        self,
        *,
        region: str,
        event_key: str,
        year: int,
        scope: Literal["consecutive_rest", "statutory"],
    ) -> tuple[date, date] | None: ...

    def is_workday(self, *, region: str, d: date) -> bool: ...

    def is_holiday(self, *, region: str, d: date) -> bool: ...

    def calendar_version(self, region: str) -> str: ...

    def calendar_version_for_year(self, *, region: str, year: int) -> str | None: ...

    def list_makeup_workdays(self, *, region: str, event_key: str, year: int) -> list[date]: ...


class JsonBusinessCalendar:
    def __init__(
        self,
        *,
        day_overrides: dict[str, dict[date, DayOverrideRecord]],
        event_spans: dict[str, dict[tuple[str, int, str], EventSpanRecord]],
        holiday_dates: dict[str, set[date]],
        region_versions: dict[str, list[str]],
        region_year_versions: dict[str, dict[int, str]],
    ) -> None:
        self._day_overrides = day_overrides
        self._event_spans = event_spans
        self._holiday_dates = holiday_dates
        self._region_versions = region_versions
        self._region_year_versions = region_year_versions

    @classmethod
    def from_root(cls, *, root: Path) -> "JsonBusinessCalendar":
        root = Path(root)
        if not root.exists():
            raise ValueError(f"Business calendar root does not exist: {root}")

        files = sorted(path for path in root.glob("*/*.json") if path.is_file())
        if not files:
            raise ValueError(f"No business calendar JSON files found under {root}")

        day_overrides: dict[str, dict[date, DayOverrideRecord]] = {}
        event_spans: dict[str, dict[tuple[str, int, str], EventSpanRecord]] = {}
        holiday_dates: dict[str, set[date]] = {}
        region_versions: dict[str, list[str]] = {}
        region_year_versions: dict[str, dict[int, str]] = {}
        rest_group_days: dict[tuple[str, str], list[date]] = {}
        spans_by_rest_group: dict[tuple[str, str], list[EventSpanRecord]] = {}

        for path in files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            calendar_file = BusinessCalendarFile.model_validate(payload)
            region = calendar_file.region
            day_map = day_overrides.setdefault(region, {})
            span_map = event_spans.setdefault(region, {})
            holiday_set = holiday_dates.setdefault(region, set())
            versions = region_versions.setdefault(region, [])
            year_versions = region_year_versions.setdefault(region, {})
            versions.append(calendar_file.calendar_version)
            years_in_file: set[int] = set()

            for day in calendar_file.days:
                if day.date in day_map:
                    raise ValueError(f"Duplicate business calendar day override for {region} {day.date.isoformat()}")
                years_in_file.add(day.date.year)
                day_map[day.date] = DayOverrideRecord(
                    region=region,
                    date=day.date,
                    work_kind=day.work_kind,
                    calendar_version=calendar_file.calendar_version,
                    labels=list(day.labels),
                    related_event_keys=list(day.related_event_keys),
                    rest_group_id=day.rest_group_id,
                )
                if day.rest_group_id:
                    rest_group_days.setdefault((region, day.rest_group_id), []).append(day.date)

            for span in calendar_file.event_spans:
                key = (span.event_key, span.year, span.scope)
                if key in span_map:
                    raise ValueError(
                        f"Duplicate event span for region={region}, event_key={span.event_key}, "
                        f"year={span.year}, scope={span.scope}"
                    )
                years_in_file.add(span.year)
                years_in_file.add(span.start.year)
                years_in_file.add(span.end.year)
                record = EventSpanRecord(
                    region=region,
                    event_key=span.event_key,
                    year=span.year,
                    scope=span.scope,
                    start=span.start,
                    end=span.end,
                    calendar_version=calendar_file.calendar_version,
                    rest_group_id=span.rest_group_id,
                )
                span_map[key] = record
                cursor = span.start
                while cursor <= span.end:
                    holiday_set.add(cursor)
                    cursor = cursor + timedelta(days=1)
                if span.rest_group_id:
                    spans_by_rest_group.setdefault((region, span.rest_group_id), []).append(record)

            for year in years_in_file:
                existing = year_versions.get(year)
                if existing is not None and existing != calendar_file.calendar_version:
                    raise ValueError(
                        f"Conflicting calendar_version for region={region}, year={year}: "
                        f"{existing} vs {calendar_file.calendar_version}"
                    )
                year_versions[year] = calendar_file.calendar_version

        cls._validate_rest_groups(rest_group_days)
        cls._validate_event_span_consistency(day_overrides, spans_by_rest_group)
        return cls(
            day_overrides=day_overrides,
            event_spans=event_spans,
            holiday_dates=holiday_dates,
            region_versions=region_versions,
            region_year_versions=region_year_versions,
        )

    @staticmethod
    def _validate_rest_groups(rest_group_days: dict[tuple[str, str], list[date]]) -> None:
        for (region, rest_group_id), dates in rest_group_days.items():
            sorted_dates = sorted(dates)
            for prev, cur in zip(sorted_dates, sorted_dates[1:]):
                if cur != prev + timedelta(days=1):
                    raise ValueError(
                        f"Rest group {rest_group_id!r} for region {region} is not consecutive: "
                        f"{prev.isoformat()} -> {cur.isoformat()}"
                    )

    @staticmethod
    def _validate_event_span_consistency(
        day_overrides: dict[str, dict[date, DayOverrideRecord]],
        spans_by_rest_group: dict[tuple[str, str], list[EventSpanRecord]],
    ) -> None:
        for (region, rest_group_id), spans in spans_by_rest_group.items():
            overrides = day_overrides.get(region, {})
            for span in spans:
                cursor = span.start
                while cursor <= span.end:
                    override = overrides.get(cursor)
                    if override is None or override.work_kind != "rest" or override.rest_group_id != rest_group_id:
                        raise ValueError(
                            f"Event span {span.event_key}/{span.year}/{span.scope} "
                            f"is inconsistent with rest_group_id {rest_group_id} on {cursor.isoformat()}"
                        )
                    cursor = cursor + timedelta(days=1)

    def get_event_span(
        self,
        *,
        region: str,
        event_key: str,
        year: int,
        scope: Literal["consecutive_rest", "statutory"],
    ) -> tuple[date, date] | None:
        span = self._event_spans.get(region, {}).get((event_key, year, scope))
        if span is None:
            return None
        return span.start, span.end

    def is_workday(self, *, region: str, d: date) -> bool:
        override = self._day_overrides.get(region, {}).get(d)
        if override is not None:
            return override.work_kind == "work"
        return d.weekday() < 5

    def is_holiday(self, *, region: str, d: date) -> bool:
        return d in self._holiday_dates.get(region, set())

    def calendar_version(self, region: str) -> str:
        versions = self._region_versions.get(region, [])
        if not versions:
            raise ValueError(f"Unknown business calendar region: {region}")
        return ",".join(sorted(set(versions)))

    def calendar_version_for_year(self, *, region: str, year: int) -> str | None:
        region_mapping = self._region_year_versions.get(region)
        if region_mapping is None:
            raise ValueError(f"Unknown business calendar region: {region}")
        return region_mapping.get(year)

    def list_makeup_workdays(self, *, region: str, event_key: str, year: int) -> list[date]:
        version = self.calendar_version_for_year(region=region, year=year)
        if version is None:
            raise ValueError(f"Missing business calendar data for region={region}, year={year}")

        day_map = self._day_overrides.get(region, {})
        return sorted(
            override.date
            for override in day_map.values()
            if override.date.year == year
            and "makeup_workday" in override.labels
            and event_key in override.related_event_keys
        )
