from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _iter_dates(start: date, end: date) -> list[date]:
    dates: list[date] = []
    cursor = start
    while cursor <= end:
        dates.append(cursor)
        cursor = cursor + timedelta(days=1)
    return dates


def _normalize_strings(values: list[str]) -> tuple[str, ...]:
    return tuple(sorted(dict.fromkeys(values)))


class CalendarDay(StrictModel):
    date: date
    work_kind: Literal["work", "rest"]
    labels: list[str] = Field(default_factory=list)
    related_event_keys: list[str] = Field(default_factory=list)
    rest_group_id: str | None = None
    note: str | None = None

    @model_validator(mode="after")
    def validate_day_semantics(self) -> "CalendarDay":
        is_makeup = "makeup_workday" in self.labels
        if self.rest_group_id and self.work_kind != "rest":
            raise ValueError("rest_group_id can only be used on rest days.")
        if self.related_event_keys and self.work_kind != "work":
            raise ValueError("related_event_keys can only be used on work days.")
        if is_makeup and self.work_kind != "work":
            raise ValueError("makeup_workday labels require work_kind='work'.")
        if is_makeup and not self.related_event_keys:
            raise ValueError("makeup_workday rows require related_event_keys.")
        if self.related_event_keys and not is_makeup:
            raise ValueError("related_event_keys require the makeup_workday label.")
        return self


class EventSpan(StrictModel):
    event_key: str
    schedule_year: int
    scope: Literal["consecutive_rest", "statutory"]
    start: date
    end: date
    rest_group_id: str | None = None

    @model_validator(mode="after")
    def validate_date_order(self) -> "EventSpan":
        if self.start > self.end:
            raise ValueError("event span start must be on or before end.")
        return self


class BusinessCalendarFile(StrictModel):
    schema_version: int
    region: str
    schedule_year: int
    calendar_version: str
    days: list[CalendarDay] = Field(default_factory=list)
    event_spans: list[EventSpan] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_file(self) -> "BusinessCalendarFile":
        if self.schema_version != 2:
            raise ValueError(f"Unsupported business calendar schema_version: {self.schema_version}")

        day_map: dict[date, CalendarDay] = {}
        for day in self.days:
            if day.date in day_map:
                raise ValueError(f"Duplicate business calendar day override for {self.region} {day.date.isoformat()}")
            day_map[day.date] = day

        event_keys: set[str] = set()
        span_keys: set[tuple[str, int, str]] = set()
        for span in self.event_spans:
            if span.schedule_year != self.schedule_year:
                raise ValueError(
                    f"Event span {span.event_key} uses schedule_year={span.schedule_year}, "
                    f"but file schedule_year={self.schedule_year}"
                )
            key = (span.event_key, span.schedule_year, span.scope)
            if key in span_keys:
                raise ValueError(
                    f"Duplicate event span for region={self.region}, event_key={span.event_key}, "
                    f"schedule_year={span.schedule_year}, scope={span.scope}"
                )
            span_keys.add(key)
            event_keys.add(span.event_key)

        for day in self.days:
            for event_key in day.related_event_keys:
                if event_key not in event_keys:
                    raise ValueError(
                        f"related_event_keys references unknown event_key={event_key!r} "
                        f"for schedule_year={self.schedule_year}"
                    )

        for span in self.event_spans:
            if not span.rest_group_id:
                continue
            for covered_date in _iter_dates(span.start, span.end):
                override = day_map.get(covered_date)
                if override is None or override.work_kind != "rest" or override.rest_group_id != span.rest_group_id:
                    raise ValueError(
                        f"Event span {span.event_key}/{span.schedule_year}/{span.scope} "
                        f"is inconsistent with rest_group_id {span.rest_group_id} on {covered_date.isoformat()}"
                    )
        return self


@dataclass(frozen=True)
class EventSpanRecord:
    region: str
    event_key: str
    schedule_year: int
    scope: Literal["consecutive_rest", "statutory"]
    start: date
    end: date
    calendar_version: str
    rest_group_id: str | None = None


@dataclass(frozen=True)
class DayOverrideContribution:
    region: str
    date: date
    work_kind: Literal["work", "rest"]
    schedule_year: int
    calendar_version: str
    labels: tuple[str, ...]
    related_event_keys: tuple[str, ...]
    rest_group_id: str | None = None

    @property
    def is_makeup_workday(self) -> bool:
        return "makeup_workday" in self.labels

    @property
    def normalized_semantics(self) -> tuple[str, bool, tuple[str, ...]]:
        return (self.work_kind, self.is_makeup_workday, self.related_event_keys)


@dataclass(frozen=True)
class HolidayMembershipContribution:
    region: str
    date: date
    event_key: str
    schedule_year: int
    calendar_version: str


@dataclass(frozen=True)
class CanonicalDayStatus:
    date: date
    is_workday: bool
    is_holiday: bool
    is_makeup_workday: bool
    labels: tuple[str, ...]
    related_event_keys: tuple[str, ...]
    source_schedule_years: tuple[int, ...]
    calendar_versions: tuple[str, ...]
    source_kind: Literal["default", "override"]

    @property
    def calendar_version(self) -> str:
        return ",".join(self.calendar_versions)


class BusinessCalendarPort(Protocol):
    def get_event_span(
        self,
        *,
        region: str,
        event_key: str,
        schedule_year: int,
        scope: Literal["consecutive_rest", "statutory"],
    ) -> tuple[date, date] | None: ...

    def get_day_status(self, *, region: str, d: date) -> CanonicalDayStatus: ...

    def is_workday(self, *, region: str, d: date) -> bool: ...

    def is_holiday(self, *, region: str, d: date) -> bool: ...

    def calendar_version(self, region: str) -> str: ...

    def calendar_version_for_schedule_year(self, *, region: str, schedule_year: int) -> str | None: ...

    def list_makeup_workdays(self, *, region: str, event_key: str, schedule_year: int) -> list[date]: ...


class JsonBusinessCalendar:
    def __init__(
        self,
        *,
        schedule_versions: dict[str, dict[int, str]],
        event_spans: dict[str, dict[tuple[str, int, str], EventSpanRecord]],
        day_overrides: dict[str, dict[date, tuple[DayOverrideContribution, ...]]],
        holiday_memberships: dict[str, dict[date, tuple[HolidayMembershipContribution, ...]]],
        region_versions: dict[str, list[str]],
    ) -> None:
        self._schedule_versions = schedule_versions
        self._event_spans = event_spans
        self._day_overrides = day_overrides
        self._holiday_memberships = holiday_memberships
        self._region_versions = region_versions

    @classmethod
    def from_root(cls, *, root: Path) -> "JsonBusinessCalendar":
        root = Path(root)
        if not root.exists():
            raise ValueError(f"Business calendar root does not exist: {root}")

        files = sorted(path for path in root.glob("*/*.json") if path.is_file())
        if not files:
            raise ValueError(f"No business calendar JSON files found under {root}")

        schedule_versions: dict[str, dict[int, str]] = {}
        event_spans: dict[str, dict[tuple[str, int, str], EventSpanRecord]] = {}
        day_overrides: dict[str, dict[date, list[DayOverrideContribution]]] = {}
        holiday_memberships: dict[str, dict[date, list[HolidayMembershipContribution]]] = {}
        region_versions: dict[str, list[str]] = {}

        for path in files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            calendar_file = BusinessCalendarFile.model_validate(payload)
            region = calendar_file.region
            region_schedule_versions = schedule_versions.setdefault(region, {})
            if calendar_file.schedule_year in region_schedule_versions:
                raise ValueError(
                    f"Duplicate schedule definition for region={region}, "
                    f"schedule_year={calendar_file.schedule_year}"
                )
            region_schedule_versions[calendar_file.schedule_year] = calendar_file.calendar_version
            region_versions.setdefault(region, []).append(calendar_file.calendar_version)

            span_map = event_spans.setdefault(region, {})
            day_map = day_overrides.setdefault(region, {})
            holiday_map = holiday_memberships.setdefault(region, {})

            for span in calendar_file.event_spans:
                key = (span.event_key, span.schedule_year, span.scope)
                if key in span_map:
                    raise ValueError(
                        f"Duplicate event span for region={region}, event_key={span.event_key}, "
                        f"schedule_year={span.schedule_year}, scope={span.scope}"
                    )
                span_map[key] = EventSpanRecord(
                    region=region,
                    event_key=span.event_key,
                    schedule_year=span.schedule_year,
                    scope=span.scope,
                    start=span.start,
                    end=span.end,
                    calendar_version=calendar_file.calendar_version,
                    rest_group_id=span.rest_group_id,
                )
                for covered_date in _iter_dates(span.start, span.end):
                    holiday_map.setdefault(covered_date, []).append(
                        HolidayMembershipContribution(
                            region=region,
                            date=covered_date,
                            event_key=span.event_key,
                            schedule_year=span.schedule_year,
                            calendar_version=calendar_file.calendar_version,
                        )
                    )

            for day in calendar_file.days:
                contribution = DayOverrideContribution(
                    region=region,
                    date=day.date,
                    work_kind=day.work_kind,
                    schedule_year=calendar_file.schedule_year,
                    calendar_version=calendar_file.calendar_version,
                    labels=_normalize_strings(day.labels),
                    related_event_keys=_normalize_strings(day.related_event_keys),
                    rest_group_id=day.rest_group_id,
                )
                existing = day_map.setdefault(day.date, [])
                if existing and any(item.normalized_semantics != contribution.normalized_semantics for item in existing):
                    raise ValueError(
                        f"Conflicting day semantics for region={region}, date={day.date.isoformat()}"
                    )
                existing.append(contribution)

        return cls(
            schedule_versions=schedule_versions,
            event_spans=event_spans,
            day_overrides={region: {d: tuple(items) for d, items in day_map.items()} for region, day_map in day_overrides.items()},
            holiday_memberships={
                region: {d: tuple(items) for d, items in holiday_map.items()}
                for region, holiday_map in holiday_memberships.items()
            },
            region_versions=region_versions,
        )

    def get_event_span(
        self,
        *,
        region: str,
        event_key: str,
        schedule_year: int,
        scope: Literal["consecutive_rest", "statutory"],
    ) -> tuple[date, date] | None:
        span = self._event_spans.get(region, {}).get((event_key, schedule_year, scope))
        if span is None:
            return None
        return span.start, span.end

    def get_day_status(self, *, region: str, d: date) -> CanonicalDayStatus:
        region_schedule_versions = self._schedule_versions.get(region)
        if region_schedule_versions is None:
            raise ValueError(f"Unknown business calendar region: {region}")

        natural_year_version = region_schedule_versions.get(d.year)
        if natural_year_version is None:
            raise ValueError(f"Missing business calendar data for region={region}, schedule_year={d.year}")

        override_contributions = self._day_overrides.get(region, {}).get(d, ())
        holiday_contributions = self._holiday_memberships.get(region, {}).get(d, ())

        labels: set[str] = set()
        related_event_keys: set[str] = set()
        source_schedule_years = {d.year}
        calendar_versions = {natural_year_version}

        if override_contributions:
            is_workday = override_contributions[0].work_kind == "work"
            for contribution in override_contributions:
                labels.update(contribution.labels)
                related_event_keys.update(contribution.related_event_keys)
                source_schedule_years.add(contribution.schedule_year)
                calendar_versions.add(contribution.calendar_version)
            source_kind: Literal["default", "override"] = "override"
        else:
            is_workday = d.weekday() < 5
            source_kind = "default"

        for contribution in holiday_contributions:
            labels.add(contribution.event_key)
            source_schedule_years.add(contribution.schedule_year)
            calendar_versions.add(contribution.calendar_version)

        return CanonicalDayStatus(
            date=d,
            is_workday=is_workday,
            is_holiday=bool(holiday_contributions),
            is_makeup_workday=bool(related_event_keys),
            labels=tuple(sorted(labels)),
            related_event_keys=tuple(sorted(related_event_keys)),
            source_schedule_years=tuple(sorted(source_schedule_years)),
            calendar_versions=tuple(sorted(calendar_versions)),
            source_kind=source_kind,
        )

    def is_workday(self, *, region: str, d: date) -> bool:
        return self.get_day_status(region=region, d=d).is_workday

    def is_holiday(self, *, region: str, d: date) -> bool:
        return self.get_day_status(region=region, d=d).is_holiday

    def calendar_version(self, region: str) -> str:
        versions = self._region_versions.get(region, [])
        if not versions:
            raise ValueError(f"Unknown business calendar region: {region}")
        return ",".join(sorted(set(versions)))

    def calendar_version_for_schedule_year(self, *, region: str, schedule_year: int) -> str | None:
        region_mapping = self._schedule_versions.get(region)
        if region_mapping is None:
            raise ValueError(f"Unknown business calendar region: {region}")
        return region_mapping.get(schedule_year)

    def list_makeup_workdays(self, *, region: str, event_key: str, schedule_year: int) -> list[date]:
        version = self.calendar_version_for_schedule_year(region=region, schedule_year=schedule_year)
        if version is None:
            raise ValueError(f"Missing business calendar data for region={region}, schedule_year={schedule_year}")

        matched: list[date] = []
        for contribution in self._day_overrides.get(region, {}).values():
            for item in contribution:
                if (
                    item.schedule_year == schedule_year
                    and item.is_makeup_workday
                    and event_key in item.related_event_keys
                ):
                    matched.append(item.date)
                    break
        return sorted(set(matched))
