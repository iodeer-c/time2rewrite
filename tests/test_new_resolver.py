from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.evaluator import resolved_plan_equals
from time_query_service.new_resolver import resolve_plan
from time_query_service.post_processor import (
    StageAComparisonOutput,
    StageAComparisonPairOutput,
    StageAOutput,
    StageAUnitOutput,
    assemble_time_plan,
)
from time_query_service.resolved_plan import Interval
from time_query_service.time_plan import (
    CalendarEvent,
    CalendarFilter,
    Carrier,
    Comparison,
    ComparisonPair,
    DerivedContent,
    DerivationSource,
    GroupedTemporalValue,
    NamedPeriod,
    PairExpansion,
    RollingByCalendarUnit,
    RollingWindow,
    ScheduleYearRef,
    StandaloneContent,
    TimePlan,
    Unit,
)
from tests.fixtures.golden_datasets import LAYER1_GOLDEN_CASES


def _calendar() -> JsonBusinessCalendar:
    return JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))


def _standalone_month(unit_id: str, year: int, month: int) -> Unit:
    return Unit(
        unit_id=unit_id,
        render_text=f"{year}年{month}月",
        surface_fragments=[{"start": 0, "end": 1}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=year, month=month),
                modifiers=[],
            ),
        ),
    )


def _stage_a_unit(**overrides: object) -> StageAUnitOutput:
    payload = {
        "unit_id": "u1",
        "render_text": "A",
        "surface_fragments": [{"start": 0, "end": 1}],
        "content_kind": "standalone",
        "self_contained_text": "A",
        "sources": [],
        "surface_hint": None,
    }
    payload.update(overrides)
    return StageAUnitOutput.model_validate(payload)


class _MissingEventCalendar:
    def __init__(self, fallback: JsonBusinessCalendar) -> None:
        self._fallback = fallback

    def get_event_span(self, *, region: str, event_key: str, schedule_year: int, scope: str):
        return None

    def get_day_status(self, *, region: str, d: date):
        return self._fallback.get_day_status(region=region, d=d)

    def is_workday(self, *, region: str, d: date) -> bool:
        return self._fallback.is_workday(region=region, d=d)

    def is_holiday(self, *, region: str, d: date) -> bool:
        return self._fallback.is_holiday(region=region, d=d)

    def calendar_version(self, region: str) -> str:
        return self._fallback.calendar_version(region)

    def calendar_version_for_schedule_year(self, *, region: str, schedule_year: int):
        return self._fallback.calendar_version_for_schedule_year(region=region, schedule_year=schedule_year)

    def list_makeup_workdays(self, *, region: str, event_key: str, schedule_year: int):
        return self._fallback.list_makeup_workdays(region=region, event_key=event_key, schedule_year=schedule_year)


def test_resolve_plan_materializes_standalone_units() -> None:
    plan = TimePlan(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u1", 2025, 3)],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert resolved.nodes["u1"].tree.role == "atom"
    assert resolved.nodes["u1"].tree.labels.absolute_core_time.start == date(2025, 3, 1)
    assert resolved.nodes["u1"].tree.labels.absolute_core_time.end == date(2025, 3, 31)


def test_resolve_plan_matches_every_tier1_layer1_golden_case() -> None:
    tier1_cases = [case for case in LAYER1_GOLDEN_CASES if case["tier"] == 1]
    assert tier1_cases

    for case in tier1_cases:
        actual = resolve_plan(case["expected_time_plan"], business_calendar=_calendar())
        result = resolved_plan_equals(case["expected_resolved_plan"], actual)
        assert result.passed, f"{case['query']}: {result.diffs}"


def test_resolve_plan_preserves_stage_b_degradation_reason() -> None:
    degraded_unit = Unit.model_validate(
        {
            "unit_id": "u1",
            "render_text": "最近5个休息日",
            "surface_fragments": [{"start": 0, "end": 7}],
            "needs_clarification": True,
            "reason_kind": "unsupported_calendar_grain_rolling",
            "content": {"content_kind": "standalone"},
        }
    )
    plan = TimePlan(
        query="最近5个休息日",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[degraded_unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert resolved.nodes["u1"].needs_clarification is True
    assert resolved.nodes["u1"].reason_kind == "unsupported_calendar_grain_rolling"
    assert resolved.nodes["u1"].tree is None


@pytest.mark.parametrize(
    "reason_kind",
    [
        "llm_hard_fail",
        "unsupported_calendar_grain_rolling",
        "unsupported_anchor_semantics",
        "semantic_conflict",
    ],
)
def test_resolve_plan_passes_through_all_stage_b_reason_kinds(reason_kind: str) -> None:
    degraded_unit = Unit.model_validate(
        {
            "unit_id": "u1",
            "render_text": "坏时间",
            "surface_fragments": [{"start": 0, "end": 2}],
            "needs_clarification": True,
            "reason_kind": reason_kind,
            "content": {"content_kind": "standalone"},
        }
    )
    plan = TimePlan(
        query="坏时间",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[degraded_unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert resolved.nodes["u1"].needs_clarification is True
    assert resolved.nodes["u1"].reason_kind == reason_kind


def test_resolve_plan_builds_single_source_derived_node() -> None:
    derived_unit = Unit(
        unit_id="u2",
        render_text="去年同期",
        surface_fragments=[{"start": 0, "end": 1}],
        content=DerivedContent(
            content_kind="derived",
            sources=[DerivationSource(source_unit_id="u1", transform={"kind": "shift_year", "offset": -1})],
        ),
    )
    plan = TimePlan(
        query="今年3月和去年同期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u1", 2025, 3), derived_unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())
    node = resolved.nodes["u2"]

    assert node.tree.role == "derived"
    assert node.derived_from == ["u1"]
    assert len(node.tree.children) == 1
    child = node.tree.children[0]
    assert child.role == "derived_source"
    assert child.labels.source_unit_id == "u1"
    assert child.labels.absolute_core_time.start == date(2024, 3, 1)
    assert child.labels.derivation_transform_summary == {"kind": "shift_year", "offset": -1}


def test_resolve_plan_builds_multi_source_derived_node_in_source_order() -> None:
    derived_unit = Unit(
        unit_id="u3",
        render_text="去年同期",
        surface_fragments=[{"start": 0, "end": 1}],
        content=DerivedContent(
            content_kind="derived",
            sources=[
                DerivationSource(source_unit_id="u1", transform={"kind": "shift_year", "offset": -1}),
                DerivationSource(source_unit_id="u2", transform={"kind": "shift_year", "offset": -1}),
            ],
        ),
    )
    plan = TimePlan(
        query="今年3月和5月，去年同期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u1", 2025, 3), _standalone_month("u2", 2025, 5), derived_unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())
    node = resolved.nodes["u3"]

    assert node.derived_from == ["u1", "u2"]
    assert [child.labels.source_unit_id for child in node.tree.children] == ["u1", "u2"]
    assert [child.labels.absolute_core_time.start for child in node.tree.children] == [date(2024, 3, 1), date(2024, 5, 1)]
    assert [interval.start for interval in node.tree.intervals] == [date(2024, 3, 1), date(2024, 5, 1)]


def test_resolve_plan_applies_partial_failure_policy_per_derived_child() -> None:
    degraded_unit = Unit.model_validate(
        {
            "unit_id": "u2",
            "render_text": "最近5个休息日",
            "surface_fragments": [{"start": 0, "end": 7}],
            "needs_clarification": True,
            "reason_kind": "semantic_conflict",
            "content": {"content_kind": "standalone"},
        }
    )
    derived_unit = Unit(
        unit_id="u3",
        render_text="去年同期",
        surface_fragments=[{"start": 0, "end": 1}],
        content=DerivedContent(
            content_kind="derived",
            sources=[
                DerivationSource(source_unit_id="u1", transform={"kind": "shift_year", "offset": -1}),
                DerivationSource(source_unit_id="u2", transform={"kind": "shift_year", "offset": -1}),
            ],
        ),
    )
    plan = TimePlan(
        query="今年3月和坏时间，去年同期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u1", 2025, 3), degraded_unit, derived_unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())
    node = resolved.nodes["u3"]

    assert node.needs_clarification is False
    assert node.reason_kind is None
    assert [child.labels.source_unit_id for child in node.tree.children] == ["u1", "u2"]
    assert node.tree.children[0].labels.degraded in (None, False)
    assert node.tree.children[1].labels.degraded is True
    assert node.tree.children[1].labels.degraded_source_reason_kind == "semantic_conflict"
    assert node.tree.children[1].labels.absolute_core_time is None
    assert [interval.start for interval in node.tree.intervals] == [date(2024, 3, 1)]


def test_resolve_plan_marks_all_sources_degraded_when_every_source_fails() -> None:
    degraded_u1 = Unit.model_validate(
        {
            "unit_id": "u1",
            "render_text": "坏时间1",
            "surface_fragments": [{"start": 0, "end": 3}],
            "needs_clarification": True,
            "reason_kind": "semantic_conflict",
            "content": {"content_kind": "standalone"},
        }
    )
    degraded_u2 = Unit.model_validate(
        {
            "unit_id": "u2",
            "render_text": "坏时间2",
            "surface_fragments": [{"start": 0, "end": 3}],
            "needs_clarification": True,
            "reason_kind": "llm_hard_fail",
            "content": {"content_kind": "standalone"},
        }
    )
    derived_unit = Unit(
        unit_id="u3",
        render_text="去年同期",
        surface_fragments=[{"start": 0, "end": 1}],
        content=DerivedContent(
            content_kind="derived",
            sources=[
                DerivationSource(source_unit_id="u1", transform={"kind": "shift_year", "offset": -1}),
                DerivationSource(source_unit_id="u2", transform={"kind": "shift_year", "offset": -1}),
            ],
        ),
    )
    plan = TimePlan(
        query="坏时间和坏时间，去年同期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[degraded_u1, degraded_u2, derived_unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())
    node = resolved.nodes["u3"]

    assert node.needs_clarification is True
    assert node.reason_kind == "all_sources_degraded"
    assert len(node.tree.children) == 2
    assert all(child.labels.degraded is True for child in node.tree.children)
    assert node.derived_from == ["u1", "u2"]


def test_resolve_plan_overrides_with_calendar_data_missing_when_calendar_event_cannot_resolve() -> None:
    plan = TimePlan(
        query="2027年国庆假期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u1",
                render_text="2027年国庆假期",
                surface_fragments=[{"start": 0, "end": 7}],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor=CalendarEvent(
                            kind="calendar_event",
                            region="CN",
                            event_key="national_day",
                            schedule_year_ref=ScheduleYearRef(year=2027),
                            scope="consecutive_rest",
                        ),
                        modifiers=[],
                    ),
                ),
            )
        ],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_MissingEventCalendar(_calendar()))

    assert resolved.nodes["u1"].needs_clarification is True
    assert resolved.nodes["u1"].reason_kind == "calendar_data_missing"
    assert resolved.nodes["u1"].tree is None


def test_resolve_plan_builds_comparisons_and_pair_level_degradation() -> None:
    degraded_unit = Unit.model_validate(
        {
            "unit_id": "u2",
            "render_text": "坏时间",
            "surface_fragments": [{"start": 0, "end": 2}],
            "needs_clarification": True,
            "reason_kind": "semantic_conflict",
            "content": {"content_kind": "standalone"},
        }
    )
    plan = TimePlan(
        query="2025年3月对比坏时间",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u1", 2025, 3), degraded_unit],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[ComparisonPair(subject_unit_id="u1", reference_unit_id="u2")],
            )
        ],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())
    pair = resolved.comparisons[0].pairs[0]

    assert pair.degraded is True
    assert pair.degraded_reason == "reference_needs_clarification"
    assert pair.subject_absolute_core_time.start == date(2025, 3, 1)
    assert pair.reference_absolute_core_time is None


def test_resolve_plan_propagates_resolver_side_reason_into_degraded_derived_source_labels() -> None:
    missing_calendar_unit = Unit(
        unit_id="u1",
        render_text="2027年国庆假期",
        surface_fragments=[{"start": 0, "end": 7}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=CalendarEvent(
                    kind="calendar_event",
                    region="CN",
                    event_key="national_day",
                    schedule_year_ref=ScheduleYearRef(year=2027),
                    scope="consecutive_rest",
                ),
                modifiers=[],
            ),
        ),
    )
    derived_unit = Unit(
        unit_id="u2",
        render_text="去年同期",
        surface_fragments=[{"start": 0, "end": 1}],
        content=DerivedContent(
            content_kind="derived",
            sources=[DerivationSource(source_unit_id="u1", transform={"kind": "shift_year", "offset": -1})],
        ),
    )
    plan = TimePlan(
        query="2027年国庆假期和去年同期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[missing_calendar_unit, derived_unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_MissingEventCalendar(_calendar()))

    assert resolved.nodes["u1"].reason_kind == "calendar_data_missing"
    assert resolved.nodes["u2"].needs_clarification is True
    assert resolved.nodes["u2"].reason_kind == "all_sources_degraded"
    assert resolved.nodes["u2"].tree.children[0].labels.degraded is True
    assert resolved.nodes["u2"].tree.children[0].labels.degraded_source_reason_kind == "calendar_data_missing"


def test_resolve_plan_uses_expansion_core_indices_for_expanded_pair() -> None:
    plan = TimePlan(
        query="2025年3月和5月对比2024年3月和5月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u1",
                render_text="2025年3月和5月",
                surface_fragments=[{"start": 0, "end": 1}],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor={
                            "kind": "enumeration_set",
                            "grain": "month",
                            "members": [
                                {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                                {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5},
                            ],
                        },
                        modifiers=[],
                    ),
                ),
            ),
            Unit(
                unit_id="u2",
                render_text="2024年3月和5月",
                surface_fragments=[{"start": 0, "end": 1}],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor={
                            "kind": "enumeration_set",
                            "grain": "month",
                            "members": [
                                {"kind": "named_period", "period_type": "month", "year": 2024, "month": 3},
                                {"kind": "named_period", "period_type": "month", "year": 2024, "month": 5},
                            ],
                        },
                        modifiers=[],
                    ),
                ),
            ),
        ],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[
                    ComparisonPair(
                        subject_unit_id="u1",
                        reference_unit_id="u2",
                        expansion=PairExpansion(
                            source_pair_index=0,
                            expansion_index=1,
                            expansion_cardinality=2,
                            subject_core_index=1,
                            reference_core_index=1,
                        ),
                    )
                ],
            )
        ],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())
    pair = resolved.comparisons[0].pairs[0]
    assert pair.subject_absolute_core_time.start == date(2025, 5, 1)
    assert pair.reference_absolute_core_time.start == date(2024, 5, 1)


def test_resolve_plan_keeps_filtered_collection_and_count_rolling_endpoints_single_core() -> None:
    workday_unit = Unit(
        unit_id="u1",
        render_text="2025年3月的工作日",
        surface_fragments=[{"start": 0, "end": 9}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
                modifiers=[CalendarFilter(kind="calendar_filter", day_class="workday")],
            ),
        ),
    )
    rolling_workday_unit = Unit(
        unit_id="u2",
        render_text="最近5个工作日",
        surface_fragments=[{"start": 0, "end": 6}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=RollingByCalendarUnit(
                    kind="rolling_by_calendar_unit",
                    length=5,
                    day_class="workday",
                    endpoint="today",
                    include_endpoint=True,
                ),
                modifiers=[],
            ),
        ),
    )
    plan = TimePlan(
        query="2025年3月的工作日对比最近5个工作日",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[workday_unit, rolling_workday_unit],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[ComparisonPair(subject_unit_id="u1", reference_unit_id="u2")],
            )
        ],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())
    pair = resolved.comparisons[0].pairs[0]

    assert pair.expansion is None
    assert pair.subject_absolute_core_time == Interval(
        start=date(2025, 3, 1),
        end=date(2025, 3, 31),
        end_inclusive=True,
    )
    assert pair.reference_absolute_core_time == Interval(
        start=date(2026, 4, 13),
        end=date(2026, 4, 17),
        end_inclusive=True,
    )


def test_resolve_plan_constructs_intervals_by_role() -> None:
    workday_unit = Unit(
        unit_id="u_workdays",
        render_text="2025年3月的工作日",
        surface_fragments=[{"start": 0, "end": 9}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
                modifiers=[CalendarFilter(kind="calendar_filter", day_class="workday")],
            ),
        ),
    )
    grouped_unit = Unit(
        unit_id="u_grouped",
        render_text="2025年每个季度",
        surface_fragments=[{"start": 0, "end": 7}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=GroupedTemporalValue(
                    kind="grouped_temporal_value",
                    parent=NamedPeriod(kind="named_period", period_type="year", year=2025),
                    child_grain="quarter",
                    selector="all",
                ),
                modifiers=[],
            ),
        ),
    )
    degraded_unit = Unit.model_validate(
        {
            "unit_id": "u_bad",
            "render_text": "坏时间",
            "surface_fragments": [{"start": 0, "end": 2}],
            "needs_clarification": True,
            "reason_kind": "semantic_conflict",
            "content": {"content_kind": "standalone"},
        }
    )
    derived_unit = Unit(
        unit_id="u_derived",
        render_text="去年同期",
        surface_fragments=[{"start": 0, "end": 1}],
        content=DerivedContent(
            content_kind="derived",
            sources=[
                DerivationSource(source_unit_id="u_month", transform={"kind": "shift_year", "offset": -1}),
                DerivationSource(source_unit_id="u_bad", transform={"kind": "shift_year", "offset": -1}),
            ],
        ),
    )
    plan = TimePlan(
        query="intervals",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u_month", 2025, 3), workday_unit, grouped_unit, degraded_unit, derived_unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert resolved.nodes["u_month"].tree.intervals == [resolved.nodes["u_month"].tree.labels.absolute_core_time]
    assert [interval.start for interval in resolved.nodes["u_grouped"].tree.intervals] == [
        date(2025, 1, 1),
        date(2025, 4, 1),
        date(2025, 7, 1),
        date(2025, 10, 1),
    ]
    assert resolved.nodes["u_workdays"].tree.intervals == [resolved.nodes["u_workdays"].tree.labels.absolute_core_time]
    assert len(resolved.nodes["u_workdays"].tree.children) > 1
    assert resolved.nodes["u_derived"].tree.intervals == [
        Interval(start=date(2024, 3, 1), end=date(2024, 3, 31), end_inclusive=True)
    ]
    assert resolved.nodes["u_derived"].tree.children[0].intervals == [
        Interval(start=date(2024, 3, 1), end=date(2024, 3, 31), end_inclusive=True)
    ]
    assert resolved.nodes["u_derived"].tree.children[1].intervals == []


def test_resolve_plan_emits_resolver_step_events(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str, dict[str, object], bool]] = []

    def _capture(layer: str, event: str, payload: dict[str, object], *, enabled: bool = True, **_: object) -> None:
        events.append((layer, event, payload, enabled))

    monkeypatch.setattr("time_query_service.new_resolver.log_pipeline_event", _capture)
    plan = TimePlan(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u1", 2025, 3)],
        comparisons=[],
    )

    resolve_plan(plan, business_calendar=_calendar(), pipeline_logging_enabled=True)

    assert events
    layer, event, payload, enabled = events[0]
    assert layer == "resolver"
    assert event == "resolver_step"
    assert enabled is True
    assert payload["unit_id"] == "u1"
    assert payload["anchor_kind"] == "named_period"
    assert payload["modifier_chain_len"] == 0
    assert isinstance(payload["duration_ms"], float)


def test_resolve_plan_preserves_clipped_natural_week_members_for_trailing_month_parent() -> None:
    unit = Unit(
        unit_id="u1",
        render_text="最近一个月每周",
        surface_fragments=[{"start": 0, "end": 7}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=GroupedTemporalValue(
                    kind="grouped_temporal_value",
                    parent=RollingWindow(
                        kind="rolling_window",
                        length=1,
                        unit="month",
                        endpoint="today",
                        include_endpoint=True,
                    ),
                    child_grain="week",
                    selector="all",
                ),
                modifiers=[],
            ),
        ),
    )
    plan = TimePlan(
        query="最近一个月每周",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert [(child.labels.absolute_core_time.start, child.labels.absolute_core_time.end) for child in resolved.nodes["u1"].tree.children] == [
        (date(2026, 3, 18), date(2026, 3, 22)),
        (date(2026, 3, 23), date(2026, 3, 29)),
        (date(2026, 3, 30), date(2026, 4, 5)),
        (date(2026, 4, 6), date(2026, 4, 12)),
        (date(2026, 4, 13), date(2026, 4, 17)),
    ]


def test_resolve_plan_preserves_clipped_natural_buckets_for_coarser_child_grains() -> None:
    units = [
        Unit(
            unit_id="u_months",
            render_text="最近一季度每月",
            surface_fragments=[{"start": 0, "end": 7}],
            content=StandaloneContent(
                content_kind="standalone",
                carrier=Carrier(
                    anchor=GroupedTemporalValue(
                        kind="grouped_temporal_value",
                        parent=RollingWindow(
                            kind="rolling_window",
                            length=1,
                            unit="quarter",
                            endpoint="today",
                            include_endpoint=True,
                        ),
                        child_grain="month",
                        selector="all",
                    ),
                    modifiers=[],
                ),
            ),
        ),
        Unit(
            unit_id="u_quarters",
            render_text="最近半年每季度",
            surface_fragments=[{"start": 0, "end": 7}],
            content=StandaloneContent(
                content_kind="standalone",
                carrier=Carrier(
                    anchor=GroupedTemporalValue(
                        kind="grouped_temporal_value",
                        parent=RollingWindow(
                            kind="rolling_window",
                            length=1,
                            unit="half_year",
                            endpoint="today",
                            include_endpoint=True,
                        ),
                        child_grain="quarter",
                        selector="all",
                    ),
                    modifiers=[],
                ),
            ),
        ),
        Unit(
            unit_id="u_halves",
            render_text="最近一年每半年",
            surface_fragments=[{"start": 0, "end": 7}],
            content=StandaloneContent(
                content_kind="standalone",
                carrier=Carrier(
                    anchor=GroupedTemporalValue(
                        kind="grouped_temporal_value",
                        parent=RollingWindow(
                            kind="rolling_window",
                            length=1,
                            unit="year",
                            endpoint="today",
                            include_endpoint=True,
                        ),
                        child_grain="half_year",
                        selector="all",
                    ),
                    modifiers=[],
                ),
            ),
        ),
    ]
    plan = TimePlan(
        query="coarser buckets",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=units,
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert [(c.labels.absolute_core_time.start, c.labels.absolute_core_time.end) for c in resolved.nodes["u_months"].tree.children] == [
        (date(2026, 1, 18), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 28)),
        (date(2026, 3, 1), date(2026, 3, 31)),
        (date(2026, 4, 1), date(2026, 4, 17)),
    ]
    assert [(c.labels.absolute_core_time.start, c.labels.absolute_core_time.end) for c in resolved.nodes["u_quarters"].tree.children] == [
        (date(2025, 10, 18), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 3, 31)),
        (date(2026, 4, 1), date(2026, 4, 17)),
    ]
    assert [(c.labels.absolute_core_time.start, c.labels.absolute_core_time.end) for c in resolved.nodes["u_halves"].tree.children] == [
        (date(2025, 4, 18), date(2025, 6, 30)),
        (date(2025, 7, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 4, 17)),
    ]


def test_resolve_plan_member_selection_collapse_and_multi_core_preservation() -> None:
    plan = TimePlan(
        query="member selection",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u_enum_first",
                render_text="2025年3月和5月中的第一个",
                surface_fragments=[{"start": 0, "end": 1}],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor={
                            "kind": "enumeration_set",
                            "grain": "month",
                            "members": [
                                {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                                {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5},
                            ],
                        },
                        modifiers=[{"kind": "member_selection", "selector": "first"}],
                    ),
                ),
            ),
            Unit(
                unit_id="u_enum_first_n",
                render_text="2025年3月和5月中的前两个",
                surface_fragments=[{"start": 0, "end": 1}],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor={
                            "kind": "enumeration_set",
                            "grain": "month",
                            "members": [
                                {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                                {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5},
                            ],
                        },
                        modifiers=[{"kind": "member_selection", "selector": "first_n", "n": 2}],
                    ),
                ),
            ),
            Unit(
                unit_id="u_gtv_first",
                render_text="2025年每个季度中的第一个",
                surface_fragments=[{"start": 0, "end": 1}],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor=GroupedTemporalValue(
                            kind="grouped_temporal_value",
                            parent=NamedPeriod(kind="named_period", period_type="year", year=2025),
                            child_grain="quarter",
                            selector="all",
                        ),
                        modifiers=[{"kind": "member_selection", "selector": "first"}],
                    ),
                ),
            ),
        ],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[
                    ComparisonPair(subject_unit_id="u_enum_first", reference_unit_id="u_month"),
                    ComparisonPair(
                        subject_unit_id="u_enum_first_n",
                        reference_unit_id="u_month",
                        expansion=PairExpansion(
                            source_pair_index=0,
                            expansion_index=0,
                            expansion_cardinality=2,
                            subject_core_index=0,
                            reference_core_index=None,
                        ),
                    ),
                    ComparisonPair(subject_unit_id="u_gtv_first", reference_unit_id="u_month"),
                ],
            )
        ],
    )
    plan.units.append(_standalone_month("u_month", 2025, 3))

    resolved = resolve_plan(plan, business_calendar=_calendar())

    enum_first_pair = resolved.comparisons[0].pairs[0]
    assert enum_first_pair.expansion is None
    assert enum_first_pair.subject_absolute_core_time == Interval(
        start=date(2025, 3, 1),
        end=date(2025, 3, 31),
        end_inclusive=True,
    )
    assert resolved.nodes["u_enum_first"].tree.labels.absolute_core_time == Interval(
        start=date(2025, 3, 1),
        end=date(2025, 3, 31),
        end_inclusive=True,
    )

    first_n_pair = resolved.comparisons[0].pairs[1]
    assert first_n_pair.expansion.subject_core_index == 0
    assert first_n_pair.subject_absolute_core_time == Interval(
        start=date(2025, 3, 1),
        end=date(2025, 3, 31),
        end_inclusive=True,
    )

    gtv_first_pair = resolved.comparisons[0].pairs[2]
    assert gtv_first_pair.expansion is None
    assert gtv_first_pair.subject_absolute_core_time == Interval(
        start=date(2025, 1, 1),
        end=date(2025, 3, 31),
        end_inclusive=True,
    )
    assert resolved.nodes["u_gtv_first"].tree.labels.absolute_core_time == Interval(
        start=date(2025, 1, 1),
        end=date(2025, 3, 31),
        end_inclusive=True,
    )


def test_resolve_plan_post_processor_canonicalizes_non_day_grain_expansion_before_resolution() -> None:
    stage_a = StageAOutput(
        query="A B C",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id="u1", render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}]),
            _stage_a_unit(unit_id="u2", render_text="B", self_contained_text="B", surface_fragments=[{"start": 2, "end": 3}]),
            _stage_a_unit(unit_id="u3", render_text="C", self_contained_text="C", surface_fragments=[{"start": 4, "end": 5}]),
        ],
        comparisons=[
            StageAComparisonOutput(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[StageAComparisonPairOutput(subject_unit_id="u1", reference_unit_id="u3")],
            )
        ],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "year", "year": 2025},
                "modifiers": [{"kind": "grain_expansion", "target_grain": "quarter"}],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
        "u2": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "year", "year": 2025},
                "modifiers": [{"kind": "grain_expansion", "target_grain": "month"}],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
        "u3": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2024, "month": 3},
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert plan.units[0].content.carrier.anchor.kind == "grouped_temporal_value"
    assert plan.units[1].content.carrier.anchor.kind == "grouped_temporal_value"

    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert resolved.nodes["u1"].tree.role == "grouped_member"
    assert len(resolved.nodes["u1"].tree.children) == 4
    assert resolved.nodes["u2"].tree.role == "grouped_member"
    assert len(resolved.nodes["u2"].tree.children) == 12
    assert len(resolved.comparisons[0].pairs) == 4
    assert [pair.expansion.subject_core_index for pair in resolved.comparisons[0].pairs] == [0, 1, 2, 3]


def test_resolve_plan_post_processor_canonicalizes_week_grain_and_preserves_weekly_pair_provenance() -> None:
    stage_a = StageAOutput(
        query="A B",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id="u1", render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}]),
            _stage_a_unit(unit_id="u2", render_text="B", self_contained_text="B", surface_fragments=[{"start": 2, "end": 3}]),
        ],
        comparisons=[
            StageAComparisonOutput(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[StageAComparisonPairOutput(subject_unit_id="u1", reference_unit_id="u2")],
            )
        ],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "year", "year": 2020},
                "modifiers": [{"kind": "grain_expansion", "target_grain": "week"}],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
        "u2": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2020, "month": 1},
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
    }

    plan = assemble_time_plan(stage_a, stage_b)
    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert plan.units[0].content.carrier.anchor.kind == "grouped_temporal_value"
    assert len(resolved.nodes["u1"].tree.children) == 53
    assert len(resolved.comparisons[0].pairs) == 53
    assert resolved.comparisons[0].pairs[0].expansion.subject_core_index == 0
    assert resolved.comparisons[0].pairs[-1].expansion.subject_core_index == 52


def test_resolve_plan_union_intervals_preserve_overlapping_calendar_events_in_order() -> None:
    unit = Unit(
        unit_id="u1",
        render_text="中秋假期和国庆假期",
        surface_fragments=[{"start": 0, "end": 8}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor={
                    "kind": "enumeration_set",
                    "grain": "calendar_event",
                    "members": [
                        {
                            "kind": "calendar_event",
                            "region": "CN",
                            "event_key": "mid_autumn",
                            "schedule_year_ref": {"year": 2025},
                            "scope": "consecutive_rest",
                        },
                        {
                            "kind": "calendar_event",
                            "region": "CN",
                            "event_key": "national_day",
                            "schedule_year_ref": {"year": 2025},
                            "scope": "consecutive_rest",
                        },
                    ],
                },
                modifiers=[],
            ),
        ),
    )
    plan = TimePlan(
        query="中秋假期和国庆假期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[unit],
        comparisons=[],
    )

    resolved = resolve_plan(plan, business_calendar=_calendar())

    assert resolved.nodes["u1"].tree.role == "union"
    assert resolved.nodes["u1"].tree.intervals == [
        Interval(start=date(2025, 10, 1), end=date(2025, 10, 8), end_inclusive=True),
        Interval(start=date(2025, 10, 1), end=date(2025, 10, 8), end_inclusive=True),
    ]


def test_resolve_plan_rejects_derivation_cycles() -> None:
    u1 = Unit(
        unit_id="u1",
        render_text="去年同期1",
        surface_fragments=[{"start": 0, "end": 1}],
        content=DerivedContent(
            content_kind="derived",
            sources=[DerivationSource(source_unit_id="u2", transform={"kind": "shift_year", "offset": -1})],
        ),
    )
    u2 = Unit(
        unit_id="u2",
        render_text="去年同期2",
        surface_fragments=[{"start": 0, "end": 1}],
        content=DerivedContent(
            content_kind="derived",
            sources=[DerivationSource(source_unit_id="u1", transform={"kind": "shift_year", "offset": -1})],
        ),
    )
    plan = TimePlan(
        query="cycle",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[u1, u2],
        comparisons=[],
    )

    with pytest.raises(ValueError):
        resolve_plan(plan, business_calendar=_calendar())
