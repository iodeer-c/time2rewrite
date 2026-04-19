from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from time_query_service.business_calendar import JsonBusinessCalendar
from time_query_service.carrier_materializer import materialize_carrier
from time_query_service.time_plan import (
    CalendarFilter,
    CalendarEvent,
    Carrier,
    DateRange,
    EnumerationSet,
    GrainExpansion,
    GroupedTemporalValue,
    MappedRange,
    MemberSelection,
    NamedPeriod,
    RollingByCalendarUnit,
    RollingWindow,
    ScheduleYearRef,
)


def _calendar() -> JsonBusinessCalendar:
    return JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))


def test_materialize_named_period_month_returns_atom_interval() -> None:
    tree = materialize_carrier(
        Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3), modifiers=[]),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "atom"
    assert tree.labels.absolute_core_time.start == date(2025, 3, 1)
    assert tree.labels.absolute_core_time.end == date(2025, 3, 31)
    assert tree.intervals == [tree.labels.absolute_core_time]


def test_materialize_trailing_week_uses_trailing_seven_days_not_iso_envelope() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingWindow(
                kind="rolling_window",
                length=1,
                unit="week",
                endpoint="today",
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.labels.absolute_core_time.start == date(2026, 4, 11)
    assert tree.labels.absolute_core_time.end == date(2026, 4, 17)


def test_materialize_trailing_month_uses_calendar_month_arithmetic() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingWindow(
                kind="rolling_window",
                length=1,
                unit="month",
                endpoint="today",
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.labels.absolute_core_time.start == date(2026, 3, 18)
    assert tree.labels.absolute_core_time.end == date(2026, 4, 17)


def test_materialize_trailing_month_with_include_endpoint_false_shifts_anchor_left_by_one_month() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingWindow(
                kind="rolling_window",
                length=1,
                unit="month",
                endpoint="today",
                include_endpoint=False,
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.labels.absolute_core_time.start == date(2026, 2, 18)
    assert tree.labels.absolute_core_time.end == date(2026, 3, 17)


def test_calendar_filter_workday_builds_filtered_collection_with_parent_bounds() -> None:
    calendar = _calendar()
    tree = materialize_carrier(
        Carrier(
            anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
            modifiers=[CalendarFilter(kind="calendar_filter", day_class="workday")],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=calendar,
    )

    assert tree.role == "filtered_collection"
    assert tree.labels.absolute_core_time.start == date(2025, 3, 1)
    assert tree.labels.absolute_core_time.end == date(2025, 3, 31)
    assert tree.children
    for child in tree.children:
        assert child.role == "atom"
        assert calendar.is_workday(region="CN", d=child.labels.absolute_core_time.start)


def test_rolling_by_calendar_unit_workday_includes_makeup_workday() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingByCalendarUnit(
                kind="rolling_by_calendar_unit",
                length=2,
                day_class="workday",
                endpoint="today",
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=date(2025, 10, 11),
        business_calendar=_calendar(),
    )

    selected_days = [child.labels.absolute_core_time.start for child in tree.children]
    assert selected_days == [date(2025, 10, 10), date(2025, 10, 11)]
    assert tree.labels.absolute_core_time.start == date(2025, 10, 10)
    assert tree.labels.absolute_core_time.end == date(2025, 10, 11)


def test_rolling_by_calendar_unit_holiday_counts_days_not_event_spans() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingByCalendarUnit(
                kind="rolling_by_calendar_unit",
                length=3,
                day_class="holiday",
                endpoint="today",
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=date(2025, 10, 3),
        business_calendar=_calendar(),
    )

    selected_days = [child.labels.absolute_core_time.start for child in tree.children]
    assert selected_days == [date(2025, 10, 1), date(2025, 10, 2), date(2025, 10, 3)]


def test_rolling_by_calendar_unit_weekend_excludes_makeup_workday_weekends() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingByCalendarUnit(
                kind="rolling_by_calendar_unit",
                length=1,
                day_class="weekend",
                endpoint="today",
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=date(2025, 9, 28),
        business_calendar=_calendar(),
    )

    selected_days = [child.labels.absolute_core_time.start for child in tree.children]
    assert selected_days == [date(2025, 9, 27)]


def test_enumeration_set_materializes_to_union_in_declared_order() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=EnumerationSet(
                kind="enumeration_set",
                grain="month",
                members=[
                    NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
                    NamedPeriod(kind="named_period", period_type="month", year=2025, month=5),
                ],
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "union"
    assert [child.labels.absolute_core_time.start for child in tree.children] == [date(2025, 3, 1), date(2025, 5, 1)]
    assert [interval.start for interval in tree.intervals] == [date(2025, 3, 1), date(2025, 5, 1)]


def test_grouped_temporal_value_year_quarter_materializes_to_grouped_members() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=GroupedTemporalValue(
                kind="grouped_temporal_value",
                parent=NamedPeriod(kind="named_period", period_type="year", year=2025),
                child_grain="quarter",
                selector="all",
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "grouped_member"
    assert [child.labels.absolute_core_time.start for child in tree.children] == [
        date(2025, 1, 1),
        date(2025, 4, 1),
        date(2025, 7, 1),
        date(2025, 10, 1),
    ]
    assert [interval.start for interval in tree.intervals] == [date(2025, 1, 1), date(2025, 4, 1), date(2025, 7, 1), date(2025, 10, 1)]


def test_grouped_temporal_value_trailing_month_week_uses_clipped_natural_buckets() -> None:
    tree = materialize_carrier(
        Carrier(
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
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "grouped_member"
    assert [(child.labels.absolute_core_time.start, child.labels.absolute_core_time.end) for child in tree.children] == [
        (date(2026, 3, 18), date(2026, 3, 22)),
        (date(2026, 3, 23), date(2026, 3, 29)),
        (date(2026, 3, 30), date(2026, 4, 5)),
        (date(2026, 4, 6), date(2026, 4, 12)),
        (date(2026, 4, 13), date(2026, 4, 17)),
    ]


def test_grouped_temporal_value_trailing_quarter_month_uses_clipped_natural_buckets() -> None:
    tree = materialize_carrier(
        Carrier(
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
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert [(child.labels.absolute_core_time.start, child.labels.absolute_core_time.end) for child in tree.children] == [
        (date(2026, 1, 18), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 28)),
        (date(2026, 3, 1), date(2026, 3, 31)),
        (date(2026, 4, 1), date(2026, 4, 17)),
    ]


def test_grouped_temporal_value_trailing_half_year_quarter_uses_clipped_natural_buckets() -> None:
    tree = materialize_carrier(
        Carrier(
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
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert [(child.labels.absolute_core_time.start, child.labels.absolute_core_time.end) for child in tree.children] == [
        (date(2025, 10, 18), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 3, 31)),
        (date(2026, 4, 1), date(2026, 4, 17)),
    ]


def test_grouped_temporal_value_trailing_year_half_year_uses_clipped_natural_buckets() -> None:
    tree = materialize_carrier(
        Carrier(
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
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert [(child.labels.absolute_core_time.start, child.labels.absolute_core_time.end) for child in tree.children] == [
        (date(2025, 4, 18), date(2025, 6, 30)),
        (date(2025, 7, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 4, 17)),
    ]


def test_mapped_range_bounded_pair_materializes_one_range_per_paired_boundary() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=MappedRange(
                kind="mapped_range",
                mode="bounded_pair",
                start=EnumerationSet(
                    kind="enumeration_set",
                    grain="day",
                    members=[
                        DateRange(kind="date_range", start_date=date(2025, 3, 1), end_date=date(2025, 3, 1)),
                        DateRange(kind="date_range", start_date=date(2025, 5, 1), end_date=date(2025, 5, 1)),
                    ],
                ),
                end=EnumerationSet(
                    kind="enumeration_set",
                    grain="day",
                    members=[
                        DateRange(kind="date_range", start_date=date(2025, 3, 10), end_date=date(2025, 3, 10)),
                        DateRange(kind="date_range", start_date=date(2025, 5, 10), end_date=date(2025, 5, 10)),
                    ],
                ),
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "grouped_member"
    assert [(interval.start, interval.end) for interval in tree.intervals] == [
        (date(2025, 3, 1), date(2025, 3, 10)),
        (date(2025, 5, 1), date(2025, 5, 10)),
    ]


def test_mapped_range_bounded_pair_applies_minimal_non_retreat_for_cross_year_month_range() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=MappedRange(
                kind="mapped_range",
                mode="bounded_pair",
                start=NamedPeriod(kind="named_period", period_type="month", year=2025, month=12),
                end=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "atom"
    assert tree.labels.absolute_core_time.start == date(2025, 12, 1)
    assert tree.labels.absolute_core_time.end == date(2026, 3, 31)


def test_mapped_range_bounded_pair_supports_cross_grain_quarter_to_month() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=MappedRange(
                kind="mapped_range",
                mode="bounded_pair",
                start=NamedPeriod(kind="named_period", period_type="quarter", year=2025, quarter=3),
                end=NamedPeriod(kind="named_period", period_type="month", year=2025, month=10),
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "atom"
    assert tree.labels.absolute_core_time.start == date(2025, 7, 1)
    assert tree.labels.absolute_core_time.end == date(2025, 10, 31)


def test_mapped_range_bounded_pair_supports_month_to_day() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=MappedRange(
                kind="mapped_range",
                mode="bounded_pair",
                start=NamedPeriod(kind="named_period", period_type="month", year=2025, month=9),
                end=NamedPeriod(kind="named_period", period_type="day", year=2025, date=date(2025, 10, 15)),
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "atom"
    assert tree.labels.absolute_core_time.start == date(2025, 9, 1)
    assert tree.labels.absolute_core_time.end == date(2025, 10, 15)


def test_mapped_range_bounded_pair_rejects_unsupported_calendar_event_endpoint() -> None:
    with pytest.raises(NotImplementedError):
        materialize_carrier(
            Carrier(
                anchor=MappedRange(
                    kind="mapped_range",
                    mode="bounded_pair",
                    start=NamedPeriod(kind="named_period", period_type="month", year=2025, month=9),
                    end=CalendarEvent(
                        kind="calendar_event",
                        region="CN",
                        event_key="national_day",
                        schedule_year_ref=ScheduleYearRef(year=2025),
                        scope="consecutive_rest",
                    ),
                ),
                modifiers=[],
            ),
            system_date=date(2026, 4, 17),
            business_calendar=_calendar(),
        )


def test_mapped_range_period_to_date_materializes_from_system_date() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=MappedRange(
                kind="mapped_range",
                mode="period_to_date",
                period_grain="month",
                anchor_ref="system_date",
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "atom"
    assert tree.labels.absolute_core_time.start == date(2026, 4, 1)
    assert tree.labels.absolute_core_time.end == date(2026, 4, 17)


def test_mapped_range_rolling_map_materializes_one_window_per_endpoint_member() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=MappedRange(
                kind="mapped_range",
                mode="rolling_map",
                length=1,
                unit="week",
                include_endpoint=True,
                endpoint_set=EnumerationSet(
                    kind="enumeration_set",
                    grain="day",
                    members=[
                        DateRange(kind="date_range", start_date=date(2026, 4, 17), end_date=date(2026, 4, 17)),
                        DateRange(kind="date_range", start_date=date(2026, 4, 30), end_date=date(2026, 4, 30)),
                    ],
                ),
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "grouped_member"
    assert [(interval.start, interval.end) for interval in tree.intervals] == [
        (date(2026, 4, 11), date(2026, 4, 17)),
        (date(2026, 4, 24), date(2026, 4, 30)),
    ]


def test_member_selection_first_preserves_grouped_member_role_with_one_child() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=GroupedTemporalValue(
                kind="grouped_temporal_value",
                parent=NamedPeriod(kind="named_period", period_type="year", year=2025),
                child_grain="quarter",
                selector="all",
            ),
            modifiers=[MemberSelection(kind="member_selection", selector="first")],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "grouped_member"
    assert len(tree.children) == 1
    assert tree.children[0].labels.absolute_core_time.start == date(2025, 1, 1)
    assert tree.intervals == [tree.children[0].labels.absolute_core_time]


def test_member_selection_first_n_keeps_multiple_grouped_members() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=GroupedTemporalValue(
                kind="grouped_temporal_value",
                parent=NamedPeriod(kind="named_period", period_type="year", year=2025),
                child_grain="quarter",
                selector="all",
            ),
            modifiers=[MemberSelection(kind="member_selection", selector="first_n", n=2)],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "grouped_member"
    assert [child.labels.absolute_core_time.start for child in tree.children] == [date(2025, 1, 1), date(2025, 4, 1)]


def test_grain_expansion_day_then_calendar_filter_selects_from_expanded_day_set() -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
            modifiers=[
                GrainExpansion(kind="grain_expansion", target_grain="day"),
                CalendarFilter(kind="calendar_filter", day_class="workday"),
            ],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "filtered_collection"
    assert tree.children
    assert tree.labels.absolute_core_time.start == date(2025, 3, 1)
    assert tree.labels.absolute_core_time.end == date(2025, 3, 31)


@pytest.mark.parametrize(
    ("anchor", "expected_first", "expected_count"),
    [
        (
            NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
            date(2025, 3, 1),
            31,
        ),
        (
            DateRange(kind="date_range", start_date=date(2025, 3, 10), end_date=date(2025, 3, 12)),
            date(2025, 3, 10),
            3,
        ),
        (
            {"kind": "relative_window", "grain": "week", "offset_units": 0},
            date(2026, 4, 13),
            7,
        ),
        (
            {
                "kind": "rolling_window",
                "length": 1,
                "unit": "week",
                "endpoint": "today",
                "include_endpoint": True,
            },
            date(2026, 4, 11),
            7,
        ),
        (
            {
                "kind": "calendar_event",
                "region": "CN",
                "event_key": "national_day",
                "schedule_year_ref": {"year": 2025},
                "scope": "consecutive_rest",
            },
            date(2025, 10, 1),
            8,
        ),
    ],
)
def test_grain_expansion_day_is_supported_on_each_continuous_anchor_kind(
    anchor: object,
    expected_first: date,
    expected_count: int,
) -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=anchor,
            modifiers=[GrainExpansion(kind="grain_expansion", target_grain="day")],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.role == "grouped_member"
    assert tree.children[0].labels.absolute_core_time.start == expected_first
    assert len(tree.children) == expected_count


def test_modifier_order_sensitivity_is_observable() -> None:
    ordered = materialize_carrier(
        Carrier(
            anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
            modifiers=[
                CalendarFilter(kind="calendar_filter", day_class="workday"),
                MemberSelection(kind="member_selection", selector="last"),
            ],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert ordered.children[0].labels.absolute_core_time.start == date(2025, 3, 31)

    with pytest.raises(ValueError):
        materialize_carrier(
            Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
                modifiers=[
                    MemberSelection(kind="member_selection", selector="last"),
                    CalendarFilter(kind="calendar_filter", day_class="workday"),
                ],
            ),
            system_date=date(2026, 4, 17),
            business_calendar=_calendar(),
        )


@pytest.mark.parametrize(
    ("endpoint", "unit", "expected_start", "expected_end"),
    [
        ("today", "day", date(2026, 4, 17), date(2026, 4, 17)),
        ("today", "week", date(2026, 4, 11), date(2026, 4, 17)),
        ("today", "month", date(2026, 3, 18), date(2026, 4, 17)),
        ("today", "quarter", date(2026, 1, 18), date(2026, 4, 17)),
        ("today", "half_year", date(2025, 10, 18), date(2026, 4, 17)),
        ("today", "year", date(2025, 4, 18), date(2026, 4, 17)),
        ("yesterday", "day", date(2026, 4, 16), date(2026, 4, 16)),
        ("yesterday", "week", date(2026, 4, 10), date(2026, 4, 16)),
        ("yesterday", "month", date(2026, 3, 17), date(2026, 4, 16)),
        ("yesterday", "quarter", date(2026, 1, 17), date(2026, 4, 16)),
        ("yesterday", "half_year", date(2025, 10, 17), date(2026, 4, 16)),
        ("yesterday", "year", date(2025, 4, 17), date(2026, 4, 16)),
        ("this_month_end", "day", date(2026, 4, 30), date(2026, 4, 30)),
        ("this_month_end", "week", date(2026, 4, 24), date(2026, 4, 30)),
        ("this_month_end", "month", date(2026, 3, 31), date(2026, 4, 30)),
        ("this_month_end", "quarter", date(2026, 1, 31), date(2026, 4, 30)),
        ("this_month_end", "half_year", date(2025, 10, 31), date(2026, 4, 30)),
        ("this_month_end", "year", date(2025, 5, 1), date(2026, 4, 30)),
        ("previous_complete", "day", date(2026, 4, 16), date(2026, 4, 16)),
        ("previous_complete", "week", date(2026, 4, 6), date(2026, 4, 12)),
        ("previous_complete", "month", date(2026, 3, 1), date(2026, 3, 31)),
        ("previous_complete", "quarter", date(2026, 1, 1), date(2026, 3, 31)),
        ("previous_complete", "half_year", date(2025, 7, 1), date(2025, 12, 31)),
        ("previous_complete", "year", date(2025, 1, 1), date(2025, 12, 31)),
    ],
)
def test_rolling_window_endpoint_matrix(endpoint: str, unit: str, expected_start: date, expected_end: date) -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingWindow(
                kind="rolling_window",
                length=1,
                unit=unit,
                endpoint=endpoint,
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=date(2026, 4, 17),
        business_calendar=_calendar(),
    )

    assert tree.labels.absolute_core_time.start == expected_start
    assert tree.labels.absolute_core_time.end == expected_end


@pytest.mark.parametrize(
    ("system_date_value", "unit", "expected_start", "expected_end"),
    [
        (date(2026, 3, 31), "month", date(2026, 3, 1), date(2026, 3, 31)),
        (date(2024, 2, 29), "year", date(2023, 3, 1), date(2024, 2, 29)),
        (date(2026, 8, 31), "half_year", date(2026, 3, 1), date(2026, 8, 31)),
        (date(2026, 5, 31), "quarter", date(2026, 3, 1), date(2026, 5, 31)),
    ],
)
def test_rolling_window_uses_clipping_aware_calendar_arithmetic(
    system_date_value: date, unit: str, expected_start: date, expected_end: date
) -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingWindow(
                kind="rolling_window",
                length=1,
                unit=unit,
                endpoint="today",
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=system_date_value,
        business_calendar=_calendar(),
    )

    assert tree.labels.absolute_core_time.start == expected_start
    assert tree.labels.absolute_core_time.end == expected_end


@pytest.mark.parametrize(
    ("unit", "system_date_value"),
    [
        ("day", date(2026, 4, 17)),
        ("week", date(2026, 4, 19)),
        ("month", date(2026, 4, 17)),
        ("quarter", date(2026, 4, 17)),
        ("half_year", date(2026, 7, 2)),
        ("year", date(2026, 1, 5)),
    ],
)
def test_rolling_window_include_endpoint_false_matches_left_shift_then_materialize(unit: str, system_date_value: date) -> None:
    excluded = materialize_carrier(
        Carrier(
            anchor=RollingWindow(
                kind="rolling_window",
                length=1,
                unit=unit,
                endpoint="today",
                include_endpoint=False,
            ),
            modifiers=[],
        ),
        system_date=system_date_value,
        business_calendar=_calendar(),
    )
    shifted_reference = materialize_carrier(
        Carrier(
            anchor=RollingWindow(
                kind="rolling_window",
                length=1,
                unit=unit,
                endpoint="yesterday" if unit == "day" else "today",
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=system_date_value if unit != "day" else system_date_value,
        business_calendar=_calendar(),
    )

    if unit != "day":
        manual = materialize_carrier(
            Carrier(
                anchor=RollingWindow(
                    kind="rolling_window",
                    length=1,
                    unit=unit,
                    endpoint="today",
                    include_endpoint=True,
                ),
                modifiers=[{"kind": "offset", "value": -1, "unit": unit}],
            ),
            system_date=system_date_value,
            business_calendar=_calendar(),
        )
        assert excluded.labels.absolute_core_time == manual.labels.absolute_core_time
    else:
        assert excluded.labels.absolute_core_time == shifted_reference.labels.absolute_core_time


def test_rolling_window_zero_length_is_rejected_clearly() -> None:
    with pytest.raises(ValueError, match="length must be > 0"):
        RollingWindow(kind="rolling_window", length=0, unit="day", endpoint="today", include_endpoint=True)

    with pytest.raises(ValueError, match="length must be > 0"):
        RollingByCalendarUnit(
            kind="rolling_by_calendar_unit",
            length=0,
            day_class="workday",
            endpoint="today",
            include_endpoint=True,
        )


@pytest.mark.parametrize(
    ("endpoint", "include_endpoint", "system_date_value", "expected_days"),
    [
        ("today", True, date(2025, 10, 11), [date(2025, 10, 10), date(2025, 10, 11)]),
        ("yesterday", True, date(2025, 10, 11), [date(2025, 10, 9), date(2025, 10, 10)]),
        ("today", False, date(2025, 10, 11), [date(2025, 10, 9), date(2025, 10, 10)]),
        ("this_month_end", True, date(2025, 9, 27), [date(2025, 9, 29), date(2025, 9, 30)]),
        ("previous_complete", True, date(2025, 10, 11), [date(2025, 10, 9), date(2025, 10, 10)]),
    ],
)
def test_rolling_by_calendar_unit_supports_all_endpoint_reference_day_variants(
    endpoint: str,
    include_endpoint: bool,
    system_date_value: date,
    expected_days: list[date],
) -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingByCalendarUnit(
                kind="rolling_by_calendar_unit",
                length=2,
                day_class="workday",
                endpoint=endpoint,
                include_endpoint=include_endpoint,
            ),
            modifiers=[],
        ),
        system_date=system_date_value,
        business_calendar=_calendar(),
    )

    assert [child.labels.absolute_core_time.start for child in tree.children] == expected_days


@pytest.mark.parametrize(
    ("system_date_value", "unit", "expected_start", "expected_end"),
    [
        (date(2026, 4, 30), "day", date(2026, 4, 29), date(2026, 4, 29)),
        (date(2026, 4, 30), "week", date(2026, 4, 20), date(2026, 4, 26)),
        (date(2026, 4, 30), "month", date(2026, 3, 1), date(2026, 3, 31)),
        (date(2026, 4, 30), "quarter", date(2026, 1, 1), date(2026, 3, 31)),
        (date(2026, 4, 30), "half_year", date(2025, 7, 1), date(2025, 12, 31)),
        (date(2026, 4, 30), "year", date(2025, 1, 1), date(2025, 12, 31)),
        (date(2026, 7, 1), "day", date(2026, 6, 30), date(2026, 6, 30)),
        (date(2026, 7, 1), "week", date(2026, 6, 22), date(2026, 6, 28)),
        (date(2026, 7, 1), "month", date(2026, 6, 1), date(2026, 6, 30)),
        (date(2026, 7, 1), "quarter", date(2026, 4, 1), date(2026, 6, 30)),
        (date(2026, 7, 1), "half_year", date(2026, 1, 1), date(2026, 6, 30)),
        (date(2026, 7, 1), "year", date(2025, 1, 1), date(2025, 12, 31)),
        (date(2026, 12, 31), "day", date(2026, 12, 30), date(2026, 12, 30)),
        (date(2026, 12, 31), "week", date(2026, 12, 21), date(2026, 12, 27)),
        (date(2026, 12, 31), "month", date(2026, 11, 1), date(2026, 11, 30)),
        (date(2026, 12, 31), "quarter", date(2026, 7, 1), date(2026, 9, 30)),
        (date(2026, 12, 31), "half_year", date(2026, 1, 1), date(2026, 6, 30)),
        (date(2026, 12, 31), "year", date(2025, 1, 1), date(2025, 12, 31)),
        (date(2026, 1, 5), "day", date(2026, 1, 4), date(2026, 1, 4)),
        (date(2026, 1, 5), "week", date(2025, 12, 29), date(2026, 1, 4)),
        (date(2026, 1, 5), "month", date(2025, 12, 1), date(2025, 12, 31)),
        (date(2026, 1, 5), "quarter", date(2025, 10, 1), date(2025, 12, 31)),
        (date(2026, 1, 5), "half_year", date(2025, 7, 1), date(2025, 12, 31)),
        (date(2026, 1, 5), "year", date(2025, 1, 1), date(2025, 12, 31)),
    ],
)
def test_previous_complete_uses_boundary_aware_complete_windows(
    system_date_value: date,
    unit: str,
    expected_start: date,
    expected_end: date,
) -> None:
    tree = materialize_carrier(
        Carrier(
            anchor=RollingWindow(
                kind="rolling_window",
                length=1,
                unit=unit,
                endpoint="previous_complete",
                include_endpoint=True,
            ),
            modifiers=[],
        ),
        system_date=system_date_value,
        business_calendar=_calendar(),
    )

    assert tree.labels.absolute_core_time.start == expected_start
    assert tree.labels.absolute_core_time.end == expected_end
