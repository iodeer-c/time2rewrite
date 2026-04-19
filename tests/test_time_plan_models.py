from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from time_query_service.resolved_plan import (
    Interval,
    IntervalTree,
    ResolvedNode,
)
from time_query_service.time_plan import (
    CalendarFilter,
    CalendarEvent,
    Carrier,
    Comparison,
    ComparisonPair,
    DateRange,
    DerivedContent,
    DerivationSource,
    EnumerationSet,
    GrainExpansion,
    GroupedTemporalValue,
    MappedRange,
    MemberSelection,
    NamedPeriod,
    Offset,
    PairExpansion,
    RelativeWindow,
    RollingByCalendarUnit,
    RollingWindow,
    StandaloneContent,
    SurfaceFragment,
    TimePlan,
    Unit,
)


def _surface_fragment() -> SurfaceFragment:
    return SurfaceFragment(start=0, end=6)


def _named_period_anchor() -> NamedPeriod:
    return NamedPeriod(kind="named_period", period_type="month", year=2025, month=3)


def _standalone_unit_payload() -> dict:
    return {
        "unit_id": "u1",
        "render_text": "2025年3月",
        "surface_fragments": [{"start": 0, "end": 6}],
        "needs_clarification": False,
        "content": {
            "content_kind": "standalone",
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [],
            },
        },
    }


@pytest.mark.parametrize(
    ("anchor_payload", "expected_type"),
    [
        ({"kind": "named_period", "period_type": "month", "year": 2025, "month": 3}, NamedPeriod),
        (
            {
                "kind": "date_range",
                "start_date": "2025-03-01",
                "end_date": "2025-03-31",
                "end_inclusive": True,
            },
            DateRange,
        ),
        ({"kind": "relative_window", "grain": "month", "offset_units": 0}, RelativeWindow),
        (
            {
                "kind": "rolling_window",
                "length": 1,
                "unit": "month",
                "endpoint": "today",
                "include_endpoint": True,
            },
            RollingWindow,
        ),
        (
            {
                "kind": "rolling_by_calendar_unit",
                "length": 5,
                "day_class": "workday",
                "endpoint": "today",
                "include_endpoint": True,
            },
            RollingByCalendarUnit,
        ),
        (
            {
                "kind": "enumeration_set",
                "grain": "month",
                "members": [
                    {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                    {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5},
                ],
            },
            EnumerationSet,
        ),
        (
            {
                "kind": "grouped_temporal_value",
                "parent": {"kind": "named_period", "period_type": "year", "year": 2025},
                "child_grain": "quarter",
                "selector": "all",
            },
            GroupedTemporalValue,
        ),
        (
            {
                "kind": "calendar_event",
                "region": "CN",
                "event_key": "qingming",
                "schedule_year_ref": {"year": 2025},
                "scope": "consecutive_rest",
            },
            CalendarEvent,
        ),
        (
            {
                "kind": "mapped_range",
                "mode": "period_to_date",
                "period_grain": "month",
                "anchor_ref": {"ref": "system_date"},
            },
            MappedRange,
        ),
    ],
)
def test_anchor_discriminated_union_dispatch(anchor_payload: dict, expected_type: type) -> None:
    carrier = Carrier.model_validate({"anchor": anchor_payload, "modifiers": []})
    assert isinstance(carrier.anchor, expected_type)


@pytest.mark.parametrize(
    ("modifier_payload", "expected_type"),
    [
        ({"kind": "grain_expansion", "target_grain": "day"}, GrainExpansion),
        ({"kind": "calendar_filter", "day_class": "workday"}, CalendarFilter),
        ({"kind": "member_selection", "selector": "first_n", "n": 2}, MemberSelection),
        ({"kind": "offset", "value": -1, "unit": "day"}, Offset),
    ],
)
def test_modifier_discriminated_union_dispatch(modifier_payload: dict, expected_type: type) -> None:
    carrier = Carrier.model_validate(
        {
            "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
            "modifiers": [modifier_payload],
        }
    )
    assert len(carrier.modifiers) == 1
    assert isinstance(carrier.modifiers[0], expected_type)


def test_healthy_standalone_unit_requires_carrier() -> None:
    with pytest.raises(ValidationError):
        Unit.model_validate(
            {
                "unit_id": "u1",
                "render_text": "最近5个休息日",
                "surface_fragments": [{"start": 0, "end": 7}],
                "needs_clarification": False,
                "content": {"content_kind": "standalone"},
            }
        )


def test_degraded_standalone_unit_may_omit_carrier() -> None:
    unit = Unit.model_validate(
        {
            "unit_id": "u1",
            "render_text": "最近5个休息日",
            "surface_fragments": [{"start": 0, "end": 7}],
            "needs_clarification": True,
            "reason_kind": "unsupported_calendar_grain_rolling",
            "content": {"content_kind": "standalone"},
        }
    )
    assert isinstance(unit.content, StandaloneContent)
    assert unit.content.carrier is None


def test_derived_content_requires_non_empty_sources() -> None:
    with pytest.raises(ValidationError):
        DerivedContent.model_validate({"content_kind": "derived", "sources": []})


def test_unit_xor_rejects_missing_reason_for_degraded_unit() -> None:
    with pytest.raises(ValidationError):
        Unit.model_validate(
            {
                "unit_id": "u1",
                "render_text": "最近5个休息日",
                "surface_fragments": [{"start": 0, "end": 7}],
                "needs_clarification": True,
                "content": {"content_kind": "standalone"},
            }
        )


def test_comparison_requires_non_empty_unique_pairs() -> None:
    with pytest.raises(ValidationError):
        Comparison.model_validate({"comparison_id": "c1", "anchor_text": "对比", "pairs": []})

    pair = {"subject_unit_id": "u1", "reference_unit_id": "u2"}
    with pytest.raises(ValidationError):
        Comparison.model_validate({"comparison_id": "c1", "anchor_text": "对比", "pairs": [pair, pair]})


def test_multi_source_derivation_accepts_one_or_more_sources() -> None:
    single = DerivedContent.model_validate(
        {
            "content_kind": "derived",
            "sources": [{"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}}],
        }
    )
    double = DerivedContent.model_validate(
        {
            "content_kind": "derived",
            "sources": [
                {"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}},
                {"source_unit_id": "u2", "transform": {"kind": "shift_year", "offset": -1}},
            ],
        }
    )

    assert len(single.sources) == 1
    assert len(double.sources) == 2


def test_closed_enums_reject_unknown_values() -> None:
    with pytest.raises(ValidationError):
        RollingWindow.model_validate(
            {
                "kind": "rolling_window",
                "length": 1,
                "unit": "month",
                "endpoint": "tomorrow",
                "include_endpoint": True,
            }
        )

    with pytest.raises(ValidationError):
        RollingByCalendarUnit.model_validate(
            {
                "kind": "rolling_by_calendar_unit",
                "length": 5,
                "day_class": "trading_day",
                "endpoint": "today",
                "include_endpoint": True,
            }
        )

    with pytest.raises(ValidationError):
        IntervalTree.model_validate({"role": "collection", "intervals": [], "children": [], "labels": {}})

    with pytest.raises(ValidationError):
        ResolvedNode.model_validate({"needs_clarification": True, "reason_kind": "bad_reason"})


def test_resolved_node_validates_derived_from_against_child_source_ids() -> None:
    tree = {
        "role": "derived",
        "intervals": [{"start": "2024-03-01", "end": "2024-03-31", "end_inclusive": True}],
        "children": [
            {
                "role": "derived_source",
                "intervals": [{"start": "2024-03-01", "end": "2024-03-31", "end_inclusive": True}],
                "children": [],
                "labels": {
                    "source_unit_id": "u1",
                    "absolute_core_time": {
                        "start": "2024-03-01",
                        "end": "2024-03-31",
                        "end_inclusive": True,
                    },
                },
            }
        ],
        "labels": {},
    }
    node = ResolvedNode.model_validate({"tree": tree, "derived_from": ["u1"]})
    assert node.derived_from == ["u1"]

    with pytest.raises(ValidationError):
        ResolvedNode.model_validate({"tree": tree, "derived_from": ["u2"]})


def test_round_trip_serialization_for_interval_rolling_pair_expansion_and_reason_kind() -> None:
    pair = ComparisonPair(
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
    rolling = RollingWindow(
        kind="rolling_window",
        length=1,
        unit="quarter",
        endpoint="previous_complete",
        include_endpoint=False,
    )
    rolling_by_calendar = RollingByCalendarUnit(
        kind="rolling_by_calendar_unit",
        length=3,
        day_class="holiday",
        endpoint="yesterday",
        include_endpoint=True,
    )
    interval = Interval(start=date(2025, 3, 1), end=date(2025, 3, 31), end_inclusive=True)
    node = ResolvedNode(needs_clarification=True, reason_kind="calendar_data_missing")

    assert ComparisonPair.model_validate(pair.model_dump(mode="json")) == pair
    assert RollingWindow.model_validate(rolling.model_dump(mode="json")) == rolling
    assert RollingByCalendarUnit.model_validate(rolling_by_calendar.model_dump(mode="json")) == rolling_by_calendar
    assert Interval.model_validate(interval.model_dump(mode="json")) == interval
    assert ResolvedNode.model_validate(node.model_dump(mode="json")) == node


def test_time_plan_accepts_basic_plan_payload() -> None:
    plan = TimePlan.model_validate(
        {
            "query": "2025年3月和去年同期对比",
            "system_date": "2026-04-19",
            "timezone": "Asia/Shanghai",
            "units": [
                _standalone_unit_payload(),
                {
                    "unit_id": "u2",
                    "render_text": "去年同期",
                    "surface_fragments": [{"start": 7, "end": 11}],
                    "needs_clarification": False,
                    "content": {
                        "content_kind": "derived",
                        "sources": [{"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}}],
                    },
                },
            ],
            "comparisons": [
                {
                    "comparison_id": "c1",
                    "anchor_text": "对比",
                    "pairs": [{"subject_unit_id": "u1", "reference_unit_id": "u2"}],
                }
            ],
        }
    )
    assert len(plan.units) == 2
    assert len(plan.comparisons) == 1
