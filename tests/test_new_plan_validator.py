from __future__ import annotations

from datetime import date

import pytest

from time_query_service.time_plan import Carrier, Comparison, ComparisonPair, GrainExpansion, NamedPeriod, StandaloneContent, TimePlan, Unit


def _standalone_month(unit_id: str, year: int, month: int) -> Unit:
    render_text = f"{year}年{month}月"
    return Unit(
        unit_id=unit_id,
        render_text=render_text,
        surface_fragments=[{"start": 0, "end": len(render_text)}],
        content=StandaloneContent(
            content_kind="standalone",
            carrier=Carrier(
                anchor=NamedPeriod(kind="named_period", period_type="month", year=year, month=month),
                modifiers=[],
            ),
        ),
    )


def test_validate_time_plan_accepts_valid_plan() -> None:
    from time_query_service.new_plan_validator import validate_time_plan

    plan = TimePlan(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u1", 2025, 3)],
        comparisons=[],
    )

    validate_time_plan(plan)


def test_validate_time_plan_raises_semantic_error_for_grouped_selector_narrowing() -> None:
    from time_query_service.new_plan_validator import TimePlanSemanticValidationError, validate_time_plan

    plan = TimePlan.model_validate(
        {
            "query": "2025年每个季度",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "2025年每个季度",
                    "surface_fragments": [{"start": 0, "end": 9}],
                    "content": {
                        "content_kind": "standalone",
                        "carrier": {
                            "anchor": {
                                "kind": "grouped_temporal_value",
                                "parent": {"kind": "named_period", "period_type": "year", "year": 2025},
                                "child_grain": "quarter",
                                "selector": "first_n",
                            },
                            "modifiers": [],
                        },
                    },
                }
            ],
            "comparisons": [],
        }
    )

    with pytest.raises(TimePlanSemanticValidationError) as excinfo:
        validate_time_plan(plan)

    assert 'selector="all"' in str(excinfo.value)


def test_validate_time_plan_raises_semantic_error_for_overlapping_natural_enumeration() -> None:
    from time_query_service.new_plan_validator import TimePlanSemanticValidationError, validate_time_plan

    plan = TimePlan.model_validate(
        {
            "query": "A",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "A",
                    "surface_fragments": [{"start": 0, "end": 1}],
                    "content": {
                        "content_kind": "standalone",
                        "carrier": {
                            "anchor": {
                                "kind": "enumeration_set",
                                "grain": "day",
                                "members": [
                                    {"kind": "date_range", "start_date": "2025-01-10", "end_date": "2025-02-15", "end_inclusive": True},
                                    {"kind": "date_range", "start_date": "2025-02-01", "end_date": "2025-03-01", "end_inclusive": True},
                                ],
                            },
                            "modifiers": [],
                        },
                    },
                }
            ],
            "comparisons": [],
        }
    )

    with pytest.raises(TimePlanSemanticValidationError) as excinfo:
        validate_time_plan(plan)

    assert "overlap" in str(excinfo.value).lower()


def test_validate_time_plan_raises_semantic_error_for_surface_fragment_mismatch() -> None:
    from time_query_service.new_plan_validator import TimePlanSemanticValidationError, validate_time_plan

    plan = TimePlan(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u1",
                render_text="2025年5月",
                surface_fragments=[{"start": 0, "end": 7}],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=3),
                        modifiers=[],
                    ),
                ),
            )
        ],
        comparisons=[],
    )

    with pytest.raises(TimePlanSemanticValidationError) as excinfo:
        validate_time_plan(plan)

    assert "surface_fragments" in str(excinfo.value)


def test_validate_time_plan_raises_semantic_error_for_transient_non_day_grain_expansion() -> None:
    from time_query_service.new_plan_validator import TimePlanSemanticValidationError, validate_time_plan

    plan = TimePlan(
        query="2025年每个季度",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            Unit(
                unit_id="u1",
                render_text="2025年每个季度",
                surface_fragments=[{"start": 0, "end": 9}],
                content=StandaloneContent(
                    content_kind="standalone",
                    carrier=Carrier(
                        anchor=NamedPeriod(kind="named_period", period_type="year", year=2025),
                        modifiers=[GrainExpansion(kind="grain_expansion", target_grain="quarter")],
                    ),
                ),
            )
        ],
        comparisons=[],
    )

    with pytest.raises(TimePlanSemanticValidationError) as excinfo:
        validate_time_plan(plan)

    assert "canonicalize" in str(excinfo.value)


def test_validate_time_plan_raises_topology_error_for_missing_comparison_reference() -> None:
    from time_query_service.new_plan_validator import TimePlanTopologyValidationError, validate_time_plan

    plan = TimePlan(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_standalone_month("u1", 2025, 3)],
        comparisons=[
            Comparison(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[ComparisonPair(subject_unit_id="u1", reference_unit_id="u_missing")],
            )
        ],
    )

    with pytest.raises(TimePlanTopologyValidationError) as excinfo:
        validate_time_plan(plan)

    assert "u_missing" in str(excinfo.value)


def test_validate_time_plan_raises_topology_error_for_derivation_cycle() -> None:
    from time_query_service.new_plan_validator import TimePlanTopologyValidationError, validate_time_plan

    plan = TimePlan.model_validate(
        {
            "query": "去年同期1去年同期2",
            "system_date": "2026-04-17",
            "timezone": "Asia/Shanghai",
            "units": [
                {
                    "unit_id": "u1",
                    "render_text": "去年同期1",
                    "surface_fragments": [{"start": 0, "end": 5}],
                    "content": {
                        "content_kind": "derived",
                        "sources": [{"source_unit_id": "u2", "transform": {"kind": "shift_year", "offset": -1}}],
                    },
                },
                {
                    "unit_id": "u2",
                    "render_text": "去年同期2",
                    "surface_fragments": [{"start": 5, "end": 10}],
                    "content": {
                        "content_kind": "derived",
                        "sources": [{"source_unit_id": "u1", "transform": {"kind": "shift_year", "offset": -1}}],
                    },
                },
            ],
            "comparisons": [],
        }
    )

    with pytest.raises(TimePlanTopologyValidationError) as excinfo:
        validate_time_plan(plan)

    assert "cycle" in str(excinfo.value).lower()
