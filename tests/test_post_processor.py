from __future__ import annotations

import json
from datetime import date

import pytest

from time_query_service.post_processor import (
    PostProcessorValidationError,
    StageAComparisonOutput,
    StageAComparisonPairOutput,
    StageAOutput,
    StageAUnitOutput,
    StageBOutput,
    assemble_time_plan,
)
from time_query_service.time_plan import (
    CalendarEvent,
    Carrier,
    DateRange,
    DerivationSource,
    NamedPeriod,
    ScheduleYearRef,
)


def _stage_a_unit(**overrides: object) -> StageAUnitOutput:
    payload = {
        "unit_id": "u1",
        "render_text": "2025年3月",
        "surface_fragments": [{"start": 0, "end": 7}],
        "content_kind": "standalone",
        "self_contained_text": "2025年3月",
        "sources": [],
        "surface_hint": None,
    }
    payload.update(overrides)
    return StageAUnitOutput.model_validate(payload)


def _stage_b_output(**overrides: object) -> StageBOutput:
    payload = {
        "carrier": {
            "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
            "modifiers": [],
        },
        "needs_clarification": False,
        "reason_kind": None,
    }
    payload.update(overrides)
    return StageBOutput.model_validate(payload)


def test_assemble_time_plan_builds_units_and_comparisons() -> None:
    stage_a = StageAOutput(
        query="2025年3月对比2024年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id="u1", render_text="2025年3月", surface_fragments=[{"start": 0, "end": 7}]),
            _stage_a_unit(unit_id="u2", render_text="2024年3月", self_contained_text="2024年3月", surface_fragments=[{"start": 9, "end": 16}]),
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
        "u1": _stage_b_output(),
        "u2": StageBOutput(
            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2024, month=3), modifiers=[])
        ),
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert [unit.unit_id for unit in plan.units] == ["u1", "u2"]
    assert plan.comparisons[0].pairs[0].subject_unit_id == "u1"
    assert plan.comparisons[0].pairs[0].reference_unit_id == "u2"


def test_assemble_time_plan_allocates_deterministic_unit_ids_for_missing_or_duplicate_ids() -> None:
    stage_a = StageAOutput(
        query="2025年3月和2025年5月，去年同期",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id=None, render_text="2025年3月", surface_fragments=[{"start": 0, "end": 7}]),
            _stage_a_unit(unit_id="dup", render_text="2025年5月", self_contained_text="2025年5月", surface_fragments=[{"start": 8, "end": 15}]),
            _stage_a_unit(unit_id="dup", render_text="去年同期", content_kind="derived", self_contained_text=None, surface_fragments=[{"start": 16, "end": 20}], sources=[
                DerivationSource(source_unit_id="dup", transform={"kind": "shift_year", "offset": -1})
            ]),
        ],
        comparisons=[],
    )
    stage_b = {
        "__index_0__": _stage_b_output(),
        "dup": StageBOutput(
            carrier=Carrier(anchor=NamedPeriod(kind="named_period", period_type="month", year=2025, month=5), modifiers=[])
        ),
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert [unit.unit_id for unit in plan.units] == ["u1", "u2", "u3"]
    assert plan.units[2].content.sources[0].source_unit_id == "u2"


def test_assemble_time_plan_preserves_degraded_standalone_without_synthesizing_carrier() -> None:
    stage_a = StageAOutput(
        query="最近5个休息日",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="最近5个休息日", self_contained_text="最近5个休息日")],
        comparisons=[],
    )

    plan = assemble_time_plan(
        stage_a,
        {
            "u1": StageBOutput(carrier=None, needs_clarification=True, reason_kind="unsupported_calendar_grain_rolling")
        },
    )

    assert plan.units[0].needs_clarification is True
    assert plan.units[0].reason_kind == "unsupported_calendar_grain_rolling"
    assert plan.units[0].content.carrier is None


def test_assemble_time_plan_layer1_attributes_json_errors_to_stage_and_unit() -> None:
    stage_a = StageAOutput(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit()],
        comparisons=[],
    )

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, {"u1": "{not-json}"})

    err = excinfo.value
    assert err.layer == 1
    assert err.stage == "stage_b"
    assert err.unit_id == "u1"


def test_assemble_time_plan_layer2_attributes_schema_errors_to_stage_and_unit() -> None:
    stage_a = StageAOutput(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit()],
        comparisons=[],
    )

    bad_stage_b = json.dumps(
        {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [{"kind": "unknown_modifier"}],
            },
            "needs_clarification": False,
        }
    )

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, {"u1": bad_stage_b})

    err = excinfo.value
    assert err.layer == 2
    assert err.stage == "stage_b"
    assert err.unit_id == "u1"


def test_assemble_time_plan_layer3_rejects_surface_fragment_mismatch() -> None:
    stage_a = StageAOutput(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="2025年5月")],
        comparisons=[],
    )

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, {"u1": _stage_b_output()})

    err = excinfo.value
    assert err.layer == 3
    assert err.stage == "post_processor"
    assert err.unit_id == "u1"


def test_assemble_time_plan_layer4_rejects_dangling_comparison_reference() -> None:
    stage_a = StageAOutput(
        query="2025年3月对比2024年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(unit_id="u1")],
        comparisons=[
            StageAComparisonOutput(
                comparison_id="c1",
                anchor_text="对比",
                pairs=[StageAComparisonPairOutput(subject_unit_id="u1", reference_unit_id="u_missing")],
            )
        ],
    )

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, {"u1": _stage_b_output()})

    err = excinfo.value
    assert err.layer == 4
    assert err.stage == "post_processor"
    assert "u_missing" in err.details


def test_assemble_time_plan_layer4_rejects_derivation_cycles() -> None:
    stage_a = StageAOutput(
        query="去年同期1去年同期2",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
                _stage_a_unit(
                    unit_id="u1",
                    render_text="去年同期1",
                    surface_fragments=[{"start": 0, "end": 5}],
                    content_kind="derived",
                    self_contained_text=None,
                    sources=[DerivationSource(source_unit_id="u2", transform={"kind": "shift_year", "offset": -1})],
                ),
                _stage_a_unit(
                    unit_id="u2",
                    render_text="去年同期2",
                    surface_fragments=[{"start": 5, "end": 10}],
                    content_kind="derived",
                    self_contained_text=None,
                    sources=[DerivationSource(source_unit_id="u1", transform={"kind": "shift_year", "offset": -1})],
            ),
        ],
        comparisons=[],
    )

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, {})

    err = excinfo.value
    assert err.layer == 4
    assert err.stage == "post_processor"
    assert "cycle" in err.details.lower()


def test_post_processor_error_formats_human_readable_message() -> None:
    error = PostProcessorValidationError(layer=3, stage="stage_b", unit_id="u2", details="bad modifier")
    assert "Layer 3" in str(error)
    assert "stage_b" in str(error)
    assert "u2" in str(error)
    assert "bad modifier" in str(error)


def test_assemble_time_plan_preserves_multi_source_derivation_inputs_in_order() -> None:
    stage_a = StageAOutput(
        query="ABC",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id="u1", render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}]),
            _stage_a_unit(unit_id="u2", render_text="B", self_contained_text="B", surface_fragments=[{"start": 1, "end": 2}]),
            _stage_a_unit(
                unit_id="u3",
                render_text="C",
                self_contained_text=None,
                surface_fragments=[{"start": 2, "end": 3}],
                content_kind="derived",
                sources=[
                    DerivationSource(source_unit_id="u1", transform={"kind": "shift_year", "offset": -1}),
                    DerivationSource(source_unit_id="u2", transform={"kind": "shift_year", "offset": -1}),
                ],
            ),
        ],
        comparisons=[],
    )
    stage_b = {
        "u1": _stage_b_output(),
        "u2": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5},
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert [source.source_unit_id for source in plan.units[2].content.sources] == ["u1", "u2"]


def test_post_processor_emits_validation_event_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str, dict[str, object]]] = []

    monkeypatch.setattr(
        "time_query_service.post_processor.log_pipeline_event",
        lambda component, event, payload, enabled=True: events.append((component, event, payload)),
    )

    stage_a = StageAOutput(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit()],
        comparisons=[],
    )

    assemble_time_plan(stage_a, {"u1": _stage_b_output()})

    assert events[-1][0] == "post_processor"
    assert events[-1][1] == "post_processor_validation"
    assert events[-1][2]["outcome"] == "success"


def test_post_processor_emits_validation_event_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str, dict[str, object]]] = []

    monkeypatch.setattr(
        "time_query_service.post_processor.log_pipeline_event",
        lambda component, event, payload, enabled=True: events.append((component, event, payload)),
    )

    stage_a = StageAOutput(
        query="2025年3月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="2025年5月")],
        comparisons=[],
    )

    with pytest.raises(PostProcessorValidationError):
        assemble_time_plan(stage_a, {"u1": _stage_b_output()})

    assert events[-1][0] == "post_processor"
    assert events[-1][1] == "post_processor_validation"
    assert events[-1][2]["outcome"] == "failure"
    assert events[-1][2]["layer"] == 3


def test_layer3_rejects_calendar_grain_rolling_approximation() -> None:
    stage_a = StageAOutput(
        query="最近5个工作日",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(
                render_text="最近5个工作日",
                self_contained_text="最近5个工作日",
                surface_fragments=[{"start": 0, "end": 7}],
                surface_hint="calendar_grain_rolling",
            )
        ],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "rolling_window", "length": 5, "unit": "day", "endpoint": "today", "include_endpoint": True},
                "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 3
    assert "RollingByCalendarUnit" in err.details
    assert "workday" in err.details


@pytest.mark.parametrize("selector", ["first_n", "last_n", "nth"])
def test_layer3_rejects_grouped_temporal_value_selector_narrowing(selector: str) -> None:
    stage_a = StageAOutput(
        query="2025年每个季度",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="2025年每个季度", self_contained_text="2025年每个季度", surface_fragments=[{"start": 0, "end": 9}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {
                    "kind": "grouped_temporal_value",
                    "parent": {"kind": "named_period", "period_type": "year", "year": 2025},
                    "child_grain": "quarter",
                    "selector": selector,
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 3
    assert 'selector="all"' in err.details
    assert "MemberSelection" in err.details


def test_layer3_accepts_grouped_temporal_value_selector_all() -> None:
    stage_a = StageAOutput(
        query="2025年每个季度",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="2025年每个季度", self_contained_text="2025年每个季度", surface_fragments=[{"start": 0, "end": 9}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {
                    "kind": "grouped_temporal_value",
                    "parent": {"kind": "named_period", "period_type": "year", "year": 2025},
                    "child_grain": "quarter",
                    "selector": "all",
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert plan.units[0].content.carrier.anchor.kind == "grouped_temporal_value"


def test_layer3_rejects_overlapping_non_calendar_event_enumeration() -> None:
    stage_a = StageAOutput(
        query="A",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
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
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 3
    assert "overlap" in err.details.lower()


def test_layer3_accepts_overlapping_distinct_calendar_events() -> None:
    stage_a = StageAOutput(
        query="A",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {
                    "kind": "enumeration_set",
                    "grain": "calendar_event",
                    "members": [
                        {
                            "kind": "calendar_event",
                            "region": "CN",
                            "event_key": "guoqing",
                            "schedule_year_ref": {"year": 2026},
                            "scope": "consecutive_rest",
                        },
                        {
                            "kind": "calendar_event",
                            "region": "CN",
                            "event_key": "zhongqiu",
                            "schedule_year_ref": {"year": 2026},
                            "scope": "consecutive_rest",
                        },
                    ],
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert plan.units[0].content.carrier.anchor.kind == "enumeration_set"
    assert len(plan.units[0].content.carrier.anchor.members) == 2


def test_layer3_rejects_duplicate_calendar_event_identity_before_overlap() -> None:
    stage_a = StageAOutput(
        query="A",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {
                    "kind": "enumeration_set",
                    "grain": "calendar_event",
                    "members": [
                        {
                            "kind": "calendar_event",
                            "region": "CN",
                            "event_key": "guoqing",
                            "schedule_year_ref": {"year": 2026},
                            "scope": "consecutive_rest",
                        },
                        {
                            "kind": "calendar_event",
                            "region": "CN",
                            "event_key": "guoqing",
                            "schedule_year_ref": {"year": 2026},
                            "scope": "consecutive_rest",
                        },
                    ],
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 3
    assert "duplicate" in err.details.lower()


def test_layer3_rejects_frozen_rolling_window_parameters() -> None:
    stage_a = StageAOutput(
        query="最近一个月",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="最近一个月", self_contained_text="最近一个月", surface_fragments=[{"start": 0, "end": 5}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "rolling_window", "length": 1, "unit": "month", "endpoint": "previous_complete", "include_endpoint": True},
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 3
    assert "frozen" in err.details.lower()
    assert "endpoint" in err.details


def test_layer3_rejects_frozen_rolling_by_calendar_unit_parameters() -> None:
    stage_a = StageAOutput(
        query="最近5个工作日",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="最近5个工作日", self_contained_text="最近5个工作日", surface_fragments=[{"start": 0, "end": 7}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {
                    "kind": "rolling_by_calendar_unit",
                    "length": 5,
                    "day_class": "workday",
                    "endpoint": "yesterday",
                    "include_endpoint": True,
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 3
    assert "frozen" in err.details.lower()
    assert "endpoint" in err.details


def test_post_processor_canonicalizes_continuous_parent_non_day_grain_expansion() -> None:
    stage_a = StageAOutput(
        query="2025年每个季度",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="2025年每个季度", self_contained_text="2025年每个季度", surface_fragments=[{"start": 0, "end": 9}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "year", "year": 2025},
                "modifiers": [{"kind": "grain_expansion", "target_grain": "quarter"}],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    plan = assemble_time_plan(stage_a, stage_b)

    anchor = plan.units[0].content.carrier.anchor
    assert anchor.kind == "grouped_temporal_value"
    assert anchor.child_grain == "quarter"
    assert plan.units[0].content.carrier.modifiers == []


def test_post_processor_preserves_trailing_modifiers_after_canonicalization() -> None:
    stage_a = StageAOutput(
        query="2025年每个季度的第一个",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="2025年每个季度的第一个", self_contained_text="2025年每个季度的第一个", surface_fragments=[{"start": 0, "end": 13}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "year", "year": 2025},
                "modifiers": [
                    {"kind": "grain_expansion", "target_grain": "quarter"},
                    {"kind": "member_selection", "selector": "first"},
                ],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    plan = assemble_time_plan(stage_a, stage_b)

    anchor = plan.units[0].content.carrier.anchor
    assert anchor.kind == "grouped_temporal_value"
    assert plan.units[0].content.carrier.modifiers[0].kind == "member_selection"
    assert plan.units[0].content.carrier.modifiers[0].selector == "first"


def test_post_processor_leaves_day_grain_expansion_in_place() -> None:
    stage_a = StageAOutput(
        query="2025年3月的工作日",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="2025年3月的工作日", self_contained_text="2025年3月的工作日", surface_fragments=[{"start": 0, "end": 11}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [
                    {"kind": "grain_expansion", "target_grain": "day"},
                    {"kind": "calendar_filter", "day_class": "workday"},
                ],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert plan.units[0].content.carrier.anchor.kind == "named_period"
    assert [modifier.kind for modifier in plan.units[0].content.carrier.modifiers] == ["grain_expansion", "calendar_filter"]


def test_layer3_rejects_non_head_non_day_grain_expansion() -> None:
    stage_a = StageAOutput(
        query="2025年每个季度",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="2025年每个季度", self_contained_text="2025年每个季度", surface_fragments=[{"start": 0, "end": 9}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "year", "year": 2025},
                "modifiers": [
                    {"kind": "member_selection", "selector": "first"},
                    {"kind": "grain_expansion", "target_grain": "quarter"},
                ],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 3
    assert "head of the chain" in err.details


def test_layer4_expands_multicore_comparison_pairs_with_provenance() -> None:
    stage_a = StageAOutput(
        query="A对比B",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id="u1", render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}]),
            _stage_a_unit(unit_id="u2", render_text="B", self_contained_text="B", surface_fragments=[{"start": 3, "end": 4}]),
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
                "anchor": {
                    "kind": "enumeration_set",
                    "grain": "month",
                    "members": [
                        {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                        {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5},
                    ],
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
        "u2": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2024, "month": 3},
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
    }

    plan = assemble_time_plan(stage_a, stage_b)

    pairs = plan.comparisons[0].pairs
    assert len(pairs) == 2
    assert pairs[0].expansion.source_pair_index == 0
    assert [pair.expansion.expansion_index for pair in pairs] == [0, 1]
    assert [pair.expansion.subject_core_index for pair in pairs] == [0, 1]
    assert all(pair.expansion.reference_core_index is None for pair in pairs)


def test_layer4_rejects_mismatched_multicore_comparison_cardinalities() -> None:
    stage_a = StageAOutput(
        query="A对比B",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id="u1", render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}]),
            _stage_a_unit(unit_id="u2", render_text="B", self_contained_text="B", surface_fragments=[{"start": 3, "end": 4}]),
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
                "anchor": {
                    "kind": "enumeration_set",
                    "grain": "month",
                    "members": [
                        {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                        {"kind": "named_period", "period_type": "month", "year": 2025, "month": 5},
                    ],
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
        "u2": {
            "carrier": {
                "anchor": {
                    "kind": "enumeration_set",
                    "grain": "quarter",
                    "members": [
                        {"kind": "named_period", "period_type": "quarter", "year": 2024, "quarter": 1},
                        {"kind": "named_period", "period_type": "quarter", "year": 2024, "quarter": 2},
                        {"kind": "named_period", "period_type": "quarter", "year": 2024, "quarter": 3},
                    ],
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
    }

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 4
    assert "cardinalit" in err.details.lower()


def test_layer4_keeps_filtered_collection_comparison_single_core() -> None:
    stage_a = StageAOutput(
        query="A对比B",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id="u1", render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}]),
            _stage_a_unit(unit_id="u2", render_text="B", self_contained_text="B", surface_fragments=[{"start": 3, "end": 4}]),
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
                "anchor": {"kind": "named_period", "period_type": "month", "year": 2025, "month": 3},
                "modifiers": [{"kind": "calendar_filter", "day_class": "workday"}],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
        "u2": {
            "carrier": {
                "anchor": {
                    "kind": "rolling_by_calendar_unit",
                    "length": 5,
                    "day_class": "workday",
                    "endpoint": "today",
                    "include_endpoint": True,
                },
                "modifiers": [],
            },
            "needs_clarification": False,
            "reason_kind": None,
        },
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert len(plan.comparisons[0].pairs) == 1
    assert plan.comparisons[0].pairs[0].expansion is None


def test_layer4_rejects_non_distributive_derivation_transform() -> None:
    stage_a = StageAOutput(
        query="AB",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[
            _stage_a_unit(unit_id="u1", render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}]),
            _stage_a_unit(
                unit_id="u2",
                render_text="B",
                self_contained_text=None,
                surface_fragments=[{"start": 1, "end": 2}],
                content_kind="derived",
                sources=[DerivationSource(source_unit_id="u1", transform={"kind": "rebase_to_parent"})],
            ),
        ],
        comparisons=[],
    )
    stage_b = {"u1": _stage_b_output()}

    with pytest.raises(PostProcessorValidationError) as excinfo:
        assemble_time_plan(stage_a, stage_b)

    err = excinfo.value
    assert err.layer == 4
    assert "distributive" in err.details.lower()


@pytest.mark.parametrize(
    "modifier",
    [
        {"kind": "grain_expansion", "target_grain": "quarter"},
        {"kind": "grain_expansion", "target_grain": "month"},
        {"kind": "grain_expansion", "target_grain": "week"},
    ],
)
def test_final_time_plan_never_retains_non_day_grain_expansion(modifier: dict[str, object]) -> None:
    stage_a = StageAOutput(
        query="A",
        system_date=date(2026, 4, 17),
        timezone="Asia/Shanghai",
        units=[_stage_a_unit(render_text="A", self_contained_text="A", surface_fragments=[{"start": 0, "end": 1}])],
        comparisons=[],
    )
    stage_b = {
        "u1": {
            "carrier": {
                "anchor": {"kind": "named_period", "period_type": "year", "year": 2025},
                "modifiers": [modifier],
            },
            "needs_clarification": False,
            "reason_kind": None,
        }
    }

    plan = assemble_time_plan(stage_a, stage_b)

    assert all(
        not (item.kind == "grain_expansion" and item.target_grain != "day")
        for item in plan.units[0].content.carrier.modifiers
    )
