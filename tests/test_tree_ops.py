from __future__ import annotations

from datetime import date

import pytest

from time_query_service.resolved_plan import Interval, IntervalTree, TreeLabels
from time_query_service.tree_ops import filter_tree, select_tree, shift_tree


def _atom(start: date, end: date | None = None, **labels: object) -> IntervalTree:
    interval = Interval(start=start, end=end or start, end_inclusive=True)
    label_payload = {"absolute_core_time": interval, **labels}
    return IntervalTree(role="atom", intervals=[interval], children=[], labels=TreeLabels.model_validate(label_payload))


def test_shift_tree_shift_year_preserves_structure_and_labels() -> None:
    tree = IntervalTree(
        role="grouped_member",
        intervals=[
            Interval(start=date(2025, 1, 1), end=date(2025, 3, 31), end_inclusive=True),
            Interval(start=date(2025, 4, 1), end=date(2025, 6, 30), end_inclusive=True),
        ],
        children=[
            _atom(date(2025, 1, 1), date(2025, 3, 31), quarter="Q1"),
            _atom(date(2025, 4, 1), date(2025, 6, 30), quarter="Q2"),
        ],
        labels=TreeLabels(
            absolute_core_time=Interval(start=date(2025, 1, 1), end=date(2025, 12, 31), end_inclusive=True)
        ),
    )

    shifted = shift_tree(tree, {"kind": "shift_year", "offset": -1})

    assert shifted.role == "grouped_member"
    assert [interval.start for interval in shifted.intervals] == [date(2024, 1, 1), date(2024, 4, 1)]
    assert shifted.children[0].labels.quarter == "Q1"
    assert shifted.children[1].labels.quarter == "Q2"
    assert shifted.labels.absolute_core_time.start == date(2024, 1, 1)
    assert shifted.labels.absolute_core_time.end == date(2024, 12, 31)


def test_shift_tree_clips_leap_day_when_shifting_year() -> None:
    tree = _atom(date(2024, 2, 29))
    shifted = shift_tree(tree, {"kind": "shift_year", "offset": -1})
    assert shifted.labels.absolute_core_time.start == date(2023, 2, 28)
    assert shifted.labels.absolute_core_time.end == date(2023, 2, 28)


@pytest.mark.parametrize(
    ("transform", "expected_start", "expected_end"),
    [
        ({"kind": "shift_month", "offset": -1}, date(2025, 2, 28), date(2025, 2, 28)),
        ({"kind": "shift_quarter", "offset": -1}, date(2024, 12, 30), date(2024, 12, 30)),
    ],
)
def test_shift_tree_supports_month_and_quarter_transforms(
    transform: dict[str, object],
    expected_start: date,
    expected_end: date,
) -> None:
    tree = _atom(date(2025, 3, 30))
    shifted = shift_tree(tree, transform)
    assert shifted.labels.absolute_core_time.start == expected_start
    assert shifted.labels.absolute_core_time.end == expected_end


def test_filter_tree_drops_children_but_preserves_parent_role() -> None:
    tree = IntervalTree(
        role="grouped_member",
        intervals=[
            Interval(start=date(2025, 1, 1), end=date(2025, 3, 31), end_inclusive=True),
            Interval(start=date(2025, 4, 1), end=date(2025, 6, 30), end_inclusive=True),
        ],
        children=[
            _atom(date(2025, 1, 1), date(2025, 3, 31)),
            _atom(date(2025, 4, 1), date(2025, 6, 30)),
        ],
        labels=TreeLabels(
            absolute_core_time=Interval(start=date(2025, 1, 1), end=date(2025, 12, 31), end_inclusive=True)
        ),
    )

    filtered = filter_tree(tree, lambda child: child.labels.absolute_core_time.start.month == 4)

    assert filtered.role == "grouped_member"
    assert len(filtered.children) == 1
    assert filtered.children[0].labels.absolute_core_time.start == date(2025, 4, 1)


def test_select_tree_supports_single_and_multi_selectors() -> None:
    tree = IntervalTree(
        role="grouped_member",
        intervals=[
            Interval(start=date(2025, 1, 1), end=date(2025, 3, 31), end_inclusive=True),
            Interval(start=date(2025, 4, 1), end=date(2025, 6, 30), end_inclusive=True),
            Interval(start=date(2025, 7, 1), end=date(2025, 9, 30), end_inclusive=True),
        ],
        children=[
            _atom(date(2025, 1, 1), date(2025, 3, 31)),
            _atom(date(2025, 4, 1), date(2025, 6, 30)),
            _atom(date(2025, 7, 1), date(2025, 9, 30)),
        ],
        labels=TreeLabels(
            absolute_core_time=Interval(start=date(2025, 1, 1), end=date(2025, 12, 31), end_inclusive=True)
        ),
    )

    first = select_tree(tree, "first")
    assert [child.labels.absolute_core_time.start for child in first.children] == [date(2025, 1, 1)]
    assert first.labels.absolute_core_time.start == date(2025, 1, 1)
    assert first.labels.absolute_core_time.end == date(2025, 3, 31)

    first_two = select_tree(tree, "first_n", n=2)
    assert [child.labels.absolute_core_time.start for child in first_two.children] == [date(2025, 1, 1), date(2025, 4, 1)]
    assert first_two.labels.absolute_core_time.start == date(2025, 1, 1)
    assert first_two.labels.absolute_core_time.end == date(2025, 6, 30)

    nth = select_tree(tree, "nth", n=2)
    assert [child.labels.absolute_core_time.start for child in nth.children] == [date(2025, 4, 1)]
    assert nth.labels.absolute_core_time.start == date(2025, 4, 1)


def test_select_tree_rejects_invalid_n() -> None:
    tree = IntervalTree(
        role="grouped_member",
        intervals=[Interval(start=date(2025, 1, 1), end=date(2025, 3, 31), end_inclusive=True)],
        children=[_atom(date(2025, 1, 1), date(2025, 3, 31))],
        labels=TreeLabels(
            absolute_core_time=Interval(start=date(2025, 1, 1), end=date(2025, 3, 31), end_inclusive=True)
        ),
    )

    with pytest.raises(ValueError):
        select_tree(tree, "nth", n=0)


def test_shift_tree_preserves_derived_and_derived_source_roles() -> None:
    leaf = _atom(date(2025, 3, 1), date(2025, 3, 31))
    derived_source = IntervalTree(
        role="derived_source",
        intervals=[leaf.labels.absolute_core_time],
        children=[leaf],
        labels=TreeLabels(
            source_unit_id="u1",
            absolute_core_time=leaf.labels.absolute_core_time,
            derivation_transform_summary={"kind": "shift_year", "offset": -1},
        ),
    )
    tree = IntervalTree(
        role="derived",
        intervals=[leaf.labels.absolute_core_time],
        children=[derived_source],
        labels=TreeLabels(),
    )

    shifted = shift_tree(tree, {"kind": "shift_year", "offset": -1})

    assert shifted.role == "derived"
    assert shifted.children[0].role == "derived_source"
    assert shifted.children[0].children[0].role == "atom"
    assert shifted.children[0].labels.source_unit_id == "u1"
    assert shifted.children[0].labels.absolute_core_time.start == date(2024, 3, 1)
