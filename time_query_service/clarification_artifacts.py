from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from time_query_service.carrier_materializer import materialize_anchor
from time_query_service.clarification_models import (
    ClarificationArtifact,
    ClarificationFact,
    ClarificationSemantics,
    CollectionClause,
    ScalarClause,
    SelectedContinuousClause,
    SelectedDiscreteClause,
    UnresolvedClause,
)
from time_query_service.resolved_plan import Interval, IntervalTree, ResolvedNode, ResolvedPlan
from time_query_service.time_plan import (
    CalendarEvent,
    CalendarFilter,
    Carrier,
    DateRange,
    DatetimeRange,
    EnumerationSet,
    GrainExpansion,
    GroupedTemporalValue,
    HolidayEventCollection,
    MemberSelection,
    NamedPeriod,
    RelativeWindow,
    RollingByCalendarUnit,
    RollingWindow,
    StandaloneContent,
    TimePlan,
)
from time_query_service.tree_ops import day_end, day_start, display_precision


MAX_INLINE_SELECTED_MEMBER_COUNT = 12


def build_clarification_artifacts(
    *,
    original_query: str,
    time_plan: TimePlan,
    resolved_plan: ResolvedPlan,
) -> list[ClarificationArtifact]:
    comparison_peers = _comparison_peer_map(resolved_plan)
    artifacts: list[ClarificationArtifact] = []
    for unit in time_plan.units:
        node = resolved_plan.nodes.get(unit.unit_id)
        artifacts.append(
            build_clarification_artifact_for_unit(
                unit=unit,
                node=node,
                system_datetime=time_plan.system_datetime,
                comparison_peers=comparison_peers.get(unit.unit_id, []),
            )
        )
    return artifacts


def build_clarification_artifact_for_unit(
    *,
    unit,
    node: ResolvedNode | None,
    system_datetime: datetime,
    comparison_peers: list[str],
) -> ClarificationArtifact:
    semantics = build_clarification_semantics_for_unit(unit=unit, node=node, system_datetime=system_datetime)
    clause = build_canonical_clause_for_unit(unit=unit, node=node, semantics=semantics)
    fact = project_public_fact_for_unit(
        unit=unit,
        node=node,
        semantics=semantics,
        clause=clause,
        comparison_peers=comparison_peers,
    )
    return ClarificationArtifact(fact=fact, semantics=semantics, clause=clause)


def build_clarification_semantics_for_unit(
    *,
    unit,
    node: ResolvedNode | None,
    system_datetime: datetime,
) -> ClarificationSemantics:
    grouping_grain = _grouping_grain_for_unit(unit)
    grouped = grouping_grain is not None
    filtered = _is_filtered_unit(unit=unit, node=node)
    selected = _is_selected_unit(unit)

    if node is None or node.needs_clarification or node.tree is None or node.tree.labels.absolute_core_time is None:
        return ClarificationSemantics(
            shape="unresolved",
            grouped=grouped,
            filtered=filtered,
            selected=selected,
            no_match=False,
            display_term=_display_term_for_unit(unit),
            member_term=_member_term_for_unit(unit),
            grouping_phrase=_grouping_phrase(grouping_grain) if grouping_grain is not None else None,
            group_bucket_mode="none" if not grouped else "semantic_only",
            effective_member_count=None,
            source_scope_kind="none",
            source_scope_text=None,
        )

    tree = node.tree
    effective_member_count = len(tree.children)
    no_match = effective_member_count == 0 and (filtered or selected)
    source_scope_kind = _selected_source_scope_kind(unit) if selected else "none"
    source_scope_text = _source_scope_text_for_unit(unit, system_datetime=system_datetime) if source_scope_kind == "continuous" else None

    if grouped:
        if tree.role == "grouped_member" and tree.children and not filtered:
            group_bucket_mode: Literal["none", "semantic_only", "materialized"] = "materialized"
        else:
            group_bucket_mode = "semantic_only"
    else:
        group_bucket_mode = "none"

    shape: Literal["scalar", "collection", "unresolved"]
    if selected and effective_member_count == 1:
        shape = "scalar"
    elif selected or grouped or filtered or (tree.role == "union" and len(tree.children) > 1):
        shape = "collection"
    else:
        shape = "scalar"

    return ClarificationSemantics(
        shape=shape,
        grouped=grouped,
        filtered=filtered,
        selected=selected,
        no_match=no_match,
        display_term=_display_term_for_unit(unit),
        member_term=_member_term_for_unit(unit),
        grouping_phrase=_grouping_phrase(grouping_grain) if grouping_grain is not None else None,
        group_bucket_mode=group_bucket_mode,
        effective_member_count=effective_member_count,
        source_scope_kind=source_scope_kind,
        source_scope_text=source_scope_text,
    )


def build_canonical_clause_for_unit(
    *,
    unit,
    node: ResolvedNode | None,
    semantics: ClarificationSemantics,
):
    if semantics.shape == "unresolved" or node is None or node.tree is None or node.tree.labels.absolute_core_time is None:
        return UnresolvedClause(label=unit.render_text)

    precision = _precision_for_unit(unit, node.tree)
    resolved_text = _format_interval(node.tree.labels.absolute_core_time, precision=precision)
    member_term = semantics.member_term or "成员"

    if semantics.selected and semantics.effective_member_count == 1:
        return ScalarClause(label=unit.render_text, resolved_text=resolved_text)

    if semantics.selected and semantics.source_scope_kind == "continuous" and semantics.source_scope_text is not None:
        summary = summarize_selected_members(member_term, _member_interval_texts(node.tree, require_children=True))
        return SelectedContinuousClause(
            label=unit.render_text,
            source_scope_text=semantics.source_scope_text,
            resolved_text=resolved_text,
            member_term=member_term,
            member_summary_text=summary,
        )

    if semantics.selected and semantics.source_scope_kind == "discrete":
        summary = summarize_selected_members(member_term, _member_interval_texts(node.tree, require_children=True))
        return SelectedDiscreteClause(
            label=unit.render_text,
            resolved_text=resolved_text,
            member_term=member_term,
            member_summary_text=summary,
        )

    detail_text = _detail_text_for_collection(unit=unit, tree=node.tree, semantics=semantics)
    if detail_text is None:
        return ScalarClause(label=unit.render_text, resolved_text=resolved_text)
    return CollectionClause(label=unit.render_text, resolved_text=resolved_text, detail_text=detail_text)


def project_public_fact_for_unit(
    *,
    unit,
    node: ResolvedNode | None,
    semantics: ClarificationSemantics,
    clause,
    comparison_peers: list[str],
) -> ClarificationFact:
    grouping_grain = _grouping_grain_for_unit(unit)
    precision = _precision_for_unit(unit, None if node is None else node.tree)
    derived_from = [] if node is None or node.derived_from is None else list(node.derived_from)
    if clause.family == "unresolved":
        return ClarificationFact(
            unit_id=unit.unit_id,
            label=unit.render_text,
            status="unresolved",
            grouping_grain=grouping_grain,
            precision=precision,
            reason_kind=None if node is None else node.reason_kind,
            derived_from=derived_from,
            comparison_peers=list(comparison_peers),
        )

    detail_text = None
    if clause.family == "collection":
        detail_text = clause.detail_text
    elif clause.family in {"selected_continuous", "selected_discrete"}:
        detail_text = clause.member_summary_text
    elif clause.family == "scalar":
        detail_text = clause.detail_text

    return ClarificationFact(
        unit_id=unit.unit_id,
        label=unit.render_text,
        status="resolved",
        resolved_text=clause.resolved_text,
        detail_text=detail_text,
        grouping_grain=grouping_grain,
        precision=precision,
        reason_kind=None if node is None else node.reason_kind,
        derived_from=derived_from,
        comparison_peers=list(comparison_peers),
    )


def summarize_selected_members(member_term: str, member_texts: list[str]) -> str:
    if not member_texts:
        return f"无符合条件的{member_term}"
    if len(member_texts) <= MAX_INLINE_SELECTED_MEMBER_COUNT:
        return f"依次为{'、'.join(member_texts)}"
    return f"共{len(member_texts)}个{member_term}，首个为{member_texts[0]}，末个为{member_texts[-1]}"


def _comparison_peer_map(resolved_plan: ResolvedPlan) -> dict[str, list[str]]:
    peers: dict[str, list[str]] = {}
    for comparison in resolved_plan.comparisons:
        for pair in comparison.pairs:
            peers.setdefault(pair.subject_unit_id, [])
            peers.setdefault(pair.reference_unit_id, [])
            if pair.reference_unit_id not in peers[pair.subject_unit_id]:
                peers[pair.subject_unit_id].append(pair.reference_unit_id)
            if pair.subject_unit_id not in peers[pair.reference_unit_id]:
                peers[pair.reference_unit_id].append(pair.subject_unit_id)
    return peers


def _grouping_grain_for_unit(unit) -> str | None:
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return None
    anchor = unit.content.carrier.anchor
    if isinstance(anchor, GroupedTemporalValue):
        return anchor.child_grain
    for modifier in unit.content.carrier.modifiers:
        if isinstance(modifier, GrainExpansion):
            return modifier.target_grain
    return None


def _grouping_phrase(grain: str | None) -> str | None:
    if grain is None:
        return None
    mapping = {
        "hour": "自然小时",
        "day": "自然日",
        "week": "自然周",
        "month": "自然月",
        "quarter": "自然季度",
        "half_year": "自然半年",
        "year": "自然年",
    }
    return mapping.get(grain, grain)


def _is_filtered_unit(*, unit, node: ResolvedNode | None) -> bool:
    if node is not None and node.tree is not None and node.tree.role == "filtered_collection":
        return True
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return False
    if isinstance(unit.content.carrier.anchor, RollingByCalendarUnit):
        return True
    return any(isinstance(modifier, CalendarFilter) for modifier in unit.content.carrier.modifiers)


def _is_selected_unit(unit) -> bool:
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return False
    anchor = unit.content.carrier.anchor
    if isinstance(anchor, GroupedTemporalValue) and anchor.selector != "all":
        return True
    return any(isinstance(modifier, MemberSelection) for modifier in unit.content.carrier.modifiers)


def _selected_source_scope_kind(unit) -> Literal["none", "continuous", "discrete"]:
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return "none"
    anchor = unit.content.carrier.anchor
    if isinstance(anchor, EnumerationSet):
        return "discrete"
    return "continuous"


def _display_term_for_unit(unit) -> str | None:
    day_class = _day_class_for_unit(unit)
    if day_class is not None:
        return _day_class_phrase(day_class)
    if unit.content.content_kind == "standalone" and unit.content.carrier is not None:
        if isinstance(unit.content.carrier.anchor, HolidayEventCollection):
            return "假期"
    return None


def _member_term_for_unit(unit) -> str | None:
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return None
    anchor = unit.content.carrier.anchor
    if isinstance(anchor, HolidayEventCollection):
        return "假期"
    if isinstance(anchor, EnumerationSet):
        return _enumeration_grain_term(anchor.grain)
    day_class = _day_class_for_unit(unit)
    if day_class is not None:
        return _day_class_phrase(day_class)
    grouping_grain = _grouping_grain_for_unit(unit)
    if grouping_grain is not None:
        return _enumeration_grain_term(grouping_grain)
    return None


def _enumeration_grain_term(grain: str | None) -> str | None:
    mapping = {
        "year": "年",
        "half_year": "半年",
        "quarter": "季度",
        "month": "月",
        "week": "周",
        "day": "日期",
        "calendar_event": "事件",
    }
    if grain is None:
        return None
    return mapping.get(grain, "成员")


def _day_class_for_unit(unit) -> str | None:
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return None
    anchor = unit.content.carrier.anchor
    if isinstance(anchor, RollingByCalendarUnit):
        return anchor.day_class
    for modifier in unit.content.carrier.modifiers:
        if isinstance(modifier, CalendarFilter):
            return modifier.day_class
    return None


def _day_class_phrase(day_class: str) -> str:
    mapping = {
        "workday": "工作日（含补班日）",
        "weekend": "周末",
        "holiday": "休假日",
        "statutory_holiday": "节假日",
        "makeup_workday": "补班日",
    }
    return mapping.get(day_class, day_class)


def _source_scope_text_for_unit(unit, *, system_datetime: datetime) -> str | None:
    if unit.content.content_kind != "standalone" or unit.content.carrier is None:
        return None
    carrier = unit.content.carrier
    anchor = carrier.anchor
    base_anchor = anchor.parent if isinstance(anchor, GroupedTemporalValue) else anchor
    if isinstance(base_anchor, HolidayEventCollection):
        base_anchor = base_anchor.parent
    interval = _source_scope_interval(base_anchor, system_datetime=system_datetime)
    if interval is None:
        return None
    return _format_interval(interval, precision="day")


def _source_scope_interval(anchor, *, system_datetime: datetime) -> Interval | None:
    if isinstance(anchor, NamedPeriod):
        tree = materialize_anchor(anchor, system_datetime=system_datetime, business_calendar=_NullCalendar(), region="CN")
        return tree.labels.absolute_core_time
    if isinstance(anchor, DateRange):
        return Interval(start=day_start(anchor.start_date), end=day_end(anchor.end_date), end_inclusive=anchor.end_inclusive)
    if isinstance(anchor, DatetimeRange):
        return Interval(start=anchor.start_datetime, end=anchor.end_datetime, end_inclusive=anchor.end_inclusive)
    if isinstance(anchor, RelativeWindow):
        tree = materialize_anchor(anchor, system_datetime=system_datetime, business_calendar=_NullCalendar(), region="CN")
        return tree.labels.absolute_core_time
    if isinstance(anchor, RollingWindow):
        tree = materialize_anchor(anchor, system_datetime=system_datetime, business_calendar=_NullCalendar(), region="CN")
        return tree.labels.absolute_core_time
    return None


class _NullCalendar:
    def get_day_status(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("business calendar access was not expected for this source scope computation")

    def get_event_span(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("business calendar access was not expected for this source scope computation")


def _detail_text_for_collection(*, unit, tree: IntervalTree, semantics: ClarificationSemantics) -> str | None:
    if semantics.grouped:
        member_texts = _member_interval_texts(tree, require_children=True)
    else:
        member_texts = _member_interval_texts(tree)
    grouping_phrase = semantics.grouping_phrase
    display_term = semantics.display_term
    if semantics.no_match:
        if semantics.selected and display_term is not None:
            return f"范围内无符合条件的{display_term}"
        if display_term is not None:
            return f"范围内无符合条件的{display_term}"
        return "范围内无符合条件的成员"

    if semantics.grouped and semantics.filtered and grouping_phrase is not None and display_term is not None:
        if member_texts:
            return f"范围内按{grouping_phrase}分组的{display_term}，成员依次为{'、'.join(member_texts)}"
        return f"范围内按{grouping_phrase}分组的{display_term}"

    if semantics.grouped and grouping_phrase is not None:
        if member_texts:
            if semantics.group_bucket_mode == "materialized":
                return f"范围内按{grouping_phrase}分组，依次为{'、'.join(member_texts)}"
            return f"范围内按{grouping_phrase}分组，成员依次为{'、'.join(member_texts)}"
        return f"范围内按{grouping_phrase}分组"

    if semantics.filtered and display_term is not None:
        if member_texts and _should_list_filtered_members(unit, member_count=len(member_texts)):
            return f"范围内的{display_term}，依次为{'、'.join(member_texts)}"
        return f"范围内的全部{display_term}"

    if len(member_texts) > 1:
        return f"依次为{'、'.join(member_texts)}"
    return None


def _member_interval_texts(tree: IntervalTree, *, require_children: bool = False) -> list[str]:
    precision = tree.labels.display_precision or "day"
    if tree.children:
        return [
            _format_interval(child.labels.absolute_core_time, precision=precision)
            for child in tree.children
            if child.labels.absolute_core_time is not None
        ]
    if require_children:
        return []
    return [_format_interval(interval, precision=precision) for interval in tree.intervals]


def _should_list_filtered_members(unit, *, member_count: int) -> bool:
    label = unit.render_text
    if "每个" in label or "每天" in label:
        return True
    return member_count <= 31


def _precision_for_unit(unit, tree: IntervalTree | None) -> Literal["day", "hour"]:
    if unit.content.content_kind == "standalone" and unit.content.carrier is not None:
        return display_precision(unit.content.carrier)
    if tree is not None and tree.labels.display_precision is not None:
        return tree.labels.display_precision
    return "day"


def _format_interval(interval: Interval, *, precision: Literal["day", "hour"]) -> str:
    if precision == "hour":
        start = _format_hour(interval.start)
        end = _format_hour(interval.end)
        if interval.start == interval.end:
            return start
        if interval.start.date() == interval.end.date():
            return f"{start}至{interval.end:%H}:00"
        return f"{start}至{end}"
    start = _format_date(interval.start.date())
    end = _format_date(interval.end.date())
    if interval.start.date() == interval.end.date():
        return start
    return f"{start}至{end}"


def _format_date(value: date) -> str:
    return f"{value.year}年{value.month}月{value.day}日"


def _format_hour(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:00")
