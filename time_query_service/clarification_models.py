from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from time_query_service.time_plan import StrictModel


ClarificationStatus = Literal["resolved", "unresolved"]


class ClarificationFact(StrictModel):
    unit_id: str
    label: str
    status: ClarificationStatus
    resolved_text: str | None = None
    detail_text: str | None = None
    grouping_grain: str | None = None
    precision: Literal["day", "hour"] | None = None
    reason_kind: str | None = None
    derived_from: list[str] = Field(default_factory=list)
    comparison_peers: list[str] = Field(default_factory=list)


class ClarificationSemantics(StrictModel):
    shape: Literal["scalar", "collection", "unresolved"]
    grouped: bool
    filtered: bool
    selected: bool
    no_match: bool
    display_term: str | None = None
    member_term: str | None = None
    grouping_phrase: str | None = None
    group_bucket_mode: Literal["none", "semantic_only", "materialized"]
    effective_member_count: int | None = None
    source_scope_kind: Literal["none", "continuous", "discrete"]
    source_scope_text: str | None = None


class ScalarClause(StrictModel):
    family: Literal["scalar"] = "scalar"
    label: str
    resolved_text: str
    detail_text: str | None = None


class CollectionClause(StrictModel):
    family: Literal["collection"] = "collection"
    label: str
    resolved_text: str
    detail_text: str


class SelectedContinuousClause(StrictModel):
    family: Literal["selected_continuous"] = "selected_continuous"
    label: str
    source_scope_text: str
    resolved_text: str
    member_term: str
    member_summary_text: str


class SelectedDiscreteClause(StrictModel):
    family: Literal["selected_discrete"] = "selected_discrete"
    label: str
    resolved_text: str
    member_term: str
    member_summary_text: str


class UnresolvedClause(StrictModel):
    family: Literal["unresolved"] = "unresolved"
    label: str


CanonicalClause = Annotated[
    ScalarClause | CollectionClause | SelectedContinuousClause | SelectedDiscreteClause | UnresolvedClause,
    Field(discriminator="family"),
]


class ClarificationArtifact(StrictModel):
    fact: ClarificationFact
    semantics: ClarificationSemantics
    clause: CanonicalClause
