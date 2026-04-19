## Context

The append-only clarified-query pipeline already resolves bounded ranges correctly when a clean bounded-range carrier reaches the resolver. The recurring failure is earlier in the chain: explicit bounded-range phrases such as `2025年9月到12月` are sometimes segmented as two endpoint units, structured as two standalone month carriers, and only later re-merged by clarification code. That leaves `clarification_plan` structurally wrong and makes the final output depend on a downstream heuristic instead of on the canonical plan.

This change fixes bounded ranges where they belong: Stage A must recognize explicit bounded ranges as one semantic unit, Stage B must produce one canonical carrier, and post-processing must reject plans that still split the range into sibling endpoints. The append-only writer remains append-only, but it stops being the primary semantic repair layer for bounded ranges.

## Goals / Non-Goals

**Goals:**
- Make explicit bounded-range phrases resolve as one time unit end-to-end.
- Canonicalize bounded-range carriers so resolver behavior does not depend on writer repair.
- Support right-boundary year inference using a minimal non-retreat rule.
- Support bounded ranges whose endpoints can normalize onto one Gregorian start/end axis, including cross-grain natural-period ranges such as `2025年Q3到10月`.
- Preserve grouped and filtered structure on top of a single bounded-range parent.
- Ensure `clarification_plan`, `clarification_items`, and `clarified_query` all reflect the same single bounded-range unit.

**Non-Goals:**
- This change does not expand bounded-range endpoints to `calendar_event`, rolling windows, calendar-day classes, or other non-natural boundary anchors.
- This change does not remove the append-only writer's defensive coalescing logic immediately; it only demotes that logic to a safety net.
- This change does not redesign comparison, derivation, or append-only output style beyond bounded-range ownership and correctness.

## Decisions

### Decision: Explicit bounded ranges are one Stage A unit, not two endpoint siblings

Stage A will treat connector-led phrases such as `A到B`, `A至B`, `A-B`, `A~B`, and `A～B` as one unit when the phrase denotes one continuous bounded range rather than an enumeration. This keeps the unit graph aligned with the user's semantics and prevents downstream repair logic from reconstructing a range from siblings.

Alternatives considered:
- Keep splitting endpoints and let the writer merge them. Rejected because it leaves `clarification_plan` wrong and keeps semantic repair downstream.
- Merge endpoints only in post-processing. Rejected as the primary path because it still lets Stage A/B drift from canonical structure.

### Decision: Canonical bounded-range carriers depend on endpoint type

The pipeline will use two canonical shapes:
- `DateRange` when both endpoints are explicit day-level dates.
- `MappedRange(mode="bounded_pair")` when the phrase is bounded by natural periods or mixed natural-period boundaries that can normalize onto a shared Gregorian axis.

This keeps the carrier vocabulary small and reuses existing bounded-pair semantics for cross-grain natural boundaries such as `2025年Q3到10月` or `2025年9月到10月15日`.

Alternatives considered:
- Introduce a new dedicated `NamedPeriodRange` anchor. Rejected because `MappedRange(bounded_pair)` already models the required normalization path.
- Lower every bounded range to raw dates in Stage B. Rejected because it throws away period semantics too early.

### Decision: Right-boundary inference uses a minimal non-retreat rule

When the right boundary omits a year but the left boundary fixes the temporal frame, the system first inherits the left boundary year and then advances the right boundary by the smallest natural cycle needed to avoid a backward interval. This handles phrases such as `去年12月到3月` as `2025年12月 .. 2026年3月` without inventing a new interpretation.

Explicit reverse ranges remain explicit errors: if the user writes both years and the interval still goes backwards, the system must reject or degrade rather than silently rewriting the year.

Alternatives considered:
- Always inherit the left boundary year. Rejected because it fails obvious cross-year phrases like `12月到3月`.
- Always prefer the next year when the right side lacks a year. Rejected because it breaks ordinary same-year phrases such as `9月到12月`.

### Decision: Cross-grain bounded ranges are in scope when both endpoints normalize onto the Gregorian axis

The change includes month-to-date, quarter-to-month, and other natural-boundary combinations as long as both endpoints can normalize to one concrete start/end interval on the Gregorian axis. This covers common bounded-range phrases without pulling in non-natural anchors such as holidays, rolling windows, or calendar classes.

Alternatives considered:
- Restrict v1 to same-grain endpoints only. Rejected because `Q3到10月` and `9月到10月15日` are common enough and can be normalized deterministically.
- Allow any endpoint type that eventually resolves to dates. Rejected because mixing in rolling or calendar-event semantics would widen the scope too far.

### Decision: post_processor is the structural gate; writer is only a safety net

If Stage A and Stage B still produce two standalone endpoint units for an obvious bounded range, the post-processor must reject that structure and retry/fail rather than letting the append-only writer “repair” it as the main path. The writer may retain a bounded-range coalescing fallback for temporary robustness, but canonical correctness is defined by the upstream single-unit structure.

Alternatives considered:
- Let writer repair remain the primary behavior. Rejected because it hides structural drift and leaves internal contracts inconsistent.
- Let resolver merge endpoint siblings automatically. Rejected because resolver should consume canonical units, not infer segmentation intent.

## Risks / Trade-offs

- [Stage A false positives on connector phrases] → Constrain the rule to continuous-range semantics and add counterexamples for enumerations such as `9月和12月`.
- [Cross-grain normalization may still surface ambiguous phrases] → Limit v1 to natural-period/date boundaries on the Gregorian axis and degrade unsupported endpoint types.
- [Writer fallback and post-processor gate may temporarily disagree] → Define post-processor rejection as the canonical rule and keep writer fallback only as a temporary resilience layer.
- [Existing tests may encode split-endpoint expectations] → Replace them with single-unit plan, fact, and `clarified_query` expectations for bounded-range queries.

## Migration Plan

1. Update Stage A prompt/examples and structural validation to emit one unit for explicit bounded ranges.
2. Update Stage B prompt/examples and carrier validation to emit `DateRange` or `MappedRange(bounded_pair)` for the single unit.
3. Add post-processor detection for split-endpoint bounded-range plans and convert them into retries/failures instead of downstream pass-through.
4. Update clarification-fact generation so canonical bounded-range plans yield one fact and one appended clarification clause without relying on endpoint merge heuristics.
5. Replace bounded-range end-to-end tests so `clarification_plan`, `clarification_items`, and `clarified_query` all assert single-unit ownership.
6. Keep writer-side coalescing as a temporary safety net until live traffic shows the upstream path is stable; then remove or reduce it in a follow-up.

## Open Questions

- None for this change. Scope, endpoint classes, and right-boundary inference are intentionally closed in the proposal and design.
