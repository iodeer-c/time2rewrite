## Why

The append-only pipeline can already resolve bounded ranges, but explicit bounded-range phrases such as `2025年9月到12月` still drift into two endpoint units in Stage A and Stage B. That forces downstream clarification code to merge endpoints after the fact, leaves `clarification_plan` structurally wrong, and makes the final output depend on writer repair instead of on a clean resolved time contract.

This change fixes bounded ranges at the right layer: explicit bounded-range phrases must become one upstream time unit, one canonical carrier, and one resolved fact. The writer should only explain that structure, not repair it.

## What Changes

- Recognize explicit bounded-range phrases as a single Stage A unit instead of two sibling endpoint units.
- Define canonical Stage B shapes for bounded ranges:
  - date-to-date ranges use `DateRange`
  - natural-period or mixed natural-period boundaries use `MappedRange(mode="bounded_pair")`
- Support right-boundary inference by a minimal non-retreat rule when the right boundary omits a year and the left boundary fixes the temporal frame.
- Admit bounded ranges whose endpoints can both normalize onto the same Gregorian start/end axis, including cross-grain natural-period ranges such as `2025年Q3到10月`.
- Keep grouped and filtered semantics attached to a single bounded-range parent instead of reconstructing a range from two endpoints downstream.
- Reject or retry plans where an obvious bounded range is still emitted as two standalone endpoint units; writer-side merging is no longer the primary repair path.
- Require `clarification_plan`, `clarification_items`, and `clarified_query` to agree on the same single bounded-range unit structure.

## Capabilities

### New Capabilities
- `bounded-range-unit-normalization`: recognize explicit bounded-range phrases as one semantic unit, carry them as one canonical carrier, and preserve that single-unit structure through planning, validation, and clarification output.

### Modified Capabilities
- `mapped-range-constructors`: extend bounded-pair construction rules to support minimal non-retreat right-boundary inference and mixed natural-period boundary normalization on a shared Gregorian axis.
- `append-only-clarification-writer`: require append-only clarification facts and `clarified_query` to preserve single-unit bounded-range ownership rather than relying on endpoint-merging heuristics.

## Impact

- Affected code: `stage_a_planner`, `stage_a_prompt`, `stage_b_planner`, `stage_b_prompt`, `post_processor`, `clarification_writer`, `service`, and bounded-range-related evaluator/tests.
- Affected behavior: `clarification_plan` and `clarification_items` for explicit bounded ranges become single-unit outputs; bounded-range writer repair becomes a safety net rather than the main semantic path.
- Affected APIs: `/query/pipeline` keeps the same shape, but explicit bounded-range queries return a single bounded-range clarification item instead of two endpoint items.
- Affected validation: Stage A/Stage B and post-processor add bounded-range structural gates; end-to-end tests and manual review shift from “did the writer merge endpoints?” to “did the pipeline carry one bounded-range unit throughout?”
