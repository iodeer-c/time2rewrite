# `feature/new-pipeline` Contributor Notes

This branch implements OpenSpec change `redesign-time-query-two-stage-pipeline`.

## Landing Strategy

New pipeline modules land directly under `time_query_service/`.

Do not create or depend on a staging package such as `time_query_service/_new_pipeline/`. The change contract assumes a direct-to-root landing strategy so cutover does not require a second round of bulk renames.

## Critical Path

Phase 1 critical path:

1. `PR1` — new data models
2. `PR2` — carrier materializer
3. `PR3` — tree ops
4. `PR4` — new resolver
5. `PR7` — post-processor
6. cutover

Parallelizable work after the critical path starts:

- `PR5` — Stage A planner
- `PR6` — Stage B planner
- `PR8` — rewrite execution
- `PR9` — evaluation framework

## Working Rules

- Treat the OpenSpec change as the source of truth.
- Add tests before implementation changes for each PR slice.
- Keep old-stack files untouched until the explicit cutover tasks say otherwise.
- Prefer small, reviewable patches that match the PR boundaries in `tasks.md`.
