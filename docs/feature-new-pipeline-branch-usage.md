# `feature/new-pipeline` Branch Usage

This branch is currently best treated as a validated solution branch, not as an immediate merge target.

The time semantics pipeline on this branch is a rewrite of the old stack. It reuses shared runtime pieces from the donor branch (`llm`, `business_calendar`, config, logging, and the service shell), but the time-planning, resolving, rewriting, and evaluation logic are new.

## Recommended Uses

### 1. Solution Review Branch

Use this branch when the goal is to review or discuss the solution itself.

- read the code, OpenSpec change, baseline metrics, and manual-review results together
- use it to validate whether the two-stage time rewrite design is structurally correct
- treat it as the best branch for architecture discussion, semantics review, and design sign-off

This is the primary use today.

### 2. Iteration Branch

Use this branch when the goal is to keep refining the solution.

- continue directly on `feature/new-pipeline`
- or cut follow-up branches from it for smaller experiments
- use it to refine semantics, prompt behavior, evaluation coverage, or edge-case handling

This is appropriate if the solution is not frozen yet.

### 3. Baseline / Comparison Branch

Use this branch as the reference point for future alternatives.

- keep the current implementation, metrics, and manual-review docs as a stable baseline
- compare future implementations against the same golden datasets and live-evaluation process
- use it to judge whether a later solution is actually better rather than just different

This is the best way to preserve the work if the repository is going to host multiple candidate approaches.

## Recommended Positioning

Use `feature/new-pipeline` as both:

- the current solution-review branch
- the baseline branch for future comparisons

That gives the branch two clear roles:

- it is the strongest current implementation of the redesign
- it is also the measurement baseline for any later alternative

## Non-Goals For This Branch

- it is not currently being treated as an immediate merge-to-`main` branch
- it is not the same thing as the donor branch `feat/append-only-annotation`
- it should not be described as an incremental extension of the old time stack

## Supporting Artifacts

- Baseline metrics: [feature-new-pipeline-baseline-metrics.md](/tmp/time2rewirte-feature-new-pipeline/docs/feature-new-pipeline-baseline-metrics.md)
- Manual review: [feature-new-pipeline-manual-review.md](/tmp/time2rewirte-feature-new-pipeline/docs/feature-new-pipeline-manual-review.md)
- PR-style summary: [feature-new-pipeline-pr-description.md](/tmp/time2rewirte-feature-new-pipeline/docs/feature-new-pipeline-pr-description.md)
- Finish checklist: [feature-new-pipeline-finish-checklist.md](/tmp/time2rewirte-feature-new-pipeline/docs/feature-new-pipeline-finish-checklist.md)
