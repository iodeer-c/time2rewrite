# `feature/new-pipeline` Finish Checklist

This branch is implementation-complete enough for review, but not fully closed operationally. Use this file as the current handoff state.

## Fresh Verification

- `pytest -q tests/test_stage_a_planner.py tests/test_golden_datasets.py tests/test_evaluator.py tests/test_rewriter.py`
  - Result: `58 passed in 0.57s`
- `openspec validate --strict redesign-time-query-two-stage-pipeline`
  - Result: `Change 'redesign-time-query-two-stage-pipeline' is valid`
  - Note: trailing `edge.openspec.dev` PostHog DNS failure is telemetry-only and does not affect validation

## Branch / Workspace State

- Worktree: `/tmp/time2rewirte-feature-new-pipeline`
- Branch: `feature/new-pipeline`
- Remote branch: `origin/feature/new-pipeline`
- Latest local commit: `e57dc56 feat: rebuild the time rewrite pipeline`
- The branch is conceptually independent at the time-pipeline layer, but Git history still shares the donor-branch base used to bootstrap the shared runtime pieces

## Ready Artifacts

- Baseline metrics: [feature-new-pipeline-baseline-metrics.md](/tmp/time2rewirte-feature-new-pipeline/docs/feature-new-pipeline-baseline-metrics.md)
- Manual review: [feature-new-pipeline-manual-review.md](/tmp/time2rewirte-feature-new-pipeline/docs/feature-new-pipeline-manual-review.md)
- PR description draft: [feature-new-pipeline-pr-description.md](/tmp/time2rewirte-feature-new-pipeline/docs/feature-new-pipeline-pr-description.md)
- Branch usage: [feature-new-pipeline-branch-usage.md](/tmp/time2rewirte-feature-new-pipeline/docs/feature-new-pipeline-branch-usage.md)

## Open Process Items

These are the remaining non-local tasks from the OpenSpec checklist:

- `8.11` merge PR7
- `9.4` merge PR8
- `10.9` publish baseline metrics in the actual PR description
- `10.10` merge PR9
- `11.10` staging revert dry-run
- `11.11` obtain reviewer sign-offs
- `11.12` squash-merge into the eventual target branch
- `12.*` post-cutover observation
- `13.*` follow-up proposals

## Recommended Next Step

- If staying in discussion mode: use the branch-usage doc, PR description draft, and metrics docs as the stable summary of the current solution.
- If moving to delivery mode later: continue from the already-pushed `feature/new-pipeline` branch and copy the PR description draft into the actual PR body so `10.9` can be closed with evidence.
