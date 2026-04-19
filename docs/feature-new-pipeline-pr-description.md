## Summary

- rebuild the time rewrite path as a two-stage planner (`Stage A` graph planning + `Stage B` carrier emission) followed by deterministic post-processing, resolving, and rewriting
- replace the legacy time stack with `TimePlan` / `ResolvedPlan`, canonical carrier shapes, comparison expansion, calendar-aware rolling, and deep-structural evaluation gates
- keep shared runtime infrastructure from the donor branch (`llm`, `business_calendar`, config, logging, service shell) while rewriting the time semantics pipeline itself

## Baseline Metrics

- Stage A: `37 / 37 = 100.0%`
- Stage B: `48 / 50 = 96.0%`
- Layer 1 overall: `90 / 100 = 90.0%`
- Tier 1: `60 / 60 = 100.0%`
- Tier 2: `30 / 30 = 100.0%`
- Tier 3: `0 / 10 = 0.0%` (`Tier 3` is informational only)
- Cutover hard gate: `PASS`

### Tier 3 Status List

- `最近5个休息日收益`
- `最近5个休息日案例1`
- `最近5个休息日案例2`
- `最近5个休息日案例3`
- `最近5个休息日案例4`
- `最近5个休息日案例5`
- `最近5个休息日案例6`
- `最近5个休息日案例7`
- `最近5个休息日案例8`
- `最近5个休息日案例9`

## Manual Review

- Reviewed queries: `30`
- Acceptable rewrites: `28`
- Expected abstentions: `1`
- Follow-up concern: `1`

### Notes

- `最近5个休息日收益` correctly abstains under the current unsupported calendar-class count rolling contract.
- `去年同期员工数有多少` is structurally valid, but bare `去年同期` still has product-level ambiguity and is tracked as a semantics follow-up rather than a blocker.
- Batch-to-spot-run stability notes were recorded for holiday overlap handling and one transient month annotation; both spot re-runs resolved correctly.

## Test Plan

- `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_stage_a_planner.py tests/test_golden_datasets.py tests/test_evaluator.py tests/test_rewriter.py`
- `openspec validate --strict redesign-time-query-two-stage-pipeline`
- `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m time_query_service.evaluator --suite stage_a --llm-config /Users/td/PycharmProjects/time2rewirte/config/llm.yaml --output /tmp/time2rewirte-feature-new-pipeline/stage_a_live_report.json`
- `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m time_query_service.evaluator --suite stage_b --llm-config /Users/td/PycharmProjects/time2rewirte/config/llm.yaml --output /tmp/time2rewirte-feature-new-pipeline/stage_b_live_report.json`
- `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m time_query_service.evaluator --suite layer1 --llm-config /Users/td/PycharmProjects/time2rewirte/config/llm.yaml --output /tmp/time2rewirte-feature-new-pipeline/layer1_live_report.json`

## Implementation Notes

- shared infra comes from the donor branch, but the time semantics pipeline is rewritten rather than incrementally evolved from the legacy time stack
- legacy modules are deleted or replaced by the new stack (`time_plan`, `resolved_plan`, `stage_a_planner`, `stage_b_planner`, `post_processor`, `carrier_materializer`, `tree_ops`, `new_resolver`, `rewriter`, `new_plan_validator`, `evaluator`)
- comparison remains a time-layer structural contract; business aggregation semantics such as `分别 / 一起 / 合计` remain outside this change
