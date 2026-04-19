# `feature/new-pipeline` Baseline Metrics

Recorded against the real LLM runtime configured in `/Users/td/PycharmProjects/time2rewirte/config/llm.yaml`.

- System date: `2026-04-17`
- Stage A report: `/tmp/time2rewirte-feature-new-pipeline/stage_a_live_report.json`
- Stage B report: `/tmp/time2rewirte-feature-new-pipeline/stage_b_live_report.json`
- Layer 1 report: `/tmp/time2rewirte-feature-new-pipeline/layer1_live_report.json`

## Summary

- Stage A: `37 / 37 = 100.0%`
- Stage B: `48 / 50 = 96.0%`
- Layer 1 overall: `90 / 100 = 90.0%`
- Tier 1: `60 / 60 = 100.0%`
- Tier 2: `30 / 30 = 100.0%`
- Tier 3: `0 / 10 = 0.0%` (`Tier 3` is informational only; failures are listed below)
- Cutover hard gate: `PASS`

## Tier 3 Status List

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

## Notes

- `build_cutover_gate_summary(...)` now normalizes JSON-round-tripped `tier_summary` keys, so the cutover summary above is safe to compute from persisted report files rather than only in-memory evaluator state.
- Rewriter contract and snapshot tests were re-run locally and are green: `tests/test_rewriter.py` => `5 passed`.
