# `feature/new-pipeline` Manual Rewrite Review

Manual rewrite-quality review performed against the real LLM runtime configured in `/Users/td/PycharmProjects/time2rewirte/config/llm.yaml`.

- Review date: `2026-04-19`
- System date used for all queries: `2026-04-17`
- Raw sample output: `/tmp/time2rewirte-feature-new-pipeline/manual_rewrite_review.json`

## Outcome Summary

- Reviewed queries: `30`
- Acceptable rewrites: `28`
- Expected abstentions: `1`
- Follow-up concern: `1`

## Expected Abstention

- `最近5个休息日收益`
  - Outcome: explicit abstention for unsupported calendar-class count rolling
  - Assessment: expected and acceptable under the current contract

## Follow-up Concern

- `去年同期员工数有多少`
  - Outcome: rewritten as `去年同期（2025年1月1日至2025年12月31日）员工数有多少`
  - Assessment: structurally valid after the Stage A fix, but the standalone semantics of bare `去年同期` remain product-ambiguous. Current behavior treats it as previous calendar year. Keep as a tracked semantics question rather than a blocker for the current change.

## Stability Watch Notes

- `2025年中秋假期和国庆假期收益`
  - One batch run produced a transient `时间待澄清` on the second holiday member.
  - Spot re-run resolved both holiday members correctly to `2025-10-01 .. 2025-10-08`.
- `今年3月和去年3月对比`
  - One batch run produced a transient incorrect `2025-04` annotation for `去年3月`.
  - Spot re-run resolved the same query correctly to `2025-03-01 .. 2025-03-31`.

## Representative Accepted Samples

- `2025年3月收益` -> `2025年3月（2025年3月1日至2025年3月31日）收益`
- `最近一周收益` -> `最近一周（2026年4月11日至2026年4月17日）收益`
- `最近5个工作日收益` -> `最近5个工作日（2026年4月13日至2026年4月17日）收益`
- `2025年3月的工作日对比2024年3月的工作日` -> both sides annotated with the correct month bounds
- `2025年每个季度收益` -> grouped phrase preserved and annotated with the parent year interval
- `国庆假期和中秋假期一起的收益是多少` -> both holiday-event members preserved as independent event-level time members
