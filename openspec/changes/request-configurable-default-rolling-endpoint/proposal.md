# request-configurable-default-rolling-endpoint

## Why

Today, implicit day+ rolling defaults are fixed to `today` inside the pipeline. That prevents a caller from requesting the same contract with a different default rolling endpoint, even though the approved policy explicitly allows request-scoped selection between `today` and `yesterday`.

The contract also needs to remain replayable. Layer 3 validation cannot depend on hidden request context, so the chosen default endpoint must persist on `TimePlan` itself.

## What Changes

- Add `default_rolling_endpoint: Literal["today", "yesterday"] = "today"` to the top level of `TimePlan`.
- Keep Stage A unchanged.
- Keep Stage B frozen to `today` as its raw output contract.
- Use route 2 only:
  - Stage B continues to emit `endpoint="today"` for implicit day+ rolling.
  - The post-processor owns the deterministic normalizer that rewrites that implicit `today` to the request-selected allowed endpoint.
- Make Layer 3 validation accept or reject rolling endpoints against the effective `allowed_endpoint`.
- Preserve grouped-parent support by rewriting the raw `RollingWindow` first, then recursively validating `GroupedTemporalValue.parent` when it is a `RollingWindow`.
- Keep hour rolling fixed to `today`.

## Non-Goals

- Do not preserve route 1 as an implementation option.
- Do not change Stage A parsing or segmentation.
- Do not open explicit rolling-anchor phrasing in this change.
- Do not change the Stage B contract away from raw `endpoint="today"`.
- Do not scope this change beyond the `TimePlan` schema and post-processor contract deltas.
