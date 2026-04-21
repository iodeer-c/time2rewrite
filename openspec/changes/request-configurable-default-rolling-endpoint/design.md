## Context

This change captures the approved rolling-endpoint policy in a replayable contract. The core requirement is that the request-selected default endpoint must survive past assembly so the same policy can be applied both during pre-plan validation and later during replay/final validation.

The approved implementation route is route 2:

- Stage A remains unchanged.
- Stage B remains frozen to `today`.
- The post-processor performs the deterministic rewrite after Stage B parsing and before Layer 3 validation.
- The chosen policy is persisted on `TimePlan`.

## Decisions

### Decision: `default_rolling_endpoint` lives on `TimePlan`

`default_rolling_endpoint` is part of the serialized `TimePlan` contract, not a hidden request-only setting. That keeps replay validation deterministic and ensures the policy can be recovered from the plan alone.

### Decision: Stage B stays pinned to `today`

Stage B does not learn request-scoped endpoint policy. It continues to emit the same raw `endpoint="today"` contract for implicit day+ rolling, which keeps the planner stable and confines the policy application to the deterministic normalizer.

### Decision: The post-processor owns the deterministic normalizer

The normalizer runs inside `assemble_time_plan` after Stage A/B parsing and before `_validate_layer3`. It rewrites only healthy implicit day+ rolling carriers from raw `today` to the request-selected allowed endpoint.

### Decision: Layer 3 validates against the effective policy

Pre-plan Layer 3 validation receives `allowed_endpoint` from `assemble_time_plan`. Final and replay validation use `plan.default_rolling_endpoint`. Both must represent the same effective policy.

### Decision: Grouped-parent support is recursive, not special-cased

Grouped-parent support comes from canonicalizing the raw `RollingWindow` first and then recursively validating `GroupedTemporalValue.parent` if it is a `RollingWindow`. The contract does not add a separate grouped-only rewrite path.

### Decision: Hour rolling stays fixed to `today`

Even though the policy is configurable for day+ rolling, hour rolling remains locked to `today` in this change.

## Risks

- If route 1 is preserved as an alternate path, the policy can drift between Stage B raw output and post-processor normalization.
- If `default_rolling_endpoint` is not persisted on `TimePlan`, replay validation cannot recover the request-selected policy.
- If grouped-parent recursion is omitted, `RollingWindow` carried inside `GroupedTemporalValue.parent` can bypass the effective endpoint check.
