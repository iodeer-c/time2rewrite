## 1. Contract capture

- [ ] 1.1 Add `default_rolling_endpoint: Literal["today", "yesterday"] = "today"` to the `TimePlan` schema delta.
- [ ] 1.2 Record the deterministic post-processor rewrite of implicit day+ rolling from raw `endpoint="today"` to the request-selected allowed endpoint.
- [ ] 1.3 Record Layer 3 acceptance and rejection against the effective `allowed_endpoint`.
- [ ] 1.4 Record recursive validation for `GroupedTemporalValue.parent` when it contains a `RollingWindow`.
- [ ] 1.5 Record that hour rolling remains fixed to `today`.

## 2. Approval boundaries

- [ ] 2.1 Keep Stage A unchanged.
- [ ] 2.2 Keep Stage B frozen to `today`.
- [ ] 2.3 Lock the implementation route to route 2 only.
- [ ] 2.4 Keep the contract focused on the approved `TimePlan` and post-processor deltas.

## 3. Validation

- [ ] 3.1 Run `openspec validate --strict request-configurable-default-rolling-endpoint`.
- [ ] 3.2 Confirm the change tree validates with no proposal, design, or spec errors.
