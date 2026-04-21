## MODIFIED Requirements

### Requirement: The post-processor SHALL deterministically rewrite implicit day+ rolling endpoints
The post-processor SHALL own the deterministic normalizer for implicit day+ rolling. After Stage A/B parsing and before Layer 3 validation, it SHALL rewrite raw Stage B rolling carriers that use the default `endpoint="today"` to the request-selected allowed endpoint.

This rewrite applies only to healthy implicit day+ rolling. Hour rolling remains fixed to `today`. Explicit rolling-anchor phrasing remains out of scope for this change.

#### Scenario: implicit day rolling is rewritten to the allowed endpoint
- **WHEN** Stage B emits an implicit day+ rolling carrier with `endpoint="today"`
- **AND** the request-selected `allowed_endpoint` is `"yesterday"`
- **THEN** the post-processor MUST rewrite the carrier to use `endpoint="yesterday"`

#### Scenario: hour rolling stays pinned to today
- **WHEN** Stage B emits an hour rolling carrier
- **THEN** the post-processor MUST preserve `endpoint="today"`
- **AND** it MUST NOT apply the request-scoped default endpoint override

### Requirement: The post-processor SHALL validate rolling carriers against the effective allowed endpoint
Layer 3 validation SHALL accept or reject rolling carriers against the effective `allowed_endpoint`. Pre-plan assembly receives the requested value via `assemble_time_plan`; final and replay validation receive the persisted `plan.default_rolling_endpoint`.

#### Scenario: matching endpoint passes validation
- **WHEN** the effective allowed endpoint is `"yesterday"`
- **AND** the rolling carrier has been normalized to `"yesterday"`
- **THEN** Layer 3 MUST accept the carrier

#### Scenario: mismatched endpoint fails validation
- **WHEN** the effective allowed endpoint is `"today"`
- **AND** the rolling carrier resolves to `"yesterday"`
- **THEN** Layer 3 MUST reject the carrier

### Requirement: The post-processor SHALL recursively validate grouped-parent rolling carriers
Grouped-parent support SHALL be implemented by first rewriting the raw `RollingWindow`, then recursively validating `GroupedTemporalValue.parent` when that parent is itself a `RollingWindow`.

#### Scenario: grouped parent inherits the rewritten rolling endpoint
- **WHEN** a `GroupedTemporalValue.parent` contains a raw `RollingWindow` with the default `endpoint="today"`
- **AND** the request-selected allowed endpoint is `"yesterday"`
- **THEN** the post-processor MUST validate the rewritten parent against the same effective policy

#### Scenario: grouped parent recursion reaches rolling validation
- **WHEN** `GroupedTemporalValue.parent` is a `RollingWindow`
- **THEN** the recursive validation MUST apply the same rolling endpoint acceptance and rejection rules as top-level rolling carriers
