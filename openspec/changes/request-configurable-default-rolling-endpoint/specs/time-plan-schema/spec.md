## MODIFIED Requirements

### Requirement: TimePlan SHALL persist the request-scoped default rolling endpoint
`TimePlan` SHALL include a top-level `default_rolling_endpoint: Literal["today", "yesterday"] = "today"` field.

The field is part of the serialized plan contract so that replay and final validation can recover the same effective policy that was used during assembly.

#### Scenario: default rolling endpoint is present on new plans
- **WHEN** a new `TimePlan` is assembled without an explicit override
- **THEN** `default_rolling_endpoint` MUST serialize as `"today"`

#### Scenario: old plans remain valid
- **WHEN** an older plan payload omits `default_rolling_endpoint`
- **THEN** the schema MUST accept it by defaulting the field to `"today"`

#### Scenario: the plan records a caller-selected policy
- **WHEN** a request selects `default_rolling_endpoint="yesterday"`
- **THEN** the assembled `TimePlan` MUST persist that value on the top-level plan

### Requirement: TimePlan SHALL preserve the rolling-endpoint contract for replayable validation
`TimePlan` SHALL carry enough information for downstream validation to apply the same effective rolling-endpoint policy during replay or final validation.

#### Scenario: replay validation uses the persisted policy
- **WHEN** a serialized plan is reloaded for validation
- **THEN** the validator MUST recover the effective endpoint policy from `plan.default_rolling_endpoint`
