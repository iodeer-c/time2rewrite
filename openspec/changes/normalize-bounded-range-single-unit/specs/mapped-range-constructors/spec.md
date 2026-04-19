## ADDED Requirements

### Requirement: Single bounded-pair carriers SHALL normalize natural boundaries onto one Gregorian axis
When a bounded-range unit is structured as `MappedRange(mode="bounded_pair")`, the resolver SHALL normalize the left endpoint to a concrete start boundary and the right endpoint to a concrete end boundary on the same Gregorian axis. This applies to same-grain and cross-grain natural-period endpoints as long as both sides admit deterministic Gregorian normalization.

#### Scenario: month-to-month bounded pair resolves to month boundaries
- **WHEN** the bounded-range carrier represents `2025年9月到12月`
- **THEN** normalization MUST yield `2025年9月1日` as the start boundary
- **AND** normalization MUST yield `2025年12月31日` as the end boundary

#### Scenario: quarter-to-month bounded pair resolves across grains
- **WHEN** the bounded-range carrier represents `2025年Q3到10月`
- **THEN** normalization MUST yield `2025年7月1日` as the start boundary
- **AND** normalization MUST yield `2025年10月31日` as the end boundary

#### Scenario: month-to-day bounded pair resolves across grains
- **WHEN** the bounded-range carrier represents `2025年9月到10月15日`
- **THEN** normalization MUST yield `2025年9月1日` as the start boundary
- **AND** normalization MUST yield `2025年10月15日` as the end boundary

### Requirement: Missing right-boundary years SHALL use minimal non-retreat inference
If a bounded-pair carrier's right boundary omits a year while the left boundary fixes the temporal frame, the system SHALL first inherit the left boundary year and SHALL then advance the right boundary by the smallest natural cycle needed to avoid a backward interval.

#### Scenario: same-year month range stays in the same year
- **WHEN** the bounded-range carrier represents `2025年9月到12月`
- **THEN** the right boundary MUST normalize as `2025年12月`
- **AND** the resolved interval MUST remain within 2025

#### Scenario: cross-year month range advances the right boundary
- **WHEN** the bounded-range carrier represents `去年12月到3月`
- **AND** the left boundary resolves to `2025年12月`
- **THEN** the right boundary MUST normalize as `2026年3月`
- **AND** the resolved interval MUST be `2025年12月1日` through `2026年3月31日`

#### Scenario: cross-year day range advances the right boundary
- **WHEN** the bounded-range carrier represents `2025年12月30日到1月2日`
- **THEN** the right boundary MUST normalize as `2026年1月2日`
- **AND** the resolved interval MUST be `2025年12月30日` through `2026年1月2日`

### Requirement: Explicit backward bounded ranges SHALL NOT be silently rewritten
If both boundaries explicitly include years and the resulting bounded range would still move backward in time, normalization MUST reject or degrade the carrier rather than silently changing a user-specified year.

#### Scenario: explicit backward month range is rejected
- **WHEN** the bounded-range carrier represents `2025年12月到2025年3月`
- **THEN** normalization MUST fail or degrade with a bounded-range validity error
- **AND** it MUST NOT silently reinterpret the right boundary as `2026年3月`

### Requirement: Non-natural endpoint classes SHALL remain out of scope for bounded_pair normalization
`MappedRange(mode="bounded_pair")` under this change SHALL accept only endpoints that normalize to Gregorian start/end boundaries from day/date or natural-period anchors. It MUST NOT admit `calendar_event`, rolling-window, or calendar-class endpoints in this change.

#### Scenario: holiday endpoint is rejected from bounded_pair normalization
- **WHEN** a bounded-range carrier attempts to represent `去年9月到国庆假期`
- **THEN** validation MUST reject or degrade that carrier as unsupported bounded-range endpoint semantics

#### Scenario: rolling endpoint is rejected from bounded_pair normalization
- **WHEN** a bounded-range carrier attempts to represent `最近一周到上周五`
- **THEN** validation MUST reject or degrade that carrier as unsupported bounded-range endpoint semantics
