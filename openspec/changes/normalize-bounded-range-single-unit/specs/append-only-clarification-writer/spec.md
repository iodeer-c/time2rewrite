## ADDED Requirements

### Requirement: Append-only clarification facts SHALL preserve single-unit bounded-range ownership
When upstream planning resolves an explicit bounded range as one canonical unit, clarification-fact generation SHALL preserve that ownership directly. The append-only writer MUST consume one bounded-range fact, not two endpoint facts that require semantic reconstruction at write time.

#### Scenario: month range yields one clarification fact
- **WHEN** the query is `2025年杭千公司9月到12月的收益是多少`
- **AND** upstream planning resolves `2025年9月到12月` as one bounded-range unit
- **THEN** clarification-fact generation MUST emit exactly one fact owned by that bounded-range unit
- **AND** it MUST NOT emit separate endpoint facts for `2025年9月` and `12月`

#### Scenario: grouped bounded range yields one parent-owned fact
- **WHEN** the query is `2025年9月到12月的各月份的收益是多少`
- **AND** upstream planning resolves the bounded range as one parent unit with grouped semantics
- **THEN** clarification-fact generation MUST emit one fact owned by the bounded-range unit
- **AND** that fact MUST explain the parent interval and grouped basis without reinterpreting the phrase as two endpoint units

### Requirement: Append-only clarified_query SHALL explain bounded ranges from canonical facts
The final `clarified_query` SHALL explain bounded ranges from the canonical clarification facts produced upstream. Writer-side endpoint coalescing MAY exist as a temporary resilience mechanism, but canonical success under this change is defined by the presence of one bounded-range fact and one appended clarification clause.

#### Scenario: clarified query explains one continuous bounded interval
- **WHEN** the canonical clarification fact for `2025年9月到12月` resolves to `2025年9月1日至2025年12月31日`
- **THEN** `clarified_query` MUST append one clause explaining that continuous interval
- **AND** it MUST NOT rely on an endpoint list such as `2025年9月1日至2025年9月30日、2025年12月1日至2025年12月31日`

#### Scenario: degraded endpoint-merging fallback is not the canonical contract
- **WHEN** the pipeline falls back to writer-side coalescing because upstream planning still emitted endpoint facts
- **THEN** that behavior MAY preserve temporary usability
- **AND** it MUST NOT be treated as satisfying the canonical bounded-range ownership contract for this change
