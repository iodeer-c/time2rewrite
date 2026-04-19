## ADDED Requirements

### Requirement: Explicit bounded ranges SHALL be emitted as one semantic unit
The planner SHALL emit one Stage A unit for an explicit bounded-range phrase when connector-led text (`到`, `至`, `-`, `~`, `～`) denotes one continuous temporal interval rather than an enumeration. The unit's `render_text` SHALL cover the full bounded-range phrase, including any attached grouped/filtering scaffold that semantically belongs to that range.

#### Scenario: month-to-month bounded range is one unit
- **WHEN** the query is `2025年9月到12月杭千公司的收益是多少`
- **THEN** Stage A MUST emit exactly one time unit for `2025年9月到12月`
- **AND** it MUST NOT emit separate sibling units for `2025年9月` and `12月`

#### Scenario: grouped bounded range stays one unit
- **WHEN** the query is `2025年9月到12月的各月份的收益是多少`
- **THEN** Stage A MUST emit exactly one time unit for `2025年9月到12月的各月份`
- **AND** it MUST NOT split the range endpoints into separate units before grouped semantics are applied

#### Scenario: enumeration is not mistaken for a bounded range
- **WHEN** the query is `2025年9月和12月的收益是多少`
- **THEN** Stage A MUST emit two separate units for `2025年9月` and `12月`
- **AND** it MUST NOT coerce the phrase into one bounded-range unit

### Requirement: Bounded-range units SHALL use one canonical carrier
Stage B SHALL structure one bounded-range unit into exactly one canonical carrier. If both endpoints are explicit day-level dates, the carrier MUST use `DateRange`. Otherwise, if both endpoints can normalize onto one Gregorian start/end axis, the carrier MUST use `MappedRange(mode="bounded_pair")`.

#### Scenario: day-to-day bounded range uses DateRange
- **WHEN** Stage B structures `2025年3月1日到3月10日`
- **THEN** the carrier MUST use `DateRange`
- **AND** the resolved interval MUST be `2025年3月1日` through `2025年3月10日`

#### Scenario: month-to-month bounded range uses bounded_pair
- **WHEN** Stage B structures `2025年9月到12月`
- **THEN** the carrier MUST use `MappedRange(mode="bounded_pair")`
- **AND** it MUST NOT emit two standalone `NamedPeriod(month)` carriers

#### Scenario: cross-grain natural bounded range uses bounded_pair
- **WHEN** Stage B structures `2025年Q3到10月`
- **THEN** the carrier MUST use `MappedRange(mode="bounded_pair")`
- **AND** the left endpoint MUST normalize from the quarter start boundary while the right endpoint normalizes to the month end boundary

### Requirement: Grouped and filtered semantics SHALL hang from one bounded-range parent
If a bounded-range phrase also carries grouped or filtering semantics, those semantics MUST be expressed over a single bounded-range parent rather than over independently structured endpoint units.

#### Scenario: grouped months hang from one bounded-range parent
- **WHEN** the query is `2025年9月到12月的各月份的收益是多少`
- **THEN** the plan MUST contain one bounded-range parent unit for `2025年9月到12月`
- **AND** grouped month semantics MUST be applied to that one parent

#### Scenario: grouped-and-filtered query hangs from one bounded-range parent
- **WHEN** the query is `2025年1月到3月每个月的每个工作日的收益是多少`
- **THEN** the plan MUST contain one bounded-range parent unit for `2025年1月到3月`
- **AND** month grouping and workday filtering MUST both derive from that one parent instead of from split endpoints

### Requirement: Split-endpoint bounded-range plans SHALL fail before clarification writing
If Stage A and Stage B still produce two standalone endpoint units for an obvious bounded range, the post-processor MUST reject that structure as invalid and MUST NOT allow the append-only writer to become the primary semantic repair layer.

#### Scenario: split month endpoints are rejected before writer
- **WHEN** the query text expresses `2025年9月到12月`
- **AND** the assembled plan contains two standalone month units that correspond to the two endpoints
- **THEN** post-processor validation MUST fail or trigger retry
- **AND** the pipeline MUST NOT treat writer-side endpoint coalescing as the canonical success path

### Requirement: Bounded-range ownership SHALL stay single-unit through clarification output
For a successful bounded-range query, `clarification_plan.units`, `clarification_items`, and the appended clause in `clarified_query` MUST all refer to the same single bounded-range unit.

#### Scenario: clarification plan and clarified query agree on one bounded range
- **WHEN** the query is `2025年杭千公司9月到12月的收益是多少`
- **THEN** `clarification_plan.units` MUST contain one bounded-range unit for `2025年9月到12月`
- **AND** `clarification_items` MUST contain one fact owned by that unit
- **AND** `clarified_query` MUST explain one bounded interval `2025年9月1日至2025年12月31日`
