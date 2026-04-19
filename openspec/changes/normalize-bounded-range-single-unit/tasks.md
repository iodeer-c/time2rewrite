## 1. Stage A bounded-range segmentation

- [x] 1.1 Add Stage A prompt rules and examples that force explicit bounded ranges (`到 / 至 / - / ~ / ～`) to emit one unit when they denote one continuous interval.
- [x] 1.2 Add Stage A negative examples proving enumerations such as `2025年9月和12月` stay as separate units.
- [x] 1.3 Add Stage A tests for month-to-month, day-to-day, cross-year, and grouped bounded-range phrases.

## 2. Stage B canonical bounded-range carriers

- [x] 2.1 Add Stage B prompt rules and examples for canonical bounded-range carriers: `DateRange` for day-level endpoints and `MappedRange(mode="bounded_pair")` for natural or mixed natural-period endpoints.
- [x] 2.2 Implement right-boundary normalization with the minimal non-retreat rule for missing right-boundary years.
- [x] 2.3 Implement bounded-pair normalization for cross-grain natural endpoints that share the Gregorian axis.
- [x] 2.4 Add Stage B and constructor tests for `2025年9月到12月`, `去年12月到3月`, `2025年Q3到10月`, and `2025年9月到10月15日`.
- [x] 2.5 Add rejection/degrade tests for unsupported bounded-range endpoint classes such as holiday and rolling endpoints.

## 3. Structural validation and planning gates

- [x] 3.1 Add post-processor detection for obvious bounded ranges incorrectly emitted as two standalone endpoint units.
- [x] 3.2 Route split-endpoint bounded-range structures to retry or validation failure instead of letting them flow to writer repair as the main path.
- [x] 3.3 Update `clarification_plan` assembly tests so bounded-range queries assert one unit rather than two endpoint units.

## 4. Clarification facts and append-only output

- [x] 4.1 Update clarification-fact generation so canonical bounded-range plans yield one fact owned by the bounded-range unit.
- [x] 4.2 Update append-only writer tests so bounded-range `clarified_query` output explains one continuous interval, not two endpoint clauses.
- [x] 4.3 Keep any writer-side endpoint coalescing only as a defensive fallback and document it as non-canonical behavior.

## 5. Evaluation and regression coverage

- [x] 5.1 Update evaluator and golden cases to assert single-unit bounded-range ownership across `clarification_plan`, `clarification_items`, and `clarified_query`.
- [x] 5.2 Add end-to-end service tests for `2025年9月到12月`, `去年12月到3月`, `2025年Q3到10月`, and `2025年1月到3月每个月的每个工作日`.
- [x] 5.3 Run `openspec validate --strict normalize-bounded-range-single-unit` and the bounded-range test suite, then record the results in the implementation branch.
