# Append-Only Clarified Query Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace in-place rewrite with append-only `clarified_query` generation while keeping Stage A/B, post-processing, and resolver as the semantic core.

**Architecture:** Keep the existing `Stage A -> Stage B -> TimePlan -> ResolvedPlan` pipeline, but replace the final rewriter with a clarification-fact extractor plus append-only writer. Preserve compatibility by returning `clarified_query` as the primary field and `rewritten_query` as a temporary alias.

**Tech Stack:** FastAPI, Pydantic, LangChain message runners, pytest

---

### Task 1: Introduce the new service/output contract

**Files:**
- Create: `time_query_service/clarification_writer.py`
- Modify: `time_query_service/service.py`
- Modify: `main.py`
- Test: `tests/test_service.py`

- [ ] **Step 1: Write the failing service tests for clarified_query**

Add expectations in `tests/test_service.py` that:
- successful pipeline responses include `original_query` and `clarified_query`
- `rewritten_query` remains present and equals `clarified_query` for compatibility
- degraded time units still yield a non-null `clarified_query`

- [ ] **Step 2: Run the targeted service tests to see them fail**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_service.py -q`

- [ ] **Step 3: Add clarification-writer entry points**

Create `time_query_service/clarification_writer.py` with:
- clarification fact models
- fact extraction stubs from `TimePlan` / `ResolvedPlan`
- append-only rendering entry point returning `clarified_query`

- [ ] **Step 4: Wire the service to return clarified_query**

Modify `time_query_service/service.py` so `process_query()`:
- builds clarification facts
- returns `original_query`
- returns `clarified_query`
- keeps `rewritten_query = clarified_query` temporarily

- [ ] **Step 5: Convert PostProcessorValidationError to 422**

Modify `main.py` to map `PostProcessorValidationError` to `HTTPException(status_code=422, ...)`.

- [ ] **Step 6: Re-run the targeted service tests**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_service.py -q`

### Task 2: Replace rewrite bindings with clarification facts

**Files:**
- Create: `time_query_service/clarification_writer.py`
- Modify: `time_query_service/resolved_plan.py` (only if helper accessors are needed)
- Modify: `time_query_service/service.py`
- Test: `tests/test_clarification_writer.py`

- [ ] **Step 1: Write failing clarification-fact tests**

Create `tests/test_clarification_writer.py` covering:
- single-unit absolute interval fact
- comparison query with two facts in Stage A order
- unresolved/degraded fact rendering

- [ ] **Step 2: Run the new clarification-writer tests to see them fail**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_clarification_writer.py -q`

- [ ] **Step 3: Implement clarification fact extraction**

In `time_query_service/clarification_writer.py`, implement:
- ordered fact extraction by `TimePlan.units`
- fact text for absolute intervals
- explicit unresolved/degraded markers

- [ ] **Step 4: Re-run clarification-writer tests**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_clarification_writer.py -q`

### Task 3: Make append-only rendering the active final path

**Files:**
- Modify: `time_query_service/clarification_writer.py`
- Modify: `time_query_service/service.py`
- Modify: `time_query_service/rewriter.py`
- Test: `tests/test_clarification_writer.py`
- Test: `tests/test_service.py`

- [ ] **Step 1: Write failing append-only rendering tests**

Add tests asserting:
- the original question body stays intact
- appended clarifications explain time at sentence tail
- comparison and grouped queries preserve original result-shape wording

- [ ] **Step 2: Run append-only tests to confirm failure**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_clarification_writer.py tests/test_service.py -q`

- [ ] **Step 3: Implement deterministic append-only rendering**

Implement deterministic rendering rules in `clarification_writer.py` for:
- single-unit intervals
- grouped buckets
- comparison facts
- unresolved/degraded wording

- [ ] **Step 4: Delegate old rewrite entry points**

Modify `time_query_service/rewriter.py` so legacy callers either delegate to append-only clarification or remain unused by the active service path.

- [ ] **Step 5: Re-run append-only tests**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_clarification_writer.py tests/test_service.py -q`

### Task 4: Relax Stage A span dependence and update evaluation

**Files:**
- Modify: `time_query_service/post_processor.py`
- Modify: `time_query_service/stage_a_prompt.py`
- Modify: `time_query_service/evaluator.py`
- Test: `tests/test_stage_a_planner.py`
- Test: `tests/test_evaluator.py`

- [ ] **Step 1: Write failing tests for Stage A without hard span dependence**

Add tests covering:
- Stage A payloads with missing `surface_fragments`
- Stage A ordering as semantic contract
- evaluator no longer gating on span coverage

- [ ] **Step 2: Run Stage A/evaluator tests to confirm failure**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_stage_a_planner.py tests/test_evaluator.py -q`

- [ ] **Step 3: Make surface_fragments optional in Stage A critical path**

Update:
- `StageAUnitOutput` to make `surface_fragments` optional or default-empty
- `stage_a_prompt.py` instructions/examples to stop treating exact offsets as the primary contract
- `post_processor.py` Layer 3 gating to no longer hard-fail on missing span coverage for append-only mode

- [ ] **Step 4: Update evaluator gating**

Change `evaluator.py` so Stage A structural matching compares:
- unit order
- `render_text`
- `self_contained_text`
- `content_kind`
- `sources`
- `comparisons`
and no longer requires fragment coverage as a merge gate.

- [ ] **Step 5: Re-run Stage A/evaluator tests**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_stage_a_planner.py tests/test_evaluator.py -q`

### Task 5: End-to-end regression and docs

**Files:**
- Modify: `tests/test_service.py`
- Modify: `docs/local-run-and-test-guide.md`
- Modify: `/Users/td/PycharmProjects/time2rewirte/openspec/changes/append-only-clarified-query/tasks.md`

- [ ] **Step 1: Add end-to-end regression cases for append-only clarified queries**

Cover:
- aggregate query
- grouped query
- comparison query
- unresolved/degraded query

- [ ] **Step 2: Run focused regression suite**

Run: `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_service.py tests/test_stage_a_planner.py tests/test_evaluator.py tests/test_clarification_writer.py -q`

- [ ] **Step 3: Update local run/test docs**

Document:
- new `clarified_query` response field
- compatibility alias behavior
- recommended local smoke tests

- [ ] **Step 4: Mark completed OpenSpec tasks**

Update `/Users/td/PycharmProjects/time2rewirte/openspec/changes/append-only-clarified-query/tasks.md` checkboxes for the tasks actually completed in this implementation pass.

- [ ] **Step 5: Run final validation**

Run:
- `/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q tests/test_service.py tests/test_stage_a_planner.py tests/test_evaluator.py tests/test_clarification_writer.py`
- `openspec validate --strict append-only-clarified-query`
