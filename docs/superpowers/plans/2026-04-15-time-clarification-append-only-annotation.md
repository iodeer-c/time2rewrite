# Time Clarification Append-Only Annotation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the query rewrite path on a fresh branch as a structured `LLM-1 -> code -> LLM-2` append-only annotation pipeline while selectively porting reusable donor infrastructure from `exp/solution-1`.

**Architecture:** Start from `main` in the clean worktree, port only reusable LLM/calendar/resolve assets from the donor branch, then implement a new `ClarificationPlan -> interval_set -> append-only annotation` subsystem. Keep deterministic time computation in code, keep both LLM roles narrow, and preserve the FastAPI service surface for pipeline execution.

**Tech Stack:** Python 3.11, FastAPI, Pydantic v2, LangChain/OpenAI runners, pytest, JSON business calendar fixtures.

---

## Working Context

- Worktree: `/tmp/time2rewirte-append-only-annotation`
- Branch: `feat/append-only-annotation`
- Approved design spec source: `/Users/td/PycharmProjects/time2rewirte/docs/superpowers/specs/2026-04-15-time-clarification-append-only-design.md`
- Donor branch workspace: `/Users/td/PycharmProjects/time2rewirte`

## Donor Assets To Reuse

- Reuse and reshape:
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/llm/__init__.py`
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/llm/config.py`
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/llm/factory.py`
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/llm/openai.py`
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/business_calendar.py`
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/config.py`
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/time_resolver.py`
  - `/Users/td/PycharmProjects/time2rewirte/config/business_calendar/CN/*.json`
  - `/Users/td/PycharmProjects/time2rewirte/config/llm.yaml.example`
- Reuse only as reference, do not port directly:
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/service.py`
  - `/Users/td/PycharmProjects/time2rewirte/tests/test_service.py`
  - `/Users/td/PycharmProjects/time2rewirte/tests/test_time_resolver.py`
- Explicitly do not port:
  - `/Users/td/PycharmProjects/time2rewirte/time_query_service/rewriter.py`
  - old validator / repair / abstain flow
  - old full-sentence rewrite prompt contracts

## Target File Layout

- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `docs/superpowers/specs/2026-04-15-time-clarification-append-only-design.md`
- Create: `config/llm.yaml.example`
- Create: `config/business_calendar/CN/2022.json`
- Create: `config/business_calendar/CN/2023.json`
- Create: `config/business_calendar/CN/2024.json`
- Create: `config/business_calendar/CN/2025.json`
- Create: `config/business_calendar/CN/2026.json`
- Create: `time_query_service/__init__.py`
- Create: `time_query_service/config.py`
- Create: `time_query_service/contracts.py`
- Create: `time_query_service/business_calendar.py`
- Create: `time_query_service/planner.py`
- Create: `time_query_service/plan_validator.py`
- Create: `time_query_service/time_resolver.py`
- Create: `time_query_service/annotation.py`
- Create: `time_query_service/service.py`
- Create: `time_query_service/llm/__init__.py`
- Create: `time_query_service/llm/config.py`
- Create: `time_query_service/llm/factory.py`
- Create: `time_query_service/llm/openai.py`
- Modify: `main.py`
- Create: `tests/__init__.py`
- Create: `tests/test_main.py`
- Create: `tests/test_llm.py`
- Create: `tests/test_business_calendar.py`
- Create: `tests/test_contracts.py`
- Create: `tests/test_planner.py`
- Create: `tests/test_time_resolver.py`
- Create: `tests/test_annotation.py`
- Create: `tests/test_service.py`

## Local Setup

- Create a worktree-local virtualenv:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```

- Run tests with the worktree-local environment:

```bash
.venv/bin/python -m pytest -q
```

## Task 1: Bootstrap The Fresh Branch

**Files:**
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `docs/superpowers/specs/2026-04-15-time-clarification-append-only-design.md`
- Create: `tests/__init__.py`
- Create: `tests/test_main.py`
- Modify: `main.py`

- [x] **Step 1: Write the failing bootstrap test**

```python
from fastapi.testclient import TestClient

from main import app


def test_root_returns_service_banner():
    client = TestClient(app)
    assert client.get("/").json() == {"message": "time-query-service"}
```

- [x] **Step 2: Run the bootstrap test and verify it fails**

Run: `.venv/bin/python -m pytest tests/test_main.py::test_root_returns_service_banner -q`

Expected: FAIL because `main.py` still returns the scaffold `"Hello World"` payload.

- [x] **Step 3: Add the minimal bootstrap implementation**

```python
app = FastAPI(title="Time Query Service")


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "time-query-service"}
```

Also:
- copy the approved design doc into `docs/superpowers/specs/2026-04-15-time-clarification-append-only-design.md`
- add `requirements.txt` based on donor runtime dependencies
- add `requirements-dev.txt` with `pytest>=8,<9`
- add `tests/__init__.py`

- [x] **Step 4: Run the bootstrap test again**

Run: `.venv/bin/python -m pytest tests/test_main.py::test_root_returns_service_banner -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add main.py requirements.txt requirements-dev.txt docs/superpowers/specs/2026-04-15-time-clarification-append-only-design.md tests/__init__.py tests/test_main.py
git commit -m "chore: bootstrap append-only annotation branch"
```

## Task 2: Port LLM Runtime Infrastructure

**Files:**
- Create: `time_query_service/llm/__init__.py`
- Create: `time_query_service/llm/config.py`
- Create: `time_query_service/llm/factory.py`
- Create: `time_query_service/llm/openai.py`
- Create: `time_query_service/config.py`
- Create: `config/llm.yaml.example`
- Create: `tests/test_llm.py`

- [x] **Step 1: Write the failing LLM config test**

```python
from pathlib import Path

from time_query_service.llm import load_llm_runtime_config


def test_load_llm_runtime_config_reads_role_map(tmp_path: Path):
    config_path = tmp_path / "llm.yaml"
    config_path.write_text(
        "default_role: planner\nroles:\n  planner:\n    model_type: openai\n    model_name: gpt-test\n    api_key: test-key\n",
        encoding="utf-8",
    )

    runtime_config = load_llm_runtime_config(config_path=config_path)

    assert runtime_config.default_role == "planner"
    assert runtime_config.roles["planner"].model_name == "gpt-test"
```

- [x] **Step 2: Run the LLM config test and verify it fails**

Run: `.venv/bin/python -m pytest tests/test_llm.py::test_load_llm_runtime_config_reads_role_map -q`

Expected: FAIL because the `time_query_service.llm` package does not exist yet.

- [x] **Step 3: Port the minimal LLM runtime modules**

```python
class LLMRuntimeConfig(BaseModel):
    default_role: str
    roles: dict[str, LLMConfig]


def load_llm_runtime_config(config_path: Path | None = None) -> LLMRuntimeConfig:
    ...
```

Port and adapt the donor modules so the new branch supports role-scoped `planner` and `annotator` runners without carrying old `parser/rewriter/semantic-anchor` naming assumptions.

- [x] **Step 4: Run the LLM tests**

Run: `.venv/bin/python -m pytest tests/test_llm.py -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add time_query_service/llm time_query_service/config.py config/llm.yaml.example tests/test_llm.py
git commit -m "feat: port llm runtime infrastructure"
```

## Task 3: Port Business Calendar Infrastructure

**Files:**
- Create: `config/business_calendar/CN/2022.json`
- Create: `config/business_calendar/CN/2023.json`
- Create: `config/business_calendar/CN/2024.json`
- Create: `config/business_calendar/CN/2025.json`
- Create: `config/business_calendar/CN/2026.json`
- Create: `time_query_service/business_calendar.py`
- Create: `tests/test_business_calendar.py`

- [x] **Step 1: Write the failing business calendar load test**

```python
from pathlib import Path

from time_query_service.business_calendar import JsonBusinessCalendar


def test_calendar_loads_cn_fixture_root():
    calendar = JsonBusinessCalendar.from_root(Path("config/business_calendar"))
    assert calendar.has_year("CN", 2026) is True
```

- [x] **Step 2: Run the business calendar test and verify it fails**

Run: `.venv/bin/python -m pytest tests/test_business_calendar.py::test_calendar_loads_cn_fixture_root -q`

Expected: FAIL because neither the fixtures nor the loader exist yet.

- [x] **Step 3: Port the loader and calendar data**

```python
class JsonBusinessCalendar:
    @classmethod
    def from_root(cls, root: Path) -> "JsonBusinessCalendar":
        ...
```

Port the donor JSON fixtures and the reusable calendar loader / query helpers. Keep the business calendar API small and deterministic so later tasks can resolve `holiday`, `workday`, `trading_day`, and `business_day` selectors against exact dates.

- [x] **Step 4: Run the business calendar tests**

Run: `.venv/bin/python -m pytest tests/test_business_calendar.py -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add config/business_calendar/CN time_query_service/business_calendar.py tests/test_business_calendar.py
git commit -m "feat: port business calendar assets"
```

## Task 4: Define Clarification Contracts

**Files:**
- Create: `time_query_service/contracts.py`
- Create: `tests/test_contracts.py`

- [x] **Step 1: Write the failing contract validation tests**

```python
import pytest

from time_query_service.contracts import ClarificationPlan


def test_contract_accepts_reference_window_node():
    plan = ClarificationPlan.model_validate(
        {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "去年同期",
                    "ordinal": 1,
                    "needs_clarification": True,
                    "node_kind": "reference_window",
                    "reason_code": "same_period_reference",
                    "resolution_spec": {
                        "reference_node_id": "n0",
                        "alignment": "same_period",
                        "shift": {"unit": "year", "value": -1},
                    },
                }
            ],
            "comparison_groups": [],
        }
    )

    assert plan.nodes[0].node_kind == "reference_window"
```

- [x] **Step 2: Run the contract tests and verify they fail**

Run: `.venv/bin/python -m pytest tests/test_contracts.py -q`

Expected: FAIL because `ClarificationPlan` and the discriminated union models do not exist yet.

- [x] **Step 3: Implement the contracts**

```python
class ClarificationPlan(BaseModel):
    nodes: list[ClarificationNode]
    comparison_groups: list[ComparisonGroup]
```

Add:
- `ClarificationNode`
- `ComparisonGroup`
- `ComparisonMember`
- `Interval`
- `IntervalSet`
- `ClarificationItem`
- discriminated `resolution_spec` variants for all approved `node_kind` values

- [x] **Step 4: Run the contract tests**

Run: `.venv/bin/python -m pytest tests/test_contracts.py -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add time_query_service/contracts.py tests/test_contracts.py
git commit -m "feat: add clarification plan contracts"
```

## Task 5: Implement Planner And Plan Validation

**Files:**
- Create: `time_query_service/planner.py`
- Create: `time_query_service/plan_validator.py`
- Create: `tests/test_planner.py`

- [x] **Step 1: Write the failing planner/validator tests**

```python
from time_query_service.plan_validator import validate_plan


def test_validate_plan_rejects_missing_comparison_member_node():
    plan = {
        "nodes": [],
        "comparison_groups": [
            {
                "group_id": "g1",
                "relation_type": "generic_compare",
                "anchor_text": "相比",
                "anchor_ordinal": 1,
                "direction": "subject_to_reference",
                "members": [{"node_id": "missing", "role": "reference"}],
            }
        ],
    }

    assert validate_plan(plan).is_valid is False
```

- [x] **Step 2: Run the planner/validator tests and verify they fail**

Run: `.venv/bin/python -m pytest tests/test_planner.py -q`

Expected: FAIL because neither the validator nor the planner wrapper exist yet.

- [x] **Step 3: Implement the planner and structural validator**

```python
class ClarificationPlanner:
    def plan_query(self, *, original_query: str, system_date: str | None, system_datetime: str | None, timezone: str) -> ClarificationPlan:
        ...
```

Add:
- `ClarificationPlanner` with narrow JSON-only prompt contract
- `validate_plan(...)` returning a typed validation result
- one retry hook for structurally invalid plans

- [x] **Step 4: Run planner/validator tests**

Run: `.venv/bin/python -m pytest tests/test_planner.py -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add time_query_service/planner.py time_query_service/plan_validator.py tests/test_planner.py
git commit -m "feat: add clarification planner and validation"
```

## Task 6: Implement Deterministic Resolution And Rendering

**Files:**
- Create: `time_query_service/time_resolver.py`
- Create: `tests/test_time_resolver.py`

- [x] **Step 1: Write the failing resolution tests**

```python
from time_query_service.time_resolver import resolve_plan


def test_resolve_workday_selector_returns_compressed_intervals():
    result = resolve_plan(
        plan={
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "本月至今每个工作日",
                    "ordinal": 1,
                    "needs_clarification": True,
                    "node_kind": "window_with_calendar_selector",
                    "reason_code": "holiday_or_business_calendar",
                    "resolution_spec": {
                        "window": {
                            "kind": "relative_window",
                            "relative_type": "to_date",
                            "unit": "month",
                            "direction": "current",
                            "value": 1,
                            "include_today": True,
                        },
                        "selector": {"selector_type": "workday"},
                    },
                }
            ],
            "comparison_groups": [],
        },
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
    )

    assert result.items[0].display_exact_time.startswith("2026年4月1日")
```

- [x] **Step 2: Run the resolution tests and verify they fail**

Run: `.venv/bin/python -m pytest tests/test_time_resolver.py -q`

Expected: FAIL because `resolve_plan` and interval rendering do not exist yet.

- [x] **Step 3: Implement the deterministic resolver**

```python
def resolve_plan(*, plan: ClarificationPlan, system_date: str | None, system_datetime: str | None, timezone: str, business_calendar: BusinessCalendarPort | None) -> ResolutionResult:
    ...
```

Use donor resolve helpers where they are still correct, but drive them from `resolution_spec` instead of the old parsed-expression schema. Implement:
- `explicit_window`
- `relative_window`
- `holiday_window`
- `offset_window`
- `reference_window`
- `window_with_regular_grain`
- `window_with_calendar_selector`
- `calendar_selector_only`

Render exact time strings from ordered non-overlapping interval sets. Fail fast on missing calendar coverage for `calendar_sensitive` selectors.

- [x] **Step 4: Run the resolution tests**

Run: `.venv/bin/python -m pytest tests/test_time_resolver.py -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add time_query_service/time_resolver.py tests/test_time_resolver.py
git commit -m "feat: implement clarification plan resolution"
```

## Task 7: Implement Append-Only Annotation Rendering

**Files:**
- Create: `time_query_service/annotation.py`
- Create: `tests/test_annotation.py`

- [x] **Step 1: Write the failing annotation tests**

```python
from time_query_service.annotation import annotate_query


def test_annotate_query_keeps_non_time_text_unchanged():
    rewritten = annotate_query(
        original_query="昨天杭千公司的收益是多少？",
        clarification_items=[
            {
                "node_id": "n1",
                "render_text": "昨天",
                "ordinal": 1,
                "display_exact_time": "2026年4月14日",
            }
        ],
        comparison_groups=[],
    )

    assert rewritten == "昨天（2026年4月14日）杭千公司的收益是多少？"
```

- [x] **Step 2: Run the annotation tests and verify they fail**

Run: `.venv/bin/python -m pytest tests/test_annotation.py -q`

Expected: FAIL because the append-only renderer does not exist yet.

- [x] **Step 3: Implement the renderer**

```python
class AppendOnlyAnnotationRenderer:
    def render(self, *, original_query: str, clarification_items: list[ClarificationItem], comparison_groups: list[ComparisonGroup]) -> str | None:
        ...
```

Add:
- a narrow LLM-backed renderer wrapper
- no-op short circuit when `clarification_items` is empty
- lightweight output guard: non-empty, final-question shape, annotation trace present when items exist

- [x] **Step 4: Run the annotation tests**

Run: `.venv/bin/python -m pytest tests/test_annotation.py -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add time_query_service/annotation.py tests/test_annotation.py
git commit -m "feat: add append-only annotation renderer"
```

## Task 8: Wire The Service Pipeline And FastAPI Endpoints

**Files:**
- Create: `time_query_service/service.py`
- Modify: `main.py`
- Create: `tests/test_service.py`

- [x] **Step 1: Write the failing service test**

```python
from time_query_service.service import QueryPipelineService


def test_process_query_returns_original_query_when_no_clarification_needed():
    service = QueryPipelineService(
        planner=...,
        resolver=...,
        annotator=...,
    )

    response = service.process_query(
        query="2025年杭千公司每天的收益是多少？",
        system_date="2026-04-15",
        timezone="Asia/Shanghai",
        rewrite=True,
    )

    assert response["rewritten_query"] == "2025年杭千公司每天的收益是多少？"
```

- [x] **Step 2: Run the service test and verify it fails**

Run: `.venv/bin/python -m pytest tests/test_service.py::test_process_query_returns_original_query_when_no_clarification_needed -q`

Expected: FAIL because the new pipeline service does not exist yet.

- [x] **Step 3: Implement the service orchestration**

```python
class QueryPipelineService:
    def process_query(self, *, query: str, system_date: str | None, system_datetime: str | None, timezone: str, rewrite: bool = False) -> dict[str, Any]:
        ...
```

Wire:
- `planner.plan_query`
- `validate_plan`
- one structural retry
- `resolve_plan`
- `annotate_query`
- FastAPI endpoints for `/query/pipeline`

If useful for debugging parity, also expose `/query/plan`, `/query/resolve`, and `/query/rewrite`, but do not restore the old full-rewrite semantics.

- [x] **Step 4: Run the service tests**

Run: `.venv/bin/python -m pytest tests/test_service.py -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add main.py time_query_service/service.py tests/test_service.py
git commit -m "feat: wire append-only pipeline service"
```

## Task 9: Add End-To-End Regression Coverage

**Files:**
- Modify: `tests/test_time_resolver.py`
- Modify: `tests/test_annotation.py`
- Modify: `tests/test_service.py`

- [x] **Step 1: Add failing regression tests for approved scenarios**

```python
def test_pipeline_returns_null_when_calendar_data_missing():
    ...


def test_pipeline_keeps_explicit_window_plus_regular_grain_unchanged():
    ...


def test_pipeline_renders_workday_ranges_without_summarizing():
    ...


def test_pipeline_preserves_same_period_comparison_structure():
    ...
```

- [x] **Step 2: Run the regression suite and verify gaps**

Run: `.venv/bin/python -m pytest tests/test_time_resolver.py tests/test_annotation.py tests/test_service.py -q`

Expected: at least one FAIL exposing a missing edge case from the approved design.

- [x] **Step 3: Fill the smallest missing implementation gaps**

```python
# Examples:
# - handle repeated render_text localization through ordinal + surface_fragments
# - fail hard on missing calendar coverage
# - keep explicit window + regular grain as no-op
```

- [x] **Step 4: Run the full suite**

Run: `.venv/bin/python -m pytest -q`

Expected: PASS

- [x] **Step 5: Commit**

```bash
git add tests/test_time_resolver.py tests/test_annotation.py tests/test_service.py time_query_service
git commit -m "test: lock append-only annotation regressions"
```

## Completion Checklist

- [x] Fresh branch contains the approved design doc
- [x] Donor assets are ported selectively, not wholesale
- [x] No old `rewriter.py` path or repair/validator stack is restored
- [x] `ClarificationPlan` is the sole planner-to-code contract
- [x] `comparison_groups` and `reference_window` are covered by tests
- [x] `calendar_sensitive` outputs render full compressed interval lists
- [x] Explicit window + regular grain no-op behavior is covered
- [x] Missing calendar coverage returns `null`
- [x] Full test suite passes in the worktree-local virtualenv
