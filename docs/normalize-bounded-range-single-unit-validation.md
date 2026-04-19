# normalize-bounded-range-single-unit 验证记录

日期：`2026-04-19`

## OpenSpec

```bash
openspec validate --strict normalize-bounded-range-single-unit
```

结果：

```text
Change 'normalize-bounded-range-single-unit' is valid
```

## 有界区间回归测试

```bash
/Users/td/PycharmProjects/time2rewirte/.venv/bin/python -m pytest -q -p no:cacheprovider \
  tests/test_stage_a_planner.py \
  tests/test_stage_b_planner.py \
  tests/test_carrier_materializer.py \
  tests/test_post_processor.py \
  tests/test_clarification_writer.py \
  tests/test_service.py \
  tests/test_evaluator.py \
  tests/test_golden_datasets.py \
  tests/test_new_resolver.py \
  tests/test_rewriter.py
```

结果：

```text
250 passed in 1.12s
```

## 本次重点覆盖

- Stage A 将显式 bounded range 收成单 unit，而不是拆成两个端点 sibling。
- Stage B 将 bounded range 落成单 carrier：`DateRange` 或 `MappedRange(mode="bounded_pair")`。
- `post_processor` 对 split endpoints 直接失败，不再让 writer 当主修复层。
- `clarification_plan`、`clarification_items`、`clarified_query` 都对齐到单 unit ownership。
- writer 只保留 split endpoints 的防御性 coalescing fallback，并在测试中明确它不是 canonical 路径。
