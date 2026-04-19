# 本地启动与测试指南（append-only clarified query）

本文档面向 `feature/append-only-clarified-query` 分支，说明如何在本地启动服务、验证 append-only 时间澄清输出，以及运行本地测试和真实 LLM 评测。

## 1. 环境准备

### 1.1 Python 虚拟环境

如果本地还没有 `.venv`：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```

已有 `.venv` 直接复用即可。

### 1.2 LLM 配置

真实管线依赖本地 `config/llm.yaml`：

```bash
cp config/llm.yaml.example config/llm.yaml
```

当前 append-only 链路会读取这些角色：

- `stage_a` 或 `planner`
- `stage_b` 或 `planner`
- `rewriter` / `annotator` / `fallback`

说明：

- `Stage A` 和 `Stage B` 仍然负责时间语义识别和 carrier 结构化。
- 最终 writer 已改成 append-only clarification writer，只负责在句末追加时间说明。

### 1.3 业务日历

默认会读取仓库内的：

- `config/business_calendar/CN/*.json`

通常不需要额外配置。若要覆盖：

```bash
export BUSINESS_CALENDAR_ROOT=/your/custom/calendar/root
```

## 2. 启动服务

```bash
.venv/bin/python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

启动后先跑根接口：

```bash
curl -s http://127.0.0.1:8000/
```

预期：

```json
{"message":"time-query-service"}
```

## 3. 主接口与返回值

当前主接口仍是：

- `POST /query/pipeline`

示例：

```bash
curl -s http://127.0.0.1:8000/query/pipeline \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "今年3月和去年同期的收益分别是多少",
    "system_date": "2026-04-17",
    "timezone": "Asia/Shanghai",
    "rewrite": true
  }'
```

append-only 方案下，关键返回字段是：

- `original_query`
- `clarification_items`
- `clarified_query`

兼容字段：

- `rewritten_query`

当前兼容策略是：

- `rewritten_query = clarified_query`

## 4. append-only 输出怎么看

append-only 模式不再做原位局部改写，而是：

- 保留原问题主体和业务词
- 在句末追加时间澄清

例如：

```json
{
  "original_query": "2025年3月收益",
  "clarified_query": "2025年3月收益（2025年3月指2025年3月1日至2025年3月31日）"
}
```

再例如：

```json
{
  "original_query": "最近5个休息日收益是多少",
  "clarified_query": "最近5个休息日收益是多少（最近5个休息日当前无法确定）"
}
```

## 5. 本地测试建议

### 5.1 append-only 关键回归

这组不依赖真实模型，适合日常开发：

```bash
.venv/bin/python -m pytest -q -p no:cacheprovider \
  tests/test_main_api.py \
  tests/test_clarification_writer.py \
  tests/test_service.py \
  tests/test_stage_a_planner.py \
  tests/test_evaluator.py \
  tests/test_rewriter.py \
  tests/test_post_processor.py \
  tests/test_new_plan_validator.py
```

### 5.2 OpenSpec 校验

```bash
openspec validate --strict append-only-clarified-query
```

如果尾部出现 `edge.openspec.dev` 的 telemetry DNS 报错，通常只是外网发送失败，不影响本地变更有效性。

## 6. 真实 LLM 评测

真实评测入口仍是：

- `time_query_service/evaluator.py`

示例：

```bash
.venv/bin/python -m time_query_service.evaluator \
  --suite all \
  --llm-config config/llm.yaml \
  --output /tmp/append_only_eval_report.json
```

当前 `layer1` 仍然以 `ResolvedPlan` 正确性为主 gate，同时会额外输出：

- `clarification_items`
- `clarified_query`
- `clarified_query_validation`
- `clarified_query_summary`

也就是说：

- 主 gate 还是时间解析是否正确
- append-only writer 会额外验证时间说明是否把必须的 clarification facts 说全了

## 7. 日常开发建议

### 7.1 只想确认 API 正常

```bash
.venv/bin/python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
curl -s http://127.0.0.1:8000/
```

### 7.2 改了 append-only writer 或 service 输出

优先跑：

```bash
.venv/bin/python -m pytest -q -p no:cacheprovider \
  tests/test_clarification_writer.py \
  tests/test_service.py \
  tests/test_main_api.py \
  tests/test_rewriter.py
```

### 7.3 改了 Stage A / post-processor / evaluator 契约

优先跑：

```bash
.venv/bin/python -m pytest -q -p no:cacheprovider \
  tests/test_stage_a_planner.py \
  tests/test_post_processor.py \
  tests/test_new_plan_validator.py \
  tests/test_evaluator.py
```
