# Time Query Service

一个基于 FastAPI、LangChain 和 DashScope Qwen 的时间字段解析服务，当前聚焦一件事：

- `LLM-1`：为回答问题生成 0 个到多个下游可执行时间字段
- `Code`：把每个时间字段求值成绝对时间区间
- `LLM-2`：可选，把原问题中的时间部分改写成绝对日期

当前顶层 schema 已收缩成数组模式：

```json
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "上周二",
      "expr": {}
    }
  ]
}
```

## 支持能力

- 自然周期：日、周、月、季度、半年、年
- 周期选择：具体月、具体季度、上半年、下半年
- 滚动窗口：最近 x 日、最近 x 周、最近 x 月、最近 x 季度、最近 x 半年、最近 x 年
- 多时间字段：同一句里按下游计算需要返回多个时间窗口
- 依赖表达：支持“去年同期”等引用前面时间表达的场景
- 子周期切片：支持“上个月前两周”“去年的前两个季度”这类大范围内部切片
- 子周期选择：支持“上个月第一周”“今年第一周”“第一个季度的第一周”这类在大周期里选第 N 个小周期
- 节假日区间：支持 `calendar_event_range`，可解析“去年国庆假期”这类命名节假日范围
- 业务日偏移：支持 `range_edge + business_day_offset`，可解析“节前最后一个工作日”这类表达

## 环境要求

- Python 3.11
- 建议使用项目根目录下的 `.venv`
- DashScope API Key

## 环境配置

先复制示例配置：

```bash
cp .env.example .env
```

然后编辑 `.env`：

```env
DASHSCOPE_API_KEY=your-dashscope-api-key
DASHSCOPE_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
DASHSCOPE_MODEL_NAME=qwen3.6-plus
BUSINESS_CALENDAR_ROOT=config/business_calendar
```

如果还没有虚拟环境：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

## 启动服务

```bash
.venv/bin/python -m uvicorn main:app --reload
```

启动后可访问：

- 服务地址：`http://127.0.0.1:8000`
- Swagger 文档：`http://127.0.0.1:8000/docs`

## 如何测试

全量单元测试：

```bash
.venv/bin/python -m unittest discover -s tests -v
```

批量跑 CSV 测试集：

```bash
.venv/bin/python -m time_query_service.evaluator time_query_testset_30.csv
```

结果会输出到 `artifacts/`：

- `*_results.jsonl`：每题完整过程
- `*_summary.csv`：可读汇总

## 接口示例

### 1. parse

```bash
curl -X POST http://127.0.0.1:8000/query/parse \
  -H "Content-Type: application/json" \
  -d '{
    "query": "今年3月和去年同期相比利润怎么样",
    "system_date": "2026-04-07",
    "timezone": "Asia/Shanghai"
  }'
```

返回示例：

```json
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "今年3月",
      "expr": {
        "op": "select_month",
        "month": 3,
        "base": {
          "op": "current_period",
          "unit": "year"
        }
      }
    },
    {
      "id": "t2",
      "text": "去年同期",
      "expr": {
        "op": "shift",
        "unit": "year",
        "value": -1,
        "base": {
          "op": "reference",
          "ref": "t1"
        }
      }
    }
  ]
}
```

### 2. resolve

```bash
curl -X POST http://127.0.0.1:8000/query/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "system_date": "2026-04-07",
    "timezone": "Asia/Shanghai",
    "parsed_time_expressions": {
      "time_expressions": [
        {
          "id": "t1",
          "text": "今年3月",
          "expr": {
            "op": "select_month",
            "month": 3,
            "base": {
              "op": "current_period",
              "unit": "year"
            }
          }
        },
        {
          "id": "t2",
          "text": "去年同期",
          "expr": {
            "op": "shift",
            "unit": "year",
            "value": -1,
            "base": {
              "op": "reference",
              "ref": "t1"
            }
          }
        }
      ]
    }
  }'
```

返回示例：

```json
{
    "resolved_time_expressions": [
      {
        "id": "t1",
        "text": "今年3月",
        "start_time": "2026-03-01 00:00:00",
        "end_time": "2026-03-31 23:59:59",
        "timezone": "Asia/Shanghai"
      },
      {
        "id": "t2",
        "text": "去年同期",
        "start_time": "2025-03-01 00:00:00",
        "end_time": "2025-03-31 23:59:59",
        "timezone": "Asia/Shanghai"
      }
    ]
}
```

### 3. rewrite

```bash
curl -X POST http://127.0.0.1:8000/query/rewrite \
  -H "Content-Type: application/json" \
  -d '{
    "original_query": "今年3月和去年同期相比利润怎么样",
    "resolved_time_expressions": {
      "resolved_time_expressions": [
        {
          "id": "t1",
          "text": "今年3月",
          "start_time": "2026-03-01 00:00:00",
          "end_time": "2026-03-31 23:59:59",
          "timezone": "Asia/Shanghai"
        },
        {
          "id": "t2",
          "text": "去年同期",
          "start_time": "2025-03-01 00:00:00",
          "end_time": "2025-03-31 23:59:59",
          "timezone": "Asia/Shanghai"
        }
      ]
    }
  }'
```

### 4. pipeline

```bash
curl -X POST http://127.0.0.1:8000/query/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "query": "今年3月和去年同期相比利润怎么样",
    "system_date": "2026-04-07",
    "timezone": "Asia/Shanghai",
    "rewrite": true
  }'
```

返回：

- `parsed_time_expressions`
- `resolved_time_expressions`
- `rewritten_query`

### 5. 子周期切片示例

```bash
curl -X POST http://127.0.0.1:8000/query/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "query": "上个月前两周的销售额是多少",
    "system_date": "2026-04-07",
    "timezone": "Asia/Shanghai",
    "rewrite": true
  }'
```

这类表达会解析为 `slice_subperiods`，而不是 `rolling`。例如“上个月前两周”会先求出“上个月”，再在这个月内按周一开始的周编号规则取前两周，最终得到 `2026-03-02 00:00:00 ~ 2026-03-15 23:59:59`。

### 6. 子周期选择示例

```bash
curl -X POST http://127.0.0.1:8000/query/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "query": "上个月的前两周的销售额分别是多少",
    "system_date": "2026-04-07",
    "timezone": "Asia/Shanghai",
    "rewrite": true
  }'
```

这类表达会拆成多个 `select_subperiod` 时间字段。例如“上个月的前两周分别是多少”会被拆成“上个月第一周”和“上个月第二周”，对应的时间窗口分别是 `2026-03-02 00:00:00 ~ 2026-03-08 23:59:59` 与 `2026-03-09 00:00:00 ~ 2026-03-15 23:59:59`。

### 7. “第 N 个周二”与“第 N 周的周二”区别

```bash
curl -X POST http://127.0.0.1:8000/query/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "query": "这个月第二个周二的销售额是多少",
    "system_date": "2026-09-10",
    "timezone": "Asia/Shanghai",
    "rewrite": true
  }'
```

这类表达会解析为 `select_occurrence`，表示“父周期内第 N 次出现的 weekday/weekend”。例如 `2026-09` 的“第二个周二”是 `2026-09-08`。

如果问题是“这个月第二周的周二”，则会先用 `select_subperiod(unit="week", index=2, ...)` 取“第二周”，再用 `select_weekday` 取该周的周二。在 `2026-09` 里，它对应的是 `2026-09-15`。这两种说法语义不同，resolver 不会再混用。

### 8. 节假日与业务日示例

```bash
curl -X POST http://127.0.0.1:8000/query/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "query": "去年国庆假期前最后一个工作日是哪天",
    "system_date": "2026-04-07",
    "timezone": "Asia/Shanghai",
    "rewrite": true
  }'
```

这类表达会先解析 `去年国庆假期` 的命名节假日区间，再通过 `range_edge(start)` 取假期开始日，最后用 `business_day_offset` 向前找到最近工作日。项目默认提供了 `config/business_calendar/CN/2025.json` 这份 demo 数据，可用于本地验证国庆相关示例。

## 实现说明

- `parse` 和 `rewrite` 依赖 DashScope/Qwen
- `resolve` 只走本地确定性代码，不依赖 LLM
- 通义千问不支持 OpenAI Structured Outputs，所以 parser 走“提示词约束 JSON + 服务端提取与校验”

## 常见问题

### 缺少 `DASHSCOPE_API_KEY`

如果 `parse` 或 `rewrite` 初始化时报：

```text
Missing DASHSCOPE_API_KEY
```

确认：

- `.env` 已填写真实 key
- 服务是在项目根目录启动的

### IDE 调试接口

可以直接使用 [`test_main.http`](/Users/td/PycharmProjects/time2rewirte/test_main.http)。
