# Time Query Service

一个基于 FastAPI、LangChain 和可配置 OpenAI-compatible LLM 的时间字段解析服务，当前聚焦一件事：

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

## 相关文档

- 当前实现现状：`docs/time_processing_current_state.md`
- 调用与使用说明：`docs/time_processing_usage_guide.md`

## 环境要求

- Python 3.11
- 建议使用项目根目录下的 `.venv`
- DashScope API Key

## 环境配置

先复制示例配置：

```bash
cp .env.example .env
```

当前 LLM 契约分成两层：

- `config/llm.yaml`：声明 `parser` / `rewriter` 两个 role 使用什么 provider、model、base URL 和额外参数
- `.env`：提供 `api_key_env` / `proxy_url_env` 指向的真实值；如果某个 role 在 YAML 里同时写了环境变量名和写死值，运行时会优先取环境变量，取不到再回退到 YAML 中的写死值

默认的 [`config/llm.yaml`](/Users/td/PycharmProjects/time2rewirte/config/llm.yaml) 已经提供了一个 Qwen/DashScope 的 OpenAI-compatible 示例。然后编辑 `.env`：

```env
PARSER_API_KEY=your-llm-api-key
REWRITER_API_KEY=your-llm-api-key
PARSER_PROXY_URL=
REWRITER_PROXY_URL=
BUSINESS_CALENDAR_ROOT=config/business_calendar
```

如果你想直接在 YAML 里写死密钥，也可以把 `config/llm.yaml` 里的某个 role 改成：

```yaml
roles:
  parser:
    model_type: openai
    model_name: qwen3.6-plus
    api_base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    api_key_env: PARSER_API_KEY
    api_key: your-inline-api-key
    proxy_url_env: PARSER_PROXY_URL
    proxy_url: http://127.0.0.1:7890
    verify_ssl: false
    additional_params:
      temperature: 0
      stream_usage: true
      extra_body:
        enable_thinking: false
```

如果你需要代理，直接在对应 role 下配置：

- `proxy_url_env`：代理地址环境变量名
- `proxy_url`：写死的代理地址，作为回退值
- `verify_ssl`：是否校验证书，默认 `true`

例如：

```yaml
roles:
  parser:
    proxy_url_env: PARSER_PROXY_URL
    proxy_url: http://127.0.0.1:7890
    verify_ssl: false
```

当前代理注入覆盖 `openai`、`tongyi`、`azure` 这几条创建路径。

工厂当前支持的 `model_type` 包括：

- `openai`
- `tongyi`
- `azure`
- `vllm`

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
  "rolling_includes_today": false,
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
      "rolling_includes_today": false,
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

补充说明：

- `rolling` 类时间窗口默认以 `system_date - 1` 为右端锚点，也就是默认**不含当天**。
- 如果要保持旧的“含当天”行为，需要在解析结果里显式设置 `"rolling_includes_today": true`。
- `POST /query/parse` 和 `POST /query/pipeline` 返回的 `parsed_time_expressions` 会显式包含 `rolling_includes_today`，即使模型原始 JSON 省略了该字段。
- 如果问题里完全没有时间信息，`POST /query/parse` 和 `POST /query/pipeline` 会默认补一个“昨天”的单日时间字段。
- 这个默认“昨天”只影响 parser 驱动的链路；直接调用 `POST /query/rewrite` 且传入空 `resolved_time_expressions` 时，仍会直接返回原问题。

无时间示例：

```bash
curl -X POST http://127.0.0.1:8000/query/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "query": "帮我看看数据",
    "system_date": "2026-04-06",
    "timezone": "Asia/Shanghai",
    "rewrite": true
  }'
```

这类请求会先补成“昨天”，即 `2026-04-05 00:00:00 ~ 2026-04-05 23:59:59`，`rewritten_query` 可能改写为 `帮我看看2026年4月5日的数据`。

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

- `parse` 和 `rewrite` 依赖平台级 `LLMConfig + LLMFactory`
- 默认示例配置使用 DashScope/Qwen 的 OpenAI-compatible 接口
- `resolve` 只走本地确定性代码，不依赖 LLM
- 通义千问不支持 OpenAI Structured Outputs，所以 parser 走“提示词约束 JSON + 服务端提取与校验”

## 常见问题

### 缺少 LLM 配置文件

如果服务初始化 LLM 时提示：

```text
Missing LLM config file
```

确认：

- [`config/llm.yaml`](/Users/td/PycharmProjects/time2rewirte/config/llm.yaml) 存在
- 服务是在项目根目录启动的

### 缺少 role 的 API Key

如果 `parse` 或 `rewrite` 初始化时报：

```text
Missing API key for role=parser
```

或：

```text
Missing API key for role=rewriter
```

确认：

- `.env` 已填写 `config/llm.yaml` 中对应 `api_key_env` 的真实值
- 或者对应 role 的 `api_key` 已直接写在 YAML 中

### 代理不生效

如果你已经配置了代理但请求仍然直连，确认：

- `.env` 已填写 `config/llm.yaml` 中对应 `proxy_url_env` 的值，或者 YAML 里的 `proxy_url` 非空
- 修改代理配置后已重启服务
- 当前 `model_type` 是 `openai`、`tongyi` 或 `azure`

### IDE 调试接口

可以直接使用 [`test_main.http`](/Users/td/PycharmProjects/time2rewirte/test_main.http)。
