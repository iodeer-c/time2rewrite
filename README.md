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
- 显式时间点与自然周期：支持 `literal_date`、`literal_datetime`、`literal_period` 和 `anchor(system_datetime)`，可表达“2026年4月1日”“2024年”“2024年第一季度”“到现在这一刻”
- 周期选择：具体月、具体季度、上半年、下半年
- partial period：支持 `period_to_date`，可表达“本月至今”“本季度截至昨天”“本月至当前时刻”
- 滚动窗口：最近 x 日、最近 x 周、最近 x 月、最近 x 季度、最近 x 半年、最近 x 年，支持按表达式单独指定锚点和是否包含锚点当天
- 精确滚动窗口：支持 `rolling_hours`、`rolling_minutes` 和 `rolling_business_days`
- 多时间字段：同一句里按下游计算需要返回多个时间窗口
- 依赖表达：支持“去年同期”等引用其他时间表达的场景，允许前向引用
- 子周期切片：支持“上个月前两周”“去年的前两个季度”这类大范围内部切片
- 子周期选择：支持“上个月第一周”“今年第一周”“第一个季度的第一周”这类在大周期里选第 N 个小周期，并支持 `complete_only=true` 过滤首尾 partial 成员
- 枚举组合：支持把枚举结果当作普通子表达式继续计算，并通过 `select_segment` / `segments_bounds` 显式转回单段区间；`select_segment` 现支持 `first / last / nth / nth_from_end`
- 集合切片：支持 `slice_segments`，可表达“本月前3个工作日”“最后2个补班日”
- mapped constructor：`period_to_date`、`rolling`、`rolling_minutes`、`rolling_business_days`、`bounded_range` 现在都支持对有序集合逐成员求值
- year-set 映射：`select_month` / `select_quarter` / `select_half_year`、`calendar_event_range`、`enumerate_makeup_workdays` 现在支持 year-valued set / `schedule_year_expr`
- 节假日区间：支持 `calendar_event_range(region, event_key, schedule_year, scope)`，可解析“去年国庆假期”这类命名节假日范围，并支持继续下钻“清明节假期每天”“清明节假期第二天”“去年国庆假期第一个周末”
- 业务日偏移：支持 `range_edge + business_day_offset`，可解析“节前最后一个工作日”这类表达
- 中国业务日历：仓库内已提供 `CN/2022` 到 `CN/2026` 的 v2 schedule 数据；节假日/补班按 `schedule_year` 查询，工作日/休息日统计按实际日期上的 canonical day fact 求值
- grouped output：`resolve` 现在同时返回 flat `resolved_time_expressions` 和递归 `resolved_time_expression_groups`
- fail-fast 语义校验：year selector 只接受 year-valued base；pairwise `bounded_range` 要求两侧集合基数一致；业务日滚动锚点必须都是单日

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

对于 hand-written `resolve` 输入，枚举类表达现在也可以继续参与后续计算。例如：

- `select_segment(last, enumerate_makeup_workdays(...))` 可表示“春节调休补班最后一天”
- `select_segment(nth_from_end, index=2, enumerate_makeup_workdays(...))` 可表示“春节调休补班倒数第二天”
- `segments_bounds(reference(t1))` 可把一个多段结果显式压成 covering range
- `reference` 不再要求只能引用前面字段，resolver 会按依赖关系求值
- `period_to_date(month, anchor(system_date))` 可表示“本月至今”
- `rolling_minutes(value=90, anchor_expr=anchor(system_datetime))` 可表示“最近90分钟”
- `rolling_business_days(region="CN", value=5, anchor_expr=anchor(system_date), include_anchor=false)` 可表示“最近5个工作日不含今天”

业务日历相关的 hand-written DSL 现在支持 `schedule_year` 和 `schedule_year_expr`：

- `calendar_event_range(region="CN", event_key="national_day", schedule_year=2025, scope="consecutive_rest")`
- `enumerate_makeup_workdays(region="CN", event_key="spring_festival", schedule_year=2025)`
- `calendar_event_range(region="CN", event_key="spring_festival", schedule_year_expr=reference("t1"), scope="consecutive_rest")`
- `enumerate_makeup_workdays(region="CN", event_key="national_day", schedule_year_expr=reference("t1"))`

命名节假日区间现在也可以作为派生连续父区间继续参与子周期操作，例如：

- `enumerate_subperiods(unit="day", base=calendar_event_range(...))`
- `select_subperiod(unit="day", index=2, base=calendar_event_range(...))`
- `slice_subperiods(mode="last", unit="day", count=2, base=calendar_event_range(...))`
- `select_occurrence(kind="weekend", ordinal=1, base=calendar_event_range(...))`

显式日期/时刻相关的 hand-written DSL：

- `literal_date(date="2026-04-01")`
- `literal_datetime(datetime="2026-04-10 14:37:00")`
- `literal_period(unit="year", year=2024)`
- `literal_period(unit="month", year=2024, month=4)`
- `literal_period(unit="quarter", year=2024, quarter=1)`
- `literal_period(unit="half_year", year=2024, half=1)`
- `anchor(name="system_datetime")`

显式自然周期 literal 的当前约定：

- `2024年`、`2024年度`、`2024年全年` 统一归一为 `literal_period(unit="year", year=2024)`
- `2024年4月`、`2024年第一季度`、`2024年上半年` 统一优先走 `literal_period`
- `今年`、`去年`、`明年`、`本年` 仍保持相对周期表达，不会被强改成 `literal_period`
- 绝对周表达例如 `2024年第3周` 不在这一批范围内

集合与 partial 相关的 hand-written DSL：

- `enumerate_subperiods(unit="month", complete_only=true, base=rolling(unit="year", value=1, anchor_expr=anchor("system_date"), include_anchor=false))`
- `slice_subperiods(mode="first", unit="quarter", count=2, complete_only=true, base=reference("t1"))`
- `slice_segments(mode="first", count=3, base=enumerate_calendar_days(region="CN", day_kind="workday", base=current_period(unit="month")))`

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
    ],
    "resolved_time_expression_groups": [
      {
        "id": "t1",
        "text": "今年3月",
        "start_time": "2026-03-01 00:00:00",
        "end_time": "2026-03-31 23:59:59",
        "timezone": "Asia/Shanghai",
        "children": []
      },
      {
        "id": "t2",
        "text": "去年同期",
        "start_time": "2025-03-01 00:00:00",
        "end_time": "2025-03-31 23:59:59",
        "timezone": "Asia/Shanghai",
        "children": []
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

- 新的 `rolling` 结构优先使用表达式级字段：`anchor_expr` 表示单日锚点，`include_anchor` 表示是否包含该锚点当天。
- 当 `anchor_expr = {"op":"anchor","name":"system_date"}` 且 `include_anchor = false` 时，`rolling` 默认以 `system_date - 1` 为右端锚点，也就是默认**不含当天**。
- `rolling_includes_today` 仍会出现在 `POST /query/parse` 和 `POST /query/pipeline` 的返回里，但它现在只是兼容旧调用方的摘要字段：只有当请求里所有 rolling 都 `include_anchor=true` 时它才会是 `true`；混合请求会返回 `false`。
- `POST /query/resolve` 在迁移期仍接受旧结构：`anchor: "system_date"` 加顶层 `rolling_includes_today`。
- `resolved_time_expressions` 仍是 flat 兼容输出；如果查询天然带有父子结构，例如“每个月最后一个工作日”“本月至今每个工作日分别”，请优先消费 `resolved_time_expression_groups`。
- `resolved_time_expression_groups` 的 root 可以是结构性 covering span；如果你要保留离散成员语义，例如最近 N 个工作日、每个月最后一个工作日，应优先消费 leaf members 或 `metadata.rewrite_hints`。
- `enumerate_calendar_days` / `enumerate_makeup_workdays` 在 grouped 输出里保留原子日成员；flat 输出为了兼容旧调用方仍可能把连续日期合并成区间段。
- `enumerate_subperiods`、`select_subperiod`、`slice_subperiods` 在 same-grain 成员上支持 identity/no-op 语义。例如“最近10个工作日每天收益分别是多少”会直接保留这 10 个日成员，不再报 `day -> day` 不支持。
- `rewrite` 当前以 full-sentence LLM-2 为主路径；deterministic rewrite 只保留安全的时间格式化场景，不再因为结果里有多个 segment 或 grouped members 就自动补出“分别”“各自”。
- `metadata.rewrite_hints` 会把 top-level source 的最小集合拓扑暴露给 rewrite；对非连续离散日集合，rewrite 会优先列出 leaf dates，避免被错误压成一个 covering range。
- 如果原问题同时包含枚举型时间轴和独立业务轴，例如 `昨天每小时每个收费站...`、`本月至今每个工作日每个收费站...`，safe rewrite 会保留原时间骨架，并把绝对时间作为补充展开，例如 `昨天每小时（即...）每个收费站...`，不再把时间轴压成裸成员列表。
- 对 `过去3年春节假期...` 这类 non-mapped multi-year holiday wording，parser / resolve / rewrite 会保留原骨架，不再静默改成 `过去3年每年春节假期...`；只有原问题显式写出 `每年 / 各年` 时，才保留 mapped-year wording。
- 对 year-set 映射得到的 `select_month / select_quarter / select_half_year`、`calendar_event_range`、`enumerate_makeup_workdays`，resolver 现在统一按 source member 做 `overlap + clip`：无交集的边界年会被省略，有交集的边界年会保留 clipped member，不再把 touched year 的完整自然区间静默带出窗口。
- 本次 rewrite 路径不包含 replay guardrail、conservative fallback 或 anchored rewrite；如果后续引入，会作为单独变更处理。
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

当前业务日历语义还补了 3 条关键契约：

- `X节假期` 与 `X节当天 / 正日 / 当日` 不再混用。前者走 `scope=consecutive_rest`，后者走 `scope=statutory`。
- `假期内第一个工作日 / 某月最后两个休息日` 这类问法，统一按“先筛选 calendar members，再做 select/slice”处理，不再误解成节前/节后 `business_day_offset`。
- 如果业务日历筛选结果为空，例如 `去年国庆假期工作日`，`resolved_time_expressions.metadata.no_match_results` 会显式给出 no-match 语义，`rewritten_query` 可能为 `null`，不再静默回显原问题。

日期识别型问句也走窄范围安全改写：

- `清明节当天是哪天`
- `清明节假期第二天是哪天`
- `2025年春节调休补班最后两天是哪两天`

这类 query 会稳定改写成 `...对应的日期是...`，不会再出现 `2026年4月5日是哪天` 这种自指问句。

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
