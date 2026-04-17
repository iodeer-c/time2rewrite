# 时间澄清 Append-Only Annotation 设计稿

## 1. 目标与结论

本方案重写当前 rewrite 主链路。

新的产品定义不是 full-sentence rewrite，而是：

```text
在不改变原问题非时间语义、非时间文本、词序和问句结构的前提下，
对需要澄清的时间表达追加精确时间注释。
```

固定输出样式：

```text
原时间表达（准确时间）
```

最终链路：

```text
original_query
-> LLM-1 生成 Clarification Plan
-> Plan 结构校验
-> code 按结构化语义计算时间
-> code 渲染 display_exact_time
-> LLM-2 进行 append-only annotation
-> 轻量运行时检查
-> rewritten_query
```

返回语义：

- 无需澄清：返回原问题
- 澄清成功：返回加注后的问题
- 任一关键阶段失败：返回 `null`

实现策略：

- 从 `main` 新开实现分支
- `exp/solution-1` 仅作为 donor/reference
- 只借基础设施与时间计算资产，不继承旧 rewrite 主链路

---

## 2. 主链路与职责边界

### 2.1 LLM-1

`LLM-1` 的角色固定为：

```text
Clarification Planner
```

职责：

1. 识别时间节点
2. 判断哪些节点需要澄清
3. 生成结构化 `Clarification Plan`

禁止：

- 不计算最终日期
- 不输出最终问题
- 不输出解释性文字
- 不偏离固定 schema

输入：

- `original_query`
- `system_date`
- `system_datetime`
- `timezone`

### 2.2 code

职责：

- 校验 `Clarification Plan` 结构
- 按 `resolution_spec` 进行确定性时间计算
- 生成标准化 `interval_set`
- 统一渲染 `display_exact_time`

禁止：

- 不猜自然语言语义
- 不决定注释位置
- 不改写句子

### 2.3 LLM-2

`LLM-2` 的角色固定为：

```text
Append-only Annotation Renderer
```

职责：

- 只在目标时间表达附近追加 `（准确时间）`

禁止：

- 不改非时间内容
- 不改词序
- 不改比较关系
- 不改问句结构
- 不输出解释

---

## 3. Clarification Plan

顶层结构：

```text
ClarificationPlan
├── nodes[]
└── comparison_groups[]
```

### 3.1 nodes[]

每个节点统一字段：

- `node_id`
- `render_text`
- `ordinal`
- `surface_fragments?`
- `needs_clarification`
- `node_kind`
- `reason_code`
- `resolution_spec`

字段含义：

- `render_text`
  - 原句中最终要被加注释的时间表达文本
- `ordinal`
  - 该时间表达在原句中的出现顺序
- `surface_fragments`
  - 非连续或重复场景下用于辅助定位的原句片段集合
- `needs_clarification`
  - 是否进入 annotation
- `node_kind`
  - 该节点的计算模式
- `reason_code`
  - 为什么需要 / 不需要澄清
- `resolution_spec`
  - 强结构化、按 `node_kind` 判别联合的计算语义对象

节点约束：

- 按原句顺序输出
- 节点集合必须非重叠
- 父节点抑制子节点
- `needs_clarification=false` 的节点也保留在 plan 中，便于调试和关系建模

### 3.2 comparison_groups[]

`comparison_groups` 只负责表达层面的“比较/引用关系”，不负责时间计算。

最终 schema：

- `group_id`
- `relation_type`
- `anchor_text`
- `anchor_ordinal`
- `direction`
- `members[]`

每个 `member`：

- `node_id`
- `role`

枚举：

- `relation_type`
  - `year_over_year`
  - `period_over_period`
  - `same_period_reference`
  - `generic_compare`

- `direction`
  - `subject_to_reference`
  - `reference_to_subject`
  - `symmetric`

- `role`
  - `subject`
  - `reference`
  - `peer`

约束：

- `members` 保持原句出现顺序
- 语义以 `role + direction` 为准，不依赖顺序猜测
- 每组至少两个成员
- 同一 `node_id` 在同一组内只能出现一次

分工：

- **计算引用**：放在 `resolution_spec`
- **表达关系**：放在 `comparison_groups`

---

## 4. 时间语义分层

时间语义分为三层：

### 4.1 window

决定时间范围本身。

例子：

- `2025年`
- `本月至今`
- `清明假期`

### 4.2 regular_grain

普通粒度，不单独澄清。

例子：

- `每天`
- `每月`
- `每季度`

### 4.3 calendar_sensitive

依赖外部日历语义才能确定具体日期集合，必须精确澄清。

例子：

- 节假日
- 工作日
- 交易日
- 营业日

`calendar_sensitive` 枚举：

- `holiday`
- `workday`
- `trading_day`
- `business_day`
- `custom_calendar_selector`

关键规则：

- `window + regular_grain`
  - 若 `window` 已明确，通常不澄清
  - 例如：`2025年每天` -> 原样返回
- 含 `calendar_sensitive` 的节点
  - 必须精确澄清到具体日期集合
  - 不允许概述式说明替代具体日期

---

## 5. node_kind 与 resolution_spec

`resolution_spec` 使用 **判别联合**，按“计算模式”设计，而不是按中文词面设计。

### 5.1 公共子对象

#### YearRef

```json
{ "mode": "absolute", "year": 2025 }
{ "mode": "relative", "offset": -1 }
```

#### OffsetSpec

- `direction`
- `value`
- `unit`

#### CalendarSelectorSpec

- `selector_type`
- `selector_key?`

### 5.2 node_kind 枚举

- `explicit_window`
- `relative_window`
- `holiday_window`
- `offset_window`
- `reference_window`
- `window_with_regular_grain`
- `window_with_calendar_selector`
- `calendar_selector_only`

### 5.3 各 node_kind 的 resolution_spec

#### explicit_window

用于明确自然期间或绝对时间窗口。

字段：

- `window_type`
- `calendar_unit`
- `year_ref`
- `month?`
- `quarter?`
- `half?`
- `start_date?`
- `end_date?`

#### relative_window

用于依赖系统日期的相对窗口。

字段：

- `relative_type`
- `unit`
- `direction`
- `value`
- `include_today`

#### holiday_window

用于节假日窗口。

字段：

- `holiday_key`
- `year_ref`
- `calendar_mode`

#### offset_window

用于“基准窗口 + 偏移”。

字段：

- `base`
- `offset`

`base` 可为：

- 内联窗口
- 节点引用

#### reference_window

用于“去年同期 / 上月同期”这类引用对齐窗口。

字段：

- `reference_node_id`
- `alignment`
- `shift`

#### window_with_regular_grain

用于“窗口 + 普通粒度”。

字段：

- `window`
- `grain`

通常 `needs_clarification=false`，除非 `window` 本身不明确。

#### window_with_calendar_selector

用于“窗口 + 日历敏感筛选”。

字段：

- `window`
- `selector`

#### calendar_selector_only

例外保留类型。

字段：

- `selector`
- `scope_mode`

只在上下文可唯一确定时使用。

---

## 6. 澄清判定规则

总原则：

```text
只有当一个时间节点对普通读者仍不够明确时，
才标记为 needs_clarification=true
```

默认需要澄清：

- 相对时间：`昨天`、`上周`
- PTD / rolling：`本月至今`、`最近10个工作日`
- 节假日 / 业务日历时间：`清明假期`、`国庆节假期`
- 基准时间 + 偏移：`去年国庆假期后3天`
- 含 `calendar_sensitive` 的节点：`本月至今每个工作日`
- reference window：`去年同期`、`上月同期`

默认不澄清：

- 已明确自然期间：`2025年3月`
- 共享前缀下已自然明确的自然期间：`2025年3月和4月`
- `window + regular_grain` 且 `window` 已明确：`2025年每天`

---

## 7. interval_set 与 display_exact_time

### 7.1 interval_set

code 只对 `needs_clarification=true` 的节点计算时间。

内部标准表示固定为：

```text
interval_set = 有序、非重叠、闭区间列表
```

规则：

- 按时间升序
- 不重叠
- 相邻可合并日期先合并
- 单日允许 `start = end`

### 7.2 display_exact_time 渲染规则

由 code 统一渲染：

- 单日：
  - `YYYY年M月D日`
- 单个连续区间：
  - `YYYY年M月D日至YYYY年M月D日`
- 多段：
  - 每段完整书写
  - 使用 `、` 连接

例：

```text
2026年4月1日至2026年4月3日、2026年4月7日至2026年4月10日、2026年4月13日至2026年4月15日
```

### 7.3 calendar_sensitive 特殊规则

对 `calendar_sensitive` 节点：

- 必须保留完整、无丢失的压缩区间列表
- 不允许概述式替代具体日期集合
- 即使结果很长，也必须完整输出

---

## 8. Prompt 合同

### 8.1 LLM-1 Prompt

角色：

```text
Clarification Planner
```

输入：

- `original_query`
- `system_date`
- `system_datetime`
- `timezone`

任务：

1. 识别时间节点
2. 判断哪些节点需要澄清
3. 生成结构化 `Clarification Plan`

禁止：

- 不计算最终日期
- 不输出最终问题
- 不输出解释性文字
- 不偏离 schema

few-shot 至少覆盖：

- 明确自然期间
- 相对时间
- 节假日
- `window + regular_grain`
- `window + calendar_sensitive`
- `reference_window`
- 比较关系

### 8.2 LLM-2 Prompt

角色：

```text
Append-only Annotation Renderer
```

输入：

- `original_query`
- `clarification_items[]`
  - `render_text`
  - `display_exact_time`
  - `ordinal`
  - `surface_fragments?`
- `comparison_groups[]`

任务：

```text
只在目标时间表达附近追加：
原时间表达（准确时间）
```

禁止：

- 不改非时间文本
- 不改词序
- 不改问句结构
- 不改比较结构
- 不输出解释

定位：

- 默认靠 `ordinal`
- 重复或不连续场景靠 `ordinal + surface_fragments`

few-shot 至少覆盖：

- 连续节点
- 并列节点
- 不连续节点
- calendar-sensitive 节点
- comparison group 节点

---

## 9. 运行时失败流与 Guard

失败分成三类：

1. 计划失败
2. 计算失败
3. 表达失败

### 9.1 Guard A：Plan 结构合法

检查：

- `nodes` 存在
- `node_kind` 与 `resolution_spec` 匹配
- `comparison_groups` 引用的 `node_id` 存在
- 枚举值合法
- `needs_clarification=true` 节点集合非重叠

处理：

- 首次失败：重试一次 `LLM-1`
- 再失败：返回 `null`

### 9.2 Guard B：计算结果合法

检查：

- 每个需澄清节点都成功生成 `interval_set`
- `interval_set` 合法
- `display_exact_time` 渲染成功

失败处理：

- 直接返回 `null`

特殊规则：

- 日历数据缺失导致的 calendar-sensitive 计算失败，直接返回 `null`
- 不做部分成功，不补一半节点

### 9.3 Guard C：LLM-2 输出合法

检查：

- 输出非空
- 输出是最终问题，不是解释性段落
- 当存在 `clarification_items` 时，输出中出现追加注释痕迹

失败处理：

- 直接返回 `null`

### 9.4 明确不做

V1 不做：

- 重语义 validator
- repair retry（除了 `LLM-1` 非法计划重试一次）
- 主体词保护检查
- aggregate / breakdown 再判定
- replay / abstain 体系

---

## 10. 最终返回合同

| 情况 | 返回 |
|---|---|
| `LLM-1` 判定无澄清节点 | 原问题 |
| `LLM-1` 计划非法，重试后仍非法 | `null` |
| code 计算失败 | `null` |
| `LLM-2` 输出失败 | `null` |
| 全部成功 | 加注后的问题 |

规则：

- 只要存在 `needs_clarification=true` 的节点且任一节点未成功完成 annotation，整个 rewrite 失败
- 不做部分成功输出

---

## 11. 分支实现策略

实现分支策略固定为：

```text
从 main 新建实现分支
exp/solution-1 仅作为 donor/reference
```

使用原则：

- 只借基础设施和时间计算资产
- 不继承旧 rewrite 主链路

建议借用：

- LLM 基础设施
- business calendar
- parser / resolver 中纯计算能力

明确不直接继承：

- 旧 `rewriter.py` 主路由
- constrained rewrite / semantic-anchor 路线
- validator / repair / abstain 心智
- 旧 full rewrite 相关 prompt / schema

新分支中，rewrite 应被视为全新子系统：

```text
Clarification Plan
-> deterministic interval_set computation
-> append-only annotation renderer
```

---

## 12. 典型例子

### 例 1
`昨天杭千公司的收益是多少？`

输出：

`昨天（2026年4月14日）杭千公司的收益是多少？`

### 例 2
`2025年3月杭千公司的收益是多少？`

输出：

原样返回

### 例 3
`2025年3月和4月收益分别是多少？`

输出：

原样返回

### 例 4
`本月至今每个工作日的收益是多少？`

输出必须包含完整压缩区间列表，例如：

`本月至今每个工作日（2026年4月1日至2026年4月3日、2026年4月7日至2026年4月10日、2026年4月13日至2026年4月15日）的收益是多少？`

### 例 5
`今年3月和去年同期相比收益增长了多少？`

- `去年同期` 为 `reference_window`
- `comparison_groups` 表达比较关系
- 输出只做 append-only annotation，不改比较结构

---

## 13. 最终结论

最终方案固定为：

```text
LLM-1 负责“判定和建模”
code 负责“确定性时间计算”
LLM-2 负责“append-only annotation”
```

系统不再以 full rewrite 为目标，而是以 **结构化时间澄清 + 就地追加注释** 为目标。

本设计应替代旧的 full-sentence rewrite / validator / repair / abstain 主链路定义；后续实现、测试与分支策略均以本设计为准。
