# Append-Only Clarified Query 人审指引

本文档用于人工抽查 append-only clarified query 的输出质量。当前方案不再评审“原位局部改写是否自然”，而是评审：

- 时间说明是否完整
- 原问题业务语义是否被保留
- 句末追加说明是否清楚且不引入新时间语义

## 1. 人审重点

对每条样例，只看下面 4 件事：

1. `clarified_query` 是否保留了原问题的业务主体、指标词、对比词和结果形状。
2. 追加的时间说明是否覆盖了所有必要的时间单元。
3. 时间说明是否严格来源于结构化 facts，而不是凭空增加新的日期或时间关系。
4. unresolved / degraded 场景是否明确说出了“当前无法确定”，而不是静默丢掉。

## 2. 不再关注的旧标准

append-only 方案下，不再把这些当主评审标准：

- 原句内部插入位置是否优雅
- `surface_fragments` 是否适合作为局部编辑锚点
- scaffold token 是否通过局部编辑保留

这些属于旧 in-place rewrite 关注点，不再是当前 change 的主目标。

## 3. 推荐抽查样例

建议优先使用 `tests/fixtures/golden_datasets.py` 中的 `APPEND_ONLY_MANUAL_REVIEW_CASES`：

1. `2025年3月收益`
   - 看单一 resolved 时间是否直接补清楚。
2. `最近一个月每周的收益是多少`
   - 看 grouped query 是否保留“每周”结果形状，并明确自然周分组依据。
3. `今年3月和去年同期的收益分别是多少`
   - 看 comparison 顺序是否和原句一致，两个时间说明是否都补出来。
4. `最近5个休息日收益是多少`
   - 看 unsupported day-class count rolling 是否被显式标成当前无法确定。
5. `2025年中秋假期和国庆假期一起的收益是多少`
   - 看 overlapping holiday-event members 是否仍然作为独立 clarification slots 被保留下来。

## 4. 判定规则

### Accept

- 原问题主体不变
- 时间说明完整
- 没引入 facts 外的新时间
- 句末说明清楚可读

### Reject

- 漏掉必要时间单元
- 把 aggregate 改成 breakdown，或把 breakdown 改成 aggregate
- 凭空加了新的日期范围、节假日范围、比较关系
- 把原问题业务词改掉或删掉

### Accept With Follow-Up

- 时间说明正确，但语言略重复或不够顺
- 这类属于 phrasing follow-up，不算阻塞正确性

## 5. 记录建议

每次人审至少记录：

- review date
- system date
- raw output path
- reviewed queries
- accepted / rejected / follow-up counts

如果出现拒绝项，优先标注为：

- `missing_clarification`
- `invented_time_semantics`
- `result_shape_drift`
- `business_wording_drift`
