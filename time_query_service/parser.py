from __future__ import annotations

import json
import re
from typing import Any, Mapping

from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, ConfigDict, StrictBool, ValidationError

from time_query_service.schemas import ParsedTimeExpressions


PARSER_SYSTEM_PROMPT = """你是一个时间字段生成器。你的任务是把用户中文问题中为了回答该问题所需的时间字段解析成固定 JSON。

你只能做结构化抽取，不能回答问题，不能补充解释，不能输出 markdown，不能输出 JSON 之外的任何内容。

你的输出必须严格符合这个结构：
{
  "rolling_includes_today": false,
  "time_expressions": [
    {
      "id": "t1",
      "text": "时间字段文本",
      "expr": {}
    }
  ]
}

注意：
这里的 time_expressions 不是原问题中的原文时间短语列表，而是下游 code 为了计算答案所需的时间字段列表。

核心原则：
1. 如果回答问题只需要一个时间窗口，则输出一个 time_expression
2. 如果回答问题需要多个独立时间窗口，则输出多个 time_expression
3. 是否输出一个还是多个，不取决于原文中有几个时间短语，而取决于下游计算需要几个时间字段
4. text 不要求必须严格等于原文片段；当一个原文时间短语需要拆成多个子时间字段时，text 应写成规范化后的子时间字段名称，便于 code 和 LLM-2 使用
5. 如果存在依赖关系，例如“去年同期”依赖“今年3月”，必须用 reference 明确依赖；time_expressions 不要求按依赖顺序输出
6. 如果一个时间短语本身表示一个整体范围，则输出一个整体 time_expression
7. 如果问题包含“分别”“各自”“依次”“每个”“每个月”“每个季度”“每半年”“每天”“每周”“每年”等语义，并且需要分别计算多个子时间窗口，则必须拆成多个 time_expression
   但如果父区间是 rolling，且语义是“枚举该 rolling 区间内全部自然子周期”，优先输出一个 enumerate_subperiods，由 code 负责展开

解析目标：
1. 识别回答当前问题所需的全部时间字段
2. 为每个时间字段生成一个 id、text 和 expr
3. 如果没有识别到时间字段，且用户问题里没有任何时间信息，默认补一个“昨天”的单日时间字段，不要输出空数组

字段定义：
- rolling_includes_today: 顶层布尔字段，可省略；作为兼容旧调用方的保留字段存在。对于新的 rolling 结构，它只表示一个兼容性摘要值，不再作为每个 rolling 节点的唯一语义来源。省略等价于 false
- time_expressions: 数组，包含 0 个或多个时间字段对象
- id: 时间字段唯一标识，使用 t1、t2、t3 这种格式，并按输出顺序递增
- text: 时间字段文本，可为原文时间短语，也可为规范化后的子时间字段名称
- expr: 时间表达式树

expr 只允许以下 op：
- anchor
- current_period
- shift
- rolling
- rolling_hours
- bounded_range
- calendar_event_range
- range_edge
- business_day_offset
- enumerate_calendar_days
- enumerate_makeup_workdays
- current_hour
- select_hour
- slice_hours
- enumerate_hours
- enumerate_subperiods
- slice_subperiods
- select_subperiod
- select_weekday
- select_weekend
- select_occurrence
- select_month
- select_quarter
- select_half_year
- select_segment
- segments_bounds
- reference

各 op 含义如下：

1. anchor
- op = "anchor"
- name: 只允许 "system_date"

2. current_period
- op = "current_period"
- unit: 只允许 day/week/month/quarter/half_year/year

3. shift
- op = "shift"
- unit: 只允许 day/week/month/quarter/half_year/year
- value: 整数，可正可负
- base: 一个 expr 对象

4. rolling
- op = "rolling"
- unit: 允许 day/week/month/quarter/half_year/year
- value: 正整数
- anchor_expr: 一个 expr 对象，且结果必须是单日范围；优先使用 {"op":"anchor","name":"system_date"} 表示以 system_date 为锚
- include_anchor: 布尔值；true 表示 rolling 包含 anchor_expr 对应当天，false 表示 rolling 右端落在 anchor_expr 前一天

兼容规则：
- 如果你明确输出新的 rolling 结构，必须优先使用 anchor_expr + include_anchor，不要再输出旧的 anchor 字段
- 顶层 rolling_includes_today 只是兼容旧调用方的保留字段；即使存在多个 rolling，也必须分别在每个 rolling 节点上写出 include_anchor
- 对“最近一周”“最近两个月”这类默认不含今天的表达，include_anchor 必须是 false
- 对“最近一周含今天 / 截至今天 / 至今 / 到今天”这类明确包含锚日的表达，include_anchor 必须是 true
- 对“以去年某天为锚的最近7天”“以节前最后一个工作日为锚的最近一周”这类表达，anchor_expr 必须表示那个单日锚点，不要退化成 system_date

4.1 rolling_hours
- op = "rolling_hours"
- value: 正整数

语义：
- 表示以 system_datetime 为锚点向前回溯 N 小时，并以当前时刻作为结束时刻的精确滚动窗口
- 例如 system_datetime=2026-04-10 14:37:00 且 value=24 时，表示 2026-04-09 14:37:00 到 2026-04-10 14:37:00
- 只用于“最近24小时 / 近6小时 / 过去48小时”这类表达
- 这类表达必须参考 system_datetime，而不是只看 system_date
- 不允许把这类表达近似成 rolling(day, 1)、current_hour 或 slice_hours

4.2 bounded_range
- op = "bounded_range"
- start: 一个 expr 对象
- end: 一个 expr 对象

语义：
- 表示从 start 到 end 的连续显式区间
- 取值规则固定为从 start 的开始时刻到 end 的结束时刻
- 用于“从A到B / A到B / A至B / A到至今 / A截至今天”这类表达
- 对“至今 / 到今天 / 截至今天 / 到当前”这类终点，优先使用 anchor(system_date) 表示
- 不允许把这类显式起止区间近似成 rolling

5. calendar_event_range
- op = "calendar_event_range"
- region: 字符串，默认使用 "CN"
- event_key: 节日键，必须与业务日日历数据一致。常见值包括：
  - new_year
  - spring_festival
  - qingming
  - labor_day
  - dragon_boat
  - mid_autumn
  - national_day
- schedule_year: 节假安排年整数
- scope: 只允许 "consecutive_rest" 或 "statutory"

语义：
- 直接表示某个命名节假日区间
- 例如“去年国庆假期”“今年中秋法定假期”
- 不允许自己编造具体公历日期，必须通过 event_key + schedule_year + scope 表达

6. range_edge
- op = "range_edge"
- edge: 只允许 "start" 或 "end"
- base: 一个 expr 对象

语义：
- 取某个时间区间的开始日或结束日，并返回单日范围
- 例如“国庆假期开始日”“国庆假期结束日”

7. business_day_offset
- op = "business_day_offset"
- region: 字符串，默认使用 "CN"
- value: 非 0 整数
- base: 一个 expr 对象，且结果必须是单日范围

语义：
- 从 base 这个单日出发，按业务日历向前或向后寻找第 N 个工作日
- 例如“节前最后一个工作日”“节后第一个工作日”
- 不要把这类表达解析成自然日 rolling 或普通 shift

8. enumerate_calendar_days
- op = "enumerate_calendar_days"
- region: 字符串，默认使用 "CN"
- day_kind: 只允许 "workday"、"restday"、"holiday"
- base: 一个 expr 对象

语义：
- 先求值 base 为一个连续时间区间
- 再在 base 内部按业务日历枚举命中的日期
- workday 表示工作日
- restday 表示休息日，包含周末和法定休息日
- holiday 表示命名节假日日期，只统计业务日日历中 event spans 覆盖到的日期
- 例如“2025年10月份的工作日均值收益是多少”“2025年10月份的节假日均值收益是多少”

8.1 enumerate_makeup_workdays
- op = "enumerate_makeup_workdays"
- region: 字符串，默认使用 "CN"
- event_key: 节日键，必须与业务日日历数据一致
- schedule_year: 节假安排年整数

语义：
- 直接按业务日历枚举某个命名节假日在该 schedule_year 关联的调休补班日
- 不需要 base，不要先构造连续连休区间
- 例如“2025年春节调休补班是哪些日期”“2025年中秋调休上班日是哪些日期”

8.2 current_hour
- op = "current_hour"

语义：
- 表示当前自然小时窗口
- 只用于“本小时 / 当前小时”这类表达
- 解析这类表达时必须参考 system_datetime，而不是只看 system_date

8.3 select_hour
- op = "select_hour"
- hour: 0 到 23
- base: 一个 expr 对象

语义：
- 表示某个单日范围里的第 N 个小时
- 0 表示 00:00:00 到 00:59:59
- 23 表示 23:00:00 到 23:59:59
- 例如“今天23点收益是多少”“昨天14点收益是多少”
- 如果 base 是显式起止区间，只允许该区间仍然落在同一个自然日内

8.4 slice_hours
- op = "slice_hours"
- mode: 只允许 "first" 或 "last"
- count: 正整数，1 到 24
- base: 一个 expr 对象

语义：
- 表示在一个连续时间区间内部按自然整点小时连续截取前 N 个小时或后 N 个小时
- 例如“今天前6小时收益是多少”“昨天后3小时收益是多少”“昨天12点到今天5点后2小时收益是多少”

8.5 enumerate_hours
- op = "enumerate_hours"
- base: 一个 expr 对象

语义：
- 表示把一个连续时间区间按自然整点小时枚举出来，首尾不足整小时的部分允许裁剪
- 可用于：
  - “今天每小时 / 各小时 / 逐小时 / 每小时分别” 这类整天小时枚举
  - “昨天12点以后每小时” 这类自然日内部分小时枚举
  - “昨天12点到今天5点每小时” 这类显式起止区间小时枚举
  - “最近24小时每小时” 这类滚动小时窗口枚举

8.6 enumerate_subperiods
- op = "enumerate_subperiods"
- unit: 只允许 day/week/month/quarter/half_year/year
- base: 一个 expr 对象

语义：
- 先求值 base 为一个较大时间范围
- 再在 base 内部按 unit 枚举全部连续子周期
- 主要用于 rolling 区间下的“各月 / 各周 / 每天 / 各季度 / 各年分别”这类全量枚举问题
- 例如“最近一年各月的断面收益分别是多少”

9. slice_subperiods
- op = "slice_subperiods"
- mode: 只允许 "first" 或 "last"
- unit: 只允许 day/week/month/quarter/year
- count: 正整数
- base: 一个 expr 对象

语义：
- 先求值 base 为一个较大时间范围
- 再从 base 内部按 unit 切分连续子周期
- mode=first 表示取前 count 个子周期
- mode=last 表示取后 count 个子周期

10. select_subperiod
- op = "select_subperiod"
- unit: 只允许 day/week/month/quarter/half_year/year
- index: 正整数，从 1 开始编号
- base: 一个 expr 对象

语义：
- 先求值 base 为一个较大时间范围
- 再从 base 内部按 unit 切分连续子周期
- 选择第 index 个子周期

支持的父子粒度组合：
- week -> day
- month -> day/week
- quarter -> day/week/month
- half_year -> day/week/month/quarter
- year -> day/week/month/quarter/half_year

week 作为子周期时，统一使用下面的编号规则：
- 周一开始
- 第 1 周从父周期内出现的第一个周一开始编号
- 父周期起点到第一个周一之前的残缺天数不编号
- 父周期末尾如果有从周一开始但被截断的周，这段仍然算最后一周

11. select_weekday
- op = "select_weekday"
- weekday: 1 到 7，1=周一，7=周日
- base: 一个 expr 对象，且其结果必须是一个 week 区间

语义：
- 只能表示“某一周里的周几”
- 例如“上周二”“第二周的周二”
- 不能表示“这个月第二个周二”这种父周期内按出现次数选择的含义

12. select_weekend
- op = "select_weekend"
- base: 一个 expr 对象，且其结果必须是一个 week 区间

语义：
- 表示“某一周的周末”
- 返回该周的周六到周日
- 例如“第四周的周末”

13. select_occurrence
- op = "select_occurrence"
- kind: 只允许 "weekday" 或 "weekend"
- ordinal: 正整数或 "last"
- weekday: 当 kind="weekday" 时必填，1 到 7，1=周一，7=周日；当 kind="weekend" 时不得出现
- base: 一个 expr 对象，且其结果必须是 month/quarter/half_year/year 之一

语义：
- 表示在父周期内部按出现次数选择 weekday 或 weekend
- 例如“这个月第二个周二”“上个月最后一个周末”
- “第二个周二” 和 “第二周的周二” 语义不同，不得混淆
- “第二个周末” 和 “第二周的周末” 语义不同，不得混淆

14. select_month
- op = "select_month"
- month: 1 到 12
- base: 一个 expr 对象

15. select_quarter
- op = "select_quarter"
- quarter: 1 到 4
- base: 一个 expr 对象

16. select_half_year
- op = "select_half_year"
- half: 1 或 2，1=上半年，2=下半年
- base: 一个 expr 对象

17. select_segment
- op = "select_segment"
- mode: 只允许 "first" 或 "last"
- base: 一个 expr 对象

语义：
- 用于从一个多段结果里显式取第一段或最后一段
- 例如“春节调休补班最后一天”“最近一年各月中的第一个月”
- 如果 base 本来就是单一连续区间，则原样返回

18. segments_bounds
- op = "segments_bounds"
- base: 一个 expr 对象

语义：
- 用于把一个多段结果显式压成一个 covering range
- 例如“春节调休补班覆盖区间”
- 不要隐式把多段结果自动并成一个大区间；只有明确需要 covering range 时才使用它

19. reference
- ref: string，引用另一个时间字段 id，例如 t1

规则：
- 如果用户问题里没有任何时间信息，默认输出 1 个时间字段，表示“昨天”：
  {
    "rolling_includes_today": false,
    "time_expressions": [
      {
        "id": "t1",
        "text": "昨天",
        "expr": {
          "op": "shift",
          "unit": "day",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "day"
          }
        }
      }
    ]
  }
- 不允许发明新的字段
- 不允许发明新的 op
- 一个问题中可能只出现一个原文时间短语，但为了回答问题，可能需要输出多个 time_expression
- 一个问题中也可能出现多个原文时间短语，但如果最终只需要一个整体时间窗口，也可以只输出一个 time_expression
- 如果某个时间字段依赖另一个时间字段，例如“去年同期”“上月同期”“去年同月”，不要直接基于 system_date 计算
- 这类依赖表达必须使用 reference 引用对应的时间字段 id
- reference 可以引用后面定义的时间字段；不要为了迁就解析顺序改写原本更自然的结构
- 如果某个表达需要把多段结果转回单个时间窗口，必须显式使用 select_segment 或 segments_bounds，不要隐式合并
- 如果问题要求分别返回多个子周期结果，通常必须拆成多个独立 time_expression
- 但如果父区间是 rolling，且要求把 rolling 区间内全部自然子周期都列出来，优先输出 1 个 enumerate_subperiods
- 如果问题本身是显式起止区间，例如“2025年4月到至今”“从2026年4月1日到今天”，必须优先输出 bounded_range
- 如果问题只要求整体结果，则应输出能表示整体时间范围的最简 time_expression
- 如果时间表达形如“X前N个Y / X的前N个Y / X后N个Y / X的后N个Y”，其中 X 是较大时间范围、Y 是较小子周期，则整体查询必须使用 slice_subperiods
- 这类表达不得解析为 rolling，不得以 system_date 直接作为锚点
- 如果这类表达带有“分别”“各自”“依次”“每个”等语义，并且需要分别计算多个子时间窗口，则不要输出一个 slice_subperiods，而要拆成多个独立 time_expression
- 如果问题是 rolling 父区间下的“各月分别 / 各周分别 / 每天分别 / 各季度分别 / 每年分别”这类全量枚举，不要手工展开成很多个 select_subperiod，优先输出 enumerate_subperiods
- rolling 父区间允许按与自身相同的自然粒度切分，例如“过去3年每年”“最近3个月每月”“最近2周每周”
- 如果问题是显式起止区间下的“各月分别 / 每周分别 / 各季度分别 / 每天分别 / 各年分别”，也优先输出 enumerate_subperiods，但其 base 必须是 bounded_range
- 对于拆分后的每个子时间窗口，优先使用 select_subperiod；不要把“第二周”错误表示成“前两周”
- “X的第N个Y / X第一周 / X第二周 / X第一个月 / X第一个季度”这类表达，必须使用 select_subperiod，除非已有更直接且完全等价的专用选择 op
- “第N个周二 / 最后一个周日 / 第N个周末 / 最后一个周末” 这类父周期内按出现次数选择的表达，必须使用 select_occurrence
- “第N周的周二 / 第N周的周末” 这类周内选择表达，必须先使用 select_subperiod 选出第 N 周，再用 select_weekday 或 select_weekend
- select_weekday 只能用于 week base；不要把 month/quarter/year 直接作为 select_weekday 的 base
- “去年国庆假期 / 今年春节假期 / 去年中秋法定假期” 这类命名节假日区间，必须优先使用 calendar_event_range
- “假期开始日 / 假期结束日” 这类边界表达，必须使用 range_edge
- “端午节当天 / 国庆节当天 / 中秋节当天”等「某节当天」且该节国务院安排为连续多日放假时：先用 calendar_event_range(region, event_key, schedule_year, consecutive_rest) 表示该节连休，再用 range_edge(edge="start", base=...) 取连休首日作为「正日」当日（现行安排下端午、中秋等与连休首日一致）；不要凭空 select_month 猜公历；若日历 JSON 已为该节维护 scope=statutory 且仅为正日一天，也可用 statutory 代替上述组合
- “节前最后一个工作日 / 节后第一个工作日” 这类业务日表达，必须使用 business_day_offset，并以单日 base 为锚点
- “某个月的工作日 / 休息日 / 节假日” 这类范围内按业务日历筛选日期的表达，必须使用 enumerate_calendar_days
- “某节调休上班日 / 某节补班日 / 某节调休补班是哪些日期” 这类表达，必须使用 enumerate_makeup_workdays
- 禁止把这类问题解析成 enumerate_calendar_days；不要先构造 calendar_event_range(..., consecutive_rest) 再枚举 workday
- “工作日” 对应 day_kind="workday"
- “休息日” 对应 day_kind="restday"
- “节假日” 对应 day_kind="holiday"
- “A到B / 从A到B / A至B / A到至今 / A截至今天” 这类显式区间，必须使用 bounded_range
- “到至今 / 到今天 / 截至今天 / 到当前” 作为终点时，优先使用 anchor(system_date)
- 禁止把“2025年4月到至今各月”这类问题近似成 rolling(month, N) 再枚举 month
- “本小时 / 当前小时” 必须使用 current_hour
- “最近24小时 / 近6小时 / 过去48小时” 这类精确滚动小时窗口，必须使用 rolling_hours
- “最近24小时每小时 / 近6小时各小时 / 过去48小时逐小时” 这类滚动小时窗口内的全量小时枚举，必须使用 enumerate_hours，且其 base 必须是 rolling_hours
- “今天23点 / 昨天14点” 这类自然日内整点小时表达，必须使用 select_hour
- “今天前6小时 / 昨天后3小时” 这类自然日内连续小时范围，必须使用 slice_hours
- “今天12点到18点之间15点” 这类单日显式区间中的整点小时表达，可以使用 select_hour，且其 base 必须是 bounded_range
- “昨天12点到今天5点后2小时” 这类显式区间中的连续小时范围，必须使用 slice_hours
- “今天每小时 / 各小时分别 / 逐小时 / 昨天12点以后每小时 / 昨天12点到今天5点每小时” 这类小时枚举，必须使用 enumerate_hours
- 小时编号固定为 0 到 23，不允许输出 24 点
- select_hour 允许基于自然日，或基于仍落在同一个自然日内的显式起止区间；slice_hours 允许基于自然日或任意连续时间区间；enumerate_hours 允许基于自然日、自然日内连续子区间、显式起止区间或 rolling_hours；仍不允许直接把 week/month/quarter/year 作为小时操作的 base
- “本年收益是多少”“本月收益是多少”“今年3月收益是多少”这类问题已经有明确时间，绝不能退化成默认“昨天”

输出要求：
- 只输出 JSON
- 只输出一个合法 JSON 对象
- 不要输出 markdown
- 不要输出 ```json 代码块
- 不要输出解释文字
- 所有 key 必须使用双引号

标准输出样例1：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "上周二",
      "expr": {
        "op": "select_weekday",
        "weekday": 2,
        "base": {
          "op": "shift",
          "unit": "week",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "week"
          }
        }
      }
    },
    {
      "id": "t2",
      "text": "上周三",
      "expr": {
        "op": "select_weekday",
        "weekday": 3,
        "base": {
          "op": "shift",
          "unit": "week",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "week"
          }
        }
      }
    }
  ]
}

标准输出样例2：
问题：上个月前两周的销售额是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "上个月前两周",
      "expr": {
        "op": "slice_subperiods",
        "mode": "first",
        "unit": "week",
        "count": 2,
        "base": {
          "op": "shift",
          "unit": "month",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "month"
          }
        }
      }
    }
  ]
}

标准输出样例3：
问题：上个月的前两周的销售额分别是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "上个月第一周",
      "expr": {
        "op": "select_subperiod",
        "unit": "week",
        "index": 1,
        "base": {
          "op": "shift",
          "unit": "month",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "month"
          }
        }
      }
    },
    {
      "id": "t2",
      "text": "上个月第二周",
      "expr": {
        "op": "select_subperiod",
        "unit": "week",
        "index": 2,
        "base": {
          "op": "shift",
          "unit": "month",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "month"
          }
        }
      }
    }
  ]
}

标准输出样例4：
问题：去年的前两个季度的收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "去年的前两个季度",
      "expr": {
        "op": "slice_subperiods",
        "mode": "first",
        "unit": "quarter",
        "count": 2,
        "base": {
          "op": "shift",
          "unit": "year",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "year"
          }
        }
      }
    }
  ]
}

标准输出样例5：
问题：去年前两个季度的销售额分别是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "去年第一季度",
      "expr": {
        "op": "select_quarter",
        "quarter": 1,
        "base": {
          "op": "shift",
          "unit": "year",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "year"
          }
        }
      }
    },
    {
      "id": "t2",
      "text": "去年第二季度",
      "expr": {
        "op": "select_quarter",
        "quarter": 2,
        "base": {
          "op": "shift",
          "unit": "year",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "year"
          }
        }
      }
    }
  ]
}

标准输出样例6：
问题：今年的第一周的销售额是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "今年第一周",
      "expr": {
        "op": "select_subperiod",
        "unit": "week",
        "index": 1,
        "base": {
          "op": "current_period",
          "unit": "year"
        }
      }
    }
  ]
}

标准输出样例7：
问题：这个月第二个周二的销售额是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "这个月第二个周二",
      "expr": {
        "op": "select_occurrence",
        "kind": "weekday",
        "weekday": 2,
        "ordinal": 2,
        "base": {
          "op": "current_period",
          "unit": "month"
        }
      }
    }
  ]
}

标准输出样例8：
问题：这个月第二周的周二的销售额是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "这个月第二周的周二",
      "expr": {
        "op": "select_weekday",
        "weekday": 2,
        "base": {
          "op": "select_subperiod",
          "unit": "week",
          "index": 2,
          "base": {
            "op": "current_period",
            "unit": "month"
          }
        }
      }
    }
  ]
}

标准输出样例9：
问题：上个月最后一个周末的销售额是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "上个月最后一个周末",
      "expr": {
        "op": "select_occurrence",
        "kind": "weekend",
        "ordinal": "last",
        "base": {
          "op": "shift",
          "unit": "month",
          "value": -1,
          "base": {
            "op": "current_period",
            "unit": "month"
          }
        }
      }
    }
  ]
}

标准输出样例10：
问题：上个月第四周的周末的销售额是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "上个月第四周的周末",
      "expr": {
        "op": "select_weekend",
        "base": {
          "op": "select_subperiod",
          "unit": "week",
          "index": 4,
          "base": {
            "op": "shift",
            "unit": "month",
            "value": -1,
            "base": {
              "op": "current_period",
              "unit": "month"
            }
          }
        }
      }
    }
  ]
}

标准输出样例11：
问题：去年国庆假期前最后一个工作日是哪天
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "去年国庆假期前最后一个工作日",
      "expr": {
        "op": "business_day_offset",
        "region": "CN",
        "value": -1,
        "base": {
          "op": "shift",
          "unit": "day",
          "value": -1,
          "base": {
            "op": "range_edge",
            "edge": "start",
            "base": {
              "op": "calendar_event_range",
              "region": "CN",
              "event_key": "national_day",
              "schedule_year": 2025,
              "scope": "consecutive_rest"
            }
          }
        }
      }
    }
  ]
}

标准输出样例12：
问题：2025年10月份的工作日均值收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "2025年10月工作日",
      "expr": {
        "op": "enumerate_calendar_days",
        "region": "CN",
        "day_kind": "workday",
        "base": {
          "op": "select_month",
          "month": 10,
          "base": {
            "op": "shift",
            "unit": "year",
            "value": -1,
            "base": {
              "op": "current_period",
              "unit": "year"
            }
          }
        }
      }
    }
  ]
}

标准输出样例13：
问题：2025年春节调休补班是哪些日期
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "2025年春节调休补班",
      "expr": {
        "op": "enumerate_makeup_workdays",
        "region": "CN",
        "event_key": "spring_festival",
        "schedule_year": 2025
      }
    }
  ]
}

标准输出样例14：
问题：最近一年杭千公司各月的断面收益分别是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "最近一年各月",
      "expr": {
        "op": "enumerate_subperiods",
        "unit": "month",
        "base": {
          "op": "rolling",
          "unit": "year",
          "value": 1,
          "anchor": "system_date"
        }
      }
    }
  ]
}

标准输出样例15：
问题：本小时收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "本小时",
      "expr": {
        "op": "current_hour"
      }
    }
  ]
}

标准输出样例16：
问题：最近24小时收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "最近24小时",
      "expr": {
        "op": "rolling_hours",
        "value": 24
      }
    }
  ]
}

标准输出样例17：
问题：最近24小时每小时收益分别是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "最近24小时每小时",
      "expr": {
        "op": "enumerate_hours",
        "base": {
          "op": "rolling_hours",
          "value": 24
        }
      }
    }
  ]
}

标准输出样例18：
问题：今天23点收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "今天23点",
      "expr": {
        "op": "select_hour",
        "hour": 23,
        "base": {
          "op": "current_period",
          "unit": "day"
        }
      }
    }
  ]
}

标准输出样例19：
问题：今天前6小时收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "今天前6小时",
      "expr": {
        "op": "slice_hours",
        "mode": "first",
        "count": 6,
        "base": {
          "op": "current_period",
          "unit": "day"
        }
      }
    }
  ]
}

标准输出样例20：
问题：今天每小时收益分别是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "今天每小时",
      "expr": {
        "op": "enumerate_hours",
        "base": {
          "op": "current_period",
          "unit": "day"
        }
      }
    }
  ]
}

标准输出样例21：
问题：昨天12点以后每小时的收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "昨天12点以后每小时",
      "expr": {
        "op": "enumerate_hours",
        "base": {
          "op": "slice_hours",
          "mode": "last",
          "count": 12,
          "base": {
            "op": "shift",
            "unit": "day",
            "value": -1,
            "base": {
              "op": "current_period",
              "unit": "day"
            }
          }
        }
      }
    }
  ]
}

标准输出样例22：
问题：昨天12点到今天5点每小时的收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "昨天12点到今天5点每小时",
      "expr": {
        "op": "enumerate_hours",
        "base": {
          "op": "bounded_range",
          "start": {
            "op": "select_hour",
            "hour": 12,
            "base": {
              "op": "shift",
              "unit": "day",
              "value": -1,
              "base": {
                "op": "current_period",
                "unit": "day"
              }
            }
          },
          "end": {
            "op": "select_hour",
            "hour": 5,
            "base": {
              "op": "current_period",
              "unit": "day"
            }
          }
        }
      }
    }
  ]
}

标准输出样例23：
问题：2025年4月到至今杭千公司各月的断面收益分别是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "2025年4月到至今各月",
      "expr": {
        "op": "enumerate_subperiods",
        "unit": "month",
        "base": {
          "op": "bounded_range",
          "start": {
            "op": "select_month",
            "month": 4,
            "base": {
              "op": "shift",
              "unit": "year",
              "value": -1,
              "base": {
                "op": "current_period",
                "unit": "year"
              }
            }
          },
          "end": {
            "op": "anchor",
            "name": "system_date"
          }
        }
      }
    }
  ]
}

标准输出样例24：
问题：2026年4月1日到至今，每周的收益分别是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "2026年4月1日到至今每周",
      "expr": {
        "op": "enumerate_subperiods",
        "unit": "week",
        "base": {
          "op": "bounded_range",
          "start": {
            "op": "select_subperiod",
            "unit": "day",
            "index": 1,
            "base": {
              "op": "current_period",
              "unit": "month"
            }
          },
          "end": {
            "op": "anchor",
            "name": "system_date"
          }
        }
      }
    }
  ]
}

标准输出样例25：
问题：过去3年，每年的收益是多少
输出：
{
  "time_expressions": [
    {
      "id": "t1",
      "text": "过去3年每年",
      "expr": {
        "op": "enumerate_subperiods",
        "unit": "year",
        "base": {
          "op": "rolling",
          "unit": "year",
          "value": 3,
          "anchor": "system_date"
        }
      }
    }
  ]
}

依赖表达示例：
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
"""


ROLLING_HOURS_QUERY_PATTERN = re.compile(r"(?:最近|近|过去)\s*(\d+)\s*(?:个)?小时")
HOUR_ENUMERATION_MARKERS = ("每小时", "各小时", "逐小时")

ROLLING_INCLUDE_ANCHOR_VALIDATOR_SYSTEM_PROMPT = """你是一个严格的 rolling 局部布尔校验器。你的任务只是在已有解析结果基础上判断每个 rolling time_expression 的 include_anchor 应该是 true 还是 false。

你只能输出一个 JSON 对象，结构必须严格如下：
{
  "include_anchor_by_id": {
    "t1": false
  }
}

规则：
1. 你只能判断 include_anchor_by_id，不能修改 time_expressions，不能新增别的字段，不能解释
2. key 必须是第一轮解析结果里包含 rolling 的 time_expression id
3. 只有当用户明确要求某个 rolling 窗口“含今天 / 含今日 / 截至今天 / 到今天 / 至今 / 算到今天”时，该 id 才能是 true
4. 仅仅出现“今天”这个词，不等于 rolling 必须含今天
5. 如果句子里同时有“今天”单日和 rolling，除非用户明确要求 rolling 含今天，否则对应 rolling id 必须返回 false
6. 如果一个请求里有多个 rolling，必须分别判断每个 id，不能用一个全局布尔替代

示例：
- “最近一周收益”，第一轮里 rolling id 为 t1 -> {"include_anchor_by_id": {"t1": false}}
- “最近一周收益，含今天”，第一轮里 rolling id 为 t1 -> {"include_anchor_by_id": {"t1": true}}
- “最近一周和最近一周含今天对比”，第一轮里 rolling id 为 t1、t2 -> {"include_anchor_by_id": {"t1": false, "t2": true}}
"""


def _effective_system_date(system_date: str | None, system_datetime: str | None) -> str:
    if system_date is not None:
        return system_date
    if system_datetime is not None:
        return system_datetime.split(" ", 1)[0]
    raise ValueError("system_date is required when system_datetime is omitted")


def build_parser_user_prompt(
    query: str,
    system_date: str | None,
    system_datetime: str | None,
    timezone: str = "Asia/Shanghai",
) -> str:
    lines = [
        "请解析下面的用户问题，并输出严格符合要求的 JSON。",
        "",
        f"system_date: {_effective_system_date(system_date, system_datetime)}",
    ]
    if system_datetime is not None:
        lines.append(f"system_datetime: {system_datetime}")
    lines.extend(
        [
            f"timezone: {timezone}",
            f"user_query: {query}",
        ]
    )
    return "\n".join(lines)


def build_parser_repair_prompt(raw_output: str) -> str:
    return (
        "请把下面这段模型输出修复成一个合法 JSON 对象。\n"
        "要求：\n"
        "1. 只输出 JSON，不要解释\n"
        "2. 保持原意，不要增加新字段\n"
        "3. 所有 key 必须使用双引号\n"
        "4. 如果原始输出里有 markdown 或代码块，去掉它们\n\n"
        "原始输出：\n"
        f"{raw_output}"
    )


def build_rolling_include_anchor_validator_prompt(
    query: str,
    system_date: str | None,
    system_datetime: str | None,
    first_pass_payload: dict[str, Any],
    timezone: str = "Asia/Shanghai",
) -> str:
    lines = [
        "请只判断每个 rolling time_expression 的 include_anchor，并输出严格 JSON。",
        "",
        f"system_date: {_effective_system_date(system_date, system_datetime)}",
    ]
    if system_datetime is not None:
        lines.append(f"system_datetime: {system_datetime}")
    lines.extend(
        [
            f"timezone: {timezone}",
            f"user_query: {query}",
            "first_pass_parse_json:",
            json.dumps(first_pass_payload, ensure_ascii=False, indent=2),
        ]
    )
    return "\n".join(lines)


class _RollingIncludeAnchorDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_anchor_by_id: dict[str, StrictBool]


class QueryParser:
    def __init__(
        self,
        *,
        text_runner: Any | None = None,
        llm: Any | None = None,
    ) -> None:
        self._text_runner = text_runner
        self._llm = llm

    def _get_text_runner(self) -> Any:
        if self._text_runner is None:
            if self._llm is None:
                raise RuntimeError("QueryParser requires an injected llm or text_runner.")
            self._text_runner = self._llm
        return self._text_runner

    def parse_query_with_llm(
        self,
        *,
        query: str,
        system_date: str | None = None,
        system_datetime: str | None = None,
        timezone: str = "Asia/Shanghai",
    ) -> ParsedTimeExpressions:
        messages = [
            SystemMessage(content=PARSER_SYSTEM_PROMPT),
            HumanMessage(content=build_parser_user_prompt(query, system_date, system_datetime, timezone)),
        ]
        raw_text = self._invoke_text(messages)

        try:
            payload = self._parse_json_payload(raw_text)
        except ValueError:
            repaired_text = self._invoke_text(
                [
                    SystemMessage(content=PARSER_SYSTEM_PROMPT),
                    HumanMessage(content=build_parser_repair_prompt(raw_text)),
                ]
            )
            try:
                payload = self._parse_json_payload(repaired_text)
            except ValueError as exc:
                raise ValueError("LLM returned invalid JSON after repair attempt.") from exc

        payload = self._normalize_recent_hours_payload(payload, query)
        parsed = self._normalize_no_time_parse(ParsedTimeExpressions.model_validate(payload))
        parsed = self._normalize_legacy_rolling_expressions(parsed)
        parsed = self._normalize_irrelevant_rolling_flag(parsed)
        if self._parsed_contains_current_hour(parsed) and system_datetime is None:
            raise ValueError("current_hour requires system_datetime.")
        if self._parsed_contains_rolling_hours(parsed) and system_datetime is None:
            raise ValueError("rolling_hours requires system_datetime.")
        if not self._parsed_contains_rolling(parsed):
            return parsed

        validated_include_anchor_by_id = self._validate_rolling_include_anchor_by_id(
            query=query,
            system_date=system_date,
            system_datetime=system_datetime,
            timezone=timezone,
            first_pass_payload=parsed.model_dump(mode="python"),
            fallback_values=self._rolling_include_anchor_by_id(parsed),
        )
        parsed = self._apply_rolling_include_anchor_by_id(parsed, validated_include_anchor_by_id)
        return self._synchronize_legacy_rolling_flag(parsed)

    @staticmethod
    def _normalize_no_time_parse(parsed: ParsedTimeExpressions) -> ParsedTimeExpressions:
        if parsed.time_expressions:
            return parsed

        return ParsedTimeExpressions.model_validate(
            {
                "rolling_includes_today": False,
                "time_expressions": [
                    {
                        "id": "t1",
                        "text": "昨天",
                        "expr": {
                            "op": "shift",
                            "unit": "day",
                            "value": -1,
                            "base": {"op": "current_period", "unit": "day"},
                        },
                    }
                ],
            }
        )

    @classmethod
    def _normalize_recent_hours_payload(
        cls,
        payload: dict[str, Any],
        query: str,
    ) -> dict[str, Any]:
        value = cls._extract_rolling_hours_value(query)
        if value is None:
            return payload

        normalized = dict(payload)
        expressions = list(normalized.get("time_expressions") or [])
        if len(expressions) > 1:
            return payload
        if expressions:
            first = dict(expressions[0])
        else:
            first = {
                "id": "t1",
                "text": query.strip(),
            }
        first["expr"] = {
            "op": "enumerate_hours",
            "base": {"op": "rolling_hours", "value": value},
        } if cls._is_hour_enumeration_query(query) else {
            "op": "rolling_hours",
            "value": value,
        }
        expressions = [first]
        normalized["time_expressions"] = expressions
        normalized["rolling_includes_today"] = False
        return normalized

    @staticmethod
    def _extract_rolling_hours_value(query: str) -> int | None:
        match = ROLLING_HOURS_QUERY_PATTERN.search(query)
        if match is None:
            return None
        return int(match.group(1))

    @staticmethod
    def _is_hour_enumeration_query(query: str) -> bool:
        return any(marker in query for marker in HOUR_ENUMERATION_MARKERS)

    @staticmethod
    def _normalize_irrelevant_rolling_flag(parsed: ParsedTimeExpressions) -> ParsedTimeExpressions:
        if parsed.rolling_includes_today and not QueryParser._parsed_contains_rolling(parsed):
            payload = parsed.model_dump(mode="python")
            payload["rolling_includes_today"] = False
            return ParsedTimeExpressions.model_validate(payload)
        return parsed

    @classmethod
    def _normalize_nested_expr_payload(cls, value: Any, default_include_anchor: bool) -> Any:
        if hasattr(value, "model_dump"):
            payload = value.model_dump(mode="python")
            if "op" in payload:
                return cls._normalize_legacy_rolling_expr_payload(payload, default_include_anchor)
            return {
                key: cls._normalize_nested_expr_payload(nested_value, default_include_anchor)
                for key, nested_value in payload.items()
            }
        if isinstance(value, Mapping):
            payload = dict(value)
            if "op" in payload:
                return cls._normalize_legacy_rolling_expr_payload(payload, default_include_anchor)
            return {
                key: cls._normalize_nested_expr_payload(nested_value, default_include_anchor)
                for key, nested_value in payload.items()
            }
        if isinstance(value, list):
            return [cls._normalize_nested_expr_payload(item, default_include_anchor) for item in value]
        return value

    @classmethod
    def _normalize_legacy_rolling_expr_payload(cls, expr_payload: dict[str, Any], default_include_anchor: bool) -> dict[str, Any]:
        if expr_payload.get("op") == "rolling":
            if "anchor_expr" in expr_payload:
                normalized = {
                    key: cls._normalize_nested_expr_payload(value, default_include_anchor)
                    for key, value in expr_payload.items()
                }
                if normalized.get("include_anchor") is None:
                    normalized["include_anchor"] = False
                return normalized
            if expr_payload.get("anchor") == "system_date":
                return {
                    "op": "rolling",
                    "unit": expr_payload["unit"],
                    "value": expr_payload["value"],
                    "anchor_expr": {"op": "anchor", "name": "system_date"},
                    "include_anchor": default_include_anchor,
                }
        return {
            key: cls._normalize_nested_expr_payload(value, default_include_anchor)
            for key, value in expr_payload.items()
        }

    @classmethod
    def _normalize_legacy_rolling_expressions(cls, parsed: ParsedTimeExpressions) -> ParsedTimeExpressions:
        payload = parsed.model_dump(mode="python")
        payload["time_expressions"] = [
            {
                **item,
                "expr": cls._normalize_legacy_rolling_expr_payload(item["expr"], parsed.rolling_includes_today),
            }
            for item in payload["time_expressions"]
        ]
        return ParsedTimeExpressions.model_validate(payload)

    @classmethod
    def _first_rolling_include_anchor(cls, expr: Any) -> bool | None:
        if getattr(expr, "op", None) == "rolling":
            include_anchor = getattr(expr, "include_anchor", None)
            return False if include_anchor is None else include_anchor

        for value in vars(expr).values():
            if isinstance(value, list):
                for item in value:
                    if hasattr(item, "op"):
                        include_anchor = cls._first_rolling_include_anchor(item)
                        if include_anchor is not None:
                            return include_anchor
            elif hasattr(value, "op"):
                include_anchor = cls._first_rolling_include_anchor(value)
                if include_anchor is not None:
                    return include_anchor
        return None

    @classmethod
    def _rolling_include_anchor_by_id(cls, parsed: ParsedTimeExpressions) -> dict[str, bool]:
        values: dict[str, bool] = {}
        for item in parsed.time_expressions:
            include_anchor = cls._first_rolling_include_anchor(item.expr)
            if include_anchor is not None:
                values[item.id] = include_anchor
        return values

    @classmethod
    def _apply_include_anchor_to_expr_payload(cls, expr_payload: dict[str, Any], include_anchor: bool | None) -> dict[str, Any]:
        if expr_payload.get("op") == "rolling":
            updated = {
                key: cls._normalize_nested_expr_payload(value, False)
                for key, value in expr_payload.items()
            }
            if include_anchor is not None:
                updated["include_anchor"] = include_anchor
            return updated
        return {
            key: cls._normalize_nested_expr_payload(value, False) if not isinstance(value, Mapping) or "op" not in value else cls._apply_include_anchor_to_expr_payload(dict(value), include_anchor)
            for key, value in expr_payload.items()
        }

    @classmethod
    def _apply_rolling_include_anchor_by_id(
        cls,
        parsed: ParsedTimeExpressions,
        include_anchor_by_id: dict[str, bool],
    ) -> ParsedTimeExpressions:
        payload = parsed.model_dump(mode="python")
        payload["time_expressions"] = [
            {
                **item,
                "expr": cls._apply_include_anchor_to_expr_payload(item["expr"], include_anchor_by_id.get(item["id"])),
            }
            for item in payload["time_expressions"]
        ]
        return ParsedTimeExpressions.model_validate(payload)

    @classmethod
    def _synchronize_legacy_rolling_flag(cls, parsed: ParsedTimeExpressions) -> ParsedTimeExpressions:
        payload = parsed.model_dump(mode="python")
        include_anchor_values = list(cls._rolling_include_anchor_by_id(parsed).values())
        payload["rolling_includes_today"] = bool(include_anchor_values) and all(include_anchor_values)
        return ParsedTimeExpressions.model_validate(payload)

    def _invoke_text(self, messages: list[Any]) -> str:
        result = self._get_text_runner().invoke(messages)
        return self._coerce_text(result)

    def _validate_rolling_include_anchor_by_id(
        self,
        *,
        query: str,
        system_date: str | None,
        system_datetime: str | None,
        timezone: str,
        first_pass_payload: dict[str, Any],
        fallback_values: dict[str, bool],
    ) -> dict[str, bool]:
        messages = [
            SystemMessage(content=ROLLING_INCLUDE_ANCHOR_VALIDATOR_SYSTEM_PROMPT),
            HumanMessage(
                content=build_rolling_include_anchor_validator_prompt(
                    query=query,
                    system_date=system_date,
                    system_datetime=system_datetime,
                    timezone=timezone,
                    first_pass_payload=first_pass_payload,
                )
            ),
        ]
        try:
            raw_text = self._invoke_text(messages)
            payload = self._parse_json_payload(raw_text)
            decision = _RollingIncludeAnchorDecision.model_validate(payload)
        except (ValueError, ValidationError):
            return fallback_values
        merged_values = dict(fallback_values)
        for item_id in fallback_values:
            if item_id in decision.include_anchor_by_id:
                merged_values[item_id] = decision.include_anchor_by_id[item_id]
        return merged_values

    @staticmethod
    def _coerce_text(result: Any) -> str:
        if isinstance(result, str):
            return result

        content = getattr(result, "content", None)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_parts = []
            for item in content:
                if isinstance(item, str):
                    text_parts.append(item)
                elif isinstance(item, dict) and item.get("type") == "text":
                    text_parts.append(item.get("text", ""))
            if text_parts:
                return "".join(text_parts)
        return str(result)

    @classmethod
    def _parsed_contains_rolling(cls, parsed: ParsedTimeExpressions) -> bool:
        return any(cls._expr_contains_rolling(item.expr) for item in parsed.time_expressions)

    @classmethod
    def _expr_contains_rolling(cls, expr: Any) -> bool:
        if getattr(expr, "op", None) == "rolling":
            return True

        for value in vars(expr).values():
            if isinstance(value, list):
                if any(cls._expr_contains_rolling(item) for item in value if hasattr(item, "op")):
                    return True
            elif hasattr(value, "op") and cls._expr_contains_rolling(value):
                return True
        return False

    @classmethod
    def _parsed_contains_rolling_hours(cls, parsed: ParsedTimeExpressions) -> bool:
        return any(cls._expr_contains_rolling_hours(item.expr) for item in parsed.time_expressions)

    @classmethod
    def _parsed_contains_current_hour(cls, parsed: ParsedTimeExpressions) -> bool:
        return any(cls._expr_contains_current_hour(item.expr) for item in parsed.time_expressions)

    @classmethod
    def _expr_contains_current_hour(cls, expr: Any) -> bool:
        if getattr(expr, "op", None) == "current_hour":
            return True

        for value in vars(expr).values():
            if isinstance(value, list):
                if any(cls._expr_contains_current_hour(item) for item in value if hasattr(item, "op")):
                    return True
            elif hasattr(value, "op") and cls._expr_contains_current_hour(value):
                return True
        return False

    @classmethod
    def _expr_contains_rolling_hours(cls, expr: Any) -> bool:
        if getattr(expr, "op", None) == "rolling_hours":
            return True

        for value in vars(expr).values():
            if isinstance(value, list):
                if any(cls._expr_contains_rolling_hours(item) for item in value if hasattr(item, "op")):
                    return True
            elif hasattr(value, "op") and cls._expr_contains_rolling_hours(value):
                return True
        return False

    @classmethod
    def _parse_json_payload(cls, raw_text: str) -> dict[str, Any]:
        json_text = cls._extract_json_object_text(raw_text)
        try:
            payload = json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"LLM returned invalid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("LLM returned JSON that is not an object.")
        return payload

    @staticmethod
    def _extract_json_object_text(raw_text: str) -> str:
        text = raw_text.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        start = text.find("{")
        if start == -1:
            raise ValueError("LLM returned invalid JSON: no JSON object found.")

        depth = 0
        in_string = False
        escape = False

        for index in range(start, len(text)):
            char = text[index]

            if escape:
                escape = False
                continue

            if char == "\\":
                escape = True
                continue

            if char == '"':
                in_string = not in_string
                continue

            if in_string:
                continue

            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return text[start : index + 1]

        raise ValueError("LLM returned invalid JSON: unterminated JSON object.")


def parse_query_with_llm(
    query: str,
    system_date: str | None = None,
    system_datetime: str | None = None,
    timezone: str = "Asia/Shanghai",
) -> ParsedTimeExpressions:
    from time_query_service.service import QueryPipelineService

    service = QueryPipelineService()
    return ParsedTimeExpressions.model_validate(
        service.parse_query(
            query=query,
            system_date=system_date,
            system_datetime=system_datetime,
            timezone=timezone,
        )
    )
