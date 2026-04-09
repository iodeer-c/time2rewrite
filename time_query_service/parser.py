from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from time_query_service.config import get_llm_config
from time_query_service.schemas import ParsedTimeExpressions


PARSER_SYSTEM_PROMPT = """你是一个时间字段生成器。你的任务是把用户中文问题中为了回答该问题所需的时间字段解析成固定 JSON。

你只能做结构化抽取，不能回答问题，不能补充解释，不能输出 markdown，不能输出 JSON 之外的任何内容。

你的输出必须严格符合这个结构：
{
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
5. 如果存在依赖关系，例如“去年同期”依赖“今年3月”，则按依赖顺序输出
6. 如果一个时间短语本身表示一个整体范围，则输出一个整体 time_expression
7. 如果问题包含“分别”“各自”“依次”“每个”“每个月”“每个季度”“每半年”“每天”“每周”“每年”等语义，并且需要分别计算多个子时间窗口，则必须拆成多个 time_expression

解析目标：
1. 识别回答当前问题所需的全部时间字段
2. 为每个时间字段生成一个 id、text 和 expr
3. 如果没有识别到时间字段，输出 {"time_expressions": []}

字段定义：
- time_expressions: 数组，包含 0 个或多个时间字段对象
- id: 时间字段唯一标识，使用 t1、t2、t3 这种格式，并按输出顺序递增
- text: 时间字段文本，可为原文时间短语，也可为规范化后的子时间字段名称
- expr: 时间表达式树

expr 只允许以下 op：
- anchor
- current_period
- shift
- rolling
- calendar_event_range
- range_edge
- business_day_offset
- enumerate_calendar_days
- enumerate_makeup_workdays
- slice_subperiods
- select_subperiod
- select_weekday
- select_weekend
- select_occurrence
- select_month
- select_quarter
- select_half_year
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
- anchor: 只允许 "system_date"

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
- year: 公历年整数
- scope: 只允许 "consecutive_rest" 或 "statutory"

语义：
- 直接表示某个命名节假日区间
- 例如“去年国庆假期”“今年中秋法定假期”
- 不允许自己编造具体公历日期，必须通过 event_key + year + scope 表达

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
- year: 公历年整数

语义：
- 直接按业务日历枚举某个命名节假日在该公历年关联的调休补班日
- 不需要 base，不要先构造连续连休区间
- 例如“2025年春节调休补班是哪些日期”“2025年中秋调休上班日是哪些日期”

9. slice_subperiods
- op = "slice_subperiods"
- mode: 只允许 "first" 或 "last"
- unit: 只允许 day/week/month/quarter
- count: 正整数
- base: 一个 expr 对象

语义：
- 先求值 base 为一个较大时间范围
- 再从 base 内部按 unit 切分连续子周期
- mode=first 表示取前 count 个子周期
- mode=last 表示取后 count 个子周期

10. select_subperiod
- op = "select_subperiod"
- unit: 只允许 day/week/month/quarter/half_year
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

17. reference
- ref: string，引用前面已经出现过的时间字段 id，例如 t1

规则：
- 不允许发明新的字段
- 不允许发明新的 op
- 一个问题中可能只出现一个原文时间短语，但为了回答问题，可能需要输出多个 time_expression
- 一个问题中也可能出现多个原文时间短语，但如果最终只需要一个整体时间窗口，也可以只输出一个 time_expression
- 如果某个时间字段依赖另一个时间字段，例如“去年同期”“上月同期”“去年同月”，不要直接基于 system_date 计算
- 这类依赖表达必须使用 reference 引用前面已经出现的时间字段 id
- reference 只能引用前面已经出现的时间字段，不能引用后面的时间字段
- 如果问题要求分别返回多个子周期结果，必须拆成多个独立 time_expression
- 如果问题只要求整体结果，则应输出能表示整体时间范围的最简 time_expression
- 如果时间表达形如“X前N个Y / X的前N个Y / X后N个Y / X的后N个Y”，其中 X 是较大时间范围、Y 是较小子周期，则整体查询必须使用 slice_subperiods
- 这类表达不得解析为 rolling，不得以 system_date 直接作为锚点
- 如果这类表达带有“分别”“各自”“依次”“每个”等语义，并且需要分别计算多个子时间窗口，则不要输出一个 slice_subperiods，而要拆成多个独立 time_expression
- 对于拆分后的每个子时间窗口，优先使用 select_subperiod；不要把“第二周”错误表示成“前两周”
- “X的第N个Y / X第一周 / X第二周 / X第一个月 / X第一个季度”这类表达，必须使用 select_subperiod，除非已有更直接且完全等价的专用选择 op
- “第N个周二 / 最后一个周日 / 第N个周末 / 最后一个周末” 这类父周期内按出现次数选择的表达，必须使用 select_occurrence
- “第N周的周二 / 第N周的周末” 这类周内选择表达，必须先使用 select_subperiod 选出第 N 周，再用 select_weekday 或 select_weekend
- select_weekday 只能用于 week base；不要把 month/quarter/year 直接作为 select_weekday 的 base
- “去年国庆假期 / 今年春节假期 / 去年中秋法定假期” 这类命名节假日区间，必须优先使用 calendar_event_range
- “假期开始日 / 假期结束日” 这类边界表达，必须使用 range_edge
- “端午节当天 / 国庆节当天 / 中秋节当天”等「某节当天」且该节国务院安排为连续多日放假时：先用 calendar_event_range(region, event_key, year, consecutive_rest) 表示该节连休，再用 range_edge(edge="start", base=...) 取连休首日作为「正日」当日（现行安排下端午、中秋等与连休首日一致）；不要凭空 select_month 猜公历；若日历 JSON 已为该节维护 scope=statutory 且仅为正日一天，也可用 statutory 代替上述组合
- “节前最后一个工作日 / 节后第一个工作日” 这类业务日表达，必须使用 business_day_offset，并以单日 base 为锚点
- “某个月的工作日 / 休息日 / 节假日” 这类范围内按业务日历筛选日期的表达，必须使用 enumerate_calendar_days
- “某节调休上班日 / 某节补班日 / 某节调休补班是哪些日期” 这类表达，必须使用 enumerate_makeup_workdays
- 禁止把这类问题解析成 enumerate_calendar_days；不要先构造 calendar_event_range(..., consecutive_rest) 再枚举 workday
- “工作日” 对应 day_kind="workday"
- “休息日” 对应 day_kind="restday"
- “节假日” 对应 day_kind="holiday"

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
              "year": 2025,
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
        "year": 2025
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


def build_parser_user_prompt(query: str, system_date: str, timezone: str) -> str:
    return (
        "请解析下面的用户问题，并输出严格符合要求的 JSON。\n\n"
        f"system_date: {system_date}\n"
        f"timezone: {timezone}\n"
        f"user_query: {query}"
    )


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


class QueryParser:
    def __init__(
        self,
        *,
        model: str | None = None,
        temperature: float = 0,
        text_runner: Any | None = None,
        llm: ChatOpenAI | None = None,
    ) -> None:
        self.model = model
        self.temperature = temperature
        self._text_runner = text_runner
        self._llm = llm

    def _get_llm(self) -> ChatOpenAI:
        if self._llm is None:
            config = get_llm_config()
            if not config.api_key:
                raise RuntimeError("Missing DASHSCOPE_API_KEY. Set it in .env or your shell environment.")
            self._llm = ChatOpenAI(
                model=self.model or config.model_name,
                temperature=self.temperature,
                api_key=config.api_key,
                base_url=config.base_url,
            )
        return self._llm

    def _get_text_runner(self) -> Any:
        if self._text_runner is None:
            self._text_runner = self._get_llm()
        return self._text_runner

    def parse_query_with_llm(self, *, query: str, system_date: str, timezone: str) -> ParsedTimeExpressions:
        messages = [
            SystemMessage(content=PARSER_SYSTEM_PROMPT),
            HumanMessage(content=build_parser_user_prompt(query, system_date, timezone)),
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

        return ParsedTimeExpressions.model_validate(payload)

    def _invoke_text(self, messages: list[Any]) -> str:
        result = self._get_text_runner().invoke(messages)
        return self._coerce_text(result)

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


def parse_query_with_llm(query: str, system_date: str, timezone: str) -> ParsedTimeExpressions:
    parser = QueryParser()
    return parser.parse_query_with_llm(query=query, system_date=system_date, timezone=timezone)
