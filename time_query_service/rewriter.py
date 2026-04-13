from __future__ import annotations

import calendar
import re
from collections import OrderedDict
from datetime import datetime
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from time_query_service.schemas import ResolvedTimeExpressions

HOUR_ENUMERATION_MARKERS = ("每小时", "各小时", "逐小时")
BREAKDOWN_MARKERS = ("分别", "各自", "依次", "逐项", "逐个")
NATURAL_PERIOD_BREAKDOWN_MARKERS = {
    "month": ("每个月", "每月", "各月"),
    "quarter": ("每个季度", "各季度"),
    "half_year": ("每个半年",),
    "year": ("每年", "各年"),
}
CALENDAR_DAY_BREAKDOWN_MARKERS = ("每个工作日", "每个休息日", "每个节假日", "每天")
AGGREGATE_MARKERS = ("总收益", "总量", "均值", "汇总", "平均", "合计", "总和")
NATURAL_PERIOD_EXCLUDE_MARKERS = ("假期", "工作日", "休息日", "节假日", "补班", "调休", "当天", "当日", "正日")
BUSINESS_AXIS_MARKERS = ("每个收费站", "各收费站", "每个站点", "各站点", "每个渠道", "各渠道")
QUARTER_LABELS = {1: "第一季度", 2: "第二季度", 3: "第三季度", 4: "第四季度"}
HALF_YEAR_LABELS = {1: "上半年", 2: "下半年"}
MULTI_ROOT_NATURAL_PERIOD_PATTERN = re.compile(
    r"(?P<years>\d{4}年(?:度|全年)?(?:\s*(?:、|，|和)\s*\d{4}年(?:度|全年)?)+)\s*(?P<suffix>每个季度|每个月|每月|每个半年|每年)"
)
MULTI_ROOT_HOLIDAY_PATTERN = re.compile(
    r"(?P<years>\d{4}年(?:\s*(?:、|，|和)\s*\d{4}年)+)\s*(?P<label>[^\s，。！？,!?；;]*假期)"
)


# REWRITER_SYSTEM_PROMPT_v1 = """你是一个查询改写器。
#
# 你的任务是根据已经计算完成的绝对时间范围，把原始问题改写成一个用户可读的问题。你只能消费输入里的 resolved_time_expressions，不能重新推理时间，也不能猜测原始语义里没有给出的时间信息。
#
# 规则：
# 1. 你不能重新推理时间，只能使用输入中提供的 resolved_time_expressions
# 2. 保持原始问题中非时间部分尽量不变
# 3. 只改写时间部分
# 4. 如果 resolved_time_expressions 为空，直接输出原始问题
# 5. 只输出一行纯文本，不要输出解释，不要输出 markdown
#
# 改写规则：
# - 如果某个时间表达的 start_time 和 end_time 在同一天，优先改写成“YYYY年M月D日”
# - 如果某个时间表达恰好覆盖同一天内的单个完整小时，优先改写成“YYYY年M月D日14点”
# - 如果某个时间表达恰好覆盖同一天内连续完整小时范围，优先改写成“YYYY年M月D日14点到15点”
# - 如果某个时间表达不是整日，也不是完整整点小时范围，必须改写成带时分秒的区间，例如“YYYY年M月D日14:37:00至YYYY年M月D日15:12:00”
# - 如果某个时间表达覆盖多天，优先改写成“YYYY年M月D日至YYYY年M月D日”
# - 如果原问题本来是在问一个单一时间窗口，就保持单时间窗口问法
# - 如果原问题本来是在问多个时间窗口分别的结果，或者原问题使用了“每个季度”“每个月”“每个工作日”“每天”等枚举型时间表达，就保留逐项结果形态
# - 如果原问题是在问总量、总收益、均值、汇总等聚合结果，即使 resolved_time_expressions 展开成多个成员，也必须保持聚合问法
# - grouped members 对应月/季度/年等自然周期列表时，结果形态必须服从原问题：如果原问题在问“每个季度 / 每个月 / 每天 / 每个工作日”等枚举型窗口，就保留逐窗口语义；否则保持原有聚合语义
# - 多个时间字段或 grouped members 只说明时间被展开，不等于用户需要多个结果
# - 你的职责是保持原问题语义不变，只把时间改写为绝对表达
# - 不要把聚合问题改写成逐项问题，不要凭空补出“分别”“各自”等词
# - 但如果原问题本来就在问“每个季度 / 每个月 / 每个工作日 / 每天”这类逐窗口结果，即使没有写出“分别”，改写后也必须保留逐窗口语义
# - 不要新增输入中不存在的时间信息
# - 如果 rewrite_hints 标记某个来源需要 `member_list`，说明 grouped root 只是结构父节点，不要把 root 的 covering span 当成直接改写结果，必须使用 leaf members
# - 如果原问题没有显式时间，但输入给了 1 个默认补出的单时间窗口，可以直接把该日期补进问题里，让结果变成自然问句
#
# 示例1
# 原问题：上周二的日期是多少
# resolved_time_expressions:
# - id: t1
# - text: 上周二
# - start_time: 2026-03-31 00:00:00
# - end_time: 2026-03-31 23:59:59
# - timezone: Asia/Shanghai
# 输出：2026年3月31日的日期是多少
#
# 示例2
# 原问题：上周二和上周三的日期分别是多少
# resolved_time_expressions:
# - id: t1
# - text: 上周二
# - start_time: 2026-03-31 00:00:00
# - end_time: 2026-03-31 23:59:59
# - timezone: Asia/Shanghai
# - id: t2
# - text: 上周三
# - start_time: 2026-04-01 00:00:00
# - end_time: 2026-04-01 23:59:59
# - timezone: Asia/Shanghai
# 输出：2026年3月31日和2026年4月1日的日期分别是多少
#
# 示例3
# 原问题：去年前两个季度的销售额分别是多少
# resolved_time_expressions:
# - id: t1
# - text: 去年第一季度
# - start_time: 2025-01-01 00:00:00
# - end_time: 2025-03-31 23:59:59
# - timezone: Asia/Shanghai
# - id: t2
# - text: 去年第二季度
# - start_time: 2025-04-01 00:00:00
# - end_time: 2025-06-30 23:59:59
# - timezone: Asia/Shanghai
# 输出：2025年第一季度和2025年第二季度的销售额分别是多少
#
# 示例4
# 原问题：上个月的前两周的销售额分别是多少
# resolved_time_expressions:
# - id: t1
# - text: 上个月第一周
# - start_time: 2026-03-02 00:00:00
# - end_time: 2026-03-08 23:59:59
# - timezone: Asia/Shanghai
# - id: t2
# - text: 上个月第二周
# - start_time: 2026-03-09 00:00:00
# - end_time: 2026-03-15 23:59:59
# - timezone: Asia/Shanghai
# 输出：2026年3月2日至2026年3月8日和2026年3月9日至2026年3月15日的销售额分别是多少
#
# 示例5
# 原问题：这个月第二个周二的销售额是多少
# resolved_time_expressions:
# - id: t1
# - text: 这个月第二个周二
# - start_time: 2026-09-08 00:00:00
# - end_time: 2026-09-08 23:59:59
# - timezone: Asia/Shanghai
# 输出：2026年9月8日的销售额是多少
#
# 示例6
# 原问题：上个月最后一个周末的销售额是多少
# resolved_time_expressions:
# - id: t1
# - text: 上个月最后一个周末
# - start_time: 2026-01-31 00:00:00
# - end_time: 2026-01-31 23:59:59
# - timezone: Asia/Shanghai
# 输出：2026年1月31日的销售额是多少
#
# 示例7
# 原问题：帮我看看数据
# resolved_time_expressions:
# - id: t1
# - text: 昨天
# - start_time: 2026-04-05 00:00:00
# - end_time: 2026-04-05 23:59:59
# - timezone: Asia/Shanghai
# 输出：帮我看看2026年4月5日的数据
#
# 示例8
# 原问题：收益是多少
# resolved_time_expressions:
# - id: t1
# - text: 昨天
# - start_time: 2026-04-05 00:00:00
# - end_time: 2026-04-05 23:59:59
# - timezone: Asia/Shanghai
# 输出：2026年4月5日的收益是多少
#
# 示例9
# 原问题：今天14点的收益是多少
# resolved_time_expressions:
# - id: t1
# - text: 今天14点
# - start_time: 2026-04-10 14:00:00
# - end_time: 2026-04-10 14:59:59
# - timezone: Asia/Shanghai
# 输出：2026年4月10日14点的收益是多少
#
# 示例10
# 原问题：今天前2小时的收益是多少
# resolved_time_expressions:
# - id: t1
# - text: 今天前2小时
# - start_time: 2026-04-10 14:00:00
# - end_time: 2026-04-10 15:59:59
# - timezone: Asia/Shanghai
# 输出：2026年4月10日14点到15点的收益是多少
#
# 示例11
# 原问题：最近24小时的收益是多少
# resolved_time_expressions:
# - id: t1
# - text: 最近24小时
# - start_time: 2026-04-09 14:37:00
# - end_time: 2026-04-10 14:37:00
# - timezone: Asia/Shanghai
# 输出：2026年4月9日14:37:00至2026年4月10日14:37:00的收益是多少
#
# 示例12
# 原问题：最近10个工作日杭千公司的总收益是多少
# resolved_time_expressions:
# - id: t1__seg_01
# - text: 最近10个工作日
# - start_time: 2026-03-27 00:00:00
# - end_time: 2026-03-27 23:59:59
# - timezone: Asia/Shanghai
# - id: t1__seg_02
# - text: 最近10个工作日
# - start_time: 2026-03-30 00:00:00
# - end_time: 2026-03-30 23:59:59
# - timezone: Asia/Shanghai
# 输出：2026年3月27日、2026年3月30日等最近10个工作日杭千公司的总收益是多少
# """

REWRITER_SYSTEM_PROMPT = """
你是一个“时间问题改写器”。

  你的唯一职责是：
  根据输入中已经由上游计算完成的时间解析结果，把 original_query 中的时间表达改写成用户可读、时间明
  确的绝对时间表达，并自然嵌回原问题。

  你不是问答助手，不是总结器，不是业务改写器。
  你不能回答问题，不能改变问题意图，不能重写业务语义。
  你只负责把“原问题里的时间关系”改写清楚。

  ================
  一、可用输入与禁止事项
  ================
  你只能使用以下输入信息：
  - original_query
  - resolved_time_expressions
  - resolved_time_expression_groups
  - metadata.rewrite_hints
  - 其他显式提供给你的结构化字段

  你禁止做以下事情：
  1. 重新推理时间
  2. 根据常识补时间
  3. 猜测输入中没有给出的比较对象、统计口径或业务语义
  4. 因为底层时间被展开，就擅自改变结果形态
  5. 泄露内部结构词或字段名，例如 root、member、group、segment、source_id、rewrite_role 等

  ================
  二、总目标
  ================
  输出一个单行纯文本问题，满足：
  1. 时间关系比原问题更清楚
  2. 时间表达是明确的绝对表达
  3. 非时间部分尽量不变
  4. 不丢失原问题信息，也不新增输入中不存在的信息
  5. 保持原问题的意图、结果形态和问法风格
  6. 输出自然、简洁、用户可读
  7. 你的职责是保持原问题语义不变，只把时间改写为绝对表达

  如果 resolved_time_expressions 为空，直接输出 original_query。

  ================
  三、最高优先级
  ================
  当规则冲突时，优先级从高到低如下：
  1. 保持原问题语义不变
  2. 保持原问题结果形态不变
  3. 只使用输入中已经给出的时间信息
  4. 只改写时间相关部分
  5. 尽量少改字，但要保证时间表达清楚
  6. 输出自然、简洁、可读

  ================
  四、先做“时间语义对齐”，再改写
  ================
  改写前，你必须先判断：原问题中的时间，在语义上扮演什么角色。

  常见角色包括：
  A. 查询/筛选范围
  例：上周的订单、最近三天的报警记录、去年营收

  B. 枚举窗口/分组粒度
  例：过去一周每天的访问量、去年每个季度的利润、本月至今每个工作日的收益

  C. 比较对象
  例：上周和这周的差异、今年3月和去年同期相比利润怎么样、昨天比前天多多少

  D. 条件修饰的一部分
  例：在上个月最后一个工作日提交的申请、今天14点之后产生的异常

  E. 默认补出的单时间窗口
  例：帮我看看数据、收益是多少
  如果原问题没有显式时间，但输入明确给出了一个默认补出的单时间窗口，才允许自然补入
  只有当输入中明确给出了一个默认补出的单时间窗口时，才允许自然补入

  如果原问题里有多个时间短语，你必须分别对齐它们各自的角色与对应时间；
  如果原问题里存在引用关系或对照关系，例如“今年3月和去年同期”，必须保持这种对应关系，不要错配，不要
  合并，不要替换成同一个时间窗口。

  ================
  五、结果形态必须保持不变
  ================
  你必须保持原问题的结果形态不变。

  常见结果形态包括：
  1. 单窗口结果
  2. 多窗口逐项结果
  3. 聚合结果
  4. 对比结果
  5. 日期识别结果
  例：是哪天、是几号、是哪两天

  关键规则：
  1. 一切以 original_query 的表达意图为准，不以成员数量为准
  2. resolved_time_expressions 被展开成多个成员，不等于用户要“分别”“各自”“逐项”
  3. grouped root 可能只是结构性的 covering span，不等于它就是用户应该看到的时间表达
  4. 原问题是聚合语义时，即使底层有多个 members，也必须保持聚合问法
  如果原问题是在问总量、总收益、均值、汇总等聚合结果，即使底层时间被展开成多个成员，也必须保持聚合问法
  4.1 如果原问题本来是在问多个时间窗口分别的结果，改写后仍然必须保持多个时间窗口分别的问法
  5. 原问题含有“每个季度 / 每个月 / 每月 / 每天 / 每个工作日 / 各月 / 各季度 / 分别”等逐窗口信号
  时，必须保留逐窗口语义
  6. 原问题是对比语义时，必须保留比较结构，不要改成普通罗列
  7. 原问题是在识别日期时，必须保留“识别日期”的问法，不要改成自指问题或直接回答式句子
  8. 如果原问题同时包含枚举型时间轴和独立的非时间枚举轴，例如“每个收费站 / 各站点 / 每个渠道”
  - 必须保留原时间轴骨架
  - 绝对时间只能作为补充展开，不能把时间轴整段替换成裸成员列表

  不要把聚合问题改写成逐项问题。
  也不要把逐项问题改写成聚合问题。

  ================
  六、如何选择 root span 与 members
  ================
  结构化时间可能同时提供 grouped root、leaf members 和 rewrite_hints。

  你必须遵守以下规则：
  1. 如果 metadata.rewrite_hints 明确要求使用 member_list
  - 必须优先使用 leaf members
  - 不能用 root 的 covering span 直接代替

  2. 如果原问题本来就是逐窗口语义
  - 优先保留逐窗口语义
  - 但不代表必须把所有 members 机械全文展开

  3. 如果 members 是连续自然周期
  例如连续的天、月、季度、半年、年
  - 优先压缩成更自然的绝对表达
  - 不要机械列出超长列表

  例如：
  - 上周每天的订单量是多少
  可改写为：
  2026年3月30日至2026年4月5日每天的订单量是多少

  4. 如果 members 是非连续集合，且原问题确实要逐项
  - 成员少时可以自然列举
  - 成员多时也不要写成机械、冗长、难读的内部展开说明

  5. 如果原问题是聚合语义
  - 即使有多个 members，也不要因为 members 多就擅自补“分别”“各自”“逐项”

  如果 resolved_time_expression_groups、resolved_time_expressions 和 rewrite_hints 同时存在，
  应优先选择“最能保留原问题语义和结果形态”的那一层信息，而不是机械地选择最粗或最细的一层。

  ================
  七、最小必要改写
  ================
  你的改写必须遵循“最小必要改写”原则。

  1. 原句中存在明确时间短语时
  - 只替换对应时间短语
  - 不改动其他业务成分
  - 不重组整句，除非不重组就无法自然表达

  2. 原句中有多个时间短语时
  - 分别对齐并替换
  - 保持原问题中的比较、条件、枚举、聚合等关系不变

  3. 原句中没有显式时间，但输入明确给了 1 个默认补出的单时间窗口时
  - 可以把该时间自然补入问题
  - 补入方式必须最自然、最短、最不改变原句结构

  4. 不要改变问法风格
  - 不要把“上周提交的申请有哪些”改成“请列出……”
  - 不要把“昨天和今天谁更高”改成“比较……”
  - 不要把问题改成回答句
  - 不要把时间改写之外的业务语义重写一遍
  - 如果裸绝对列表会让“每个收费站 / 各站点 / 每个渠道”看起来像唯一枚举轴，必须保留原时间短语并在后面补绝对展开

  例如：
  - 原问题：昨天每小时每个收费站的收益分别是多少
  - 更合适的改写：昨天每小时（即2026年4月9日0点、1点、2点等）每个收费站的收益分别是多少
  - 原问题：帮我看看数据
  - 更合适的改写：帮我看看2026年4月5日的数据
  - 类似“上个月第一周”“上个月第二周”“这个月第二个周二”“上个月最后一个周末”这类时间短语，也必须只把时间改成绝对表达，不改问法骨架

  ================
  八、时间渲染规则
  ================
  你只负责把已给定的 start_time / end_time 渲染成最自然的绝对时间表达。

  优先规则如下：

  1. 若 start_time 和 end_time 为同一天整日
  格式：
  YYYY年M月D日

  2. 若为同一天内单个完整小时
  格式：
  YYYY年M月D日14点

  3. 若为同一天内连续完整小时范围
  格式：
  YYYY年M月D日14点到15点

  4. 若不是整日，也不是完整整点小时范围
  格式：
  YYYY年M月D日14:37:00至YYYY年M月D日15:12:00

  5. 若跨多天
  格式：
  YYYY年M月D日至YYYY年M月D日

  补充规则：
  - “整日”指 00:00:00 到 23:59:59
  - “单个完整小时”指 14:00:00 到 14:59:59
  - “连续完整小时范围”指 14:00:00 到 15:59:59，对用户显示为 14点到15点

  如果一个时间窗口恰好是完整自然周期，且这样表达更自然且不丢边界信息，可以直接使用：
  - YYYY年M月
  - YYYY年第一季度 / 第二季度 / 第三季度 / 第四季度
  - YYYY年上半年 / 下半年
  - YYYY年

  但如果只是部分自然周期，必须保留真实覆盖范围，不要伪装成完整自然周期。

  ================
  九、文本规范化
  ================
  输出必须是干净自然的一行文本。

  你可以做以下规范化：
  1. 去掉首尾空格
  2. 去掉重复空格
  3. 去掉日期内部异常空格
  4. 去掉脏字符
  5. 时间最终展示以 start_time / end_time 为准，而不是照抄脏的 text 字段

  ================
  十、示例
  ================
  示例1
  原问题：上周二的日期是多少
  输出：2026年3月31日的日期是多少

  示例2
  原问题：今年3月和去年同期相比利润怎么样
  输出：2026年3月和2025年3月相比利润怎么样

  示例3
  原问题：最近10个工作日杭千公司的总收益是多少
  已知底层是 10 个离散工作日成员
  输出：2026年3月27日、2026年3月30日、2026年3月31日、2026年4月1日、2026年4月2日、2026年4月3日、2026
  年4月7日、2026年4月8日、2026年4月9日、2026年4月10日杭千公司的总收益是多少

  示例4
  原问题：本月至今每个工作日的收益是多少
  输出：2025年1月2日、2025年1月3日、2025年1月6日、2025年1月7日、2025年1月8日、2025年1月9日、2025年1
  月10日的收益分别是多少

  示例5
  原问题：上周每天的订单量是多少
  输出：2026年3月30日至2026年4月5日每天的订单量是多少

  示例6
  原问题：收益是多少
  输入明确给了 1 个默认补出的单时间窗口
  输出：2026年4月5日的收益是多少

  ================
  十一、最终输出要求
  ================
  你最终只输出一行纯文本问题：
  - 不要解释
  - 不要输出 markdown
  - 不要输出标签
  - 不要输出中间分析
  - 不要输出内部结构信息

  只输出改写后的最终问题。
"""


def build_rewriter_user_prompt(original_query: str, resolved_time_expressions: ResolvedTimeExpressions) -> str:
    lines = [f"original_query: {original_query}", "resolved_time_expressions:"]
    if not resolved_time_expressions.resolved_time_expressions:
        lines.append("- []")
    else:
        for item in resolved_time_expressions.resolved_time_expressions:
            lines.extend(
                [
                    f"- id: {item.id}",
                    f"- text: {item.text}",
                    f"  source_id: {item.source_id}",
                    f"  source_text: {item.source_text}",
                    f"  start_time: {item.start_time}",
                    f"  end_time: {item.end_time}",
                    f"  timezone: {item.timezone}",
                    f"  is_partial: {item.is_partial}",
                ]
            )
    if resolved_time_expressions.metadata is not None:
        lines.append("metadata:")
        lines.append(f"- calendar_version: {resolved_time_expressions.metadata.calendar_version}")
        lines.append(f"- enumerated_counts: {resolved_time_expressions.metadata.enumerated_counts}")
        lines.append(f"- rewrite_hints: {resolved_time_expressions.metadata.rewrite_hints}")
    structural_parent_ids = (
        {
            source_id
            for source_id, hint in resolved_time_expressions.metadata.rewrite_hints.items()
            if hint.preferred_rendering == "member_list"
        }
        if resolved_time_expressions.metadata is not None and resolved_time_expressions.metadata.rewrite_hints is not None
        else set()
    )
    lines.append("resolved_time_expression_groups:")
    if not resolved_time_expressions.resolved_time_expression_groups:
        lines.append("- []")
    else:
        for group in resolved_time_expressions.resolved_time_expression_groups:
            _append_group_prompt_lines(lines, group, indent=0, structural_parent_ids=structural_parent_ids)
    return "\n".join(lines)


def _append_group_prompt_lines(lines: list[str], group: Any, indent: int, structural_parent_ids: set[str]) -> None:
    prefix = "  " * indent
    lines.extend(
        [
            f"{prefix}- id: {group.id}",
            f"{prefix}- text: {group.text}",
            f"{prefix}  source_id: {group.source_id}",
            f"{prefix}  source_text: {group.source_text}",
            f"{prefix}  start_time: {group.start_time}",
            f"{prefix}  end_time: {group.end_time}",
            f"{prefix}  timezone: {group.timezone}",
            f"{prefix}  is_partial: {group.is_partial}",
        ]
    )
    if group.id in structural_parent_ids:
        lines.append(f"{prefix}  rewrite_role: structural_parent")
    if not group.children:
        return
    lines.append(f"{prefix}  children:")
    for child in group.children:
        _append_group_prompt_lines(lines, child, indent + 1, structural_parent_ids)


def _parse_resolved_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def _format_date(dt: datetime) -> str:
    return f"{dt.year}年{dt.month}月{dt.day}日"


def _format_datetime(dt: datetime) -> str:
    return f"{_format_date(dt)}{dt.strftime('%H:%M:%S')}"


def _is_full_day_range(start: datetime, end: datetime) -> bool:
    return start == start.replace(hour=0, minute=0, second=0, microsecond=0) and end == end.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=0,
    )


def _is_hour_aligned_range(start: datetime, end: datetime) -> bool:
    return (
        start.minute == 0
        and start.second == 0
        and start.microsecond == 0
        and end.minute == 59
        and end.second == 59
        and end.microsecond == 0
    )


def _format_range(start_time: str, end_time: str) -> str:
    start = _parse_resolved_time(start_time)
    end = _parse_resolved_time(end_time)
    if _is_full_day_range(start, end):
        if start.date() == end.date():
            return _format_date(start)
        return f"{_format_date(start)}至{_format_date(end)}"
    if start.date() == end.date() and _is_hour_aligned_range(start, end):
        if start.hour == end.hour:
            return f"{_format_date(start)}{start.hour}点"
        return f"{_format_date(start)}{start.hour}点到{end.hour}点"
    return f"{_format_datetime(start)}至{_format_datetime(end)}"


def _extract_calendar_day_label(source_text: str) -> str:
    if "工作日" in source_text:
        return "工作日"
    if "休息日" in source_text:
        return "休息日"
    if "节假日" in source_text:
        return "节假日"
    return source_text


def _is_explicit_breakdown_query(query: str) -> bool:
    return any(marker in query for marker in BREAKDOWN_MARKERS)


def _has_enumerative_natural_period_intent(query: str, unit: str) -> bool:
    return any(marker in query for marker in NATURAL_PERIOD_BREAKDOWN_MARKERS.get(unit, ()))


def _has_enumerative_calendar_day_intent(query: str) -> bool:
    return any(marker in query for marker in CALENDAR_DAY_BREAKDOWN_MARKERS)


def _has_aggregate_intent(query: str) -> bool:
    return any(marker in query for marker in AGGREGATE_MARKERS)


def _has_enumerative_holiday_intent(query: str) -> bool:
    return "每年" in query and "假期" in query and not _has_aggregate_intent(query)


def _is_full_day_dt(start: datetime, end: datetime) -> bool:
    return start == start.replace(hour=0, minute=0, second=0, microsecond=0) and end == end.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=0,
    )


def _is_natural_month_member(start: datetime, end: datetime) -> bool:
    if start.year != end.year or start.month != end.month:
        return False
    last_day = calendar.monthrange(start.year, start.month)[1]
    return _is_full_day_dt(start, end) and start.day == 1 and end.day == last_day


def _quarter_for_month(month: int) -> int:
    return ((month - 1) // 3) + 1


def _is_natural_quarter_member(start: datetime, end: datetime) -> bool:
    if start.year != end.year or _quarter_for_month(start.month) != _quarter_for_month(end.month):
        return False
    quarter = _quarter_for_month(start.month)
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    last_day = calendar.monthrange(start.year, end_month)[1]
    return (
        _is_full_day_dt(start, end)
        and start.month == start_month
        and start.day == 1
        and end.month == end_month
        and end.day == last_day
    )


def _half_for_month(month: int) -> int:
    return 1 if month <= 6 else 2


def _is_natural_half_year_member(start: datetime, end: datetime) -> bool:
    if start.year != end.year or _half_for_month(start.month) != _half_for_month(end.month):
        return False
    half = _half_for_month(start.month)
    start_month = 1 if half == 1 else 7
    end_month = 6 if half == 1 else 12
    last_day = calendar.monthrange(start.year, end_month)[1]
    return (
        _is_full_day_dt(start, end)
        and start.month == start_month
        and start.day == 1
        and end.month == end_month
        and end.day == last_day
    )


def _is_natural_year_member(start: datetime, end: datetime) -> bool:
    return (
        _is_full_day_dt(start, end)
        and start.year == end.year
        and start.month == 1
        and start.day == 1
        and end.month == 12
        and end.day == 31
    )


def _detect_grouped_natural_period_unit(text: str) -> str | None:
    if any(marker in text for marker in NATURAL_PERIOD_EXCLUDE_MARKERS):
        return None
    if "季度" in text:
        return "quarter"
    if "月" in text:
        return "month"
    if "半年" in text:
        return "half_year"
    if "每年" in text or "各年" in text or "年度" in text or "年全年" in text:
        return "year"
    return None


def _render_natural_period_member(start_time: str, end_time: str, unit: str) -> str | None:
    start = _parse_resolved_time(start_time)
    end = _parse_resolved_time(end_time)
    if unit == "month":
        if start.year != end.year or start.month != end.month:
            return None
        label = f"{start.year}年{start.month}月"
        return label if _is_natural_month_member(start, end) else f"{label}（{_format_range(start_time, end_time)}）"
    if unit == "quarter":
        if start.year != end.year or _quarter_for_month(start.month) != _quarter_for_month(end.month):
            return None
        quarter = _quarter_for_month(start.month)
        label = f"{start.year}年{QUARTER_LABELS[quarter]}"
        return label if _is_natural_quarter_member(start, end) else f"{label}（{_format_range(start_time, end_time)}）"
    if unit == "half_year":
        if start.year != end.year or _half_for_month(start.month) != _half_for_month(end.month):
            return None
        half = _half_for_month(start.month)
        label = f"{start.year}年{HALF_YEAR_LABELS[half]}"
        return label if _is_natural_half_year_member(start, end) else f"{label}（{_format_range(start_time, end_time)}）"
    if unit == "year":
        if start.year != end.year:
            return None
        label = f"{start.year}年"
        return label if _is_natural_year_member(start, end) else f"{label}（{_format_range(start_time, end_time)}）"
    return None


def _is_hour_enumeration_source(text: str) -> bool:
    return any(marker in text for marker in HOUR_ENUMERATION_MARKERS)


def _is_hour_segment(start: datetime, end: datetime) -> bool:
    if _is_full_day_range(start, end):
        return False
    return start.date() == end.date() and (end - start).total_seconds() <= 3599


def _format_hour_segment_for_list(start: datetime, end: datetime, *, include_date: bool) -> str:
    if _is_hour_aligned_range(start, end):
        if include_date:
            return f"{_format_date(start)}{start.hour}点"
        return f"{start.hour}点"
    if start.date() == end.date():
        start_text = start.strftime("%H:%M:%S")
        end_text = end.strftime("%H:%M:%S")
        if include_date:
            return f"{_format_date(start)}{start_text}至{end_text}"
        return f"{start_text}至{end_text}"
    return f"{_format_datetime(start)}至{_format_datetime(end)}"


def _ensure_per_window_question(query: str) -> str:
    if "分别" in query or "各自" in query:
        return query
    for source, target in (
        ("是多少？", "分别是多少？"),
        ("是多少?", "分别是多少?"),
        ("是多少", "分别是多少"),
        ("是什么？", "分别是什么？"),
        ("是什么?", "分别是什么?"),
        ("是什么", "分别是什么"),
        ("有多少？", "分别有多少？"),
        ("有多少?", "分别有多少?"),
        ("有多少", "分别有多少"),
    ):
        if source in query:
            return query.replace(source, target, 1)
    return query


def _ensure_plural_hour_question(query: str) -> str:
    return _ensure_per_window_question(query)


def _join_rendered_members(rendered_members: list[str]) -> str:
    if not rendered_members:
        return ""
    if len(rendered_members) == 1:
        return rendered_members[0]
    if len(rendered_members) == 2:
        return f"{rendered_members[0]}和{rendered_members[1]}"
    return "、".join(rendered_members)


def _has_non_time_business_axis(query: str) -> bool:
    return any(marker in query for marker in BUSINESS_AXIS_MARKERS)


def _build_time_axis_replacement(
    *,
    original_query: str,
    source_text: str,
    expansion: str,
) -> str:
    if _has_non_time_business_axis(original_query):
        return f"{source_text}（即{expansion}）"
    return expansion


def _rewrite_enumerated_hours(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    grouped: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
    for item in resolved.resolved_time_expressions:
        if item.source_id is None:
            continue
        source_text = item.source_text or item.text
        if not (_is_hour_enumeration_source(source_text) or _is_hour_enumeration_source(original_query)):
            continue
        start = _parse_resolved_time(item.start_time)
        end = _parse_resolved_time(item.end_time)
        if not _is_hour_segment(start, end):
            return None
        group = grouped.setdefault(
            item.source_id,
            {
                "source_text": source_text,
                "segments": [],
            },
        )
        group["segments"].append((start, end))

    if not grouped:
        return None

    rewritten = original_query
    for group in grouped.values():
        labels: list[str] = []
        last_date = None
        for start, end in group["segments"]:
            include_date = start.date() != last_date
            labels.append(_format_hour_segment_for_list(start, end, include_date=include_date))
            last_date = start.date()
        source_text = group["source_text"]
        replacement = _build_time_axis_replacement(
            original_query=rewritten,
            source_text=source_text,
            expansion="、".join(labels),
        )
        updated = rewritten.replace(source_text, replacement, 1)
        if updated == rewritten:
            return None
        rewritten = updated

    return _ensure_per_window_question(rewritten)


def _rewrite_enumerated_calendar_days(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if resolved.metadata is None or resolved.metadata.enumerated_counts is None:
        return None

    grouped: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
    has_enumerated_segments = False

    for item in resolved.resolved_time_expressions:
        if item.source_id is None:
            continue
        has_enumerated_segments = True
        group = grouped.setdefault(
            item.source_id,
            {
                "source_text": item.source_text or item.text,
                "ranges": [],
            },
        )
        group["ranges"].append(_format_range(item.start_time, item.end_time))

    if not has_enumerated_segments:
        return None

    clauses = []
    for group in grouped.values():
        label = _extract_calendar_day_label(group["source_text"])
        clauses.append(f"{label}为{'、'.join(group['ranges'])}")

    prefix = "，其中"
    return f"{original_query}{prefix}{'；其中'.join(clauses)}"


def _rewrite_non_contiguous_discrete_days(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if resolved.metadata is None or resolved.metadata.rewrite_hints is None:
        return None

    rewritten = original_query
    changed = False
    for source_id, hint in resolved.metadata.rewrite_hints.items():
        if hint.topology != "discrete_set":
            continue
        if hint.member_grain != "day" or hint.is_contiguous or hint.preferred_rendering != "member_list":
            continue

        members = [item for item in resolved.resolved_time_expressions if item.source_id == source_id]
        if not members:
            continue
        source_text = members[0].source_text or members[0].text
        replacement = _build_time_axis_replacement(
            original_query=rewritten,
            source_text=source_text,
            expansion="、".join(_format_range(item.start_time, item.end_time) for item in members),
        )
        updated = rewritten.replace(source_text, replacement, 1)
        if updated == rewritten:
            return None
        rewritten = updated
        changed = True

    return rewritten if changed else None


def _rewrite_single_calendar_event_range(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if len(resolved.resolved_time_expressions) != 1:
        return None
    item = resolved.resolved_time_expressions[0]
    if item.source_id is not None:
        return None
    if item.text not in original_query:
        return None
    if "假期" not in item.text:
        return None
    replacement = _format_range(item.start_time, item.end_time)
    rewritten = original_query.replace(item.text, replacement, 1)
    if rewritten == original_query:
        return None
    return rewritten


def _rewrite_single_calendar_event_day(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if len(resolved.resolved_time_expressions) != 1:
        return None
    item = resolved.resolved_time_expressions[0]
    if item.source_id is not None:
        return None
    if item.text not in original_query:
        return None
    if not any(marker in item.text for marker in ("当天", "当日", "正日", "第一天", "最后一天")):
        return None
    replacement = _format_range(item.start_time, item.end_time)
    rewritten = original_query.replace(item.text, replacement, 1)
    if rewritten == original_query:
        return None
    return rewritten


def _is_holiday_range_candidate_text(text: str | None) -> bool:
    if not text or "假期" not in text:
        return False
    excluded_markers = ("工作日", "休息日", "节假日", "每天", "当天", "当日", "正日", "第一天", "最后一天", "周末")
    return not any(marker in text for marker in excluded_markers)


def _is_holiday_range_leaf(node: Any) -> bool:
    text = getattr(node, "source_text", None) or getattr(node, "text", None)
    if not _is_holiday_range_candidate_text(text):
        return False
    start_time = getattr(node, "start_time", None)
    end_time = getattr(node, "end_time", None)
    if start_time is None or end_time is None:
        return False
    return True


def _holiday_range_items_from_resolved(resolved: ResolvedTimeExpressions) -> list[Any]:
    return [
        item
        for item in resolved.resolved_time_expressions
        if _is_holiday_range_leaf(item)
    ]


def _render_holiday_range_leaves(leaves: list[Any]) -> str:
    return _join_rendered_members([_format_range(leaf.start_time, leaf.end_time) for leaf in leaves])


def _apply_holiday_result_shape(original_query: str, rewritten: str) -> str:
    if _is_explicit_breakdown_query(original_query) or _has_enumerative_holiday_intent(original_query):
        return _ensure_per_window_question(rewritten)
    return rewritten


def _rewrite_multi_root_holiday_ranges(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if "假期" not in original_query:
        return None
    if any(marker in original_query for marker in ("工作日", "休息日", "节假日", "每天", "当天", "当日", "正日", "第一天", "最后一天", "周末")):
        return None

    mapped_holiday_roots = [
        group
        for group in resolved.resolved_time_expression_groups
        if group.children and _is_holiday_range_candidate_text(group.text)
    ]
    if len(mapped_holiday_roots) == 1 and mapped_holiday_roots[0].text in original_query:
        leaves = [leaf for leaf in _collect_leaf_groups(mapped_holiday_roots[0]) if _is_holiday_range_leaf(leaf)]
        if len(leaves) >= 2:
            replacement = _build_time_axis_replacement(
                original_query=original_query,
                source_text=mapped_holiday_roots[0].text,
                expansion=_render_holiday_range_leaves(leaves),
            )
            rewritten = original_query.replace(mapped_holiday_roots[0].text, replacement, 1)
            if rewritten != original_query:
                return _apply_holiday_result_shape(original_query, rewritten)

    explicit_holiday_roots = [
        group
        for group in resolved.resolved_time_expression_groups
        if not group.children and _is_holiday_range_leaf(group)
    ]
    if len(explicit_holiday_roots) >= 2:
        match = MULTI_ROOT_HOLIDAY_PATTERN.search(original_query)
        if match is None:
            return None
        source_text = match.group(0)
        replacement = _build_time_axis_replacement(
            original_query=original_query,
            source_text=source_text,
            expansion=_render_holiday_range_leaves(explicit_holiday_roots),
        )
        rewritten = f"{original_query[:match.start()]}{replacement}{original_query[match.end():]}"
        if rewritten != original_query:
            return _apply_holiday_result_shape(original_query, rewritten)

    holiday_items = _holiday_range_items_from_resolved(resolved)
    if len(holiday_items) >= 2:
        source_ids = {item.source_id for item in holiday_items}
        source_texts = {
            item.source_text
            for item in holiday_items
            if item.source_id is not None and item.source_text is not None
        }
        if len(source_ids) == 1 and None not in source_ids and len(source_texts) == 1:
            source_text = next(iter(source_texts))
            if source_text in original_query:
                replacement = _build_time_axis_replacement(
                    original_query=original_query,
                    source_text=source_text,
                    expansion=_render_holiday_range_leaves(holiday_items),
                )
                rewritten = original_query.replace(source_text, replacement, 1)
                if rewritten != original_query:
                    return _apply_holiday_result_shape(original_query, rewritten)
        elif all(item.source_id is None for item in holiday_items):
            match = MULTI_ROOT_HOLIDAY_PATTERN.search(original_query)
            if match is None:
                return None
            source_text = match.group(0)
            replacement = _build_time_axis_replacement(
                original_query=original_query,
                source_text=source_text,
                expansion=_render_holiday_range_leaves(holiday_items),
            )
            rewritten = f"{original_query[:match.start()]}{replacement}{original_query[match.end():]}"
            if rewritten != original_query:
                return _apply_holiday_result_shape(original_query, rewritten)

    return None


def _rewrite_date_identification_query(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if not any(marker in original_query for marker in ("是哪天", "是哪两天", "是几号")):
        return None
    if not resolved.resolved_time_expressions:
        return None
    primary = resolved.resolved_time_expressions[0]
    if primary.source_id is not None:
        source_text = primary.source_text or primary.text
        members = [item for item in resolved.resolved_time_expressions if (item.source_id or item.id) == primary.source_id]
    else:
        source_text = primary.text
        members = resolved.resolved_time_expressions
    if not source_text:
        return None
    rendered = "和".join(_format_range(item.start_time, item.end_time) for item in members)
    return f"{source_text}对应的日期是{rendered}"


def _collect_leaf_groups(node: Any) -> list[Any]:
    if not getattr(node, "children", None):
        return [node]
    leaves: list[Any] = []
    for child in node.children:
        leaves.extend(_collect_leaf_groups(child))
    return leaves


def _rewrite_grouped_atomic_calendar_days(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if not resolved.resolved_time_expression_groups:
        return None
    if not _has_enumerative_calendar_day_intent(original_query):
        return None

    root = resolved.resolved_time_expression_groups[0]
    if not root.children:
        return None
    leaves = _collect_leaf_groups(root)
    labels = [_format_range(leaf.start_time, leaf.end_time) for leaf in leaves]
    replacement = _build_time_axis_replacement(
        original_query=original_query,
        source_text=root.text,
        expansion="、".join(labels),
    )
    rewritten = original_query.replace(root.text, replacement, 1)
    if rewritten == original_query:
        return None
    return _ensure_per_window_question(rewritten)


def _rewrite_multi_root_natural_period_query(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    match = MULTI_ROOT_NATURAL_PERIOD_PATTERN.search(original_query)
    if match is None:
        return None

    suffix = match.group("suffix")
    if suffix == "每年":
        year_groups = [group for group in resolved.resolved_time_expression_groups if not group.children]
        if len(year_groups) < 2:
            return None
        rendered_members: list[str] = []
        for group in year_groups:
            rendered = _render_natural_period_member(group.start_time, group.end_time, "year")
            if rendered is None:
                return None
            rendered_members.append(rendered)
    else:
        unit_by_suffix = {
            "每个月": "month",
            "每月": "month",
            "每个季度": "quarter",
            "每个半年": "half_year",
        }
        unit = unit_by_suffix[suffix]
        groups = [
            group
            for group in resolved.resolved_time_expression_groups
            if group.children and _detect_grouped_natural_period_unit(group.text) == unit
        ]
        if len(groups) < 2:
            return None

        rendered_members = []
        for group in groups:
            for leaf in _collect_leaf_groups(group):
                rendered = _render_natural_period_member(leaf.start_time, leaf.end_time, unit)
                if rendered is None:
                    return None
                rendered_members.append(rendered)

    source_text = match.group(0)
    replacement = _build_time_axis_replacement(
        original_query=original_query,
        source_text=source_text,
        expansion="、".join(rendered_members),
    )
    rewritten = f"{original_query[:match.start()]}{replacement}{original_query[match.end():]}"
    rewritten = _ensure_per_window_question(rewritten)
    return rewritten if rewritten != original_query else None


def _rewrite_grouped_natural_period_aggregate(
    *,
    original_query: str,
    resolved: ResolvedTimeExpressions,
) -> str | None:
    if len(resolved.resolved_time_expression_groups) != 1:
        return None
    root = resolved.resolved_time_expression_groups[0]
    if not root.children or root.text not in original_query:
        return None

    unit = _detect_grouped_natural_period_unit(root.text)
    if unit is None:
        return None

    leaves = _collect_leaf_groups(root)
    if not leaves:
        return None

    rendered_members: list[str] = []
    for leaf in leaves:
        rendered = _render_natural_period_member(leaf.start_time, leaf.end_time, unit)
        if rendered is None:
            return None
        rendered_members.append(rendered)

    replacement = _build_time_axis_replacement(
        original_query=original_query,
        source_text=root.text,
        expansion="、".join(rendered_members),
    )
    rewritten = original_query.replace(root.text, replacement, 1)
    if _is_explicit_breakdown_query(original_query) or _has_enumerative_natural_period_intent(original_query, unit):
        rewritten = _ensure_per_window_question(rewritten)
    if rewritten == original_query:
        return None
    return rewritten


class QueryRewriter:
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
                raise RuntimeError("QueryRewriter requires an injected llm or text_runner.")
            self._text_runner = self._llm
        return self._text_runner

    def rewrite_query_with_llm(
        self,
        *,
        original_query: str,
        resolved_time_expressions: dict[str, Any] | ResolvedTimeExpressions,
    ) -> str | None:
        resolved = ResolvedTimeExpressions.model_validate(resolved_time_expressions)
        if resolved.metadata is not None and resolved.metadata.no_match_results:
            return None
        if not resolved.resolved_time_expressions:
            return original_query

        enumerated_hour_rewrite = _rewrite_enumerated_hours(
            original_query=original_query,
            resolved=resolved,
        )
        if enumerated_hour_rewrite is not None:
            return enumerated_hour_rewrite

        date_identification_rewrite = _rewrite_date_identification_query(
            original_query=original_query,
            resolved=resolved,
        )
        if date_identification_rewrite is not None:
            return date_identification_rewrite

        holiday_single_day_rewrite = _rewrite_single_calendar_event_day(
            original_query=original_query,
            resolved=resolved,
        )
        if holiday_single_day_rewrite is not None:
            return holiday_single_day_rewrite

        holiday_single_range_rewrite = _rewrite_single_calendar_event_range(
            original_query=original_query,
            resolved=resolved,
        )
        if holiday_single_range_rewrite is not None:
            return holiday_single_range_rewrite

        multi_root_holiday_rewrite = _rewrite_multi_root_holiday_ranges(
            original_query=original_query,
            resolved=resolved,
        )
        if multi_root_holiday_rewrite is not None:
            return multi_root_holiday_rewrite

        grouped_atomic_rewrite = _rewrite_grouped_atomic_calendar_days(
            original_query=original_query,
            resolved=resolved,
        )
        if grouped_atomic_rewrite is not None:
            return grouped_atomic_rewrite

        discrete_day_rewrite = _rewrite_non_contiguous_discrete_days(
            original_query=original_query,
            resolved=resolved,
        )
        if discrete_day_rewrite is not None:
            return discrete_day_rewrite

        multi_root_natural_period_rewrite = _rewrite_multi_root_natural_period_query(
            original_query=original_query,
            resolved=resolved,
        )
        if multi_root_natural_period_rewrite is not None:
            return multi_root_natural_period_rewrite

        grouped_natural_period_aggregate_rewrite = _rewrite_grouped_natural_period_aggregate(
            original_query=original_query,
            resolved=resolved,
        )
        if grouped_natural_period_aggregate_rewrite is not None:
            return grouped_natural_period_aggregate_rewrite

        enumerated_rewrite = _rewrite_enumerated_calendar_days(
            original_query=original_query,
            resolved=resolved,
        )
        if enumerated_rewrite is not None:
            return enumerated_rewrite

        messages = [
            SystemMessage(content=REWRITER_SYSTEM_PROMPT),
            HumanMessage(content=build_rewriter_user_prompt(original_query, resolved)),
        ]
        result = self._get_text_runner().invoke(messages)
        return self._coerce_text(result).strip()

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


def rewrite_query_with_llm(
    original_query: str,
    resolved_time_expressions: dict[str, Any] | ResolvedTimeExpressions,
) -> str | None:
    from time_query_service.service import QueryPipelineService

    service = QueryPipelineService()
    return service.rewrite_query(
        original_query=original_query,
        resolved_time_expressions=resolved_time_expressions,
    )
