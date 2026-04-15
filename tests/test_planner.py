import json
import re

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from time_query_service.contracts import ClarificationPlan
from time_query_service.plan_validator import validate_plan
from time_query_service.planner import ClarificationPlanner
from time_query_service.planner_prompt import PLANNER_FEW_SHOTS, PLANNER_SYSTEM_PROMPT, build_planner_messages


class FakeResponse:
    def __init__(self, content: str) -> None:
        self.content = content


class FakeSequenceRunner:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[list[object]] = []

    def invoke(self, messages: list[object]) -> FakeResponse:
        self.calls.append(messages)
        if not self._responses:
            raise AssertionError("No more fake responses available.")
        return self._responses.pop(0)


def test_validate_plan_rejects_missing_comparison_member_node():
    result = validate_plan(
        {
            "nodes": [],
            "comparison_groups": [
                {
                    "group_id": "g1",
                    "relation_type": "generic_compare",
                    "anchor_text": "相比",
                    "anchor_ordinal": 1,
                    "direction": "subject_to_reference",
                    "members": [{"node_id": "missing", "role": "reference"}],
                }
            ],
        }
    )

    assert result.is_valid is False
    assert any("missing" in error for error in result.errors)


def test_planner_system_prompt_is_chinese_and_contains_core_constraints():
    assert "时间澄清规划器" in PLANNER_SYSTEM_PROMPT
    assert "只输出一个 JSON object" in PLANNER_SYSTEM_PROMPT
    assert "render_text" in PLANNER_SYSTEM_PROMPT
    assert "comparison_groups" in PLANNER_SYSTEM_PROMPT
    assert "reference_window" in PLANNER_SYSTEM_PROMPT
    assert "window_with_regular_grain" in PLANNER_SYSTEM_PROMPT
    assert re.search(r"[\u4e00-\u9fff]{20,}", PLANNER_SYSTEM_PROMPT)


def test_build_planner_messages_includes_few_shot_pairs_before_request_payload():
    assert PLANNER_FEW_SHOTS, "Expected at least one few-shot example."

    messages = build_planner_messages(
        original_query="昨天杭千公司的收益是多少？",
        system_date="2026-04-15",
        system_datetime="2026-04-15 09:30:00",
        timezone="Asia/Shanghai",
    )

    assert isinstance(messages[0], SystemMessage)
    assert isinstance(messages[-1], HumanMessage)

    expected_message_count = 1 + (2 * len(PLANNER_FEW_SHOTS)) + 1
    assert len(messages) == expected_message_count

    for index, shot in enumerate(PLANNER_FEW_SHOTS):
        human_message = messages[1 + (index * 2)]
        ai_message = messages[2 + (index * 2)]
        assert isinstance(human_message, HumanMessage)
        assert isinstance(ai_message, AIMessage)

        assert json.loads(human_message.content) == shot["input"]
        assert json.loads(ai_message.content) == shot["output"]

    payload = json.loads(messages[-1].content)
    assert payload["original_query"] == "昨天杭千公司的收益是多少？"
    assert payload["system_date"] == "2026-04-15"
    assert payload["system_datetime"] == "2026-04-15 09:30:00"
    assert payload["timezone"] == "Asia/Shanghai"


def test_planner_few_shots_have_valid_clarification_plan_outputs():
    for shot in PLANNER_FEW_SHOTS:
        ClarificationPlan.model_validate(shot["output"])


def test_planner_retries_once_when_first_plan_is_invalid():
    runner = FakeSequenceRunner(
        [
            FakeResponse(
                json.dumps(
                    {
                        "nodes": [],
                        "comparison_groups": [
                            {
                                "group_id": "g1",
                                "relation_type": "generic_compare",
                                "anchor_text": "相比",
                                "anchor_ordinal": 1,
                                "direction": "subject_to_reference",
                                "members": [{"node_id": "missing", "role": "reference"}],
                            }
                        ],
                    }
                )
            ),
            FakeResponse(json.dumps({"nodes": [], "comparison_groups": []})),
        ]
    )

    planner = ClarificationPlanner(text_runner=runner)

    plan = planner.plan_query(
        original_query="今年3月和去年同期相比收益增长了多少？",
        system_date="2026-04-15",
        system_datetime="2026-04-15 09:30:00",
        timezone="Asia/Shanghai",
    )

    assert plan.nodes == []
    assert plan.comparison_groups == []
    assert len(runner.calls) == 2
