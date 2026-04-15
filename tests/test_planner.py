import json

from time_query_service.plan_validator import validate_plan
from time_query_service.planner import ClarificationPlanner


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
