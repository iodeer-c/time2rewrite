from time_query_service.contracts import ClarificationPlan


def test_contract_accepts_reference_window_node():
    plan = ClarificationPlan.model_validate(
        {
            "nodes": [
                {
                    "node_id": "n1",
                    "render_text": "去年同期",
                    "ordinal": 1,
                    "needs_clarification": True,
                    "node_kind": "reference_window",
                    "reason_code": "same_period_reference",
                    "resolution_spec": {
                        "reference_node_id": "n0",
                        "alignment": "same_period",
                        "shift": {"unit": "year", "value": -1},
                    },
                }
            ],
            "comparison_groups": [],
        }
    )

    assert plan.nodes[0].node_kind == "reference_window"
    assert plan.nodes[0].resolution_spec.reference_node_id == "n0"
