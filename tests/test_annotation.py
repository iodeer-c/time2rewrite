from time_query_service.annotation import annotate_query


def test_annotate_query_keeps_non_time_text_unchanged():
    rewritten = annotate_query(
        original_query="昨天杭千公司的收益是多少？",
        clarification_items=[
            {
                "node_id": "n1",
                "render_text": "昨天",
                "ordinal": 1,
                "display_exact_time": "2026年4月14日",
            }
        ],
        comparison_groups=[],
    )

    assert rewritten == "昨天（2026年4月14日）杭千公司的收益是多少？"


def test_annotate_query_returns_original_query_when_no_items():
    assert (
        annotate_query(
            original_query="2025年杭千公司每天的收益是多少？",
            clarification_items=[],
            comparison_groups=[],
        )
        == "2025年杭千公司每天的收益是多少？"
    )
