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


def test_annotate_query_uses_ordinal_for_repeated_render_text():
    rewritten = annotate_query(
        original_query="昨天和昨天的收益分别是多少？",
        clarification_items=[
            {
                "node_id": "n2",
                "render_text": "昨天",
                "ordinal": 2,
                "display_exact_time": "2026年4月14日",
            }
        ],
        comparison_groups=[],
    )

    assert rewritten == "昨天和昨天（2026年4月14日）的收益分别是多少？"


def test_annotate_query_can_fallback_to_surface_fragments_for_non_contiguous_node():
    rewritten = annotate_query(
        original_query="今年杭千公司每个工作日的收益是多少？",
        clarification_items=[
            {
                "node_id": "n1",
                "render_text": "今年每个工作日",
                "ordinal": 1,
                "surface_fragments": ["今年", "每个工作日"],
                "display_exact_time": "2026年1月1日至2026年12月31日内的工作日",
            }
        ],
        comparison_groups=[],
    )

    assert rewritten == "今年（2026年1月1日至2026年12月31日内的工作日）杭千公司每个工作日的收益是多少？"
