from pathlib import Path

from time_query_service.llm import load_llm_runtime_config


def test_load_llm_runtime_config_reads_role_map(tmp_path: Path):
    config_path = tmp_path / "llm.yaml"
    config_path.write_text(
        "\n".join(
            [
                "default_role: planner",
                "roles:",
                "  planner:",
                "    model_type: openai",
                "    model_name: gpt-test",
                "    api_key: test-key",
            ]
        ),
        encoding="utf-8",
    )

    runtime_config = load_llm_runtime_config(config_path=config_path)

    assert runtime_config.default_role == "planner"
    assert runtime_config.roles["planner"].model_name == "gpt-test"
