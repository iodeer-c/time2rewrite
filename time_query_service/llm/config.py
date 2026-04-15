from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from time_query_service.config import PROJECT_ROOT, load_project_dotenv


DEFAULT_LLM_CONFIG_PATH = PROJECT_ROOT / "config" / "llm.yaml"


def _to_hashable(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((key, _to_hashable(item)) for key, item in value.items()))
    if isinstance(value, list):
        return tuple(_to_hashable(item) for item in value)
    return value


class LLMConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    model_type: str
    model_name: str
    api_key: str | None = None
    api_key_env: str | None = None
    api_base_url: str | None = None
    proxy_url: str | None = None
    proxy_url_env: str | None = None
    verify_ssl: bool = True
    additional_params: dict[str, Any] = Field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(
            (
                self.model_type.lower(),
                self.model_name,
                self.api_key,
                self.api_key_env,
                self.api_base_url,
                self.proxy_url,
                self.proxy_url_env,
                self.verify_ssl,
                _to_hashable(self.additional_params),
            )
        )


class LLMRuntimeConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    default_role: str
    roles: dict[str, LLMConfig]

    @model_validator(mode="after")
    def validate_default_role(self) -> "LLMRuntimeConfig":
        if self.default_role not in self.roles:
            raise ValueError(f"default_role={self.default_role} is not present in roles")
        return self

    def get_role_config(self, role: str | None = None) -> LLMConfig:
        resolved_role = role or self.default_role
        config = self.roles.get(resolved_role)
        if config is None:
            raise RuntimeError(f"Missing LLM configuration for role={resolved_role}")
        return config


def load_llm_runtime_config(
    *,
    config_path: Path | None = None,
    dotenv_path: Path | None = None,
) -> LLMRuntimeConfig:
    load_project_dotenv(dotenv_path=dotenv_path)
    path = config_path or DEFAULT_LLM_CONFIG_PATH
    if not path.exists():
        raise RuntimeError(f"Missing LLM config file: {path}")

    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    runtime_config = LLMRuntimeConfig.model_validate(payload)

    resolved_roles: dict[str, LLMConfig] = {}
    for role, config in runtime_config.roles.items():
        resolved_key = _resolve_api_key(config)
        if not resolved_key:
            raise RuntimeError(f"Missing API key for role={role}")
        resolved_roles[role] = config.model_copy(
            update={
                "api_key": resolved_key,
                "proxy_url": _resolve_proxy_url(config),
            }
        )

    return runtime_config.model_copy(update={"roles": resolved_roles})


def _resolve_api_key(config: LLMConfig) -> str | None:
    if config.api_key_env:
        api_key = os.getenv(config.api_key_env)
        if api_key:
            return api_key
    if config.api_key:
        return config.api_key
    return None


def _resolve_proxy_url(config: LLMConfig) -> str | None:
    if config.proxy_url_env:
        proxy_url = os.getenv(config.proxy_url_env)
        if proxy_url:
            return proxy_url
    if config.proxy_url:
        return config.proxy_url
    return None
