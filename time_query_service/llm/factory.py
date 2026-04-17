from __future__ import annotations

from functools import lru_cache
from typing import Any, Callable

import httpx
from langchain_openai import AzureChatOpenAI

from time_query_service.llm.config import LLMConfig
from time_query_service.llm.openai import BaseChatOpenAI


LLMBuilder = Callable[[LLMConfig], Any]


def _build_http_client(config: LLMConfig) -> httpx.Client | None:
    if not config.proxy_url:
        return None
    return httpx.Client(proxy=config.proxy_url, verify=config.verify_ssl)


def _build_openai_compatible_llm(config: LLMConfig) -> Any:
    kwargs: dict[str, Any] = {
        "model": config.model_name,
        "api_key": config.api_key or "Empty",
    }
    if config.api_base_url:
        kwargs["base_url"] = config.api_base_url
    http_client = _build_http_client(config)
    if http_client is not None:
        kwargs["http_client"] = http_client
    kwargs.update(config.additional_params)
    return BaseChatOpenAI(**kwargs)


def _build_azure_llm(config: LLMConfig) -> Any:
    additional_params = dict(config.additional_params)
    api_version = additional_params.pop("api_version", None)
    deployment_name = additional_params.pop("deployment_name", None)
    kwargs: dict[str, Any] = {
        "azure_endpoint": config.api_base_url,
        "api_key": config.api_key or "Empty",
        "model_name": config.model_name,
        "api_version": api_version,
        "deployment_name": deployment_name,
        **additional_params,
    }
    http_client = _build_http_client(config)
    if http_client is not None:
        kwargs["http_client"] = http_client
    return AzureChatOpenAI(
        **kwargs,
    )


def _build_vllm_llm(config: LLMConfig) -> Any:
    from langchain_community.llms import VLLMOpenAI

    return VLLMOpenAI(
        openai_api_key=config.api_key or "Empty",
        openai_api_base=config.api_base_url,
        model_name=config.model_name,
        streaming=True,
        **config.additional_params,
    )


class LLMFactory:
    _llm_types: dict[str, LLMBuilder] = {
        "openai": _build_openai_compatible_llm,
        "tongyi": _build_openai_compatible_llm,
        "azure": _build_azure_llm,
        "vllm": _build_vllm_llm,
    }

    @classmethod
    @lru_cache(maxsize=32)
    def create_llm(cls, config: LLMConfig) -> Any:
        llm_builder = cls._llm_types.get(config.model_type.lower())
        if llm_builder is None:
            raise ValueError(f"Unsupported LLM type: {config.model_type}")
        return llm_builder(config)

    @classmethod
    def register_llm(cls, model_type: str, llm_builder: LLMBuilder) -> None:
        cls._llm_types[model_type.lower()] = llm_builder
        cls.create_llm.cache_clear()
