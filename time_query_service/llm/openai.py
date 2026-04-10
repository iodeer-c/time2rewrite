from collections.abc import Iterator, Mapping
from typing import Any, cast

from langchain_core.language_models import LanguageModelInput
from langchain_core.messages import (
    AIMessageChunk,
    BaseMessage,
    BaseMessageChunk,
    ChatMessageChunk,
    FunctionMessageChunk,
    HumanMessageChunk,
    SystemMessageChunk,
)
from langchain_core.messages.ai import UsageMetadata
from langchain_core.messages.tool import ToolMessageChunk, tool_call_chunk
from langchain_core.outputs import ChatGenerationChunk
from langchain_core.outputs.chat_generation import ChatGeneration
from langchain_core.runnables import RunnableConfig, ensure_config
from langchain_openai import ChatOpenAI
from langchain_openai.chat_models.base import _create_usage_metadata


def _convert_delta_to_message_chunk(
    payload: Mapping[str, Any],
    default_class: type[BaseMessageChunk],
) -> BaseMessageChunk:
    message_id = payload.get("id")
    role = cast(str, payload.get("role"))
    content = cast(str, payload.get("content") or "")
    additional_kwargs: dict[str, Any] = {}
    if "reasoning_content" in payload:
        additional_kwargs["reasoning_content"] = payload.get("reasoning_content")
    if payload.get("function_call"):
        function_call = dict(payload["function_call"])
        if "name" in function_call and function_call["name"] is None:
            function_call["name"] = ""
        additional_kwargs["function_call"] = function_call
    tool_call_chunks = []
    if raw_tool_calls := payload.get("tool_calls"):
        additional_kwargs["tool_calls"] = raw_tool_calls
        try:
            tool_call_chunks = [
                tool_call_chunk(
                    name=tool_call["function"].get("name"),
                    args=tool_call["function"].get("arguments"),
                    id=tool_call.get("id"),
                    index=tool_call["index"],
                )
                for tool_call in raw_tool_calls
            ]
        except KeyError:
            pass

    if role == "user" or default_class == HumanMessageChunk:
        return HumanMessageChunk(content=content, id=message_id)
    if role == "assistant" or default_class == AIMessageChunk:
        return AIMessageChunk(
            content=content,
            additional_kwargs=additional_kwargs,
            id=message_id,
            tool_call_chunks=tool_call_chunks,
        )
    if role in ("system", "developer") or default_class == SystemMessageChunk:
        if role == "developer":
            additional_kwargs = {"__openai_role__": "developer"}
        else:
            additional_kwargs = {}
        return SystemMessageChunk(content=content, id=message_id, additional_kwargs=additional_kwargs)
    if role == "function" or default_class == FunctionMessageChunk:
        return FunctionMessageChunk(content=content, name=payload["name"], id=message_id)
    if role == "tool" or default_class == ToolMessageChunk:
        return ToolMessageChunk(content=content, tool_call_id=payload["tool_call_id"], id=message_id)
    if role or default_class == ChatMessageChunk:
        return ChatMessageChunk(content=content, role=role, id=message_id)
    return default_class(content=content, id=message_id)


class BaseChatOpenAI(ChatOpenAI):
    usage_metadata: dict[str, Any] = {}

    def get_last_generation_info(self) -> dict[str, Any] | None:
        return self.usage_metadata

    def _stream(self, *args: Any, **kwargs: Any) -> Iterator[ChatGenerationChunk]:
        kwargs["stream_usage"] = True
        for chunk in super()._stream(*args, **kwargs):
            if chunk.message.usage_metadata is not None:
                self.usage_metadata = chunk.message.usage_metadata
            yield chunk

    def _convert_chunk_to_generation_chunk(
        self,
        chunk: dict[str, Any],
        default_chunk_class: type[BaseMessageChunk],
        base_generation_info: dict[str, Any] | None,
    ) -> ChatGenerationChunk | None:
        if chunk.get("type") == "content.delta":
            return None
        token_usage = chunk.get("usage")
        choices = chunk.get("choices", []) or chunk.get("chunk", {}).get("choices", [])

        usage_metadata: UsageMetadata | None = (
            _create_usage_metadata(token_usage)
            if token_usage and token_usage.get("prompt_tokens")
            else None
        )
        if len(choices) == 0:
            return ChatGenerationChunk(
                message=default_chunk_class(content="", usage_metadata=usage_metadata)
            )

        choice = choices[0]
        if choice["delta"] is None:
            return None

        message_chunk = _convert_delta_to_message_chunk(choice["delta"], default_chunk_class)
        generation_info = {**base_generation_info} if base_generation_info else {}

        if finish_reason := choice.get("finish_reason"):
            generation_info["finish_reason"] = finish_reason
            if model_name := chunk.get("model"):
                generation_info["model_name"] = model_name
            if system_fingerprint := chunk.get("system_fingerprint"):
                generation_info["system_fingerprint"] = system_fingerprint

        logprobs = choice.get("logprobs")
        if logprobs:
            generation_info["logprobs"] = logprobs

        if usage_metadata and isinstance(message_chunk, AIMessageChunk):
            message_chunk.usage_metadata = usage_metadata

        return ChatGenerationChunk(message=message_chunk, generation_info=generation_info or None)

    def invoke(
        self,
        input: LanguageModelInput,
        config: RunnableConfig | None = None,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> BaseMessage:
        config = ensure_config(config)
        chat_result = cast(
            ChatGeneration,
            self.generate_prompt(
                [self._convert_input(input)],
                stop=stop,
                callbacks=config.get("callbacks"),
                tags=config.get("tags"),
                metadata=config.get("metadata"),
                run_name=config.get("run_name"),
                run_id=config.pop("run_id", None),
                **kwargs,
            ).generations[0][0],
        ).message

        self.usage_metadata = (
            chat_result.response_metadata["token_usage"]
            if "token_usage" in chat_result.response_metadata
            else chat_result.usage_metadata
        )
        return chat_result
