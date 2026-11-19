"""ChatModel on the Claude API, through the official Anthropic SDK."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import anthropic

from datalake.agent.errors import ModelError, ModelRefusedError
from datalake.agent.model import ChatMessage

# If the main model declines a request on safety grounds, the API re-runs it on a
# fallback model inside the same call. Without this a declined request simply stops.
_FALLBACK_BETA = "server-side-fallback-2026-07-01"


class AnthropicChatModel:
    """Keeps running totals of tokens used, so a run can record what it cost."""

    def __init__(
        self,
        client: anthropic.AsyncAnthropic,
        model: str = "claude-opus-5-5",
        effort: str = "medium",
        max_tokens: int = 16000,
    ) -> None:
        self._client = client
        self._model = model
        self._effort = effort
        self._max_tokens = max_tokens
        self.input_tokens = 0
        self.output_tokens = 0

    async def complete(self, messages: Sequence[ChatMessage]) -> str:
        # The instructions do not change between questions, so they are cached.
        system: list[dict[str, Any]] = [
            {"type": "text", "text": m.content, "cache_control": {"type": "ephemeral"}}
            for m in messages
            if m.role == "system"
        ]
        turns = [{"role": m.role, "content": m.content} for m in messages if m.role != "system"]
        try:
            response = await self._client.beta.messages.create(
                model=self._model,
                max_tokens=self._max_tokens,
                system=system,  # pyright: ignore[reportArgumentType]
                messages=turns,  # pyright: ignore[reportArgumentType]
                output_config={"effort": self._effort},  # pyright: ignore[reportArgumentType]
                betas=[_FALLBACK_BETA],
                fallbacks="default",
            )
        except anthropic.AuthenticationError as exc:
            raise ModelError("the model rejected the API key") from exc
        except anthropic.RateLimitError as exc:
            raise ModelError("the model is rate limited; try again shortly") from exc
        except anthropic.APIStatusError as exc:
            raise ModelError(
                f"the model returned an error ({exc.status_code}): {exc.message}"
            ) from exc
        except anthropic.APIConnectionError as exc:
            raise ModelError("the model could not be reached") from exc

        self.input_tokens += response.usage.input_tokens
        self.output_tokens += response.usage.output_tokens
        if response.stop_reason == "refusal":
            raise ModelRefusedError("the model declined to answer this request")
        text = "".join(block.text for block in response.content if block.type == "text")
        if response.stop_reason == "max_tokens":
            raise ModelError("the model's reply was cut off before it finished")
        if not text.strip():
            raise ModelError("the model returned no text")
        return text
