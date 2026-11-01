from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import AIMessage, ToolMessage


@dataclass(frozen=True)
class ChatResult:
    answer: str
    tools_used: list[str] = field(default_factory=list)
    capabilities_used: list[str] = field(default_factory=list)
    research_plan: dict[str, Any] | None = None
    planning_mode: str = "unknown"


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") in {"text", "output_text"}:
                parts.append(str(block.get("text", "")))
        return "\n".join(part for part in parts if part)
    return str(content)


def _tool_payload(content: Any) -> dict[str, Any] | None:
    if isinstance(content, Mapping):
        return dict(content)
    if isinstance(content, str):
        try:
            parsed = json.loads(content)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def ask_agent(
    agent,
    question: str,
    history: list[dict[str, str]] | None = None,
) -> ChatResult:
    messages = list(history or [])[-12:]
    messages.append({"role": "user", "content": question})
    state = agent.invoke({"messages": messages})

    tools_used: list[str] = []
    capabilities_used: list[str] = []
    answer = ""
    for message in state.get("messages", []):
        if isinstance(message, ToolMessage):
            tools_used.append(message.name or "unknown_tool")
            payload = _tool_payload(message.content)
            if payload and payload.get("capability_id"):
                capabilities_used.append(str(payload["capability_id"]))
        if isinstance(message, AIMessage) and message.content:
            answer = _content_to_text(message.content)
    if not answer:
        raise RuntimeError("Agent 未產生文字回答")
    return ChatResult(
        answer=answer,
        tools_used=list(dict.fromkeys(tools_used)),
        capabilities_used=list(dict.fromkeys(capabilities_used)),
        research_plan=state.get("research_plan"),
        planning_mode=str(state.get("planning_mode", "unknown")),
    )

