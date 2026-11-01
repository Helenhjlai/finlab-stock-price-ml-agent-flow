import json
import os
from pathlib import Path

import pytest

from finlab_agent.agent import build_agent
from finlab_agent.config import Settings
from finlab_agent.service import ask_agent


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_AGENT_EVAL", "false").lower() != "true",
    reason="設定 RUN_LIVE_AGENT_EVAL=true 才會呼叫 Databricks 模型端點",
)


def test_live_agent_plans_discovers_and_executes_expected_capabilities(price_repository):
    settings = Settings.from_env()
    agent = build_agent(settings, price_repository)
    case_path = Path(__file__).parent / "fixtures" / "tool_routing.jsonl"
    cases = [json.loads(line) for line in case_path.read_text(encoding="utf-8").splitlines()]
    failures = []
    for case in cases:
        result = ask_agent(agent, case["question"])
        missing = [
            item
            for item in case["expected_capabilities"]
            if item not in result.capabilities_used
        ]
        step_count = len((result.research_plan or {}).get("steps", []))
        if missing or step_count < case["minimum_plan_steps"]:
            failures.append(
                {
                    "question": case["question"],
                    "expected": case["expected_capabilities"],
                    "actual": result.capabilities_used,
                    "planning_mode": result.planning_mode,
                    "plan_steps": step_count,
                    "gateway_tools": result.tools_used,
                }
            )
    assert failures == []

