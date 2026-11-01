from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from .capabilities import CapabilityRegistry
from .schemas import ResearchPlan, ResearchStep


_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_STOCK_ID = re.compile(r"(?<![\d-])(\d{4})(?![\d-])")
_STRATEGY = re.compile(r"tw_(?:ma20_60|momentum_top2)")


def _latest_question(messages: list[Any]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            return str(message.content)
        if isinstance(message, dict) and message.get("role") == "user":
            return str(message.get("content", ""))
    raise ValueError("找不到使用者問題")


def _pretty_percent(value: Any) -> str:
    return "—" if value is None else f"{float(value):.2%}"


@dataclass
class DemoResearchAgent:
    """Deterministic UI demo that never calls an LLM."""

    registry: CapabilityRegistry

    def _run_capability(
        self, query: str, capability_id: str, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        discovery = self.registry.search(query, max_results=3)
        if capability_id not in {
            item["capability_id"] for item in discovery["matches"]
        }:
            # Exact capability metadata is still selected through a fresh catalog search.
            discovery = self.registry.search(capability_id, max_results=3)
        return self.registry.execute(
            discovery["capability_token"], capability_id, arguments
        )

    def invoke(self, state: dict[str, Any]) -> dict[str, Any]:
        messages = list(state.get("messages", []))
        question = _latest_question(messages)
        dates = _DATE.findall(question)
        stock_ids = list(dict.fromkeys(_STOCK_ID.findall(question)))
        steps: list[ResearchStep] = []
        executions: list[dict[str, Any]] = []

        wants_screen = any(term in question for term in ("篩選", "找出", "選股", "營收年增"))
        wants_backtest = "回測" in question or bool(_STRATEGY.search(question))
        wants_indicators = any(
            term in question for term in ("收盤價", "MA20", "MA60", "均線", "動能", "技術")
        ) and not wants_screen

        if wants_indicators and stock_ids and dates:
            steps.append(
                ResearchStep(
                    step_id=f"step_{len(steps) + 1}",
                    question="取得單一股票的價格與技術指標",
                    capability_query="股票收盤價、均線與價格動能",
                    required_inputs=["stock_id", "as_of"],
                )
            )
            executions.append(
                self._run_capability(
                    "股票收盤價、均線與價格動能",
                    "market.indicators/v1",
                    {"stock_id": stock_ids[0], "as_of": dates[-1]},
                )
            )

        if wants_screen and dates:
            rules: list[dict[str, Any]] = []
            if "營收" in question:
                rules.append({"field": "revenue_yoy", "operator": ">", "value": 0.15})
            if "MA60" in question or "60 日均線" in question:
                rules.append(
                    {"field": "price_above_ma60", "operator": "==", "value": True}
                )
            if not rules:
                rules.append(
                    {"field": "price_above_ma60", "operator": "==", "value": True}
                )
            steps.append(
                ResearchStep(
                    step_id=f"step_{len(steps) + 1}",
                    question="依允許的條件篩選股票",
                    capability_query="多條件股票篩選與排序",
                    required_inputs=["rules", "as_of"],
                )
            )
            executions.append(
                self._run_capability(
                    "多條件股票篩選與排序",
                    "market.screen/v1",
                    {
                        "rules": rules,
                        "stock_ids": stock_ids or None,
                        "as_of": dates[-1],
                        "sort_by": "revenue_yoy" if "營收" in question else "stock_id",
                        "sort_order": "desc" if "營收" in question else "asc",
                        "limit": 20,
                    },
                )
            )

        if wants_backtest and len(dates) >= 2:
            strategy_match = _STRATEGY.search(question)
            strategy_id = strategy_match.group(0) if strategy_match else "tw_ma20_60"
            steps.append(
                ResearchStep(
                    step_id=f"step_{len(steps) + 1}",
                    question="評估白名單策略的歷史績效與風險",
                    capability_query="策略回測、報酬、最大回撤與 Sharpe",
                    required_inputs=["strategy_id", "start_date", "end_date"],
                )
            )
            executions.append(
                self._run_capability(
                    "策略回測、報酬、最大回撤與 Sharpe",
                    "strategy.backtest/v1",
                    {
                        "strategy_id": strategy_id,
                        "start_date": dates[-2],
                        "end_date": dates[-1],
                    },
                )
            )

        if not steps:
            plan = ResearchPlan(
                goal="展示目前可用的研究能力",
                steps=[
                    ResearchStep(
                        step_id="step_1",
                        question="需要哪一種示範？",
                        capability_query="列出示範研究能力",
                        required_inputs=[],
                    )
                ],
                needs_clarification=True,
                clarification_question="請點選左側 Demo 範例，或輸入股票代號、日期及想做的指標／篩選／回測。",
            )
            return {
                "messages": [*messages, AIMessage(content=plan.clarification_question)],
                "research_plan": plan.model_dump(mode="json"),
                "planning_mode": "deterministic_demo",
            }

        plan = ResearchPlan(goal=question, steps=steps)
        sections = ["這是 **Demo 模式** 結果：未呼叫任何 LLM。"]
        tool_messages: list[ToolMessage] = []
        for index, execution in enumerate(executions, start=1):
            result = execution["result"]
            capability_id = execution["capability_id"]
            data = result["data"]
            if capability_id == "market.indicators/v1":
                indicator = data["indicator"]
                sections.append(
                    "\n".join(
                        [
                            f"### {data['stock_id']} 技術指標",
                            f"- 資料日：{indicator['date']}",
                            f"- 收盤價：{indicator['close']:.2f}",
                            f"- MA20／MA60：{indicator['ma20']:.2f}／{indicator['ma60']:.2f}",
                            f"- 20 日動能：{_pretty_percent(indicator['momentum_20d'])}",
                        ]
                    )
                )
            elif capability_id == "market.screen/v1":
                sections.append(
                    f"### 股票篩選\n符合數量：{data['match_count']}\n\n"
                    + "```json\n"
                    + json.dumps(data["matches"], ensure_ascii=False, indent=2)
                    + "\n```"
                )
            else:
                metrics = data["metrics"]
                sections.append(
                    "\n".join(
                        [
                            f"### {data['strategy_id']} 回測",
                            f"- 累積報酬：{_pretty_percent(metrics.get('cumulative_return'))}",
                            f"- 年化報酬：{_pretty_percent(metrics.get('annual_return'))}",
                            f"- 最大回撤：{_pretty_percent(metrics.get('max_drawdown'))}",
                            f"- Sharpe：{metrics.get('sharpe_ratio')}",
                            f"- 交易數：{metrics.get('trade_count')}",
                        ]
                    )
                )
            if result.get("warnings"):
                sections.append("限制：" + "；".join(result["warnings"]))
            tool_messages.append(
                ToolMessage(
                    content=json.dumps(execution, ensure_ascii=False, default=str),
                    name="execute_capability",
                    tool_call_id=f"demo-{index}",
                )
            )

        return {
            "messages": [*messages, *tool_messages, AIMessage(content="\n\n".join(sections))],
            "research_plan": plan.model_dump(mode="json"),
            "planning_mode": "deterministic_demo",
        }
