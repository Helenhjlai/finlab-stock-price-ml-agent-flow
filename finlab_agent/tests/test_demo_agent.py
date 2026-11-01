from finlab_agent.agent import build_agent
from finlab_agent.config import Settings
from finlab_agent.service import ask_agent


def test_demo_mode_runs_without_model_and_returns_plan(price_repository):
    settings = Settings(data_backend="sample", model_backend="demo")
    agent = build_agent(settings, price_repository)
    result = ask_agent(
        agent,
        "請告訴我 2330 在 2024-05-20 的收盤價、MA20、MA60 與價格動能。",
    )
    assert result.planning_mode == "deterministic_demo"
    assert result.capabilities_used == ["market.indicators/v1"]
    assert result.research_plan["steps"][0]["step_id"] == "step_1"
    assert "未呼叫任何 LLM" in result.answer


def test_demo_mode_can_execute_multiple_capabilities(price_repository):
    settings = Settings(data_backend="sample", model_backend="demo")
    agent = build_agent(settings, price_repository)
    result = ask_agent(
        agent,
        "先查 2330 截至 2024-05-20 的均線與動能，再用 tw_ma20_60 "
        "回測 2024-01-02 到 2024-05-20，綜合說明技術狀態與策略風險。",
    )
    assert result.capabilities_used == [
        "market.indicators/v1",
        "strategy.backtest/v1",
    ]
    assert len(result.research_plan["steps"]) == 2
