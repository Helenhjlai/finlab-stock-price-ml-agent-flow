import pytest
from pydantic import ValidationError

from finlab_agent.schemas import RunStrategyBacktestInput
from finlab_agent.tools import create_finlab_tools


def _by_name(tools):
    return {item.name: item for item in tools}


def test_indicator_tool_returns_validated_provenance(price_repository):
    tools = _by_name(create_finlab_tools(price_repository))
    result = tools["get_stock_indicators"].invoke(
        {
            "stock_id": "2330",
            "as_of": "2024-05-20",
        }
    )
    assert result["status"] == "ok"
    assert result["tool"] == "get_stock_indicators"
    assert result["provenance"]["source"] == "sample_csv"
    assert set(result["data"]["indicator"]) == {
        "date",
        "close",
        "ma20",
        "ma60",
        "momentum_20d",
    }


def test_backtest_tool_always_contains_required_risk_disclosures(price_repository):
    tools = _by_name(create_finlab_tools(price_repository))
    result = tools["run_strategy_backtest"].invoke(
        {
            "strategy_id": "tw_ma20_60",
            "start_date": "2024-01-02",
            "end_date": "2024-05-20",
        }
    )
    warnings = " ".join(result["warnings"])
    for phrase in ("投資建議", "前視偏誤", "交易成本", "存活者偏誤"):
        assert phrase in warnings


def test_backtest_rejects_non_whitelisted_strategy_or_code():
    with pytest.raises(ValidationError, match="strategy_id"):
        RunStrategyBacktestInput(
            strategy_id="exec_user_python",
            start_date="2024-01-02",
            end_date="2024-05-20",
        )

