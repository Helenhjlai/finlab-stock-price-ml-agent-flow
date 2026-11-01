import math

import pandas as pd
import pytest

from finlab_agent.calculations import (
    calculate_indicators,
    run_backtest,
    screen_stocks,
)
from finlab_agent.schemas import ScreenRule, StrategyName


def test_sma_matches_pandas_rolling_mean(prices):
    result = calculate_indicators(
        prices,
        stock_id="2330",
    )
    expected = (
        prices[prices["stock_id"] == "2330"]
        .set_index("date")["close"]
        .rolling(20)
        .mean()
        .iloc[-1]
    )
    assert result["ma20"].iloc[-1] == pytest.approx(expected)


def test_screening_applies_all_rules(prices, price_repository):
    result = screen_stocks(
        prices,
        price_repository.load_factors(end_date=pd.Timestamp("2024-05-20").date()),
        rules=[
            ScreenRule(field="revenue_yoy", operator=">", value=0.15),
            ScreenRule(field="price_above_ma60", operator="==", value=True),
        ],
        sort_by="revenue_yoy",
        sort_order="desc",
        limit=10,
    )
    assert list(result["stock_id"]) == ["2454", "2330"]


def test_backtest_metrics_are_finite_and_costs_are_included(prices):
    result = run_backtest(
        prices=prices[prices["stock_id"].isin(["2330", "2454"])],
        strategy=StrategyName.SMA_CROSS,
        fast_window=5,
        slow_window=20,
        momentum_window=10,
        top_n=1,
        rebalance_frequency="M",
        fee_bps=14.25,
        slippage_bps=10,
    )
    assert result.metrics["observations"] == 100
    assert result.metrics["estimated_cost"] > 0
    assert result.metrics["trade_count"] > 0
    for key in ("cumulative_return", "annual_return", "max_drawdown"):
        assert math.isfinite(result.metrics[key])


def test_signal_is_shifted_one_day_to_avoid_lookahead():
    dates = pd.bdate_range("2024-01-02", periods=7)
    close = [10, 10, 10, 20, 20, 20, 20]
    frame = pd.DataFrame(
        {
            "date": dates,
            "stock_id": "2330",
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1_000,
        }
    )
    result = run_backtest(
        prices=frame,
        strategy=StrategyName.SMA_CROSS,
        fast_window=2,
        slow_window=3,
        momentum_window=2,
        top_n=1,
        rebalance_frequency="D",
        fee_bps=0,
        slippage_bps=0,
    )
    assert result.metrics["cumulative_return"] == pytest.approx(0.0)

