from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from .schemas import ScreenRule, StrategyName


def _rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(
        alpha=1 / window, min_periods=window, adjust=False
    ).mean()
    relative_strength = gain / loss.replace(0, np.nan)
    result = 100 - (100 / (1 + relative_strength))
    return result.where(loss.ne(0), 100.0)


def calculate_indicators(
    prices: pd.DataFrame,
    stock_id: str,
) -> pd.DataFrame:
    subset = prices[prices["stock_id"] == stock_id].set_index("date").sort_index()
    if subset.empty:
        raise ValueError(f"找不到股票 {stock_id} 的價格資料")

    close = subset["close"]
    output = pd.DataFrame({"close": close})
    output["ma20"] = close.rolling(20, min_periods=20).mean()
    output["ma60"] = close.rolling(60, min_periods=60).mean()
    output["momentum_20d"] = close.pct_change(20)
    return output


_OPERATORS = {
    ">": lambda left, right: left > right,
    ">=": lambda left, right: left >= right,
    "<": lambda left, right: left < right,
    "<=": lambda left, right: left <= right,
    "==": lambda left, right: left == right,
}


def screen_stocks(
    prices: pd.DataFrame,
    factors: pd.DataFrame,
    rules: list[ScreenRule],
    sort_by: str,
    sort_order: str,
    limit: int,
) -> pd.DataFrame:
    if prices.empty:
        raise ValueError("篩選期間沒有價格資料")
    close = prices.pivot(index="date", columns="stock_id", values="close").sort_index()
    volume = prices.pivot(index="date", columns="stock_id", values="volume").sort_index()
    latest = pd.DataFrame(index=close.columns)
    latest.index.name = "stock_id"
    latest["close"] = close.ffill().iloc[-1]
    latest["sma_20"] = close.rolling(20, min_periods=20).mean().iloc[-1]
    latest["sma_60"] = close.rolling(60, min_periods=60).mean().iloc[-1]
    latest["price_above_ma60"] = latest["close"] > latest["sma_60"]
    latest["return_20d"] = close.pct_change(20).iloc[-1]
    latest["avg_volume_20d"] = volume.rolling(20, min_periods=20).mean().iloc[-1]
    if factors.empty:
        latest["revenue_yoy"] = np.nan
    else:
        factor_latest = (
            factors.sort_values("date").groupby("stock_id", as_index=True).tail(1)
        )
        latest["revenue_yoy"] = factor_latest.set_index("stock_id")["revenue_yoy"]

    mask = pd.Series(True, index=latest.index)
    for rule in rules:
        mask &= _OPERATORS[rule.operator](latest[rule.field], rule.value).fillna(False)
    ascending = sort_order == "asc"
    return (
        latest.loc[mask]
        .sort_values(sort_by, ascending=ascending, na_position="last")
        .head(limit)
        .reset_index()
    )


@dataclass(frozen=True)
class BacktestResult:
    metrics: dict[str, float | int | None]
    equity_curve: pd.Series
    warnings: list[str]


def _rebalance_targets(targets: pd.DataFrame, frequency: str) -> pd.DataFrame:
    if frequency == "D":
        return targets.fillna(0.0)
    period = targets.index.to_period(frequency)
    decision_dates = targets.groupby(period).tail(1).index
    rebalanced = pd.DataFrame(np.nan, index=targets.index, columns=targets.columns)
    rebalanced.loc[decision_dates] = targets.loc[decision_dates]
    return rebalanced.ffill().fillna(0.0)


def run_backtest(
    prices: pd.DataFrame,
    strategy: StrategyName,
    fast_window: int,
    slow_window: int,
    momentum_window: int,
    top_n: int,
    rebalance_frequency: str,
    fee_bps: float,
    slippage_bps: float,
    evaluation_start: date | None = None,
    evaluation_end: date | None = None,
) -> BacktestResult:
    if prices.empty:
        raise ValueError("回測期間沒有價格資料")
    close = prices.pivot(index="date", columns="stock_id", values="close").sort_index()
    close = close.ffill()
    if len(close) <= max(slow_window, momentum_window):
        raise ValueError("歷史資料不足以計算策略視窗")

    if strategy == StrategyName.SMA_CROSS:
        fast = close.rolling(fast_window, min_periods=fast_window).mean()
        slow = close.rolling(slow_window, min_periods=slow_window).mean()
        raw = (fast > slow).astype(float)
    elif strategy == StrategyName.MOMENTUM_TOP_N:
        momentum = close.pct_change(momentum_window)
        ranks = momentum.rank(axis=1, ascending=False, method="first")
        raw = (ranks <= top_n).astype(float).where(momentum.notna(), 0.0)
    else:  # pragma: no cover - protected by the enum
        raise ValueError(f"不支援策略: {strategy}")

    denominator = raw.sum(axis=1).replace(0, np.nan)
    targets = raw.div(denominator, axis=0).fillna(0.0)
    targets = _rebalance_targets(targets, rebalance_frequency)

    # Signals formed after close on t can only earn returns from t+1 onward.
    effective_weights = targets.shift(1).fillna(0.0)
    asset_returns = close.pct_change(fill_method=None).fillna(0.0)
    gross_returns = (effective_weights * asset_returns).sum(axis=1)
    weight_changes = effective_weights.diff().abs()
    turnover = weight_changes.sum(axis=1)
    turnover.iloc[0] = effective_weights.iloc[0].abs().sum()
    cost_rate = (fee_bps + slippage_bps) / 10_000
    net_returns = gross_returns - turnover * cost_rate
    if evaluation_start:
        mask = net_returns.index >= pd.Timestamp(evaluation_start)
        net_returns = net_returns.loc[mask]
        turnover = turnover.loc[mask]
        weight_changes = weight_changes.loc[mask]
    if evaluation_end:
        mask = net_returns.index <= pd.Timestamp(evaluation_end)
        net_returns = net_returns.loc[mask]
        turnover = turnover.loc[mask]
        weight_changes = weight_changes.loc[mask]
    if net_returns.empty:
        raise ValueError("回測評估期間沒有交易日")
    equity = (1 + net_returns).cumprod()

    years = max((equity.index[-1] - equity.index[0]).days / 365.25, 1 / 365.25)
    annual_return = float(equity.iloc[-1] ** (1 / years) - 1)
    annual_volatility = float(net_returns.std(ddof=1) * math.sqrt(252))
    sharpe = (
        float(net_returns.mean() / net_returns.std(ddof=1) * math.sqrt(252))
        if net_returns.std(ddof=1) > 0
        else None
    )
    drawdown = equity / equity.cummax() - 1
    metrics: dict[str, float | int | None] = {
        "cumulative_return": float(equity.iloc[-1] - 1),
        "annual_return": annual_return,
        "annual_volatility": annual_volatility,
        "sharpe_ratio": sharpe,
        "max_drawdown": float(drawdown.min()),
        "total_turnover": float(turnover.sum()),
        "estimated_cost": float((turnover * cost_rate).sum()),
        "trade_count": int((weight_changes > 1e-12).sum().sum()),
        "observations": int(len(equity)),
    }
    warnings = [
        "回測績效不代表未來報酬，且不是投資建議。",
        "訊號在收盤後形成並延後一個交易日生效，以避免前視偏誤。",
        "交易成本僅含設定的手續費與滑價，未模擬流動性、漲跌停與市場衝擊。",
        "本結果可能有存活者偏誤；正式研究需使用歷史時點可得的股票池。",
    ]
    return BacktestResult(metrics=metrics, equity_curve=equity, warnings=warnings)

