from __future__ import annotations

from datetime import timedelta
from typing import Any

from langchain.tools import tool

from .calculations import calculate_indicators, run_backtest, screen_stocks
from .repository import PriceRepository
from .schemas import GetIndicatorsInput, RunStrategyBacktestInput, ScreenStocksInput
from .strategies import get_strategy
from .validation import validate_tool_result


def _records(frame, tail: int | None = None) -> list[dict[str, Any]]:
    selected = frame.tail(tail) if tail else frame
    if selected.index.name is not None or not hasattr(selected.index, "start"):
        selected = selected.reset_index()
    return selected.to_dict(orient="records")


def create_finlab_tools(repository: PriceRepository):
    @tool("get_stock_indicators", args_schema=GetIndicatorsInput)
    def get_stock_indicators(
        stock_id: str,
        as_of,
    ) -> dict[str, Any]:
        """取得單一股票在截止日的收盤價、MA20、MA60 與 20 日價格動能。

        當問題詢問某股票的價格、20/60 日均線或價格動能時使用。
        不可用此工具回答股票篩選或策略績效問題。
        """
        args = GetIndicatorsInput(stock_id=stock_id, as_of=as_of)
        warmup_start = args.as_of - timedelta(days=180)
        prices = repository.load_prices([args.stock_id], warmup_start, args.as_of)
        output = calculate_indicators(prices, args.stock_id)
        latest = output.dropna().tail(1)
        if latest.empty:
            raise ValueError("截止日前至少需要 60 個交易日資料")
        return validate_tool_result(
            {
                "tool": "get_stock_indicators",
                "as_of": latest.index.max(),
                "data": {
                    "stock_id": args.stock_id,
                    "indicator": _records(latest)[0],
                },
                "warnings": ["技術指標僅描述歷史資料，不構成買賣建議。"],
                "provenance": {
                    "source": repository.source_name,
                    "calculation": "pandas",
                    "momentum_definition": "close / close.shift(20) - 1",
                },
            }
        )

    @tool("screen_stocks", args_schema=ScreenStocksInput)
    def screen_stocks_tool(
        rules: list[dict[str, Any]],
        stock_ids: list[str] | None = None,
        as_of=None,
        sort_by: str = "stock_id",
        sort_order: str = "asc",
        limit: int = 20,
    ) -> dict[str, Any]:
        """依多個量化條件篩選股票。

        當問題要求找出或排序符合營收年增率、價格高於 MA60、價格、均線、
        成交量或 20 日動能條件的股票時使用。所有規則採 AND,不可用於回測。
        """
        args = ScreenStocksInput(
            rules=rules,
            stock_ids=stock_ids,
            as_of=as_of,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
        )
        warmup_start = args.as_of - timedelta(days=180)
        prices = repository.load_prices(args.stock_ids, warmup_start, args.as_of)
        factors = repository.load_factors(args.stock_ids, args.as_of)
        output = screen_stocks(
            prices, factors, args.rules, args.sort_by, args.sort_order, args.limit
        )
        effective_date = prices["date"].max() if not prices.empty else None
        return validate_tool_result(
            {
                "tool": "screen_stocks",
                "as_of": effective_date,
                "data": {"matches": _records(output), "match_count": len(output)},
                "warnings": [
                    "篩選結果取決於資料品質與指定時點，不構成投資建議。",
                    "正式研究應確認股票池沒有存活者偏誤。",
                ],
                "provenance": {
                    "source": repository.source_name,
                    "calculation": "pandas",
                    "rules": [rule.model_dump() for rule in args.rules],
                    "factor_as_of": factors["date"].max() if not factors.empty else None,
                },
            }
        )

    @tool("run_strategy_backtest", args_schema=RunStrategyBacktestInput)
    def run_strategy_backtest_tool(
        strategy_id: str,
        start_date,
        end_date,
    ) -> dict[str, Any]:
        """以白名單策略 ID 執行可重現且含交易成本的歷史回測。

        當問題要求策略績效、年化報酬、Sharpe、最大回撤或權益曲線時使用。
        只接受 tw_ma20_60 或 tw_momentum_top2;絕不執行任意 Python。
        """
        args = RunStrategyBacktestInput(
            strategy_id=strategy_id, start_date=start_date, end_date=end_date
        )
        strategy = get_strategy(args.strategy_id)
        warmup_days = max(strategy.slow_window, strategy.momentum_window) * 3
        warmup_start = args.start_date - timedelta(days=warmup_days)
        prices = repository.load_prices(strategy.stock_ids, warmup_start, args.end_date)
        result = run_backtest(
            prices=prices,
            strategy=strategy.engine,
            fast_window=strategy.fast_window,
            slow_window=strategy.slow_window,
            momentum_window=strategy.momentum_window,
            top_n=strategy.top_n,
            rebalance_frequency=strategy.rebalance_frequency,
            fee_bps=strategy.fee_bps,
            slippage_bps=strategy.slippage_bps,
            evaluation_start=args.start_date,
            evaluation_end=args.end_date,
        )
        curve = result.equity_curve.rename("equity").to_frame()
        return validate_tool_result(
            {
                "tool": "run_strategy_backtest",
                "as_of": result.equity_curve.index.max(),
                "data": {
                    "strategy_id": strategy.strategy_id,
                    "strategy_description": strategy.description,
                    "metrics": result.metrics,
                    "equity_curve_tail": _records(curve, tail=60),
                },
                "warnings": result.warnings,
                "provenance": {
                    "source": repository.source_name,
                    "calculation": "pandas",
                    "start_date": args.start_date,
                    "end_date": args.end_date,
                    "warmup_start": warmup_start,
                    "fee_bps": strategy.fee_bps,
                    "slippage_bps": strategy.slippage_bps,
                },
            }
        )

    return [get_stock_indicators, screen_stocks_tool, run_strategy_backtest_tool]
