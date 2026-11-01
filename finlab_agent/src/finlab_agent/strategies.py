from __future__ import annotations

from dataclasses import dataclass

from .schemas import StrategyName


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    description: str
    engine: StrategyName
    stock_ids: tuple[str, ...]
    fast_window: int = 20
    slow_window: int = 60
    momentum_window: int = 60
    top_n: int = 2
    rebalance_frequency: str = "M"
    fee_bps: float = 14.25
    slippage_bps: float = 10.0


# 策略白名單是受版本控制的程式碼。LLM 只能選 ID，不能改寫策略或執行程式。
STRATEGY_CATALOG: dict[str, StrategyDefinition] = {
    "tw_ma20_60": StrategyDefinition(
        strategy_id="tw_ma20_60",
        description="2330、2454、0050 的 MA20 > MA60 等權重策略，每月再平衡",
        engine=StrategyName.SMA_CROSS,
        stock_ids=("2330", "2454", "0050"),
    ),
    "tw_momentum_top2": StrategyDefinition(
        strategy_id="tw_momentum_top2",
        description="2330、2454、0050 的 60 日動能前二名，每月再平衡",
        engine=StrategyName.MOMENTUM_TOP_N,
        stock_ids=("2330", "2454", "0050"),
        top_n=2,
    ),
}


def get_strategy(strategy_id: str) -> StrategyDefinition:
    try:
        return STRATEGY_CATALOG[strategy_id]
    except KeyError as exc:  # protected by the public input schema
        raise ValueError(f"未知策略 ID: {strategy_id}") from exc
