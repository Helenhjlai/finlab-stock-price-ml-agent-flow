from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

import numpy as np

from .schemas import ToolEnvelope


BACKTEST_REQUIRED_WARNINGS = ("投資建議", "前視偏誤", "交易成本", "存活者偏誤")


def to_builtin(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (float, np.floating)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, Mapping):
        return {str(key): to_builtin(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [to_builtin(item) for item in value]
    return str(value)


def validate_tool_result(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = to_builtin(payload)
    envelope = ToolEnvelope.model_validate(normalized)
    if envelope.tool == "run_strategy_backtest":
        joined = " ".join(envelope.warnings)
        missing = [term for term in BACKTEST_REQUIRED_WARNINGS if term not in joined]
        if missing:
            raise ValueError(f"回測結果缺少風險說明: {missing}")
    return envelope.model_dump(mode="json")

