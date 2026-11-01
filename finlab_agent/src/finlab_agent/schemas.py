from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class GetIndicatorsInput(BaseModel):
    stock_id: str = Field(description="股票代號，例如 2330")
    as_of: date = Field(description="分析截止日期，格式 YYYY-MM-DD")

    @field_validator("stock_id")
    @classmethod
    def clean_stock_id(cls, value: str) -> str:
        value = value.strip().upper()
        if not value or len(value) > 20:
            raise ValueError("股票代號長度不合法")
        return value


class ScreenRule(BaseModel):
    field: Literal[
        "close",
        "sma_20",
        "sma_60",
        "price_above_ma60",
        "return_20d",
        "avg_volume_20d",
        "revenue_yoy",
    ]
    operator: Literal[">", ">=", "<", "<=", "=="]
    value: float | bool


class ScreenStocksInput(BaseModel):
    rules: list[ScreenRule] = Field(min_length=1, max_length=8)
    stock_ids: list[str] | None = Field(
        default=None,
        max_length=500,
        description="可選股票池；省略代表資料表中的全部股票",
    )
    as_of: date = Field(description="篩選截止日期，格式 YYYY-MM-DD")
    sort_by: Literal[
        "stock_id", "close", "sma_20", "sma_60", "return_20d", "revenue_yoy"
    ] = "stock_id"
    sort_order: Literal["asc", "desc"] = "asc"
    limit: int = Field(default=20, ge=1, le=100)


class StrategyName(str, Enum):
    SMA_CROSS = "sma_cross"
    MOMENTUM_TOP_N = "momentum_top_n"


class RunStrategyBacktestInput(BaseModel):
    strategy_id: Literal["tw_ma20_60", "tw_momentum_top2"]
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def check_dates(self) -> "RunStrategyBacktestInput":
        if self.start_date >= self.end_date:
            raise ValueError("start_date 必須早於 end_date")
        return self


class ToolEnvelope(BaseModel):
    status: Literal["ok", "error"] = "ok"
    tool: str
    as_of: str | None = None
    data: dict[str, Any]
    warnings: list[str] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)


class CapabilitySearchInput(BaseModel):
    query: str = Field(
        min_length=2,
        max_length=300,
        description="用自然語言描述某個研究步驟需要的能力，不要填工具名稱",
    )
    max_results: int = Field(default=5, ge=1, le=10)


class CapabilityExecuteInput(BaseModel):
    capability_token: str = Field(
        min_length=8,
        max_length=200,
        description="search_capabilities 回傳的短效授權 token",
    )
    capability_id: str = Field(
        min_length=3,
        max_length=100,
        description="搜尋結果中的 capability_id",
    )
    arguments: dict[str, Any] = Field(
        default_factory=dict,
        description="依搜尋結果 input_schema 組成的能力參數",
    )


class ResearchStep(BaseModel):
    step_id: str = Field(description="穩定、簡短的步驟 ID，例如 step_1")
    question: str = Field(description="這一步要回答的子問題")
    capability_query: str = Field(
        description="用來搜尋能力目錄的語意查詢，不可假設工具名稱"
    )
    required_inputs: list[str] = Field(
        default_factory=list,
        description="執行此步驟必須從問題或前一步取得的參數",
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description="必須先完成的 step_id",
    )


class ResearchPlan(BaseModel):
    goal: str = Field(description="使用者真正想完成的研究目標")
    steps: list[ResearchStep] = Field(min_length=1, max_length=8)
    assumptions: list[str] = Field(default_factory=list)
    needs_clarification: bool = False
    clarification_question: str | None = None

    @model_validator(mode="after")
    def check_clarification(self) -> "ResearchPlan":
        step_ids = [step.step_id for step in self.steps]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("ResearchPlan 的 step_id 不可重複")
        known = set(step_ids)
        for step in self.steps:
            if any(item not in known for item in step.depends_on):
                raise ValueError("depends_on 必須引用同一計畫中的 step_id")
            if step.step_id in step.depends_on:
                raise ValueError("步驟不可依賴自己")
        if self.needs_clarification and not self.clarification_question:
            raise ValueError("需要澄清時必須提供 clarification_question")
        return self

