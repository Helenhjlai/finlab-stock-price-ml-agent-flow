import json
from pathlib import Path

import pytest

from finlab_agent.capabilities import (
    build_capability_registry,
    create_capability_gateway_tools,
)


MANIFEST = Path(__file__).parents[1] / "contracts" / "capabilities.v1.json"


def _gateway_by_name(registry):
    return {tool.name: tool for tool in create_capability_gateway_tools(registry)}


def test_capability_catalog_matches_executable_registry(price_repository):
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    registry = build_capability_registry(price_repository, MANIFEST)
    enabled = {
        item["capability_id"]
        for item in payload["capabilities"]
        if item["enabled"]
    }
    assert set(registry.capability_ids) == enabled


def test_search_then_execute_indicator_capability(price_repository):
    registry = build_capability_registry(price_repository, MANIFEST)
    gateway = _gateway_by_name(registry)
    discovery = gateway["search_capabilities"].invoke(
        {"query": "查詢股票收盤價、均線與價格動能", "max_results": 2}
    )
    capability_ids = [item["capability_id"] for item in discovery["matches"]]
    assert "market.indicators/v1" in capability_ids
    match = next(
        item
        for item in discovery["matches"]
        if item["capability_id"] == "market.indicators/v1"
    )
    assert {"stock_id", "as_of"}.issubset(match["input_schema"]["properties"])

    execution = gateway["execute_capability"].invoke(
        {
            "capability_token": discovery["capability_token"],
            "capability_id": "market.indicators/v1",
            "arguments": {"stock_id": "2330", "as_of": "2024-05-20"},
        }
    )
    assert execution["status"] == "ok"
    assert execution["result"]["tool"] == "get_stock_indicators"


def test_discovery_token_cannot_execute_capability_outside_results(price_repository):
    registry = build_capability_registry(price_repository, MANIFEST)
    discovery = registry.search("策略回測 最大回撤 Sharpe", max_results=1)
    assert discovery["matches"][0]["capability_id"] == "strategy.backtest/v1"
    with pytest.raises(ValueError, match="不在該次搜尋結果"):
        registry.execute(
            discovery["capability_token"],
            "market.indicators/v1",
            {"stock_id": "2330", "as_of": "2024-05-20"},
        )


def test_invalid_discovery_token_is_rejected(price_repository):
    registry = build_capability_registry(price_repository, MANIFEST)
    with pytest.raises(ValueError, match="無效或已過期"):
        registry.execute(
            "not-a-real-token",
            "market.indicators/v1",
            {"stock_id": "2330", "as_of": "2024-05-20"},
        )
