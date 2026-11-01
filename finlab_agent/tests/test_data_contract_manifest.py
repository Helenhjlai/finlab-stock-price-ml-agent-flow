import json
from pathlib import Path


MANIFEST = Path(__file__).parents[1] / "contracts" / "data_products.v1.json"
CAPABILITY_MANIFEST = Path(__file__).parents[1] / "contracts" / "capabilities.v1.json"


def test_contract_manifest_has_unique_products_and_keys():
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    products = payload["products"]
    names = [item["product_name"] for item in products]
    assert len(names) == len(set(names))
    for product in products:
        assert product["contract_version"].startswith("v")
        assert product["table_name"].count(".") == 2
        assert product["primary_key"]


def test_agent_tools_only_depend_on_agent_read_products():
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    products = {
        f"{item['product_name']}/{item['contract_version']}": item
        for item in payload["products"]
    }
    for dependencies in payload["tool_dependencies"].values():
        for dependency in dependencies:
            assert dependency in products
            assert products[dependency]["access_class"] == "agent_read"


def test_training_table_is_not_agent_readable():
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    training = next(
        item
        for item in payload["products"]
        if item["product_name"] == "forecast_training_features"
    )
    assert training["contains_future_label"] is True
    assert training["access_class"] == "training_only"


def test_daily_prices_v2_contains_security_identity_and_etf_fields():
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    prices = next(
        item for item in payload["products"] if item["product_name"] == "daily_prices"
    )
    assert prices["contract_version"] == "v2"
    expected = {
        "stock_id",
        "stock_id_numeric",
        "name",
        "is_etf",
        "etf_type_code",
        "etf_type",
    }
    assert expected.issubset(prices["fields"])


def test_capabilities_only_depend_on_declared_agent_read_products():
    data_payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    capability_payload = json.loads(
        CAPABILITY_MANIFEST.read_text(encoding="utf-8")
    )
    products = {
        f"{item['product_name']}/{item['contract_version']}": item
        for item in data_payload["products"]
    }
    for capability in capability_payload["capabilities"]:
        assert capability["capability_id"].count("/") == 1
        for dependency in capability["required_data_products"]:
            assert dependency in products
            assert products[dependency]["access_class"] == "agent_read"
