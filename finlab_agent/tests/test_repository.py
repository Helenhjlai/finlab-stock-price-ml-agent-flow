import pandas as pd
import pytest

from finlab_agent.repository import DatabricksSqlPriceRepository, normalize_prices


def test_normalize_prices_rejects_duplicate_keys(prices):
    duplicated = pd.concat([prices, prices.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="重複"):
        normalize_prices(duplicated)


def test_databricks_repository_rejects_unsafe_table_name():
    with pytest.raises(ValueError, match="catalog.schema.table"):
        DatabricksSqlPriceRepository("warehouse", "table; DROP TABLE x")

