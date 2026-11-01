from pathlib import Path

import pytest

from finlab_agent.repository import CsvPriceRepository


@pytest.fixture()
def price_repository() -> CsvPriceRepository:
    return CsvPriceRepository(Path(__file__).parent / "fixtures" / "prices.csv")


@pytest.fixture()
def prices(price_repository):
    return price_repository.load_prices()

