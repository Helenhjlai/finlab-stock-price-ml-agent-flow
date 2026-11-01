from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Protocol, Sequence

import pandas as pd

from .config import Settings


REQUIRED_PRICE_COLUMNS = {"date", "stock_id", "open", "high", "low", "close", "volume"}
REQUIRED_FACTOR_COLUMNS = {"date", "stock_id", "revenue_yoy"}
_UC_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


class PriceRepository(Protocol):
    source_name: str

    def load_prices(
        self,
        stock_ids: Sequence[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame: ...

    def load_factors(
        self,
        stock_ids: Sequence[str] | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame: ...


def normalize_prices(frame: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_PRICE_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"價格資料缺少欄位: {sorted(missing)}")

    result = frame.loc[:, sorted(REQUIRED_PRICE_COLUMNS)].copy()
    result["date"] = pd.to_datetime(result["date"], errors="raise").dt.normalize()
    result["stock_id"] = result["stock_id"].astype(str).str.strip().str.upper()
    for column in ("open", "high", "low", "close", "volume"):
        result[column] = pd.to_numeric(result[column], errors="coerce")

    if result[["date", "stock_id"]].duplicated().any():
        raise ValueError("價格資料包含重複的 date + stock_id")
    if result[["open", "high", "low", "close"]].isna().any().any():
        raise ValueError("OHLC 欄位不可包含空值或非數字")
    if (result[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("OHLC 價格必須大於 0")

    return result.sort_values(["date", "stock_id"]).reset_index(drop=True)


def normalize_factors(frame: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_FACTOR_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"因子資料缺少欄位: {sorted(missing)}")
    result = frame.loc[:, sorted(REQUIRED_FACTOR_COLUMNS)].copy()
    result["date"] = pd.to_datetime(result["date"], errors="raise").dt.normalize()
    result["stock_id"] = result["stock_id"].astype(str).str.strip().str.upper()
    result["revenue_yoy"] = pd.to_numeric(result["revenue_yoy"], errors="coerce")
    if result[["date", "stock_id"]].duplicated().any():
        raise ValueError("因子資料包含重複的 date + stock_id")
    return result.sort_values(["date", "stock_id"]).reset_index(drop=True)


class CsvPriceRepository:
    source_name = "sample_csv"

    def __init__(self, path: str | Path, factor_path: str | Path | None = None):
        self.path = Path(path)
        self.factor_path = Path(factor_path) if factor_path else self.path.with_name("factors.csv")

    def load_prices(
        self,
        stock_ids: Sequence[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        frame = normalize_prices(pd.read_csv(self.path))
        if stock_ids:
            wanted = {str(item).strip().upper() for item in stock_ids}
            frame = frame[frame["stock_id"].isin(wanted)]
        if start_date:
            frame = frame[frame["date"] >= pd.Timestamp(start_date)]
        if end_date:
            frame = frame[frame["date"] <= pd.Timestamp(end_date)]
        return frame.reset_index(drop=True)

    def load_factors(
        self,
        stock_ids: Sequence[str] | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        frame = normalize_factors(pd.read_csv(self.factor_path))
        if stock_ids:
            wanted = {str(item).strip().upper() for item in stock_ids}
            frame = frame[frame["stock_id"].isin(wanted)]
        if end_date:
            frame = frame[frame["date"] <= pd.Timestamp(end_date)]
        return frame.reset_index(drop=True)


class DatabricksSqlPriceRepository:
    """Read a canonical OHLCV Unity Catalog table with app OAuth credentials."""

    source_name = "databricks_sql"

    def __init__(
        self,
        warehouse_id: str,
        table_name: str,
        factor_table_name: str = "finlab.gold.stock_factors",
        max_rows: int = 100_000,
    ):
        if not _UC_IDENTIFIER.fullmatch(table_name):
            raise ValueError("FINLAB_PRICE_TABLE 必須是安全的 catalog.schema.table 名稱")
        if not _UC_IDENTIFIER.fullmatch(factor_table_name):
            raise ValueError("FINLAB_FACTOR_TABLE 必須是安全的 catalog.schema.table 名稱")
        self.warehouse_id = warehouse_id
        self.table_name = table_name
        self.factor_table_name = factor_table_name
        self.max_rows = max_rows

    def _connect(self):
        from databricks import sql
        from databricks.sdk.core import Config

        config = Config()
        hostname = config.host.removeprefix("https://").removeprefix("http://")
        return sql.connect(
            server_hostname=hostname,
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            credentials_provider=lambda: config.authenticate,
            _use_arrow_native_complex_types=False,
        )

    def load_prices(
        self,
        stock_ids: Sequence[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        clauses: list[str] = []
        parameters: list[object] = []
        if stock_ids:
            cleaned = [str(item).strip().upper() for item in stock_ids]
            placeholders = ", ".join(["?"] * len(cleaned))
            clauses.append(f"stock_id IN ({placeholders})")
            parameters.extend(cleaned)
        if start_date:
            clauses.append("date >= ?")
            parameters.append(start_date.isoformat())
        if end_date:
            clauses.append("date <= ?")
            parameters.append(end_date.isoformat())

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"""
            SELECT date, stock_id, open, high, low, close, volume
            FROM {self.table_name}
            {where}
            ORDER BY date, stock_id
            LIMIT {int(self.max_rows)}
        """
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query, parameters)
            frame = cursor.fetchall_arrow().to_pandas()
        if len(frame) >= self.max_rows:
            raise ValueError(
                f"查詢達到 {self.max_rows} 列上限；請縮小日期或股票範圍"
            )
        return normalize_prices(frame)

    def load_factors(
        self,
        stock_ids: Sequence[str] | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        clauses: list[str] = []
        parameters: list[object] = []
        if stock_ids:
            cleaned = [str(item).strip().upper() for item in stock_ids]
            placeholders = ", ".join(["?"] * len(cleaned))
            clauses.append(f"stock_id IN ({placeholders})")
            parameters.extend(cleaned)
        if end_date:
            clauses.append("date <= ?")
            parameters.append(end_date.isoformat())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"""
            SELECT date, stock_id, revenue_yoy
            FROM {self.factor_table_name}
            {where}
            ORDER BY date, stock_id
            LIMIT {int(self.max_rows)}
        """
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query, parameters)
            frame = cursor.fetchall_arrow().to_pandas()
        if len(frame) >= self.max_rows:
            raise ValueError(f"因子查詢達到 {self.max_rows} 列上限；請縮小股票範圍")
        return normalize_factors(frame)


def build_repository(settings: Settings) -> PriceRepository:
    settings.validate()
    if settings.data_backend == "databricks":
        assert settings.warehouse_id is not None
        return DatabricksSqlPriceRepository(
            warehouse_id=settings.warehouse_id,
            table_name=settings.price_table,
            factor_table_name=settings.factor_table,
            max_rows=settings.max_query_rows,
        )
    assert settings.sample_prices_path is not None
    return CsvPriceRepository(settings.sample_prices_path, settings.sample_factors_path)
