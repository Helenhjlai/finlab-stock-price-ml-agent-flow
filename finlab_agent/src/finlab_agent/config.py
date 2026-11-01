from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Runtime configuration loaded from environment variables.

    Databricks Apps injects the warehouse, table and model resource values.
    Local development defaults to the deterministic CSV fixture.
    """

    data_backend: str = "sample"
    model_backend: str = "demo"
    warehouse_id: str | None = None
    price_table: str = "finlab.gold.daily_prices"
    factor_table: str = "finlab.gold.stock_factors"
    model_endpoint: str = "databricks-meta-llama-3-3-70b-instruct"
    sample_prices_path: Path | None = None
    sample_factors_path: Path | None = None
    max_query_rows: int = 500_000
    enable_mlflow_tracing: bool = False
    mlflow_experiment: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        project_root = Path(__file__).resolve().parents[2]
        default_fixture = project_root / "tests" / "fixtures" / "prices.csv"
        default_factors = project_root / "tests" / "fixtures" / "factors.csv"
        return cls(
            data_backend=os.getenv("FINLAB_DATA_BACKEND", "sample").strip().lower(),
            model_backend=os.getenv("FINLAB_MODEL_BACKEND", "demo").strip().lower(),
            warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
            price_table=os.getenv(
                "FINLAB_PRICE_TABLE", "finlab.gold.daily_prices"
            ),
            factor_table=os.getenv(
                "FINLAB_FACTOR_TABLE", "finlab.gold.stock_factors"
            ),
            model_endpoint=os.getenv(
                "DATABRICKS_MODEL_ENDPOINT",
                "databricks-meta-llama-3-3-70b-instruct",
            ),
            sample_prices_path=Path(
                os.getenv("FINLAB_SAMPLE_PRICES", str(default_fixture))
            ),
            sample_factors_path=Path(
                os.getenv("FINLAB_SAMPLE_FACTORS", str(default_factors))
            ),
            max_query_rows=int(os.getenv("FINLAB_MAX_QUERY_ROWS", "500000")),
            enable_mlflow_tracing=os.getenv("ENABLE_MLFLOW_TRACING", "false").lower()
            == "true",
            mlflow_experiment=os.getenv("MLFLOW_EXPERIMENT_NAME"),
        )

    def validate(self) -> None:
        if self.data_backend not in {"sample", "databricks"}:
            raise ValueError("FINLAB_DATA_BACKEND 必須是 sample 或 databricks")
        if self.model_backend not in {"demo", "databricks"}:
            raise ValueError("FINLAB_MODEL_BACKEND 必須是 demo 或 databricks")
        if self.data_backend == "databricks" and not self.warehouse_id:
            raise ValueError("databricks backend 需要 DATABRICKS_WAREHOUSE_ID")
        if self.max_query_rows < 1:
            raise ValueError("FINLAB_MAX_QUERY_ROWS 必須大於 0")

