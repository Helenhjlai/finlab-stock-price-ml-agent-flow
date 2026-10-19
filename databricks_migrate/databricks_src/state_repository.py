"""Delta-backed crawler watermarks and per-dataset run history."""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from typing import Any


@dataclass(frozen=True)
class CrawlerState:
    dataset: str
    last_success_at: dt.datetime | None
    max_event_time: dt.datetime | None
    last_row_count: int | None
    last_status: str | None
    last_error: str | None
    updated_at: dt.datetime | None


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    dataset: str
    started_at: dt.datetime
    finished_at: dt.datetime
    requested_from: dt.datetime | None
    requested_to: dt.datetime | None
    rows_fetched: int
    rows_merged: int
    status: str
    error_class: str | None = None
    error_message: str | None = None


class StateRepository:
    """Own the two operational Delta tables used by the daily Job."""

    def __init__(self, spark: Any, *, catalog: str, ops_schema: str) -> None:
        self.spark = spark
        self.catalog = catalog
        self.ops_schema = ops_schema
        self.state_table = f"{catalog}.{ops_schema}.crawler_state"
        self.run_log_table = f"{catalog}.{ops_schema}.crawler_run_log"

    def ensure_tables(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.ops_schema}")
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.state_table} (
              dataset STRING NOT NULL,
              last_success_at TIMESTAMP,
              max_event_time TIMESTAMP,
              last_row_count BIGINT,
              last_status STRING,
              last_error STRING,
              updated_at TIMESTAMP
            ) USING DELTA
            """
        )
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.run_log_table} (
              run_id STRING NOT NULL,
              dataset STRING NOT NULL,
              started_at TIMESTAMP,
              finished_at TIMESTAMP,
              requested_from TIMESTAMP,
              requested_to TIMESTAMP,
              rows_fetched BIGINT,
              rows_merged BIGINT,
              status STRING,
              error_class STRING,
              error_message STRING
            ) USING DELTA
            """
        )

    def get_state(self, dataset: str) -> CrawlerState | None:
        from pyspark.sql import functions as F

        rows = (
            self.spark.table(self.state_table)
            .where(F.col("dataset") == dataset)
            .orderBy(F.col("updated_at").desc())
            .limit(1)
            .collect()
        )
        if not rows:
            return None
        values = rows[0].asDict()
        return CrawlerState(**values)

    def mark_success(
        self,
        *,
        dataset: str,
        finished_at: dt.datetime,
        batch_max_event_time: dt.datetime | None,
        row_count: int,
    ) -> None:
        self._upsert_state(
            dataset=dataset,
            last_success_at=finished_at,
            max_event_time=batch_max_event_time,
            last_row_count=row_count,
            last_status="success",
            last_error=None,
            updated_at=finished_at,
        )

    def mark_failure(
        self,
        *,
        dataset: str,
        finished_at: dt.datetime,
        error: Exception,
    ) -> None:
        self._upsert_state(
            dataset=dataset,
            last_success_at=None,
            max_event_time=None,
            last_row_count=None,
            last_status="failed",
            last_error=f"{type(error).__name__}: {error}"[:4000],
            updated_at=finished_at,
        )

    def _upsert_state(
        self,
        *,
        dataset: str,
        last_success_at: dt.datetime | None,
        max_event_time: dt.datetime | None,
        last_row_count: int | None,
        last_status: str,
        last_error: str | None,
        updated_at: dt.datetime,
    ) -> None:
        from delta.tables import DeltaTable

        schema = """
            dataset string,
            last_success_at timestamp,
            max_event_time timestamp,
            last_row_count long,
            last_status string,
            last_error string,
            updated_at timestamp
        """
        source = self.spark.createDataFrame(
            [(
                dataset,
                last_success_at,
                max_event_time,
                last_row_count,
                last_status,
                last_error,
                updated_at,
            )],
            schema=schema,
        )
        target = DeltaTable.forName(self.spark, self.state_table)
        (
            target.alias("target")
            .merge(source.alias("source"), "target.dataset = source.dataset")
            .whenMatchedUpdate(
                set={
                    "last_success_at": "coalesce(source.last_success_at, target.last_success_at)",
                    "max_event_time": "coalesce(source.max_event_time, target.max_event_time)",
                    "last_row_count": "coalesce(source.last_row_count, target.last_row_count)",
                    "last_status": "source.last_status",
                    "last_error": "source.last_error",
                    "updated_at": "source.updated_at",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def append_run(self, record: RunRecord) -> None:
        schema = """
            run_id string,
            dataset string,
            started_at timestamp,
            finished_at timestamp,
            requested_from timestamp,
            requested_to timestamp,
            rows_fetched long,
            rows_merged long,
            status string,
            error_class string,
            error_message string
        """
        frame = self.spark.createDataFrame(
            [(
                record.run_id,
                record.dataset,
                record.started_at,
                record.finished_at,
                record.requested_from,
                record.requested_to,
                record.rows_fetched,
                record.rows_merged,
                record.status,
                record.error_class,
                record.error_message[:4000] if record.error_message else None,
            )],
            schema=schema,
        )
        frame.write.format("delta").mode("append").saveAsTable(self.run_log_table)

