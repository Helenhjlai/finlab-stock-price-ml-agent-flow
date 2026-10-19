"""Normalize crawler output and idempotently upsert it into Delta tables."""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import re
from typing import Any


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KEY_COLUMNS = ("stock_id", "date")
METADATA_COLUMNS = ("_source", "_ingested_at", "_run_id")


def _validate_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe table identifier: {value!r}")
    return value


class SchemaMismatchError(RuntimeError):
    """Raised when a source website introduces columns not in the Delta table."""


@dataclass(frozen=True)
class MergeResult:
    table_name: str
    input_rows: int
    created_table: bool


def normalize_pandas_frame(pdf: Any, dataset: str) -> Any:
    """Return a regular pandas DataFrame safe for Spark Arrow conversion.

    The crawler stores keys in a MultiIndex and several source columns mix
    Python ``int`` and ``str`` values.  Spark/Arrow cannot reliably infer those
    mixed object columns, so they are converted to pandas nullable strings.
    Numeric and datetime dtypes remain numeric and datetime.
    """
    import pandas as pd

    if pdf is None:
        return pd.DataFrame(columns=list(KEY_COLUMNS))
    if not isinstance(pdf, pd.DataFrame):
        raise TypeError(f"{dataset}: expected pandas.DataFrame, got {type(pdf)!r}")

    frame = pdf.copy()
    if not set(KEY_COLUMNS).issubset(frame.columns):
        frame = frame.reset_index()

    missing = set(KEY_COLUMNS) - set(frame.columns)
    if missing:
        raise KeyError(f"{dataset}: missing required columns {sorted(missing)}")
    if frame.columns.duplicated().any():
        duplicates = frame.columns[frame.columns.duplicated()].tolist()
        raise ValueError(f"{dataset}: duplicate column names {duplicates}")

    frame["stock_id"] = frame["stock_id"].astype("string")
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")

    object_columns = frame.select_dtypes(include=["object"]).columns
    for column in object_columns:
        frame[column] = frame[column].astype("string")

    return frame.drop_duplicates(list(KEY_COLUMNS), keep="last")


class DeltaWriter:
    """Persist batches with Delta MERGE on ``stock_id`` and ``date``."""

    def __init__(self, spark: Any, *, catalog: str, schema: str) -> None:
        self.spark = spark
        self.catalog = _validate_identifier(catalog)
        self.schema = _validate_identifier(schema)

    def table_name(self, dataset: str) -> str:
        return f"{self.catalog}.{self.schema}.{_validate_identifier(dataset)}"

    def merge_pandas(
        self,
        pdf: Any,
        *,
        dataset: str,
        run_id: str,
        source: str = "finlab.crawler",
    ) -> MergeResult:
        frame = normalize_pandas_frame(pdf, dataset)
        if frame.empty:
            return MergeResult(self.table_name(dataset), 0, False)
        spark_frame = self.spark.createDataFrame(frame)
        return self.merge_spark(
            spark_frame,
            dataset=dataset,
            run_id=run_id,
            source=source,
        )

    def merge_spark(
        self,
        spark_frame: Any,
        *,
        dataset: str,
        run_id: str,
        source: str,
    ) -> MergeResult:
        """Merge a Spark DataFrame and fail explicitly on new source columns.

        Missing old columns are filled with nulls.  New source columns are not
        added automatically because a changed TWSE/MOPS header should be
        reviewed before mutating the production schema.
        """
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        table_name = self.table_name(dataset)
        required = set(KEY_COLUMNS)
        missing = required - set(spark_frame.columns)
        if missing:
            raise KeyError(f"{dataset}: missing Spark columns {sorted(missing)}")

        prepared = (
            spark_frame
            .withColumn("stock_id", F.col("stock_id").cast("string"))
            .withColumn("date", F.col("date").cast("timestamp"))
            .withColumn("_source", F.lit(source))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_run_id", F.lit(run_id))
            .where(F.col("stock_id").isNotNull() & F.col("date").isNotNull())
        )

        window = Window.partitionBy(*KEY_COLUMNS).orderBy(F.col("_ingested_at").desc())
        prepared = (
            prepared
            .withColumn("_row_number", F.row_number().over(window))
            .where(F.col("_row_number") == 1)
            .drop("_row_number")
        )
        input_rows = prepared.count()
        if input_rows == 0:
            return MergeResult(table_name, 0, False)

        if not self.spark.catalog.tableExists(table_name):
            (
                prepared.writeTo(table_name)
                .using("delta")
                .tableProperty("delta.columnMapping.mode", "name")
                .create()
            )
            return MergeResult(table_name, input_rows, True)

        target_schema = self.spark.table(table_name).schema
        target_names = {field.name for field in target_schema.fields}

        # Tables created during an earlier manual migration might not yet have
        # our three controlled metadata columns.  Adding these known columns is
        # safe; arbitrary new website columns still fail below for review.
        metadata_definitions = {
            "_source": "STRING",
            "_ingested_at": "TIMESTAMP",
            "_run_id": "STRING",
        }
        missing_metadata = [name for name in METADATA_COLUMNS if name not in target_names]
        if missing_metadata:
            definitions = ", ".join(
                f"`{name}` {metadata_definitions[name]}" for name in missing_metadata
            )
            self.spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({definitions})")
            target_schema = self.spark.table(table_name).schema

        target_columns = [field.name for field in target_schema.fields]
        source_columns = set(prepared.columns)
        extra_columns = source_columns - set(target_columns)
        if extra_columns:
            raise SchemaMismatchError(
                f"{dataset}: source introduced columns {sorted(extra_columns)}. "
                "Review the crawler output and ALTER TABLE deliberately before retrying."
            )

        # A source endpoint can omit an old optional field.  Preserve the table
        # schema and set that field to null for newly inserted rows.
        for field in target_schema.fields:
            if field.name not in source_columns:
                prepared = prepared.withColumn(field.name, F.lit(None).cast(field.dataType))
        prepared = prepared.select(*target_columns)

        target = DeltaTable.forName(self.spark, table_name)
        (
            target.alias("target")
            .merge(
                prepared.alias("source"),
                "target.stock_id = source.stock_id AND target.date = source.date",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return MergeResult(table_name, input_rows, False)


def max_event_time(pdf: Any) -> dt.datetime | None:
    """Find a batch watermark after normalization without relying on index levels."""
    if pdf is None or len(pdf) == 0:
        return None
    value = pdf["date"].max() if "date" in pdf.columns else pdf.index.get_level_values("date").max()
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    return value
