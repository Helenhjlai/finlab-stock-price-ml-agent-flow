"""Normalize crawler output and idempotently upsert it into Delta tables."""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import re
from typing import Any

from .bronze_provenance import (
    EXTENDED_PROVENANCE_COLUMNS,
    EXTENDED_PROVENANCE_TYPES,
    payload_hash_expression,
    source_record_id_expression,
)


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_KEY_COLUMNS = ("stock_id", "date")
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


def _validate_columns(columns: tuple[str, ...], label: str) -> tuple[str, ...]:
    if not columns:
        raise ValueError(f"{label} cannot be empty")
    if len(columns) != len(set(columns)):
        raise ValueError(f"{label} cannot contain duplicates")
    return tuple(_validate_identifier(column) for column in columns)


def _merge_condition(key_columns: tuple[str, ...]) -> str:
    keys = _validate_columns(key_columns, "key_columns")
    return " AND ".join(
        f"target.`{column}` = source.`{column}`" for column in keys
    )


def _uses_extended_provenance(
    *, table_exists: bool, existing_columns: set[str]
) -> bool:
    """New tables use v1; existing tables opt in only after a complete backfill."""
    if not table_exists:
        return True
    present = set(EXTENDED_PROVENANCE_COLUMNS).intersection(existing_columns)
    if not present:
        return False
    missing = set(EXTENDED_PROVENANCE_COLUMNS) - present
    if missing:
        raise SchemaMismatchError(
            "Bronze provenance/v1 is only partially installed; run "
            f"backfill_bronze_provenance for the table (missing={sorted(missing)})"
        )
    return True


def normalize_pandas_frame(
    pdf: Any,
    dataset: str,
    *,
    key_columns: tuple[str, ...] = DEFAULT_KEY_COLUMNS,
    event_time_column: str = "date",
    available_time_column: str | None = None,
) -> Any:
    """Return a regular pandas DataFrame safe for Spark Arrow conversion.

    The crawler stores keys in a MultiIndex and several source columns mix
    Python ``int`` and ``str`` values.  Spark/Arrow cannot reliably infer those
    mixed object columns, so they are converted to pandas nullable strings.
    Numeric and datetime dtypes remain numeric and datetime.
    """
    import pandas as pd

    keys = _validate_columns(key_columns, "key_columns")
    event_time = _validate_identifier(event_time_column)
    available_time = (
        _validate_identifier(available_time_column) if available_time_column else None
    )

    if pdf is None:
        return pd.DataFrame(columns=list(dict.fromkeys((*keys, event_time))))
    if not isinstance(pdf, pd.DataFrame):
        raise TypeError(f"{dataset}: expected pandas.DataFrame, got {type(pdf)!r}")

    frame = pdf.copy()
    required = set(keys) | {event_time}
    if available_time:
        required.add(available_time)
    if not required.issubset(frame.columns):
        frame = frame.reset_index()

    missing = required - set(frame.columns)
    if missing:
        raise KeyError(f"{dataset}: missing required columns {sorted(missing)}")
    if frame.columns.duplicated().any():
        duplicates = frame.columns[frame.columns.duplicated()].tolist()
        raise ValueError(f"{dataset}: duplicate column names {duplicates}")

    if "stock_id" in frame.columns:
        frame["stock_id"] = frame["stock_id"].astype("string")
    frame[event_time] = pd.to_datetime(frame[event_time], errors="raise")
    if available_time and available_time != event_time:
        frame[available_time] = pd.to_datetime(frame[available_time], errors="raise")

    object_columns = frame.select_dtypes(include=["object"]).columns
    for column in object_columns:
        frame[column] = frame[column].astype("string")

    return frame.drop_duplicates(list(keys), keep="last")


class DeltaWriter:
    """Persist batches with a dataset-declared, allow-listed Delta MERGE key."""

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
        key_columns: tuple[str, ...] = DEFAULT_KEY_COLUMNS,
        event_time_column: str = "date",
        available_time_column: str | None = None,
    ) -> MergeResult:
        frame = normalize_pandas_frame(
            pdf,
            dataset,
            key_columns=key_columns,
            event_time_column=event_time_column,
            available_time_column=available_time_column,
        )
        if frame.empty:
            return MergeResult(self.table_name(dataset), 0, False)
        spark_frame = self.spark.createDataFrame(frame)
        return self.merge_spark(
            spark_frame,
            dataset=dataset,
            run_id=run_id,
            source=source,
            key_columns=key_columns,
            event_time_column=event_time_column,
            available_time_column=available_time_column,
        )

    def merge_spark(
        self,
        spark_frame: Any,
        *,
        dataset: str,
        run_id: str,
        source: str,
        key_columns: tuple[str, ...] = DEFAULT_KEY_COLUMNS,
        event_time_column: str = "date",
        available_time_column: str | None = None,
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
        keys = _validate_columns(key_columns, "key_columns")
        event_time = _validate_identifier(event_time_column)
        available_time = (
            _validate_identifier(available_time_column) if available_time_column else None
        )
        required = set(keys) | {event_time}
        if available_time:
            required.add(available_time)
        missing = required - set(spark_frame.columns)
        if missing:
            raise KeyError(f"{dataset}: missing Spark columns {sorted(missing)}")

        table_exists = self.spark.catalog.tableExists(table_name)
        existing_names = (
            set(self.spark.table(table_name).columns) if table_exists else set()
        )
        extended_provenance = _uses_extended_provenance(
            table_exists=table_exists,
            existing_columns=existing_names,
        )

        prepared = spark_frame
        if "stock_id" in prepared.columns:
            prepared = prepared.withColumn("stock_id", F.col("stock_id").cast("string"))
        if available_time and available_time != event_time:
            prepared = prepared.withColumn(
                available_time, F.col(available_time).cast("timestamp")
            )
        input_columns = tuple(prepared.columns)
        prepared = (
            prepared
            .withColumn(event_time, F.col(event_time).cast("timestamp"))
            .withColumn("_source", F.lit(source))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_run_id", F.lit(run_id))
            .where(
                F.col(event_time).isNotNull()
                & F.expr(" AND ".join(f"`{key}` IS NOT NULL" for key in keys))
            )
        )
        if extended_provenance:
            if available_time:
                published_at = F.col(available_time).cast("timestamp")
            elif "_source_published_at" in input_columns:
                published_at = F.col("_source_published_at").cast("timestamp")
            else:
                published_at = F.lit(None).cast("timestamp")
            prepared = (
                prepared
                .withColumn(
                    "_source_record_id",
                    source_record_id_expression(dataset, keys),
                )
                .withColumn("_source_published_at", published_at)
            )

        window = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
        prepared = (
            prepared
            .withColumn("_row_number", F.row_number().over(window))
            .where(F.col("_row_number") == 1)
            .drop("_row_number")
        )
        input_rows = prepared.count()
        if input_rows == 0:
            return MergeResult(table_name, 0, False)

        if not table_exists:
            if extended_provenance:
                prepared = prepared.withColumn(
                    "_payload_hash", payload_hash_expression(prepared.columns)
                )
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
            **EXTENDED_PROVENANCE_TYPES,
        }
        active_metadata = METADATA_COLUMNS + (
            EXTENDED_PROVENANCE_COLUMNS if extended_provenance else ()
        )
        missing_metadata = [name for name in active_metadata if name not in target_names]
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
        if extended_provenance:
            # Hash the aligned target business schema, so a temporarily omitted
            # optional source field has the same representation as an explicit
            # NULL and matches the historical backfill rule.
            prepared = prepared.withColumn(
                "_payload_hash", payload_hash_expression(target_columns)
            )
        prepared = prepared.select(*target_columns)

        target = DeltaTable.forName(self.spark, table_name)
        merge_condition = _merge_condition(keys)
        merge = target.alias("target").merge(prepared.alias("source"), merge_condition)
        if extended_provenance:
            merge = merge.whenMatchedUpdateAll(
                condition=(
                    "target._payload_hash IS NULL OR "
                    "NOT (target._payload_hash <=> source._payload_hash) OR "
                    "NOT (target._source_published_at <=> source._source_published_at) OR "
                    "NOT (target._source <=> source._source)"
                )
            )
        else:
            merge = merge.whenMatchedUpdateAll()
        merge.whenNotMatchedInsertAll().execute()
        return MergeResult(table_name, input_rows, False)


def max_event_time(pdf: Any, event_time_column: str = "date") -> dt.datetime | None:
    """Find a batch watermark after normalization without relying on index levels."""
    if pdf is None or len(pdf) == 0:
        return None
    column = _validate_identifier(event_time_column)
    value = (
        pdf[column].max()
        if column in pdf.columns
        else pdf.index.get_level_values(column).max()
    )
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value)
    elif isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        value = dt.datetime.combine(value, dt.time.min)
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    return value
