# Databricks notebook source
"""Read-only contract audit for existing FinLab Bronze Delta tables.

Run before and after deploying crawler framework changes. This notebook never
writes, alters, optimizes, vacuums, or repairs Bronze data.
"""

# COMMAND ----------

import json
import os
import sys

from pyspark.sql import functions as F

PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from databricks_src.bronze_provenance import EXTENDED_PROVENANCE_COLUMNS
from databricks_src.crawler_registry import HISTORICAL_TABLES, get_output_spec

# COMMAND ----------

dbutils.widgets.text("catalog", "finlab", "01 catalog")
dbutils.widgets.text("bronze_schema", "bronze", "02 bronze schema")
dbutils.widgets.text("datasets", "all", "03 datasets (all or comma-separated)")

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
selected_value = dbutils.widgets.get("datasets").strip()

if selected_value.lower() == "all":
    datasets = HISTORICAL_TABLES
else:
    datasets = tuple(item.strip() for item in selected_value.split(",") if item.strip())
    unknown = set(datasets) - set(HISTORICAL_TABLES)
    if unknown:
        raise ValueError(f"Unknown historical table(s): {sorted(unknown)}")

# COMMAND ----------

results: list[dict] = []
failures: list[str] = []

for dataset in datasets:
    spec = get_output_spec(dataset)
    table_name = f"{catalog}.{bronze_schema}.{dataset}"
    base = {
        "dataset": dataset,
        "table_name": table_name,
        "primary_keys": json.dumps(spec.primary_keys),
        "event_time_column": spec.event_time_column,
    }
    if not spark.catalog.tableExists(table_name):
        results.append(
            {
                **base,
                "status": "missing_table",
                "rows": None,
                "distinct_keys": None,
                "null_key_rows": None,
                "min_event_time": None,
                "max_event_time": None,
                "provenance_status": "unknown",
                "null_record_ids": None,
                "null_payload_hashes": None,
                "null_source_published_at": None,
                "schema_json": None,
            }
        )
        failures.append(f"{dataset}: table does not exist")
        continue

    frame = spark.table(table_name)
    required = set(spec.primary_keys) | {spec.event_time_column}
    missing = sorted(required - set(frame.columns))
    if missing:
        results.append(
            {
                **base,
                "status": "missing_columns",
                "rows": None,
                "distinct_keys": None,
                "null_key_rows": None,
                "min_event_time": None,
                "max_event_time": None,
                "provenance_status": "unknown",
                "null_record_ids": None,
                "null_payload_hashes": None,
                "null_source_published_at": None,
                "schema_json": frame.schema.json(),
            }
        )
        failures.append(f"{dataset}: missing columns {missing}")
        continue

    null_key = None
    for key in spec.primary_keys:
        condition = F.col(key).isNull()
        null_key = condition if null_key is None else null_key | condition

    metrics = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.struct(*[F.col(key) for key in spec.primary_keys])).alias(
            "distinct_keys"
        ),
        F.coalesce(F.sum(F.when(null_key, 1).otherwise(0)), F.lit(0)).alias(
            "null_key_rows"
        ),
        F.min(spec.event_time_column).cast("string").alias("min_event_time"),
        F.max(spec.event_time_column).cast("string").alias("max_event_time"),
    ).first()
    status = "ok"
    if metrics.rows != metrics.distinct_keys or metrics.null_key_rows:
        status = "invalid_keys"
        failures.append(
            f"{dataset}: rows={metrics.rows}, distinct_keys={metrics.distinct_keys}, "
            f"null_key_rows={metrics.null_key_rows}"
        )

    provenance_columns = set(EXTENDED_PROVENANCE_COLUMNS)
    present_provenance = provenance_columns.intersection(frame.columns)
    provenance_status = "legacy"
    null_record_ids = None
    null_payload_hashes = None
    null_source_published_at = None
    if present_provenance and present_provenance != provenance_columns:
        provenance_status = "partial"
        status = "invalid_provenance"
        missing_provenance = sorted(provenance_columns - present_provenance)
        failures.append(
            f"{dataset}: partial provenance/v1, missing={missing_provenance}"
        )
    elif present_provenance == provenance_columns:
        provenance_status = "v1"
        provenance_metrics = frame.agg(
            F.sum(F.when(F.col("_source_record_id").isNull(), 1).otherwise(0)).alias(
                "null_record_ids"
            ),
            F.sum(F.when(F.col("_payload_hash").isNull(), 1).otherwise(0)).alias(
                "null_payload_hashes"
            ),
            F.sum(
                F.when(F.col("_source_published_at").isNull(), 1).otherwise(0)
            ).alias("null_source_published_at"),
        ).first()
        null_record_ids = provenance_metrics.null_record_ids or 0
        null_payload_hashes = provenance_metrics.null_payload_hashes or 0
        null_source_published_at = provenance_metrics.null_source_published_at or 0
        if null_record_ids or null_payload_hashes:
            provenance_status = "v1_incomplete"
            status = "invalid_provenance"
            failures.append(
                f"{dataset}: provenance nulls: record_ids={null_record_ids}, "
                f"payload_hashes={null_payload_hashes}"
            )
    results.append(
        {
            **base,
            "status": status,
            "rows": metrics.rows,
            "distinct_keys": metrics.distinct_keys,
            "null_key_rows": metrics.null_key_rows,
            "min_event_time": metrics.min_event_time,
            "max_event_time": metrics.max_event_time,
            "provenance_status": provenance_status,
            "null_record_ids": null_record_ids,
            "null_payload_hashes": null_payload_hashes,
            "null_source_published_at": null_source_published_at,
            "schema_json": frame.schema.json(),
        }
    )

result_schema = """
    dataset string,
    table_name string,
    primary_keys string,
    event_time_column string,
    status string,
    rows long,
    distinct_keys long,
    null_key_rows long,
    min_event_time string,
    max_event_time string,
    provenance_status string,
    null_record_ids long,
    null_payload_hashes long,
    null_source_published_at long,
    schema_json string
"""
result_frame = spark.createDataFrame(results, schema=result_schema)
display(result_frame.orderBy("dataset"))
print("BRONZE_CONTRACT_AUDIT_JSON=" + json.dumps(results, ensure_ascii=False, default=str))

if failures:
    raise RuntimeError("Bronze contract audit failed:\n" + "\n".join(failures))

print("BRONZE_CONTRACT_AUDIT_OK")

