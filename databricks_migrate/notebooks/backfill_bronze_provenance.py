# Databricks notebook source
"""One-table-at-a-time migration to Bronze provenance/v1.

The default is a read-only dry run. Applying the migration adds three nullable
columns and backfills deterministic record/payload hashes. A source publication
timestamp is never invented: historical values remain NULL when the source did
not provide one.
"""

# COMMAND ----------

import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from databricks_src.bronze_provenance import (
    EXTENDED_PROVENANCE_COLUMNS,
    EXTENDED_PROVENANCE_TYPES,
    payload_hash_expression,
    source_record_id_expression,
)
from databricks_src.crawler_registry import HISTORICAL_TABLES, get_output_spec

# COMMAND ----------

dbutils.widgets.text("catalog", "finlab", "01 catalog")
dbutils.widgets.text("bronze_schema", "bronze", "02 bronze schema")
dbutils.widgets.text("dataset", "price", "03 exactly one dataset")
dbutils.widgets.dropdown("apply_changes", "false", ["false", "true"], "04 apply")
dbutils.widgets.text("confirmation", "", "05 type BACKFILL_PROVENANCE")

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
dataset = dbutils.widgets.get("dataset").strip()
apply_changes = dbutils.widgets.get("apply_changes").lower() == "true"
confirmation = dbutils.widgets.get("confirmation").strip()

if dataset not in HISTORICAL_TABLES:
    raise ValueError(f"Unknown dataset: {dataset!r}")
if apply_changes and confirmation != "BACKFILL_PROVENANCE":
    raise ValueError("Applying migration requires confirmation=BACKFILL_PROVENANCE")

spec = get_output_spec(dataset)
table_name = f"{catalog}.{bronze_schema}.{dataset}"
if not spark.catalog.tableExists(table_name):
    raise RuntimeError(f"Bronze table does not exist: {table_name}")

# COMMAND ----------

frame = spark.table(table_name)
required = set(spec.primary_keys) | {spec.event_time_column}
missing_required = sorted(required - set(frame.columns))
if missing_required:
    raise RuntimeError(f"Missing contract columns: {missing_required}")

null_key = None
for key in spec.primary_keys:
    condition = F.col(key).isNull()
    null_key = condition if null_key is None else null_key | condition

quality = frame.agg(
    F.count("*").alias("rows"),
    F.countDistinct(F.struct(*[F.col(key) for key in spec.primary_keys])).alias(
        "distinct_keys"
    ),
    F.coalesce(F.sum(F.when(null_key, 1).otherwise(0)), F.lit(0)).alias(
        "null_key_rows"
    ),
).first()
if quality.rows != quality.distinct_keys or quality.null_key_rows:
    raise RuntimeError(
        f"Key audit failed: rows={quality.rows}, distinct={quality.distinct_keys}, "
        f"null_key_rows={quality.null_key_rows}"
    )

missing_provenance = [
    column for column in EXTENDED_PROVENANCE_COLUMNS if column not in frame.columns
]
print("table:", table_name)
print("rows:", quality.rows)
print("primary keys:", spec.primary_keys)
print("missing provenance columns:", missing_provenance)
print("apply changes:", apply_changes)

if not apply_changes:
    print("BRONZE_PROVENANCE_DRY_RUN_OK")
    dbutils.notebook.exit("BRONZE_PROVENANCE_DRY_RUN_OK")

# COMMAND ----------

if missing_provenance:
    definitions = ", ".join(
        f"`{column}` {EXTENDED_PROVENANCE_TYPES[column]}"
        for column in missing_provenance
    )
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({definitions})")

frame = spark.table(table_name)
source = frame.select(
    *spec.primary_keys,
    source_record_id_expression(dataset, spec.primary_keys).alias(
        "_new_source_record_id"
    ),
    payload_hash_expression(frame.columns).alias("_new_payload_hash"),
)
condition = " AND ".join(
    f"target.`{key}` = source.`{key}`" for key in spec.primary_keys
)
target = DeltaTable.forName(spark, table_name)
(
    target.alias("target")
    .merge(source.alias("source"), condition)
    .whenMatchedUpdate(
        set={
            "_source_record_id": "source._new_source_record_id",
            "_payload_hash": "source._new_payload_hash",
        }
    )
    .execute()
)

spark.sql(
    f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
    "'finlab.bronze_provenance_version' = 'v1')"
)

post = spark.table(table_name).agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("_source_record_id").isNull(), 1).otherwise(0)).alias(
        "null_record_ids"
    ),
    F.sum(F.when(F.col("_payload_hash").isNull(), 1).otherwise(0)).alias(
        "null_payload_hashes"
    ),
    F.sum(F.when(F.col("_source_published_at").isNull(), 1).otherwise(0)).alias(
        "null_source_published_at"
    ),
).first()
print("post migration:", post.asDict())
if post.rows != quality.rows or post.null_record_ids or post.null_payload_hashes:
    raise RuntimeError("Provenance backfill validation failed")

print("BRONZE_PROVENANCE_BACKFILL_OK")

