# Databricks notebook source
"""One-time load: managed Volume Parquet files -> bronze Delta tables."""

# COMMAND ----------

import datetime as dt
import os
import sys
import uuid

from pyspark.sql import functions as F

# The deployed layout is /Workspace/Users/<email>/finlab_project/{notebooks,databricks_src,finlab}.
PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from databricks_src.crawler_registry import HISTORICAL_TABLES
from databricks_src.delta_writer import DeltaWriter
from databricks_src.state_repository import RunRecord, StateRepository

# COMMAND ----------

dbutils.widgets.text("catalog", "finlab", "01 catalog")
dbutils.widgets.text("bronze_schema", "bronze", "02 bronze schema")
dbutils.widgets.text("ops_schema", "ops", "03 ops schema")
dbutils.widgets.text("landing_volume", "landing", "04 landing volume")
dbutils.widgets.text("datasets", "all", "05 datasets (all or comma-separated)")

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
ops_schema = dbutils.widgets.get("ops_schema").strip()
landing_volume = dbutils.widgets.get("landing_volume").strip()
selected_value = dbutils.widgets.get("datasets").strip()

spark.conf.set("spark.sql.session.timeZone", "Asia/Taipei")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{ops_schema}")

writer = DeltaWriter(spark, catalog=catalog, schema=bronze_schema)
state_repo = StateRepository(spark, catalog=catalog, ops_schema=ops_schema)
state_repo.ensure_tables()

if selected_value.lower() == "all":
    datasets = HISTORICAL_TABLES
else:
    datasets = tuple(item.strip() for item in selected_value.split(",") if item.strip())
    unknown = set(datasets) - set(HISTORICAL_TABLES)
    if unknown:
        raise ValueError(f"Unknown historical table(s): {sorted(unknown)}")

landing_root = f"/Volumes/{catalog}/{ops_schema}/{landing_volume}"
print("landing root:", landing_root)
print("datasets:", datasets)

# COMMAND ----------

run_id = str(uuid.uuid4())
failures: list[str] = []

for dataset in datasets:
    started_at = dt.datetime.now()
    parquet_path = f"{landing_root}/{dataset}.parquet"
    print(f"\n[{dataset}] reading {parquet_path}")

    try:
        source_df = spark.read.parquet(parquet_path)
        if "stock_id" not in source_df.columns or "date" not in source_df.columns:
            raise KeyError(f"{dataset}: Parquet must contain stock_id and date")

        source_df = (
            source_df
            .withColumn("stock_id", F.col("stock_id").cast("string"))
            .withColumn("date", F.col("date").cast("timestamp"))
        )
        bounds = source_df.agg(
            F.min("date").alias("min_date"),
            F.max("date").alias("max_date"),
        ).first()

        result = writer.merge_spark(
            source_df,
            dataset=dataset,
            run_id=run_id,
            source="historical_pickle",
        )
        finished_at = dt.datetime.now()
        state_repo.mark_success(
            dataset=dataset,
            finished_at=finished_at,
            batch_max_event_time=bounds["max_date"],
            row_count=result.input_rows,
        )
        state_repo.append_run(
            RunRecord(
                run_id=run_id,
                dataset=dataset,
                started_at=started_at,
                finished_at=finished_at,
                requested_from=bounds["min_date"],
                requested_to=bounds["max_date"],
                rows_fetched=result.input_rows,
                rows_merged=result.input_rows,
                status="success",
            )
        )
        print(
            f"[{dataset}] rows={result.input_rows:,}, "
            f"created={result.created_table}, max_date={bounds['max_date']}"
        )
    except Exception as exc:
        finished_at = dt.datetime.now()
        state_repo.mark_failure(dataset=dataset, finished_at=finished_at, error=exc)
        state_repo.append_run(
            RunRecord(
                run_id=run_id,
                dataset=dataset,
                started_at=started_at,
                finished_at=finished_at,
                requested_from=None,
                requested_to=None,
                rows_fetched=0,
                rows_merged=0,
                status="failed",
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
        )
        failures.append(f"{dataset}: {type(exc).__name__}: {exc}")
        print(f"[{dataset}] FAILED: {exc}")

if failures:
    raise RuntimeError("Historical load failures:\n" + "\n".join(failures))

print("Historical load completed. run_id=", run_id)

