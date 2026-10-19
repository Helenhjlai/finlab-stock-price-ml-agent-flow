# Databricks notebook source
"""Lakeflow Job entry point: crawl due datasets and MERGE them into Delta."""

# COMMAND ----------

import datetime as dt
import os
import sys
import uuid

PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from databricks_src.crawler_registry import get_registry, invoke_crawler, plan_requests
from databricks_src.delta_writer import DeltaWriter, max_event_time
from databricks_src.state_repository import RunRecord, StateRepository

# COMMAND ----------

default_today = dt.date.today()
default_bootstrap = default_today - dt.timedelta(days=7)

dbutils.widgets.text("catalog", "finlab", "01 catalog")
dbutils.widgets.text("bronze_schema", "bronze", "02 bronze schema")
dbutils.widgets.text("ops_schema", "ops", "03 ops schema")
dbutils.widgets.text("datasets", "all", "04 datasets (all or comma-separated)")
dbutils.widgets.text("run_date", default_today.isoformat(), "05 logical run date")
dbutils.widgets.text("bootstrap_start", default_bootstrap.isoformat(), "06 start if state is missing")

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
ops_schema = dbutils.widgets.get("ops_schema").strip()
selected_value = dbutils.widgets.get("datasets").strip()
today = dt.date.fromisoformat(dbutils.widgets.get("run_date"))
bootstrap_start = dt.date.fromisoformat(dbutils.widgets.get("bootstrap_start"))

if bootstrap_start > today:
    raise ValueError("bootstrap_start cannot be after run_date")

selected = None
if selected_value.lower() != "all":
    selected = [item.strip() for item in selected_value.split(",") if item.strip()]

registry = get_registry(selected)
spark.conf.set("spark.sql.session.timeZone", "Asia/Taipei")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

writer = DeltaWriter(spark, catalog=catalog, schema=bronze_schema)
state_repo = StateRepository(spark, catalog=catalog, ops_schema=ops_schema)
state_repo.ensure_tables()

print("run date:", today)
print("bootstrap start:", bootstrap_start)
print("datasets:", [spec.name for spec in registry])

# COMMAND ----------

def as_midnight(value: dt.date | None) -> dt.datetime | None:
    return dt.datetime.combine(value, dt.time.min) if value else None


run_id = str(uuid.uuid4())
failures: list[str] = []

for spec in registry:
    started_at = dt.datetime.now()
    state = state_repo.get_state(spec.name)
    requests = plan_requests(
        spec,
        max_event_time=state.max_event_time if state else None,
        today=today,
        bootstrap_start=bootstrap_start,
    )
    print(f"\n[{spec.name}] requests={requests}")

    rows_fetched = 0
    rows_merged = 0
    batch_watermark: dt.datetime | None = None

    try:
        for requested_date in requests:
            raw = invoke_crawler(spec, requested_date)
            if raw is None or len(raw) == 0:
                print(f"[{spec.name}] {requested_date}: empty result")
                continue
            if isinstance(raw, dict):
                raise NotImplementedError(
                    f"{spec.name}: multi-table crawler output needs a dedicated adapter"
                )

            rows_fetched += len(raw)
            watermark = max_event_time(raw)
            if watermark is not None:
                batch_watermark = max(batch_watermark, watermark) if batch_watermark else watermark

            result = writer.merge_pandas(
                raw,
                dataset=spec.name,
                run_id=run_id,
                source=f"finlab.crawler.{spec.crawler_function}",
            )
            rows_merged += result.input_rows
            print(
                f"[{spec.name}] {requested_date}: fetched={len(raw):,}, "
                f"merged={result.input_rows:,}"
            )

        finished_at = dt.datetime.now()
        state_repo.mark_success(
            dataset=spec.name,
            finished_at=finished_at,
            batch_max_event_time=batch_watermark,
            row_count=rows_merged,
        )
        state_repo.append_run(
            RunRecord(
                run_id=run_id,
                dataset=spec.name,
                started_at=started_at,
                finished_at=finished_at,
                requested_from=as_midnight(requests[0]) if requests else None,
                requested_to=as_midnight(requests[-1]) if requests else None,
                rows_fetched=rows_fetched,
                rows_merged=rows_merged,
                status="success",
            )
        )
    except Exception as exc:
        finished_at = dt.datetime.now()
        state_repo.mark_failure(dataset=spec.name, finished_at=finished_at, error=exc)
        state_repo.append_run(
            RunRecord(
                run_id=run_id,
                dataset=spec.name,
                started_at=started_at,
                finished_at=finished_at,
                requested_from=as_midnight(requests[0]) if requests else None,
                requested_to=as_midnight(requests[-1]) if requests else None,
                rows_fetched=rows_fetched,
                rows_merged=rows_merged,
                status="failed",
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
        )
        message = f"{spec.name}: {type(exc).__name__}: {exc}"
        failures.append(message)
        print("FAILED:", message)

if failures:
    raise RuntimeError("Daily update failures:\n" + "\n".join(failures))

print("Daily update completed. run_id=", run_id)

