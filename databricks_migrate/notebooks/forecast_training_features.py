# Databricks notebook source
# MAGIC %md
# MAGIC # Materialize `finlab.gold.forecast_training_features`
# MAGIC
# MAGIC This notebook creates the monthly point-in-time feature table consumed by
# MAGIC `train_forecast_models.py`.  It uses only values known on each observation
# MAGIC date, adjusts prices with the migrated corporate-action ratios when those
# MAGIC tables are available, and builds the next-period cross-sectional rank label.

# COMMAND ----------

import json
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "finlab", "01 catalog")
dbutils.widgets.text("bronze_schema", "bronze", "02 source schema")
dbutils.widgets.text("gold_schema", "gold", "03 target schema")
dbutils.widgets.text(
    "output_table", "forecast_training_features", "04 target table"
)
dbutils.widgets.text("registry_table", "data_contract_registry", "05 contract registry")
dbutils.widgets.text("start_date", "2013-01-01", "06 first observation date")
dbutils.widgets.text("feature_delay_days", "5", "07 days after revenue release date")
dbutils.widgets.dropdown("adjust_prices", "true", ["true", "false"], "08 corporate actions")
dbutils.widgets.text("minimum_periods", "48", "09 quality gate: periods")
dbutils.widgets.dropdown(
    "require_all_adjustment_sources", "true", ["true", "false"], "10 strict adjustment inputs"
)

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()
output_name = dbutils.widgets.get("output_table").strip()
registry_name = dbutils.widgets.get("registry_table").strip()
start_date = dbutils.widgets.get("start_date").strip()
feature_delay_days = int(dbutils.widgets.get("feature_delay_days"))
adjust_prices = dbutils.widgets.get("adjust_prices").lower() == "true"
minimum_periods = int(dbutils.widgets.get("minimum_periods"))
require_all_adjustment_sources = (
    dbutils.widgets.get("require_all_adjustment_sources").lower() == "true"
)

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for value in (catalog, bronze_schema, gold_schema, output_name, registry_name):
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
if feature_delay_days < 0:
    raise ValueError("feature_delay_days cannot be negative")

source_prefix = f"{catalog}.{bronze_schema}"
output_table = f"{catalog}.{gold_schema}.{output_name}"
registry_table = f"{catalog}.{gold_schema}.{registry_name}"
price_table = f"{source_prefix}.price"
pe_table = f"{source_prefix}.pe"
revenue_table = f"{source_prefix}.monthly_report"

for table_name in (price_table, pe_table, revenue_table):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required source table does not exist: {table_name}")

print("sources:", price_table, pe_table, revenue_table)
print("target:", output_table)

# COMMAND ----------

def resolve_column(frame: DataFrame, logical_name: str, candidates: list[str]) -> str:
    matches = [candidate for candidate in candidates if candidate in frame.columns]
    if not matches:
        raise KeyError(
            f"Cannot find {logical_name!r}; tried {candidates}. "
            f"Available columns: {frame.columns}"
        )
    return matches[0]


def numeric(column_name: str):
    """Convert legacy crawler strings such as '1,234' and '--' to double."""
    return F.regexp_replace(F.trim(F.col(f"`{column_name}`").cast("string")), ",", "").cast(
        "double"
    )


def source_table(name: str) -> DataFrame:
    return (
        spark.table(name)
        .withColumn("stock_id", F.col("stock_id").cast("string"))
        .withColumn("date", F.to_timestamp("date"))
        .where(F.col("stock_id").rlike(r"^[0-9]{4}$") & F.col("date").isNotNull())
    )


price_source = source_table(price_table)
open_column = resolve_column(price_source, "open", ["開盤價", "開盤"])
high_column = resolve_column(price_source, "high", ["最高價", "最高"])
low_column = resolve_column(price_source, "low", ["最低價", "最低"])
close_column = resolve_column(price_source, "close", ["收盤價", "收盤"])

# Keep enough warm-up history for acc240, which reads 480 trading rows back.
history_start = F.date_sub(F.to_date(F.lit(start_date)), 800)
prices = (
    price_source.where(F.to_date("date") >= history_start)
    .select(
        "stock_id",
        "date",
        numeric(open_column).alias("open"),
        numeric(high_column).alias("high"),
        numeric(low_column).alias("low"),
        numeric(close_column).alias("close"),
    )
    .where(F.col("open").gt(0) & F.col("close").gt(0))
    .dropDuplicates(["stock_id", "date"])
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Corporate-action adjustment
# MAGIC
# MAGIC The legacy `Data.get_adj()` multiplies price by the cumulative product of
# MAGIC divide/capital-reduction ratios.  The timeline below reproduces that idea,
# MAGIC including ratio events that fall on non-trading days.

# COMMAND ----------

ratio_specs = [
    ("otc_cap_reduction", "otc_cap_divide_ratio"),
    ("twse_cap_reduction", "twse_cap_divide_ratio"),
    ("otc_divide_ratio", "otc_divide_ratio"),
    ("twse_divide_ratio", "twse_divide_ratio"),
]
ratio_frames: list[DataFrame] = []
missing_ratio_tables: list[str] = []
empty_ratio_tables: list[str] = []
if adjust_prices:
    for table_suffix, ratio_column in ratio_specs:
        table_name = f"{source_prefix}.{table_suffix}"
        if not spark.catalog.tableExists(table_name):
            print(f"WARNING: adjustment source missing, skipped: {table_name}")
            missing_ratio_tables.append(table_name)
            continue
        frame = source_table(table_name)
        actual_column = resolve_column(frame, ratio_column, [ratio_column])
        usable_ratios = frame.select(
            "stock_id", "date", numeric(actual_column).alias("adjustment_ratio")
        ).where(F.col("adjustment_ratio").gt(0))
        if require_all_adjustment_sources and usable_ratios.limit(1).count() == 0:
            empty_ratio_tables.append(table_name)
        ratio_frames.append(
            usable_ratios
        )

if adjust_prices and (missing_ratio_tables or empty_ratio_tables) and require_all_adjustment_sources:
    raise RuntimeError(
        "Corporate-action source check failed; missing="
        + repr(missing_ratio_tables)
        + ", empty="
        + repr(empty_ratio_tables)
    )

if ratio_frames:
    events = ratio_frames[0]
    for frame in ratio_frames[1:]:
        events = events.unionByName(frame)
    events = events.groupBy("stock_id", "date").agg(
        F.exp(F.sum(F.log("adjustment_ratio"))).alias("adjustment_ratio")
    )
    if require_all_adjustment_sources and events.limit(1).count() == 0:
        raise RuntimeError("Corporate-action sources exist but contain no usable ratio events")

    price_timeline = prices.select(
        "stock_id",
        "date",
        F.lit(1).alias("event_order"),
        "open",
        "high",
        "low",
        "close",
        F.lit(1.0).alias("adjustment_ratio"),
        F.lit(True).alias("is_price"),
    )
    event_timeline = events.select(
        "stock_id",
        "date",
        F.lit(0).alias("event_order"),
        F.lit(None).cast("double").alias("open"),
        F.lit(None).cast("double").alias("high"),
        F.lit(None).cast("double").alias("low"),
        F.lit(None).cast("double").alias("close"),
        "adjustment_ratio",
        F.lit(False).alias("is_price"),
    )
    cumulative = Window.partitionBy("stock_id").orderBy("date", "event_order").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    prices = (
        price_timeline.unionByName(event_timeline)
        .withColumn("adjustment_factor", F.exp(F.sum(F.log("adjustment_ratio")).over(cumulative)))
        .where("is_price")
        .select(
            "stock_id",
            "date",
            (F.col("open") * F.col("adjustment_factor")).alias("open"),
            (F.col("high") * F.col("adjustment_factor")).alias("high"),
            (F.col("low") * F.col("adjustment_factor")).alias("low"),
            (F.col("close") * F.col("adjustment_factor")).alias("close"),
        )
    )
elif adjust_prices:
    print("WARNING: no adjustment tables were found; using raw prices")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Daily technical and valuation features

# COMMAND ----------

valuation_source = source_table(pe_table)
pe_column = resolve_column(valuation_source, "price/earnings", ["本益比", "本益比(倍)"])
pb_column = resolve_column(valuation_source, "price/book", ["股價淨值比", "股價淨值比(倍)"])
valuation = (
    valuation_source.select(
        "stock_id",
        "date",
        numeric(pe_column).alias("pe"),
        numeric(pb_column).alias("pb"),
    )
    .dropDuplicates(["stock_id", "date"])
)

daily = prices.join(valuation, ["stock_id", "date"], "left")
ordered = Window.partitionBy("stock_id").orderBy("date")
daily = daily.withColumn("entry_open", F.lead("open", 1).over(ordered))

for period in (5, 10, 20, 60, 120, 240):
    rolling = ordered.rowsBetween(-(period - 1), 0)
    rolling_mean = F.avg("close").over(rolling)
    rolling_low = F.min("close").over(rolling)
    rolling_high = F.max("close").over(rolling)
    lag_n = F.lag("close", period).over(ordered)
    lag_2n = F.lag("close", 2 * period).over(ordered)
    daily = (
        daily.withColumn(f"bias{period}", F.col("close") / rolling_mean)
        .withColumn(f"acc{period}", lag_n / (lag_2n + F.col("close")) * F.lit(2.0))
        .withColumn(
            f"rsv{period}",
            F.when(rolling_high != rolling_low, (F.col("close") - rolling_low) / (rolling_high - rolling_low)),
        )
    )

technical_columns = [
    *(f"bias{period}" for period in (5, 10, 20, 60, 120, 240)),
    *(f"acc{period}" for period in (5, 10, 20, 60, 120, 240)),
    *(f"rsv{period}" for period in (5, 10, 20, 60, 120, 240)),
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Monthly point-in-time observations and labels
# MAGIC
# MAGIC Monthly revenue is dated at its release checkpoint, then delayed by the
# MAGIC widget value (default five days, matching the original notebook).  Daily
# MAGIC values are carried forward only from dates at or before the observation.

# COMMAND ----------

revenue_source = source_table(revenue_table)
revenue_column = resolve_column(revenue_source, "monthly revenue", ["當月營收"])
revenue = (
    revenue_source.select(
        "stock_id",
        F.date_add(F.to_date("date"), feature_delay_days).cast("timestamp").alias("date"),
        numeric(revenue_column).alias("revenue"),
    )
    .where(F.col("revenue").gt(0))
    .dropDuplicates(["stock_id", "date"])
)
monthly_order = Window.partitionBy("stock_id").orderBy("date")
revenue = revenue.withColumn("revenue_growth", F.col("revenue") / F.lag("revenue", 1).over(monthly_order))
for lag in range(1, 10):
    revenue = revenue.withColumn(f"mom{lag}", F.lag("revenue_growth", lag).over(monthly_order))

market_feature_columns = [
    "pe",
    "pb",
    *technical_columns,
]
feature_columns = [
    *(f"mom{lag}" for lag in range(1, 10)),
    *market_feature_columns,
]
carry_columns = ["entry_open", *market_feature_columns]

daily_timeline = daily.select(
    "stock_id",
    "date",
    F.lit(0).alias("event_order"),
    F.lit(False).alias("is_observation"),
    *[F.col(name).cast("double").alias(name) for name in carry_columns],
    *[F.lit(None).cast("double").alias(f"revenue_{name}") for name in [f"mom{i}" for i in range(1, 10)]],
)
observation_timeline = revenue.select(
    "stock_id",
    "date",
    F.lit(1).alias("event_order"),
    F.lit(True).alias("is_observation"),
    *[F.lit(None).cast("double").alias(name) for name in carry_columns],
    *[F.col(f"mom{i}").cast("double").alias(f"revenue_mom{i}") for i in range(1, 10)],
)

asof_window = Window.partitionBy("stock_id").orderBy("date", "event_order").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
timeline = daily_timeline.unionByName(observation_timeline)
for name in carry_columns:
    timeline = timeline.withColumn(name, F.last(name, ignorenulls=True).over(asof_window))

observations = timeline.where("is_observation")
for lag in range(1, 10):
    observations = observations.withColumn(f"mom{lag}", F.col(f"revenue_mom{lag}"))

observations = observations.select("date", "stock_id", *feature_columns, "entry_open")
next_month = Window.partitionBy("stock_id").orderBy("date")
observations = observations.withColumn("next_entry_open", F.lead("entry_open", 1).over(next_month))
observations = observations.withColumn("return", F.col("next_entry_open") / F.col("entry_open"))

cross_section = Window.partitionBy("date").orderBy("return")
training = (
    observations.where(
        (F.to_date("date") >= F.to_date(F.lit(start_date)))
        & F.col("return").isNotNull()
        & ~F.isnan("return")
        & F.col("return").gt(0)
    )
    .withColumn("rank", F.percent_rank().over(cross_section))
    .select("date", "stock_id", *feature_columns, "return", "rank")
    .withColumn("_feature_version", F.lit("v1"))
    .withColumn("_materialized_at", F.current_timestamp())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quality gates and idempotent publish

# COMMAND ----------

quality = training.agg(
    F.count("*").alias("rows"),
    F.countDistinct(F.struct("date", "stock_id")).alias("distinct_keys"),
    F.countDistinct("date").alias("periods"),
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date"),
    F.min("rank").alias("min_rank"),
    F.max("rank").alias("max_rank"),
).first()

if quality.rows == 0:
    raise RuntimeError("Feature materialization produced no rows")
if quality.rows != quality.distinct_keys:
    raise RuntimeError(
        f"Duplicate date/stock_id keys: rows={quality.rows}, distinct={quality.distinct_keys}"
    )
if quality.periods < minimum_periods:
    raise RuntimeError(
        f"Only {quality.periods} periods; minimum_periods={minimum_periods}. "
        "Load more bronze history or lower the gate deliberately."
    )
if quality.min_rank < 0 or quality.max_rank > 1:
    raise RuntimeError(f"rank outside [0, 1]: {quality.min_rank}, {quality.max_rank}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
(
    training.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable(output_table)
)
spark.sql(
    f"ALTER TABLE {output_table} SET TBLPROPERTIES ("
    "'finlab.data_product' = 'forecast_training_features', "
    "'finlab.contract_version' = 'v1', "
    "'finlab.owner' = 'finlab-ml')"
)

registry_update = spark.createDataFrame(
    [
        {
            "product_name": "forecast_training_features",
            "contract_version": "v1",
            "table_name": output_table,
            "grain": "one row per observation date and stock_id",
            "primary_key_json": json.dumps(["date", "stock_id"]),
            "event_time_column": "date",
            "available_time_column": "date",
            "freshness_sla": "monthly before candidate model training",
            "owner": "finlab-ml",
            "status": "active",
            "schema_json": json.dumps(
                {
                    "required": {
                        "date": "timestamp",
                        "stock_id": "string",
                        "return": "double",
                        "rank": "double",
                    },
                    "contains_future_label": True,
                    "consumer": "training_jobs_only",
                },
                ensure_ascii=False,
            ),
            "description": "Point-in-time features with future labels for tuning and training only",
        }
    ]
).withColumn("updated_at", F.current_timestamp())

if not spark.catalog.tableExists(registry_table):
    registry_update.write.format("delta").saveAsTable(registry_table)
else:
    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, registry_table)
        .alias("target")
        .merge(
            registry_update.alias("source"),
            "target.product_name = source.product_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
spark.sql(
    f"ALTER TABLE {registry_table} SET TBLPROPERTIES ("
    "'finlab.data_product' = 'data_contract_registry', "
    "'finlab.contract_version' = 'v1', "
    "'finlab.owner' = 'finlab-data')"
)

print(quality.asDict())
display(spark.table(output_table).orderBy(F.col("date").desc(), "stock_id").limit(100))

# COMMAND ----------
# MAGIC %md
# MAGIC The last source month is intentionally absent because its next-period label is
# MAGIC unknown.  Run `tune_forecast_models.py` next; use its `best_params.json` with
# MAGIC `train_forecast_models.py --params-file ...` for the final untouched-test run.
