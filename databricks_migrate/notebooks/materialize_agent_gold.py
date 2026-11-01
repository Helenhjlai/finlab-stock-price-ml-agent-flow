# Databricks notebook source
# MAGIC %md
# MAGIC # Materialize FinLab Agent core Gold data products
# MAGIC
# MAGIC This notebook publishes the two stable v1 contracts consumed by
# MAGIC `finlab_agent`: `daily_prices` and `stock_factors`.  It also upserts
# MAGIC contract metadata into `data_contract_registry`.
# MAGIC
# MAGIC The first run performs a full, idempotent rebuild.  Later daily runs may use
# MAGIC an overlapping incremental MERGE; periodic full rebuilds remain required because
# MAGIC an old corporate-action revision can change historical adjusted prices.

# COMMAND ----------

import datetime as dt
import json
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "finlab", "01 catalog")
dbutils.widgets.text("bronze_schema", "bronze", "02 source schema")
dbutils.widgets.text("gold_schema", "gold", "03 target schema")
dbutils.widgets.text("price_table", "daily_prices", "04 price product")
dbutils.widgets.text("factor_table", "stock_factors", "05 factor product")
dbutils.widgets.text("registry_table", "data_contract_registry", "06 contract registry")
dbutils.widgets.text("start_date", "2013-01-01", "07 first price date")
dbutils.widgets.text("factor_delay_days", "5", "08 conservative availability delay")
dbutils.widgets.dropdown("adjust_prices", "true", ["true", "false"], "09 corporate actions")
dbutils.widgets.text("minimum_price_rows", "10000", "10 price quality gate")
dbutils.widgets.text("minimum_factor_periods", "24", "11 factor quality gate")
dbutils.widgets.dropdown("refresh_mode", "full", ["full", "incremental"], "12 refresh mode")
dbutils.widgets.text("incremental_lookback_days", "14", "13 incremental overlap")
dbutils.widgets.dropdown(
    "require_all_adjustment_sources", "true", ["true", "false"], "14 strict adjustment inputs"
)

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()
price_product = dbutils.widgets.get("price_table").strip()
factor_product = dbutils.widgets.get("factor_table").strip()
registry_product = dbutils.widgets.get("registry_table").strip()
start_date = dbutils.widgets.get("start_date").strip()
factor_delay_days = int(dbutils.widgets.get("factor_delay_days"))
adjust_prices = dbutils.widgets.get("adjust_prices").lower() == "true"
minimum_price_rows = int(dbutils.widgets.get("minimum_price_rows"))
minimum_factor_periods = int(dbutils.widgets.get("minimum_factor_periods"))
refresh_mode = dbutils.widgets.get("refresh_mode").strip().lower()
incremental_lookback_days = int(dbutils.widgets.get("incremental_lookback_days"))
require_all_adjustment_sources = (
    dbutils.widgets.get("require_all_adjustment_sources").lower() == "true"
)

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for value in (
    catalog,
    bronze_schema,
    gold_schema,
    price_product,
    factor_product,
    registry_product,
):
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
if factor_delay_days < 0:
    raise ValueError("factor_delay_days cannot be negative")
if minimum_price_rows < 1 or minimum_factor_periods < 1:
    raise ValueError("quality gates must be positive")
if refresh_mode not in {"full", "incremental"}:
    raise ValueError("refresh_mode must be full or incremental")
if incremental_lookback_days < 1:
    raise ValueError("incremental_lookback_days must be positive")

bronze_prefix = f"{catalog}.{bronze_schema}"
gold_prefix = f"{catalog}.{gold_schema}"
price_output = f"{gold_prefix}.{price_product}"
factor_output = f"{gold_prefix}.{factor_product}"
registry_output = f"{gold_prefix}.{registry_product}"

spark.conf.set("spark.sql.session.timeZone", "Asia/Taipei")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

print("price output:", price_output)
print("factor output:", factor_output)
print("registry:", registry_output)

# COMMAND ----------

def require_table(table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required source table does not exist: {table_name}")


def source_table(table_name: str) -> DataFrame:
    require_table(table_name)
    return (
        spark.table(table_name)
        # Bronze keeps the crawler's compound value, for example
        # "2330 台積電" or "00679B 元大美債20年".  Keep it for the
        # dataset-specific identity parser below; do not filter it here.
        .withColumn("_raw_stock_id", F.trim(F.col("stock_id").cast("string")))
        .drop("stock_id")
        .withColumn("date", F.to_timestamp("date"))
        .where(F.col("_raw_stock_id").isNotNull() & F.col("date").isNotNull())
    )


def resolve_column(frame: DataFrame, logical_name: str, candidates: list[str]) -> str:
    matches = [candidate for candidate in candidates if candidate in frame.columns]
    if not matches:
        raise KeyError(
            f"Cannot find {logical_name!r}; tried {candidates}. "
            f"Available columns: {frame.columns}"
        )
    return matches[0]


def numeric(column_name: str):
    return F.regexp_replace(
        F.trim(F.col(f"`{column_name}`").cast("string")), ",", ""
    ).cast("double")


# Public-company shares use four numeric digits.  ETFs start with 00 and may
# use four to six numeric digits, or five numeric digits plus an official type
# suffix.  Keeping the suffix in stock_id prevents collisions such as an
# official "00980B" being collapsed onto a numeric security stem.
COMMON_STOCK_PATTERN = r"^[0-9]{4}$"
ETF_PATTERN = r"^00(?:[0-9]{2,4}|[0-9]{3}[ABCDKLMRSTUV])$"

ETF_TYPE_LABELS = {
    "PLAIN": "一般型 ETF（無英文字尾）",
    "A": "股票型主動式 ETF",
    "D": "債券型主動式 ETF",
    "T": "多資產主動式 ETF",
    "B": "債券型 ETF",
    "C": "債券型 ETF－外幣受益憑證",
    "K": "股票型 ETF－外幣受益憑證",
    "L": "槓桿型 ETF",
    "M": "槓桿型 ETF－外幣受益憑證",
    "R": "反向型 ETF",
    "S": "反向型 ETF－外幣受益憑證",
    "U": "期貨型 ETF",
    "V": "期貨型 ETF－外幣受益憑證",
}
ETF_TYPE_MAP = F.create_map(
    *[item for key, value in ETF_TYPE_LABELS.items() for item in (F.lit(key), F.lit(value))]
)


def normalize_security_identity(frame: DataFrame) -> DataFrame:
    """Split crawler compound stock_id and keep supported shares / ETFs only.

    Examples:
      2330 台積電            -> stock_id=2330,  name=台積電, is_etf=false
      0050 元大台灣50        -> stock_id=0050,  type=PLAIN
      00679B 元大美債20年    -> stock_id=00679B, numeric=00679, type=B

    `stock_id_numeric` exists for numeric grouping/search.  Joins and primary
    keys must continue to use the official full `stock_id`.
    """
    normalized_raw = F.trim(F.regexp_replace(F.col("_raw_stock_id"), r"\s+", " "))
    official_code = F.upper(
        F.regexp_extract(normalized_raw, r"^([0-9]{4,6}[A-Za-z]?)", 1)
    )
    numeric_code = F.regexp_extract(official_code, r"^([0-9]{4,6})", 1)
    suffix = F.regexp_extract(official_code, r"([A-Z])$", 1)
    name = F.trim(
        F.regexp_replace(normalized_raw, r"^[0-9]{4,6}[A-Za-z]?\s*", "")
    )
    is_etf = official_code.startswith("00")
    etf_type_code = (
        F.when(~is_etf, F.lit(None).cast("string"))
        .when(suffix != "", suffix)
        .otherwise(F.lit("PLAIN"))
    )
    etf_type = F.when(
        is_etf,
        F.coalesce(
            F.element_at(ETF_TYPE_MAP, etf_type_code),
            F.concat(F.lit("其他 ETF 類型（"), etf_type_code, F.lit("）")),
        ),
    ).otherwise(F.lit(None).cast("string"))

    return (
        frame.withColumn("stock_id", official_code)
        .withColumn("stock_id_numeric", numeric_code)
        .withColumn("name", F.when(name != "", name).otherwise(F.lit(None)))
        .withColumn("is_etf", is_etf)
        .withColumn("etf_type_code", etf_type_code)
        .withColumn("etf_type", etf_type)
        .where(
            F.col("stock_id").rlike(COMMON_STOCK_PATTERN)
            | F.col("stock_id").rlike(ETF_PATTERN)
        )
    )


def validate_security_identity_parser() -> None:
    """Tiny executable Spark example and fail-fast parser contract."""
    examples = spark.createDataFrame(
        [
            ("2330 台積電", dt.datetime(2026, 1, 2)),
            ("0050   元大台灣50", dt.datetime(2026, 1, 2)),
            ("00679B 元大美債20年", dt.datetime(2026, 1, 2)),
            ("00980A 主動野村臺灣優選", dt.datetime(2026, 1, 2)),
            ("台股指數", dt.datetime(2026, 1, 2)),
        ],
        ["_raw_stock_id", "date"],
    )
    actual = {
        row.stock_id: (
            row.stock_id_numeric,
            row.name,
            row.is_etf,
            row.etf_type_code,
        )
        for row in normalize_security_identity(examples)
        .select("stock_id", "stock_id_numeric", "name", "is_etf", "etf_type_code")
        .collect()
    }
    expected = {
        "2330": ("2330", "台積電", False, None),
        "0050": ("0050", "元大台灣50", True, "PLAIN"),
        "00679B": ("00679", "元大美債20年", True, "B"),
        "00980A": ("00980", "主動野村臺灣優選", True, "A"),
    }
    if actual != expected:
        raise RuntimeError(
            f"security identity parser self-check failed: actual={actual!r}"
        )


validate_security_identity_parser()


def write_contract_table(
    frame: DataFrame,
    table_name: str,
    product: str,
    version: str,
    *,
    merge_keys: list[str] | None = None,
) -> None:
    if refresh_mode == "incremental" and merge_keys and spark.catalog.tableExists(table_name):
        from delta.tables import DeltaTable

        condition = " AND ".join(
            f"target.`{key}` = source.`{key}`" for key in merge_keys
        )
        (
            DeltaTable.forName(spark, table_name)
            .alias("target")
            .merge(frame.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            frame.write.mode("overwrite")
            .option("overwriteSchema", "true")
            .format("delta")
            .saveAsTable(table_name)
        )
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        f"'finlab.data_product' = '{product}', "
        f"'finlab.contract_version' = '{version}', "
        "'finlab.owner' = 'finlab-data')"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Canonical daily prices (`daily_prices/v2`)

# COMMAND ----------

raw_price = normalize_security_identity(source_table(f"{bronze_prefix}.price"))
open_column = resolve_column(raw_price, "open", ["開盤價", "開盤"])
high_column = resolve_column(raw_price, "high", ["最高價", "最高"])
low_column = resolve_column(raw_price, "low", ["最低價", "最低"])
close_column = resolve_column(raw_price, "close", ["收盤價", "收盤"])
volume_column = resolve_column(raw_price, "volume", ["成交股數", "成交量"])

price_read_start = dt.date.fromisoformat(start_date)
if refresh_mode == "incremental" and spark.catalog.tableExists(price_output):
    current_max_date = spark.table(price_output).agg(F.max("date")).first()[0]
    if current_max_date is not None:
        price_read_start = max(
            price_read_start,
            current_max_date - dt.timedelta(days=incremental_lookback_days),
        )
print("price source range starts at:", price_read_start)

price_source = raw_price.where(
    F.to_date("date") >= F.to_date(F.lit(price_read_start.isoformat()))
)
security_identity = (
    price_source.select(
        "stock_id",
        "date",
        "stock_id_numeric",
        "name",
        "is_etf",
        "etf_type_code",
        "etf_type",
    )
    .dropDuplicates(["stock_id", "date"])
)

prices = (
    price_source.select(
        "stock_id",
        "date",
        numeric(open_column).alias("open"),
        numeric(high_column).alias("high"),
        numeric(low_column).alias("low"),
        numeric(close_column).alias("close"),
        numeric(volume_column).alias("volume"),
    )
    .where(
        F.col("open").gt(0)
        & F.col("high").gt(0)
        & F.col("low").gt(0)
        & F.col("close").gt(0)
    )
    .dropDuplicates(["stock_id", "date"])
)

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
        table_name = f"{bronze_prefix}.{table_suffix}"
        if not spark.catalog.tableExists(table_name):
            print(f"WARNING: adjustment source missing, skipped: {table_name}")
            missing_ratio_tables.append(table_name)
            continue
        frame = normalize_security_identity(source_table(table_name))
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

adjustment_basis = "raw"
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
        "volume",
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
        F.lit(None).cast("double").alias("volume"),
        "adjustment_ratio",
        F.lit(False).alias("is_price"),
    )
    cumulative = Window.partitionBy("stock_id").orderBy("date", "event_order").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    prices = (
        price_timeline.unionByName(event_timeline)
        .withColumn(
            "adjustment_factor",
            F.exp(F.sum(F.log("adjustment_ratio")).over(cumulative)),
        )
        .where("is_price")
        .select(
            "stock_id",
            "date",
            (F.col("open") * F.col("adjustment_factor")).alias("open"),
            (F.col("high") * F.col("adjustment_factor")).alias("high"),
            (F.col("low") * F.col("adjustment_factor")).alias("low"),
            (F.col("close") * F.col("adjustment_factor")).alias("close"),
            "volume",
        )
    )
    adjustment_basis = "corporate_action_adjusted_v1"
elif adjust_prices:
    print("WARNING: no adjustment tables were found; publishing raw prices")

if refresh_mode == "incremental" and spark.catalog.tableExists(price_output):
    existing_price_frame = spark.table(price_output)
    if "_contract_version" not in existing_price_frame.columns:
        raise RuntimeError(
            "Existing daily_prices has no _contract_version; run a full v2 rebuild"
        )
    existing_contracts = {
        row[0]
        for row in existing_price_frame.select("_contract_version")
        .distinct()
        .limit(2)
        .collect()
    }
    if existing_contracts != {"daily_prices/v2"}:
        raise RuntimeError(
            f"Cannot incrementally merge daily_prices/v2 into {existing_contracts}; "
            "run refresh_mode=full for the contract migration"
        )
    existing_basis = {
        row[0]
        for row in existing_price_frame
        .select("adjustment_basis")
        .distinct()
        .limit(2)
        .collect()
    }
    if existing_basis != {adjustment_basis}:
        raise RuntimeError(
            f"Cannot merge adjustment basis {adjustment_basis!r} into existing {existing_basis}; "
            "run a full refresh or use a new contract version"
        )

daily_prices = (
    prices.join(security_identity, ["stock_id", "date"], "left")
    .withColumn("date", F.to_date("date"))
    .withColumn("adjustment_basis", F.lit(adjustment_basis))
    .withColumn("_contract_version", F.lit("daily_prices/v2"))
    .withColumn("_materialized_at", F.current_timestamp())
    .select(
        "date",
        "stock_id",
        "stock_id_numeric",
        "name",
        "is_etf",
        "etf_type_code",
        "etf_type",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adjustment_basis",
        "_contract_version",
        "_materialized_at",
    )
)

price_quality = daily_prices.agg(
    F.count("*").alias("rows"),
    F.countDistinct(F.struct("date", "stock_id")).alias("distinct_keys"),
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date"),
    F.sum(F.when(F.col("name").isNull(), 1).otherwise(0)).alias("missing_name"),
    F.sum(
        F.when(F.col("is_etf") & F.col("etf_type").isNull(), 1).otherwise(0)
    ).alias("unclassified_etf"),
    F.sum(F.when(F.col("high") < F.col("low"), 1).otherwise(0)).alias("invalid_high_low"),
    F.sum(F.when(F.col("volume") < 0, 1).otherwise(0)).alias("invalid_volume"),
).first()
if price_quality.rows < minimum_price_rows:
    raise RuntimeError(
        f"daily_prices has {price_quality.rows} rows; minimum_price_rows={minimum_price_rows}"
    )
if price_quality.rows != price_quality.distinct_keys:
    raise RuntimeError("daily_prices contains duplicate date + stock_id keys")
if price_quality.invalid_high_low:
    raise RuntimeError(f"daily_prices has {price_quality.invalid_high_low} rows with high < low")
if price_quality.invalid_volume:
    raise RuntimeError(f"daily_prices has {price_quality.invalid_volume} rows with volume < 0")
if price_quality.unclassified_etf:
    raise RuntimeError(
        f"daily_prices has {price_quality.unclassified_etf} unclassified ETF rows"
    )
if price_quality.missing_name:
    print(
        f"WARNING: daily_prices has {price_quality.missing_name} rows without a parsed name; "
        "publish security_master/v1 before making name non-nullable"
    )

write_contract_table(
    daily_prices,
    price_output,
    "daily_prices",
    "v2",
    merge_keys=["date", "stock_id"],
)
print("daily_prices:", price_quality.asDict())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Point-in-time revenue factors (`stock_factors/v1`)

# COMMAND ----------

raw_revenue = normalize_security_identity(
    source_table(f"{bronze_prefix}.monthly_report")
)
revenue_column = resolve_column(raw_revenue, "monthly revenue", ["當月營收"])
revenue = (
    raw_revenue.select(
        "stock_id",
        F.to_date("date").alias("source_release_date"),
        numeric(revenue_column).alias("revenue"),
    )
    .where(F.col("revenue").gt(0))
    .dropDuplicates(["stock_id", "source_release_date"])
)

factor_base = (
    revenue.withColumn(
        "available_at", F.date_add("source_release_date", factor_delay_days)
    )
    .withColumn("revenue_period_end", F.last_day(F.add_months("source_release_date", -1)))
)
prior_month = factor_base.select(
    "stock_id",
    F.add_months("revenue_period_end", 1).alias("revenue_period_end"),
    F.col("revenue").alias("_revenue_1m_ago"),
)
prior_year = factor_base.select(
    "stock_id",
    F.add_months("revenue_period_end", 12).alias("revenue_period_end"),
    F.col("revenue").alias("_revenue_12m_ago"),
)
stock_factors = (
    factor_base.join(prior_month, ["stock_id", "revenue_period_end"], "left")
    .join(prior_year, ["stock_id", "revenue_period_end"], "left")
    .withColumn("revenue_mom", F.col("revenue") / F.col("_revenue_1m_ago") - 1)
    .withColumn("revenue_yoy", F.col("revenue") / F.col("_revenue_12m_ago") - 1)
    .withColumn("date", F.col("available_at"))
    .withColumn("_contract_version", F.lit("stock_factors/v1"))
    .withColumn("_materialized_at", F.current_timestamp())
    .select(
        "date",
        "stock_id",
        "available_at",
        "source_release_date",
        "revenue_period_end",
        "revenue",
        "revenue_mom",
        "revenue_yoy",
        "_contract_version",
        "_materialized_at",
    )
)

factor_quality = stock_factors.agg(
    F.count("*").alias("rows"),
    F.countDistinct(F.struct("date", "stock_id")).alias("distinct_keys"),
    F.countDistinct("date").alias("periods"),
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date"),
    F.sum(F.when(F.col("available_at") < F.col("source_release_date"), 1).otherwise(0)).alias(
        "invalid_availability"
    ),
).first()
if factor_quality.rows == 0:
    raise RuntimeError("stock_factors is empty")
if factor_quality.rows != factor_quality.distinct_keys:
    raise RuntimeError("stock_factors contains duplicate date + stock_id keys")
if factor_quality.periods < minimum_factor_periods:
    raise RuntimeError(
        f"stock_factors has {factor_quality.periods} periods; "
        f"minimum_factor_periods={minimum_factor_periods}"
    )
if factor_quality.invalid_availability:
    raise RuntimeError("stock_factors has available_at before source_release_date")

write_contract_table(stock_factors, factor_output, "stock_factors", "v1")
print("stock_factors:", factor_quality.asDict())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Contract registry

# COMMAND ----------

registry_rows = [
    {
        "product_name": "daily_prices",
        "contract_version": "v2",
        "table_name": price_output,
        "grain": "one row per trading date and stock_id",
        "primary_key_json": json.dumps(["date", "stock_id"]),
        "event_time_column": "date",
        "available_time_column": "date",
        "freshness_sla": "T+1 by 20:30 Asia/Taipei",
        "owner": "finlab-data",
        "status": "active",
        "schema_json": json.dumps(
            {
                "required": {
                    "date": "date",
                    "stock_id": "string",
                    "stock_id_numeric": "string",
                    "name": "string (nullable until security_master/v1)",
                    "is_etf": "boolean",
                    "etf_type_code": "string (nullable for non-ETF)",
                    "etf_type": "string (nullable for non-ETF)",
                    "open": "double",
                    "high": "double",
                    "low": "double",
                    "close": "double",
                    "volume": "double",
                }
            },
            ensure_ascii=False,
        ),
        "description": (
            "Canonical OHLCV plus parsed security identity and ETF classification "
            "for indicator, screening, and backtest tools"
        ),
    },
    {
        "product_name": "stock_factors",
        "contract_version": "v1",
        "table_name": factor_output,
        "grain": "one row per factor available date and stock_id",
        "primary_key_json": json.dumps(["date", "stock_id"]),
        "event_time_column": "revenue_period_end",
        "available_time_column": "available_at",
        "freshness_sla": "monthly after revenue publication checkpoint",
        "owner": "finlab-data",
        "status": "active",
        "schema_json": json.dumps(
            {
                "required": {
                    "date": "date",
                    "stock_id": "string",
                    "revenue_yoy": "double",
                },
                "point_in_time": ["available_at", "source_release_date", "revenue_period_end"],
            },
            ensure_ascii=False,
        ),
        "description": "Point-in-time monthly revenue factors for screening tools",
    },
]

registry_updates = spark.createDataFrame(registry_rows).withColumn(
    "updated_at", F.current_timestamp()
)

if not spark.catalog.tableExists(registry_output):
    registry_updates.write.format("delta").saveAsTable(registry_output)
else:
    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, registry_output)
    (
        target.alias("target")
        .merge(
            registry_updates.alias("source"),
            "target.product_name = source.product_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

spark.sql(
    f"ALTER TABLE {registry_output} SET TBLPROPERTIES ("
    "'finlab.data_product' = 'data_contract_registry', "
    "'finlab.contract_version' = 'v1', "
    "'finlab.owner' = 'finlab-data')"
)

display(spark.table(registry_output).orderBy("product_name"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Consumer smoke queries

# COMMAND ----------

display(
    spark.table(price_output)
    .select(
        "date",
        "stock_id",
        "stock_id_numeric",
        "name",
        "is_etf",
        "etf_type_code",
        "etf_type",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )
    .orderBy(F.col("date").desc(), "stock_id")
    .limit(20)
)
display(
    spark.table(factor_output)
    .select("date", "stock_id", "revenue_yoy", "available_at", "revenue_period_end")
    .orderBy(F.col("date").desc(), "stock_id")
    .limit(20)
)

print("AGENT_GOLD_MATERIALIZATION_OK")
