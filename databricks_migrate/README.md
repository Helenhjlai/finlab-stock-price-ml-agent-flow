# FinLab crawler 的 Databricks 實作骨架

歷史匯入若出現 `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`，請參閱
[日期精度修復指南](../docs/parquet_timestamp_debug_zh-TW.md)。既有 Parquet 可用
`repair_parquet_timestamps.py` 逐批修復，不必從大型 pkl 重新轉檔。

若 Free Edition 執行時出現 `PERSIST TABLE is not supported on serverless
compute`，請參閱 [serverless 相容性除錯指南](../docs/databricks_serverless_debug_zh-TW.md)。
本專案的 Delta writer 不使用 `cache()`、`persist()` 或 `unpersist()`。

這個目錄是原本 `finlab/crawler.py` 與 Databricks Delta Lake 之間的整合層。原 crawler package 保持不動；新程式不呼叫 `auto_update.py`、`update_table()`、`to_pickle()` 或 `commit()`，而是直接取得 crawler 回傳的 pandas DataFrame，再 MERGE 至 Unity Catalog Delta tables。

## 1. 本機與 Databricks 的目錄對應

本機實際目錄：

```text
finlab_practice/
├─ finlab/                              # 現有 crawler package，不修改
└─ databricks_migrate/
   ├─ README.md                         # 本文件
   ├─ pkl_to_parquet.py                 # 本機一次性 pickle -> Parquet
   ├─ export/parquet/                   # 本機產出的 Parquet；不要 sync 到 Workspace
   ├─ requirements-databricks.txt
   ├─ requirements-ml.txt                  # forecast training Job dependencies
   ├─ databricks_src/
   │  ├─ __init__.py
   │  ├─ crawler_registry.py            # dataset、頻率、crawler 呼叫與日期規劃
   │  ├─ delta_writer.py                # pandas normalize + Delta MERGE
   │  ├─ state_repository.py            # crawler_state + crawler_run_log
   │  └─ train_forecast_models.py       # NN/LGB/RF 訓練、評估及模型打包
   ├─ notebooks/
   │  ├─ load_historical_parquet.py     # 一次性歷史載入
   │  └─ finlab_daily_update.py         # 每日 Lakeflow Job 入口
   └─ tests/
      └─ test_registry.py
```

同步到 Databricks 後的目標 Workspace：

```text
/Workspace/Users/<你的 email>/finlab_project/
├─ finlab/
├─ databricks_src/
└─ notebooks/
   ├─ load_historical_parquet
   └─ finlab_daily_update
```

資料不放在 Workspace 程式目錄：

```text
/Volumes/finlab/ops/landing/*.parquet   # 一次性 landing files

finlab.bronze.<dataset>                 # 正式 Delta tables
finlab.ops.crawler_state                # 每個 dataset 的 watermark
finlab.ops.crawler_run_log              # 每次執行結果
```

## 2. Whole picture：模組如何連接

### 歷史資料，只執行一次

```text
history/tables/*.pkl
        |
        | pkl_to_parquet.py（本機）
        v
databricks_migrate/export/parquet/*.parquet
        |
        | databricks fs cp（本機 CLI）
        v
/Volumes/finlab/ops/landing/*.parquet
        |
        | notebooks/load_historical_parquet.py
        v
DeltaWriter.merge_spark()
        |
        +--> finlab.bronze.<dataset>
        +--> StateRepository.mark_success()
        +--> finlab.ops.crawler_run_log
```

### 每日更新

```text
Lakeflow Job：每天 20:00 Asia/Taipei
        |
        v
notebooks/finlab_daily_update.py
        |
        +--> crawler_registry.get_registry()
        +--> StateRepository.get_state(dataset)
        +--> crawler_registry.plan_requests(...)
        +--> crawler_registry.invoke_crawler(...)
        |          |
        |          +--> finlab.crawler.crawl_price(date)
        |          +--> finlab.crawler.crawl_pe(date)
        |          +--> ...
        |
        +--> DeltaWriter.merge_pandas()
        |          |
        |          +--> normalize_pandas_frame()
        |          +--> Spark DataFrame
        |          +--> Delta MERGE
        |
        +--> StateRepository.mark_success()/mark_failure()
        +--> StateRepository.append_run()
```

## 3. 每個檔案負責什麼

### `crawler_registry.py`

`DatasetSpec` 是每個資料集的設定：

```python
DatasetSpec(
    name="price",
    crawler_function="crawl_price",
    cadence=Cadence.DAILY,
    takes_date=True,
    lookback_days=7,
)
```

它將表名與 crawler function 明確綁定，不使用 `eval()`，也不掃描所有 `crawl_*` 函式。`plan_requests()` 讀取 state 的 `max_event_time` 後回推 lookback，例如 price 最新資料是 7/18，就重新抓最近七天。重新抓取是刻意的：來源若修正資料，Delta MERGE 會更新同一 key，而不是建立重複列。

頻率規則：

| cadence | 規劃方式 |
|---|---|
| daily | 最近 watermark 減 lookback，略過週六日；交易所假日會得到 empty result |
| monthly | 產生要檢查月份的每月一日，crawler 內部再轉成月營收日期 |
| snapshot | 每次 Job 呼叫一次無參數函式，整批 MERGE |
| quarterly | 已保留設計，但財報 crawler 尚未純化，因此預設停用 |

`invoke_crawler()` 才在執行當下 import `finlab.crawler`，且只有 bounded retry。它不會 import `auto_update.py`。

### `delta_writer.py`

`normalize_pandas_frame()` 做四件事：

1. 將 legacy MultiIndex 的 `stock_id/date` 還原為一般欄位。
2. 將 `stock_id` 固定為 string、`date` 固定為 datetime。
3. 將混合 `int/str` 的 pandas object 欄位轉成 nullable string，避免 ArrowInvalid。
4. 在單一 batch 中依 `stock_id + date` 去重，保留最後一筆。

`DeltaWriter.merge_spark()` 加入：

```text
_source       實際 crawler function 或 historical_pickle
_ingested_at  寫入時間
_run_id       對應 crawler_run_log 的 run ID
```

第一次執行、table 不存在：

```python
prepared.write.format("delta").mode("errorifexists").saveAsTable(table_name)
```

之後執行：

```sql
MERGE INTO target
USING source
ON target.stock_id = source.stock_id
AND target.date = source.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

因此：

- 相同 `stock_id/date`：新資料覆蓋舊資料，包含來源事後修正。
- 新 `stock_id/date`：新增一列。
- 同一批或同一天 Job 重跑：不會因相同 key 增加列數。
- 來源突然出現新欄位：丟出 `SchemaMismatchError`，要求人工確認，不默默改 production schema。
- 來源少了既有 optional 欄位：新列補 null，保留目標 schema。

`benchmark.date` 含盤中時間，所以同一天的 5 分鐘資料仍是不同 key。

### `state_repository.py`

`crawler_state` 每個 dataset 只有一列：

| 欄位 | 用途 |
|---|---|
| `dataset` | primary logical key |
| `last_success_at` | 最近一次完整成功的 Job 時間 |
| `max_event_time` | Delta 中已成功寫入的最大資料時間 |
| `last_row_count` | 上次 merge batch 大小 |
| `last_status` | success/failed |
| `last_error` | 最後錯誤摘要 |
| `updated_at` | state 更新時間 |

失敗時只更新 status/error，不改動 `last_success_at` 與 `max_event_time`。隔日 planner 仍會從舊 watermark 重試，不會留下永久缺洞。

`crawler_run_log` 每個 dataset、每次 Job 一列，保留 request range、fetched/merged rows 與錯誤內容，用於除錯及日後告警。

### `load_historical_parquet.py`

這是 Databricks source notebook，只跑一次。它：

1. 讀取 widgets 中的 catalog/schema/volume/datasets。
2. 從 Volume 逐表讀 Parquet。
3. 呼叫 `DeltaWriter.merge_spark()` 建立或 merge bronze table。
4. 將該表最大 date 寫進 crawler state。
5. 寫 run log。

因為它也使用 MERGE，所以單張表失敗後可以只指定該 dataset 重跑；不必全部重來。

### `finlab_daily_update.py`

這是唯一會掛到每日 Job 的入口。它沒有 crawler 實作與 SQL schema 細節，只負責 orchestration：

```python
state = state_repo.get_state(spec.name)
requests = plan_requests(..., max_event_time=state.max_event_time)
raw = invoke_crawler(spec, requested_date)
writer.merge_pandas(raw, dataset=spec.name, run_id=run_id)
state_repo.mark_success(...)
state_repo.append_run(...)
```

一個 dataset 失敗後仍會繼續其他 dataset，最後只要有任何失敗便讓 notebook `raise RuntimeError`。Lakeflow Job 因此會顯示 Failed，但已成功的 datasets 不會回滾，也不會在下次重複新增資料。

## 4. 為什麼不修改 `crawler.py` 的 `to_pickle()`

Databricks notebook 直接呼叫 `crawl_price()`、`crawl_pe()` 等回傳 DataFrame 的函式，完全繞過 legacy persistence：

```text
使用：crawl_price(date) -> DataFrame
不用：update_table() -> to_pickle() -> commit()
```

單純把 `to_pickle()` 換成單檔 `to_parquet()` 仍會每天讀取、concat、去重、重寫整個歷史檔，沒有 Delta MERGE 的增量優勢。保留舊函式是為了本機舊 notebooks 尚未切換前仍能運作。

目前唯一例外是 `crawl_finance_statement_by_date()`：它本身含檔案寫入副作用，因此 registry 明確 `enabled=False`。最終需把它重構成只回傳四個當季 DataFrames，才能使用相同 DeltaWriter。

## 5. 部署到 Databricks Workspace

### 5.1 同步程式，不同步 Parquet

在 `finlab_practice` 執行，替換 email/profile：

```powershell
$email = "your.name@gmail.com"
$profileName = "finlab-free"
$workspaceRoot = "/Users/$email/finlab_project"

databricks sync ".\finlab" "$workspaceRoot/finlab" `
  --exclude "*.pyc" --exclude "__pycache__" --profile $profileName

databricks sync ".\databricks_migrate\databricks_src" "$workspaceRoot/databricks_src" `
  --exclude "*.pyc" --exclude "__pycache__" --profile $profileName

databricks sync ".\databricks_migrate\notebooks" "$workspaceRoot/notebooks" `
  --profile $profileName
```

不要對整個 `databricks_migrate` 執行 sync，否則會把本機大型 `export/parquet` 當 Workspace files 上傳。Parquet 必須使用 `databricks fs cp ... dbfs:/Volumes/...`。

### 5.2 Serverless Environment dependencies

在兩個 notebook 的 Environment 面板加入 `requirements-databricks.txt` 中的套件。先不鎖版完成相容性測試，成功後將實際版本鎖定。pandas、PySpark 與 Delta 已由 Databricks serverless runtime 提供。

## 6. 建議執行順序

1. 在 Databricks 建立 `finlab` catalog、`bronze`/`ops` schemas 與 `landing` Volume。
2. 本機執行 `python databricks_migrate/pkl_to_parquet.py` 完成 pickle 轉 Parquet。
3. 用 CLI 將 13 個 Parquet 上傳到 `/Volumes/finlab/ops/landing/`。
4. 先執行 `load_historical_parquet`，widget datasets 設 `monthly_report`。
5. 用 SQL 驗證 rows、distinct key、min/max date。
6. 再逐步載入其他 tables；`benchmark` 最後。
7. 執行 `finlab_daily_update`，datasets 先設 `price`，run_date 選已知交易日。
8. 對同一天重跑，確認 row count 不因相同 key 增加。
9. datasets 改為 `all`，以 Run now 連續成功三次。
10. 在 Jobs & Pipelines 建立 notebook task，指向 `/Users/<email>/finlab_project/notebooks/finlab_daily_update`，設定每天 20:00、Asia/Taipei、maximum concurrent runs = 1。

## 7. 驗證 SQL

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT STRUCT(stock_id, date)) AS distinct_keys,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM finlab.bronze.price;
```

```sql
SELECT *
FROM finlab.ops.crawler_state
ORDER BY dataset;
```

```sql
SELECT *
FROM finlab.ops.crawler_run_log
ORDER BY started_at DESC, dataset;
```

核心驗收條件是 `rows = distinct_keys`、重跑不增加重複 key、失敗不推進 watermark，以及最新交易日能被下一次 Job 正確更新。

## 8. 本機結構測試

這些測試不需要 Spark：

```powershell
python -m unittest databricks_migrate.tests.test_registry -v
```

DeltaWriter 與 StateRepository 的整合測試必須在 Databricks serverless notebook 執行，因為本機目前沒有 Spark/Delta runtime。

## 9. Forecast model training

模型訓練不放進每日 crawler Job。先由獨立 feature task 產生月頻
`finlab.gold.forecast_training_features`，再以 Python script task 執行
`databricks_src/train_forecast_models.py`。它會依時間切 train/validation/test、只在
train fit preprocessing、訓練 NN/LightGBM/Random Forest、評估 ensemble，並將完整
bundle 寫入 Unity Catalog Volume；也可選擇記錄到 MLflow 與註冊 Unity Catalog model。

完整流程、命令、bundle 結構和官方參考文件見：
`docs/forecast_model_training_and_packaging_zh-TW.md`。

