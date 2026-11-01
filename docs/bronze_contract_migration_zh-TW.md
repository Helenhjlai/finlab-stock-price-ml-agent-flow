# 既有 Bronze 的 Crawler Contract 相容性遷移

最後更新：2026-10-23

適用情境：`finlab.bronze.*` 已有正式歷史資料，現在要把 `DatasetSpec`／`DeltaWriter`
泛化成不同主鍵與 event-time 契約，並讓所有新舊 Bronze 表採用一致的 provenance/v1
來源追蹤契約。

## 1. 這次會改什麼、不改什麼

會改：

- `DatasetSpec` 可宣告 `primary_keys`、`event_time_column`、`available_time_column`。
- `DeltaWriter` 依 dataset contract 去重與 MERGE。
- 每日 Job 明確把 contract 傳給 writer。
- 新增唯讀 `audit_bronze_contracts` notebook。
- 新增受控的 `backfill_bronze_provenance` notebook，逐表加入及回填 provenance/v1。

不改：

- 現有 Bronze table name、業務欄位與資料列。
- 現有資料集預設主鍵，仍是 `stock_id + date`。
- 現有 watermark 欄位，仍從 `date` 計算。
- Gold consumer table 或 Agent resource。

provenance/v1 會新增三欄：

|欄位|型別|舊資料如何處理|新資料如何產生|
|---|---|---|---|
|`_source_record_id`|STRING|以 `dataset + primary keys` 計算 SHA-256|每日 writer 用同一規則計算|
|`_source_published_at`|TIMESTAMP|來源沒有真實發布時間時保留 `NULL`，不偽造|由 `DatasetSpec.available_time_column` 填入；未定義仍為 `NULL`|
|`_payload_hash`|STRING|將所有非 `_` 開頭的業務欄位轉成 canonical JSON 後計算 SHA-256|每日 writer 用同一規則計算|

回填會產生新的 Delta table version，因為必須更新歷史列；不會重爬來源，也不會改變
業務資料值或列數。

不要執行：

- `load_historical_parquet`；
- Bronze full overwrite／CTAS 替換；
- `VACUUM`；
- 在 audit 前 ALTER key/date 欄位。

## 2. 為什麼不用重爬，但仍要逐表回填

這次是 writer contract 的程式介面遷移，不是資料語意遷移。現有 registry 中所有資料集
都明確測試為：

```text
primary_keys = (stock_id, date)
event_time_column = date
```

因此新程式對舊表產生的 MERGE condition 與原程式相同。只有未來新增例如
`security_master_snapshot` 時，才設定：

```python
DatasetSpec(
    ...,
    primary_keys=("snapshot_date", "market", "stock_id"),
    event_time_column="snapshot_date",
)
```

三個 provenance 欄位是共通 Bronze 契約，因此舊表也要套用。但不能直接讓今晚的
全資料集 Job 一次 ALTER／重寫所有表：若其中一表 key 品質不合格或工作中斷，影響範圍
會過大。因此遷移 notebook 預設 dry run，且一次只接受一個 dataset。

## 3. 部署前 baseline audit

同步程式後先開啟：

```text
/Workspace/Users/<email>/finlab_project/notebooks/audit_bronze_contracts
```

Widgets：

|Widget|值|
|---|---|
|`catalog`|`finlab`|
|`bronze_schema`|`bronze`|
|`datasets`|`all`|

Notebook 只執行 `spark.table(...).agg(...)`，不寫入或 ALTER。每表輸出：

- contract primary keys；
- row count／distinct key count／null key count；
- min／max event time；
- provenance 狀態（`legacy`、`v1`、`partial` 或 `v1_incomplete`）與各欄 NULL 數；
- 完整 Spark schema JSON。

成功必須看到：

```text
BRONZE_CONTRACT_AUDIT_OK
```

保存 `BRONZE_CONTRACT_AUDIT_JSON` 與 Databricks run URL，作為 baseline。

若 audit 發現既有重複 key，不要讓新 writer 靜默決定保留哪筆；先查 Bronze Delta history、
`_ingested_at`／`_run_id` 與來源 revision，再另外提出資料修復。

## 4. 部署相容程式

同步：

```powershell
$profileName = "finlab-free"
$workspaceRoot = "/Users/<email>/finlab_project"

databricks sync `
  ".\databricks_migrate\databricks_src" `
  "$workspaceRoot/databricks_src" `
  --exclude "__pycache__" --exclude "*.pyc" `
  --profile $profileName

databricks sync `
  ".\databricks_migrate\notebooks" `
  "$workspaceRoot/notebooks" `
  --exclude "__pycache__" --exclude "*.pyc" `
  --profile $profileName
```

先暫停排程，只以 Run now 做 canary；不要同時讓舊版與新版 daily update 執行。

## 5. 部署後唯讀回歸

在任何 crawler 寫入前，再跑一次 `audit_bronze_contracts`。

在尚未執行回填前，與 baseline 比較：

- `rows` 完全相同；
- `distinct_keys` 完全相同；
- `min_event_time`／`max_event_time` 完全相同；
- `schema_json` 完全相同；
- 所有 status 仍為 `ok`。

回填後 schema 會按設計多三欄，不能再要求 `schema_json` 與舊 baseline 完全相同；改為確認
`provenance_status = v1`、`null_record_ids = 0`、`null_payload_hashes = 0`。舊資料的
`null_source_published_at` 可大於 0。

這一步驗證「部署程式本身沒有修改資料」。

## 6. 逐表套用 provenance/v1

先暫停每日 Job。以 `price` 為例開啟：

```text
/Workspace/Users/<email>/finlab_project/notebooks/backfill_bronze_provenance
```

第一次只做 dry run：

|Widget|值|
|---|---|
|`catalog`|`finlab`|
|`bronze_schema`|`bronze`|
|`dataset`|`price`|
|`apply_changes`|`false`|
|`confirmation`|空白|

必須先看到 `BRONZE_PROVENANCE_DRY_RUN_OK`，而且 `rows = distinct_keys`、
`null_key_rows = 0`。確認後才改成：

```text
apply_changes = true
confirmation = BACKFILL_PROVENANCE
```

成功訊息是 `BRONZE_PROVENANCE_BACKFILL_OK`。驗證：

```sql
SELECT
  COUNT(*) AS rows,
  SUM(CASE WHEN _source_record_id IS NULL THEN 1 ELSE 0 END) AS null_record_ids,
  SUM(CASE WHEN _payload_hash IS NULL THEN 1 ELSE 0 END) AS null_payload_hashes,
  SUM(CASE WHEN _source_published_at IS NULL THEN 1 ELSE 0 END) AS null_published_at
FROM finlab.bronze.price;

SHOW TBLPROPERTIES finlab.bronze.price ('finlab.bronze_provenance_version');
```

舊資料的 `null_published_at` 可以大於 0；這表示來源沒有可驗證的發布時間，不是回填失敗。
其餘兩欄必須全數非 NULL。依序對現有每個 Bronze dataset 重複執行。

## 7. 單表每日更新 canary

先跑 `finlab_daily_update`：

```text
datasets = price
run_date = 最近一個已完成交易日
bootstrap_start = 不重要；已有 crawler_state 時會依 watermark + lookback
```

驗收：

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT STRUCT(stock_id, date)) AS distinct_keys,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM finlab.bronze.price;

SELECT *
FROM finlab.ops.crawler_state
WHERE dataset = 'price';

SELECT *
FROM finlab.ops.crawler_run_log
WHERE dataset = 'price'
ORDER BY started_at DESC
LIMIT 5;
```

通過條件：

- `rows = distinct_keys`；
- 重跑同一 `run_date` 不產生新 key；
- `max_event_time` 不倒退；
- 只有 lookback 範圍可能因來源修訂而更新；
- table schema 沒有意外新增／刪除欄位。

接著依風險從低到高 canary：

```text
price → pe → monthly_report → bargin_report → benchmark → snapshot ratio tables
```

`benchmark` 最後跑，因為它的 `date` 含盤中時間，而不是單純交易日。

已完成回填的表，writer 會偵測表上既有 provenance 欄位並繼續維護。尚未回填的舊表
維持原狀，不會在 lookback 更新時只填一部分歷史列；若只存在部分 provenance 欄位，
writer 會直接報錯，要求先完成回填。

## 8. Gold 回歸，不直接覆蓋 production

Bronze canary 通過後，用既有 `materialize_agent_gold` 先寫 candidate：

```text
price_table = daily_prices_contract_candidate
factor_table = stock_factors_contract_candidate
registry_table = data_contract_registry_contract_candidate
refresh_mode = full
```

比較正式表與 candidate：

```sql
SELECT 'prod' source, COUNT(*) rows, MIN(date) min_date, MAX(date) max_date
FROM finlab.gold.daily_prices
UNION ALL
SELECT 'candidate', COUNT(*), MIN(date), MAX(date)
FROM finlab.gold.daily_prices_contract_candidate;
```

對固定股票與日期比對 OHLC、MA、revenue_yoy，再執行 Agent 的三個 capability eval。
結果一致後才將正式 materialization 以同一契約重跑；這次 framework 泛化不需要切換
Agent resource table。

## 9. 新資料集如何加入

Bronze 已存在不代表應沿用錯誤 key。新增資料集時先 audit 真實 grain，再登錄：

```python
DatasetSpec(
    name="security_master_snapshot",
    crawler_function="crawl_security_master",
    cadence=Cadence.SNAPSHOT,
    takes_date=False,
    lookback_days=0,
    primary_keys=("snapshot_date", "market", "stock_id"),
    event_time_column="snapshot_date",
    available_time_column="snapshot_date",
)
```

若新增的 Bronze 表已經存在：

1. 先以 SQL 求出候選 key 的 `rows` 與 `distinct_keys`。
2. 確認 event time／available time 的真正語意。
3. 將 spec 設為現有 table 的真實契約。
4. 先跑 audit，不要先啟用 crawler。
5. 執行 `backfill_bronze_provenance` dry run 與正式回填。
6. 通過後才做單表 canary。

若新增的 Bronze 表還不存在，第一次 daily run 建表時就會自動包含全部六個
metadata/provenance 欄位，不需要額外 widget。

## 10. 回滾

若部署後 audit 尚未寫入資料，直接回退 application code 即可。

若 canary 已 MERGE：

1. 暫停 daily Job。
2. 查 `DESCRIBE HISTORY finlab.bronze.<dataset>`。
3. 比對新版本實際變更；正常來源 revision 可以保留。
4. 只有確認為錯誤 key／錯誤 MERGE 時，才以已驗證版本執行 Delta RESTORE。
5. 重跑 audit，再恢復舊版 Job。

不要因 row count 改變就直接 RESTORE；先區分正常新增交易日、來源修訂與程式錯誤。

