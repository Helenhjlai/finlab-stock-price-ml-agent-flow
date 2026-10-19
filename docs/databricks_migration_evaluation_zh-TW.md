# FinLab crawler 搬遷至 Databricks：評估紀錄與實作指南

> 評估日期：2026-07-18  
> 適用環境：Databricks Free Edition（AWS us-east-2）、Google 帳號登入  
> Workspace ID：`7474649614053494`  
> Open Sharing ID：`aws:us-east-2:77425890-e9d9-4b60-80bf-7724fcd90025`

## 1. 結論先行

這個專案適合把「最終表格」從 pickle 搬成 Unity Catalog 管理的 Delta tables，並以一個每天執行一次的 Databricks Job 負責判斷各資料集是否需要更新。不要把整個 `history` 目錄原樣搬上去。

建議的目標狀態如下：

- `history/tables/*.pkl`：搬成 Delta tables，是第一階段的主要資料。
- `history/items/*.pkl`：不要搬。它們是 `commit()` 由 tables 再拆出的 5,408 個衍生 pickle，可由 Delta table 或 view 取代。
- `history/financial_statement/**/*.html`：不要全量搬。這批約 18.5 GiB、30,438 個檔案，只保留最近一次季度的暫存檔，或完全不保留。
- `history/date_range.pickle`：改成 `finlab.ops.crawler_state` Delta table。
- 每日 Job：每天固定執行，但每個 crawler 按自己的頻率判斷是否真的抓取；日資料每日、月營收每月、財報每季、全歷史快照類每日重新抓後以 key 更新。
- 寫入方式：使用 Delta `MERGE`，主鍵為 `stock_id + date`；不能再用「讀完整 pickle、concat、整檔覆寫」的方式。

但目前有一個必須先驗證的硬限制：Databricks Free Edition 的 serverless compute 只允許有限的可信任外連網域。此 crawler 直接呼叫 TWSE、TPEx 與 MOPS；這些網域不保證在 Free Edition allowlist 中。若連線測試失敗，Free Edition 又不能設定企業版的 serverless egress policy，因此應改採「本機或其他 runner 負責爬取，Databricks 負責 Delta 儲存與查詢」的混合架構。

## 2. 我如何評估現況

### 2.1 檢查的程式

本次實際檢查：

- `L02_crawlers.ipynb`
- `auto_update.py`
- `finlab/crawler.py`
- `finlab/data.py`
- `history/tables`
- `history/items`
- `history/financial_statement`

Notebook 與 `auto_update.py` 實際登錄了以下 10 個更新工作：

1. `price`
2. `bargin_report`（原專案就是此拼字，不建議在搬遷時直接更名）
3. `pe`
4. `benchmark`
5. `monthly_report`
6. `financial_statement`
7. `twse_divide_ratio`
8. `otc_divide_ratio`
9. `twse_cap_reduction`
10. `otc_cap_reduction`

其中 `financial_statement` 會另外產出 `balance_sheet`、`cash_flows`、`income_sheet`、`income_sheet_cumulate` 四張表。因此 `history/tables` 最後共有 13 個 pickle。

### 2.2 現有資料量

盤點結果：

| 區域 | 檔案數 | 大小 |
|---|---:|---:|
| `history/tables` | 13 | 9.29 GiB |
| `history/items` | 5,408 | 15.57 GiB |
| `history/financial_statement` | 30,438 | 18.50 GiB |
| 合計 | 35,859 | 43.35 GiB |

`history/tables` 明細：

| 表名 | pickle 大小 | 來源/用途 |
|---|---:|---|
| `benchmark` | 3.22 GiB | TWSE 大盤 5 分鐘指數，一個交易日有多筆時間點 |
| `balance_sheet` | 1.55 GiB | 季財報資產負債表 |
| `price` | 1.48 GiB | TWSE + TPEx 日價量 |
| `income_sheet_cumulate` | 1.16 GiB | 累計損益表 |
| `income_sheet` | 0.97 GiB | 單季損益表 |
| `bargin_report` | 0.38 GiB | 三大法人買賣超 |
| `cash_flows` | 0.30 GiB | 現金流量表 |
| `pe` | 0.20 GiB | 本益比/殖利率/股價淨值比等日資料 |
| `monthly_report` | 0.025 GiB | 上市櫃月營收 |
| `twse_divide_ratio` | 約 2.6 MiB | 上市除權息歷史快照 |
| `otc_divide_ratio` | 約 2.5 MiB | 上櫃除權息歷史快照 |
| `twse_cap_reduction` | 約 43 KiB | 上市減資歷史快照 |
| `otc_cap_reduction` | 約 145 KiB | 上櫃減資歷史快照 |

這個結果是我不建議把整個 `history` 上傳的主要原因。`items` 是重複衍生資料，財報 HTML 是中間產物；兩者合計已超過 34 GiB。

### 2.3 crawler 規則與來源網域

`finlab/crawler.py` 會連線到：

- `www.twse.com.tw`
- `dts.twse.com.tw`
- `www.tpex.org.tw`
- `mops.twse.com.tw`
- `mopsov.twse.com.tw`
- `github.com`（`crawl_gitlab_backup` 的函式名稱雖是 gitlab，實際 URL 是 GitHub）

主要資料規則：

| 資料集 | 函式 | 現行 range | 合理更新頻率 | Delta key |
|---|---|---|---|---|
| `price` | `crawl_price(date)` | `date_range` | 台灣交易日每日一次 | `stock_id,date` |
| `bargin_report` | `crawl_bargin(date)` | `date_range` | 台灣交易日每日一次 | `stock_id,date` |
| `pe` | `crawl_pe(date)` | `date_range` | 台灣交易日每日一次 | `stock_id,date` |
| `benchmark` | `crawl_benchmark(date)` | `date_range` | 台灣交易日每日一次 | `stock_id,date`；此處 `date` 含時間 |
| `monthly_report` | `crawl_monthly_report(date)` | `month_range` | 每月 10 日後檢查，並回補前月 | `stock_id,date` |
| 財報四表 | `crawl_finance_statement_by_date(date)` | `season_range` | 3/31、5/15、8/14、11/14 後檢查 | `stock_id,date` |
| `twse_divide_ratio` | `crawl_twse_divide_ratio()` | 無參數、整批 | 每日或每週重抓最近歷史快照 | `stock_id,date` |
| `otc_divide_ratio` | `crawl_otc_divide_ratio()` | 無參數、整批 | 每日或每週重抓最近歷史快照 | `stock_id,date` |
| `twse_cap_reduction` | `crawl_twse_cap_reduction()` | 無參數、整批 | 每日或每週重抓最近歷史快照 | `stock_id,date` |
| `otc_cap_reduction` | `crawl_otc_cap_reduction()` | 無參數、整批 | 每日或每週重抓最近歷史快照 | `stock_id,date` |

`crawler.py` 另外還有 `crawl_capital()`、`interest()`、`crawl_split_twe()`、`crawl_split_otc()` 與 `crawl_gitlab_backup()`，但它們沒有被 `L02_crawlers.ipynb` 或 `auto_update.py` 登錄。因此「所有會由目前更新流程爬取的資料」應以上述 10 個工作、13 張表為正式範圍；其他函式要在第二階段先定義表名、key 與更新頻率再加入，不能只因為名稱含 `crawl` 就自動排程。

## 3. 現行實作不適合直接搬到 Databricks 的原因

### 3.1 每次更新都重讀、重寫整個 pickle

`to_pickle()` 的流程是：

1. 讀入整個舊 pickle。
2. `pd.concat()` 新舊資料。
3. 對 `stock_id,date` 去重。
4. 全表排序。
5. 寫成新 pickle，再替換舊檔。

這會讓每日更新的記憶體與 I/O 成本隨歷史長度持續增加。大檔已達 1–3 GiB，反序列化後的 RAM 通常遠大於檔案大小，對 Free Edition serverless 非常不利。Delta `MERGE` 只更新受影響的資料檔，才是正確替代方案。

### 3.2 `history/items` 是重複資料

`commit()` 會把每個 table 的每個欄位再拆成個別 pickle，供 `finlab.data.Data.get()` 讀取。這是 5,408 個檔案、15.57 GiB 的來源。在 Databricks 應以 table/view 查詢欄位，不再執行 `commit()`。

### 3.3 財報更新會反覆處理所有歷史檔

`crawl_finance_statement_by_date()` 下載 HTML/ZIP 後，`html2db()` 會重新讀取歷史財報 pickle、合併並覆寫四張大表。這不應每天執行，也不應在新架構中保留全量中間 HTML。應只解析「本次季度」，再 MERGE 該季度資料。

### 3.4 日期 checkpoint 的語意不夠安全

`date_range()` 與 `month_range()` 都包含起始日期，所以現行工作會再次抓最後一次日期。重抓本身是好事，但 `drop_duplicates()` 預設保留舊資料，來源若修正舊值，修正值可能無法覆蓋。Delta 應使用 `WHEN MATCHED THEN UPDATE`，讓新抓資料取代同 key 舊資料。

另外，不能只靠「最大日期」判斷完整性。假日、來源暫時失敗、單一交易所成功而另一個失敗，都可能造成中間缺洞。新的 state table 應記錄 run 狀態、資料筆數與錯誤，而不只是最大日期。

### 3.5 原程式中值得先修正的問題

- `update_table()` 處理 crawler 回傳 `dict` 時，迴圈最後呼叫的是 `to_pickle(df, table_name)`，不是 `to_pickle(d, i)`；這是明顯變數使用錯誤。
- `bargin_otc()` 組 URL 的 `date` 後方看起來缺少 `=`，需要以實際 endpoint 測試確認。
- `requests_get()` 最多重試數次，但每次固定 sleep 60 秒，Job 可能被長時間卡住；應改成有上限的 exponential backoff。
- 大量固定 `sleep(5)`、`sleep(10)` 讓一次補歷史非常慢；日常增量仍應保守限速，但補歷史要分批、可續跑。
- `ipywidgets`、`tqdm_notebook`、相對路徑 `history/...` 與 `commit()` 都是 notebook/local-file 導向，不應進入 production Job 核心。
- `benchmark` 的 key 需要保留 timestamp；若把它轉成純 date，會讓同一天的 5 分鐘資料互相覆蓋。

## 4. Free Edition 可行性判斷

依 Databricks 官方文件，Free Edition：

- 只有 serverless compute，不能自訂 classic cluster。
- 對外網路只允許有限的可信任網域。
- 每個帳號最多同時執行 5 個 job tasks。
- 使用 fair-usage quota；超過時 compute 可能停到當日甚至當月 quota 重置。
- 只有一個 workspace 與一個 metastore。
- 不支援自訂 workspace storage location。
- 沒有 SLA，且僅允許非商業用途。

官方限制頁沒有公布可依賴的固定儲存 GB 數，因此不能假設 43.35 GiB 一定可長期放入。這也是只保留 Delta 最終表、移除重複衍生檔與中間 HTML 的另一個理由。

參考：[Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)、[Free Edition 說明](https://docs.databricks.com/aws/en/getting-started/free-edition)、[Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/)。

### 4.1 必做的網路 smoke test

先在 Databricks 建立一個 Python notebook，使用 serverless compute 執行：

```python
import requests

URLS = [
    "https://www.twse.com.tw/zh/",
    "https://www.tpex.org.tw/",
    "https://mops.twse.com.tw/",
    "https://mopsov.twse.com.tw/",
]

for url in URLS:
    try:
        response = requests.get(url, timeout=15)
        print(url, response.status_code, len(response.content))
    except Exception as exc:
        print(url, type(exc).__name__, str(exc))
```

驗收標準不是所有網址都必須回 200；有些首頁可能有防爬規則。重點是不能出現 DNS、connection policy、egress blocked 或 timeout 類錯誤。接著再用 `crawler.py` 裡的實際 CSV URL 測試一次。

若網域被擋：

- 不要繼續嘗試在 Free Edition 內繞過網路政策。
- Free Edition 沒有 account console 與企業版 egress policy 可供自行 allowlist。
- 改成「外部 runner 爬取 -> 產生 Parquet -> 上傳/寫入 Databricks」；或者改用付費/Free Trial workspace 驗證完整 serverless egress 設定。

## 5. 建議架構

```text
Lakeflow Job（每天一次，Asia/Taipei）
        |
        v
每日 orchestrator：判斷各 dataset 是否到期
        |
        +--> 日資料：price / bargin_report / pe / benchmark
        +--> 月資料：monthly_report
        +--> 季資料：financial statements
        +--> 快照：divide_ratio / cap_reduction
        |
        v
Pandas crawler 回傳本次批次 DataFrame
        |
        v
Spark DataFrame -> 暫存 view -> Delta MERGE
        |
        +--> finlab.bronze.<dataset>
        +--> finlab.ops.crawler_state
        +--> finlab.ops.crawler_run_log
```

如果 Free Edition 無法直接連來源網站，只替換最上游：

```text
Windows Task Scheduler / GitHub Actions / 其他 runner
        |
        +--> crawler -> 每批 Parquet
        |
        v
Databricks managed Volume（landing）
        |
        v
每日 Databricks Job -> Delta MERGE
```

GitHub Actions 是否適用要先確認來源網站是否允許其 IP 與使用方式，也要遵守 TWSE、TPEx、MOPS 的服務條款與合理限速。

## 6. Unity Catalog 與資料模型

建議建立：

```sql
CREATE CATALOG IF NOT EXISTS finlab;
CREATE SCHEMA IF NOT EXISTS finlab.bronze;
CREATE SCHEMA IF NOT EXISTS finlab.silver;
CREATE SCHEMA IF NOT EXISTS finlab.ops;
CREATE VOLUME IF NOT EXISTS finlab.ops.landing;
```

若 Free Edition 預設權限不允許自行建立 catalog，就在預設 catalog 下建立 `finlab_bronze`、`finlab_silver`、`finlab_ops` 三個 schema。Volume 用來放非表格的 landing Parquet、暫存 ZIP 或最近一季 HTML；表格資料要放 Delta table，不要把 pickle 長期放 Volume。官方也明確區分：表格資料用 Unity Catalog tables，非表格檔案用 Volumes。

### 6.1 state table

```sql
CREATE TABLE IF NOT EXISTS finlab.ops.crawler_state (
  dataset STRING NOT NULL,
  last_success_at TIMESTAMP,
  max_event_time TIMESTAMP,
  last_row_count BIGINT,
  last_status STRING,
  last_error STRING,
  updated_at TIMESTAMP
) USING DELTA;
```

### 6.2 run log

```sql
CREATE TABLE IF NOT EXISTS finlab.ops.crawler_run_log (
  run_id STRING,
  dataset STRING,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  requested_from TIMESTAMP,
  requested_to TIMESTAMP,
  rows_fetched BIGINT,
  rows_merged BIGINT,
  status STRING,
  error_class STRING,
  error_message STRING
) USING DELTA;
```

### 6.3 table key 與欄位策略

- 保留 `stock_id` 為 STRING，不要轉數字，避免代碼格式或未來非純數字商品出錯。
- `date` 統一為 TIMESTAMP；日/月/季資料時間可為午夜，benchmark 保留盤中時間。
- 增加 `_source`、`_ingested_at`、`_run_id`。
- 來源欄位名稱可保留中文，但要建立一份固定的欄名 mapping；不要每次動態產生不同名稱。
- 同一批資料在 MERGE 前先以 key 去重，保留最後一筆。
- 不建議只分割 `date` partition。現有資料量下可先不手動 partition，讓 Delta 自動最佳化；大量 benchmark 若查詢確實以日期為主，再評估 clustering。

## 7. 一次性歷史搬遷

### 7.1 第一階段只搬 13 張 tables

不要先搬 `items` 與財報 HTML。歷史搬遷順序建議由小到大：

1. 四張 divide/cap 表。
2. `monthly_report`。
3. `pe`、`bargin_report`。
4. `price`。
5. 四張財報表。
6. 最後處理最大的 `benchmark`。

這樣可以先驗證 schema、MERGE 與排程，而不必等 9.29 GiB 全部完成。

### 7.2 本機先把 pickle 轉 Parquet

Spark 不能原生讀 pandas pickle，而且 pickle 反序列化有執行任意程式碼的安全風險。只應讀取自己產生且可信任的 pickle。不要從網路下載陌生 pickle 後直接 `pd.read_pickle()`。

在有足夠 RAM 的本機 Python 環境中逐檔轉換：

```python
from pathlib import Path
import pandas as pd

source_dir = Path("history/tables")
target_dir = Path("export/parquet")
target_dir.mkdir(parents=True, exist_ok=True)

for source in sorted(source_dir.glob("*.pkl")):
    print("reading", source)
    df = pd.read_pickle(source)
    df = df.reset_index()
    df["stock_id"] = df["stock_id"].astype("string")
    df["date"] = pd.to_datetime(df["date"], errors="raise")

    # crawler 的部分 object 欄位同時含 int 與 str，例如
    # otc_cap_reduction.恢復買賣日期。寫入前統一為 nullable string，
    # 否則 PyArrow 可能先推斷 int64，之後遇到字串而失敗。
    object_columns = df.select_dtypes(include=["object"]).columns
    df[object_columns] = df[object_columns].astype("string")

    df = df.drop_duplicates(["stock_id", "date"], keep="last")
    df.to_parquet(
        target_dir / f"{source.stem}.parquet",
        index=False,
        engine="pyarrow",
        compression="snappy",
    )
    del df
```

注意：3.22 GiB 的 `benchmark.pkl` 載入記憶體後可能需要遠高於 3.22 GiB 的 RAM。轉檔前先以小表測試；若記憶體不足，就要在原本能成功執行 crawler 的高記憶體環境轉換，不能期待 pickle 可像 CSV 一樣分段讀取。

### 7.3 上傳 landing Volume

可先用 Catalog Explorer：

1. 進入 Catalog。
2. 找到 `finlab.ops.landing`。
3. 選擇 **Add -> Upload to this volume**。
4. 上傳轉好的 Parquet。

大量檔案較適合 Databricks CLI。以下是以本專案所在的 Windows 電腦及 Google 帳號登入 Free Edition 為前提的完整步驟。

#### 步驟 1：取得正確的 Workspace URL

1. 用瀏覽器登入 Databricks 並進入目前的 workspace。
2. 從網址列複製 `https://` 開頭到網域結尾的部分。
3. 移除 `/?o=7474649614053494`、`#workspace/...`、`/editor/...` 等後段路徑。

例如瀏覽器顯示：

```text
https://dbc-xxxxxxxx-xxxx.cloud.databricks.com/?o=7474649614053494#workspace/...
```

CLI 要使用的是：

```text
https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

Workspace ID `7474649614053494` 不能單獨代替這個 URL；Open Sharing ID 也不是 CLI host。

#### 步驟 2：在 Windows 安裝新版 Databricks CLI

開啟 PowerShell 或 Command Prompt：

```powershell
winget search databricks
winget install Databricks.DatabricksCLI
```

安裝完成後關閉並重新開啟 PowerShell，再確認：

```powershell
where.exe databricks
databricks -v
```

版本至少要是 `0.205.0`；不要使用由 `pip install databricks-cli` 安裝的 0.18 legacy CLI。若已安裝但版本太舊，可執行：

```powershell
winget upgrade Databricks.DatabricksCLI
```

官方安裝說明：[Databricks CLI tutorial](https://docs.databricks.com/aws/en/dev-tools/cli/tutorial)。

#### 步驟 3：使用 Google 帳號完成 OAuth U2M 登入

在 PowerShell 先設定本次 terminal 使用的值；請換成步驟 1 的實際 URL：

```powershell
$workspaceHost = "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
$profileName = "finlab-free"

databricks auth login --host $workspaceHost --profile $profileName
```

接著：

1. CLI 會顯示驗證網址，通常也會自動開啟瀏覽器。
2. 選擇建立這個 Free Edition workspace 時使用的同一個 Google 帳號。
3. 在 Databricks 頁面允許 CLI 存取。
4. 看到成功訊息後回到 PowerShell。

OAuth U2M 是互動式使用者登入；不需要把 Google 密碼或 access token 貼到 terminal。新版 CLI 會把短效 token 存進 Windows Credential Manager 並自動更新。這個登入只供本機 CLI 上傳與部署使用；Databricks 內部的 scheduled Job 不需要依賴本機保持登入。

#### 步驟 4：確認 profile 與登入身分

```powershell
databricks auth profiles
databricks auth describe --profile $profileName
databricks current-user me --profile $profileName
```

`current-user me` 應回傳你的 Databricks 使用者資訊。若它連到錯誤 workspace，先登出該 profile 再重做步驟 3：

```powershell
databricks auth logout $profileName
```

官方驗證說明：[Databricks CLI authentication](https://docs.databricks.com/aws/en/dev-tools/cli/authentication)。

#### 步驟 5：先在 Databricks 建立 landing Volume

在 Databricks notebook 的 SQL cell 執行：

```sql
CREATE CATALOG IF NOT EXISTS finlab;
CREATE SCHEMA IF NOT EXISTS finlab.ops;
CREATE VOLUME IF NOT EXISTS finlab.ops.landing;
```

若 Free Edition 不允許建立 `finlab` catalog，請改用 UI 中已存在且你有權限的 catalog，並同步替換下方所有路徑。先在 Catalog Explorer 確認能看到該 Volume。

#### 步驟 6：先預覽，再批次上傳 Parquet

在 PowerShell 切換到本專案目錄，確認本機輸出檔：

```powershell
Set-Location "C:\D\finlab_ml\finlab_ml_course\finlab_practice"
Get-ChildItem -LiteralPath "export\parquet" -Filter "*.parquet"
```

先用一個小檔驗證權限與路徑：

```powershell
databricks fs cp `
  "export/parquet/monthly_report.parquet" `
  "dbfs:/Volumes/finlab/ops/landing/monthly_report.parquet" `
  --overwrite `
  --profile $profileName

databricks fs ls "dbfs:/Volumes/finlab/ops/landing" --profile $profileName
```

小檔成功後再整批上傳：

```powershell
databricks fs cp `
  "export/parquet" `
  "dbfs:/Volumes/finlab/ops/landing" `
  --recursive `
  --overwrite `
  --profile $profileName
```

注意兩種路徑格式不同：

- CLI 操作 Volume 使用 `dbfs:/Volumes/<catalog>/<schema>/<volume>/...`。
- Databricks notebook 內的 Spark/Python 通常使用 `/Volumes/<catalog>/<schema>/<volume>/...`。

`--overwrite` 只應在確定同名 landing 檔可以被替換時使用。正式 Delta table 不應靠這個指令覆寫，而要使用後續的 `MERGE`。CLI Volume 指令參考：[Databricks `fs` commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/fs-commands)。

#### 步驟 7：從 Databricks 再確認上傳結果

在 Databricks Python notebook 執行：

```python
display(dbutils.fs.ls("/Volumes/finlab/ops/landing"))

test_df = spark.read.parquet(
    "/Volumes/finlab/ops/landing/monthly_report.parquet"
)
test_df.printSchema()
display(test_df.limit(10))
```

看得到 schema 與資料後，才進入 7.4 建立 Delta table。

### 7.4 載入 Delta

是，這一節的程式應在 **Databricks workspace 裡的 Python notebook** 執行，並選擇 Free Edition 的 serverless compute。原因是程式使用 Databricks 已提供的 `spark` session、Unity Catalog table 與 `/Volumes/...` 路徑；不是放回本機 `pkl_to_parquet.ipynb` 執行。

建議建立一個 `load_historical_parquet` notebook。第一次建立表時逐表執行：

```python
dataset = "monthly_report"
path = f"/Volumes/finlab/ops/landing/{dataset}.parquet"

from pyspark.sql import functions as F

df = (
    spark.read.parquet(path)
    .dropDuplicates(["stock_id", "date"])
    .withColumn("_source", F.lit("historical_pickle"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_run_id", F.lit("initial_migration"))
)

(df.write
   .format("delta")
   .mode("errorifexists")
   .saveAsTable(f"finlab.bronze.{dataset}"))
```

每張表搬完立刻驗證，再進下一張。不要一次上傳全部才檢查。

同一張表如果已經建立，`.mode("errorifexists")` 會刻意停止，避免誤覆寫。需要重做時不要直接改成 `overwrite`；先確認 table 是否只是測試資料，再明確決定要 drop/recreate，或改用第 8 節的 `MERGE`。

### 7.5 smoke test 成功後，將 `crawler.py` 接到 Databricks

連線測試全部回 200，代表 Free Edition serverless 至少能連到目前測試的 TWSE、TPEx 與 MOPS 網域。下一步分成「上傳程式」、「安裝依賴」、「單一 crawler 驗證」和「接上 Delta MERGE」。

#### 步驟 1：將本機 `finlab` package 同步成 Workspace files

程式碼應放 Workspace files 或 Git folder，資料才放 Volume。先取得登入 email：

```powershell
databricks current-user me --profile $profileName
```

假設回傳的 email 是 `your.name@gmail.com`。先預覽同步內容，不要把 43 GiB 的 `history` 一起同步：

```powershell
databricks sync `
  ".\finlab" `
  "/Users/your.name@gmail.com/finlab_project/finlab" `
  --exclude "*.pyc" `
  --exclude "__pycache__" `
  --dry-run `
  --profile $profileName
```

確認預覽只有 `finlab/*.py` 後，移除 `--dry-run` 正式同步：

```powershell
databricks sync `
  ".\finlab" `
  "/Users/your.name@gmail.com/finlab_project/finlab" `
  --exclude "*.pyc" `
  --exclude "__pycache__" `
  --profile $profileName
```

這是本機到 workspace 的單向同步。程式變更後可以重跑同一指令；開發期間也可使用 `--watch`，但日常排程不需要保持本機 watch。參考：[Databricks `sync` command](https://docs.databricks.com/aws/en/dev-tools/cli/reference/sync-commands)。

正式長期開發建議再把程式放到 GitHub，於 Databricks 建立 Git folder；第一輪驗證用 `sync` 比較直接。Workspace files 與 Python module 的官方行為：[Work with Python modules](https://docs.databricks.com/aws/en/files/workspace-modules)。

#### 步驟 2：建立 crawler 測試 notebook 並設定依賴

在 `/Workspace/Users/your.name@gmail.com/finlab_project/` 下建立 Python notebook，例如 `test_crawler`，選擇 serverless compute。

先用 notebook 的 **Environment** 面板加入下列 PyPI dependencies，或在第一個 cell 暫時使用 `%pip`：

```python
%pip install lxml python-dateutil requests tqdm ipywidgets
```

若安裝後 Databricks 要求重新啟動 Python，依畫面操作，再從 import cell 重跑。正式 scheduled Job 應把 dependencies 固定在 serverless environment，而不是每次執行 Job 都臨時安裝未鎖版套件；確認相容版本後應鎖定版本。

#### 步驟 3：把 package root 加入 Python path 並 import

```python
import sys

PROJECT_ROOT = "/Workspace/Users/your.name@gmail.com/finlab_project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from finlab.crawler import crawl_price
```

若 notebook 就位於 `finlab_project` 根目錄，Databricks 通常已把目前目錄加入 `sys.path`，但明確加入絕對路徑較容易排查 Job 的工作目錄差異。開發時修改 `crawler.py` 後，可使用：

```python
%load_ext autoreload
%autoreload 2
```

正式 Job 不需要 `autoreload`。

#### 步驟 4：只呼叫一個純 crawler，先不要跑 `auto_update.py`

用確定有交易資料的日期測試，例如：

```python
import datetime as dt

test_date = dt.date(2026, 7, 17)  # 請換成最近一個台灣交易日
price_pdf = crawl_price(test_date)

print(type(price_pdf), price_pdf.shape)
display(price_pdf.reset_index().head(20))
assert len(price_pdf) > 0, f"{test_date} 沒抓到 price 資料"
assert price_pdf.index.names == ["stock_id", "date"]
```

不要在 Databricks import 或執行目前的 `auto_update.py`：它在檔案頂層就會立即跑全部 crawler，並呼叫 `update_table()`、`to_pickle()`、`commit()` 寫入相對路徑 `history/...`。Databricks 版應直接呼叫 `crawl_price()` 等抓取函式，將回傳 DataFrame 交給 Delta 儲存層。

目前也不要直接把 `crawl_finance_statement_by_date()` 放進每日 Job。它仍會下載/重讀本機 `history/financial_statement` 並全量重建四張 pickle；財報要先按 Phase 4 拆成「只解析當季並回傳四個 DataFrame」。

#### 步驟 5：處理 pandas/Arrow 型別，再接 Delta MERGE

與 pickle 轉 Parquet 相同，crawler 回傳的 object 欄位可能混有 `int` 與 `str`。先統一 object 欄位，再呼叫第 8 節的 `merge_batch()`：

```python
import pandas as pd
import uuid

def normalize_crawler_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.reset_index()
    pdf["stock_id"] = pdf["stock_id"].astype("string")
    pdf["date"] = pd.to_datetime(pdf["date"], errors="raise")

    object_columns = pdf.select_dtypes(include=["object"]).columns
    pdf[object_columns] = pdf[object_columns].astype("string")

    return pdf.drop_duplicates(["stock_id", "date"], keep="last")

price_pdf = normalize_crawler_batch(price_pdf)
run_id = str(uuid.uuid4())

rows = merge_batch(
    price_pdf.set_index(["stock_id", "date"]),
    "finlab.bronze.price",
    run_id,
)
print("merged rows:", rows, "run_id:", run_id)
```

上例將 index 設回去，是因為第 8 節現有 `merge_batch()` 會先執行 `reset_index()`。正式實作時應統一介面：建議讓所有 crawler adapter 都回傳一般欄位，而 `merge_batch()` 不再自行猜測 index 狀態。

#### 步驟 6：用 SQL 驗證且測試冪等性

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT STRUCT(stock_id, date)) AS distinct_keys,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM finlab.bronze.price;
```

對同一個 `test_date` 再跑一次 crawler 與 MERGE，`rows` 不應因相同 key 而增加。確認單一 `price` 通過後，再依序接 `bargin_report`、`pe`、`benchmark`、月營收與四個小型快照 crawler，最後才處理財報。

#### 步驟 7：從測試 notebook 過渡到每日 Job

不要直接把所有測試 cells 堆在原本的 `L02_crawlers.ipynb`。建議拆成：

```text
finlab_project/
├─ finlab/                         # 原 crawler package
├─ databricks_src/
│  ├─ crawler_registry.py          # dataset、頻率、函式與 lookback 設定
│  ├─ delta_writer.py              # normalize + MERGE
│  └─ state_repository.py          # crawler_state / run_log
└─ notebooks/
   ├─ load_historical_parquet      # 只做一次的歷史匯入
   └─ finlab_daily_update          # Job 入口，只負責 orchestration
```

當 `finlab_daily_update` 能以 **Run now** 成功且同日重跑不增加重複 key，再按第 9 節建立每天一次的 schedule。

## 8. 每日增量程式的核心寫法

每個 crawler 只回傳「本次批次」，儲存層由獨立函式處理。核心 MERGE 可採：

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

def merge_batch(pdf, table_name: str, run_id: str) -> int:
    if pdf is None or len(pdf) == 0:
        return 0

    pdf = pdf.reset_index()
    pdf["stock_id"] = pdf["stock_id"].astype("string")
    pdf["date"] = pdf["date"].astype("datetime64[ns]")

    sdf = (
        spark.createDataFrame(pdf)
        .withColumn("_source", F.lit("finlab.crawler"))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    window = Window.partitionBy("stock_id", "date").orderBy(F.col("_ingested_at").desc())
    sdf = sdf.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")

    if not spark.catalog.tableExists(table_name):
        sdf.write.format("delta").saveAsTable(table_name)
        return sdf.count()

    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("t")
        .merge(sdf.alias("s"), "t.stock_id = s.stock_id AND t.date = s.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return sdf.count()
```

正式版還要處理 schema drift：先將來源欄位套用固定 mapping，再決定允許新增欄位或讓 Job fail fast。財務資料不建議無條件開啟全域 auto schema merge，否則來源欄名小改就可能產生重複欄位。

### 8.1 抓取範圍

- 日資料：從 `max_event_time - 7 days` 到今天，重抓最近 7 天以吸收補登與修正，再由 MERGE 覆蓋。
- 月營收：重抓最近 2 個月。
- 季財報：重抓最近 2 季，或至少重抓最新一季。
- divide/cap 快照：函式本來就回整批，可整批 MERGE；資料量很小。
- 若某日其中一個市場回空表，不要立刻把 state 推進到成功；記錄 partial/failed，隔日重試。

## 9. Job 與 schedule 設定

### 9.1 Free Edition 建議：一個 Job、一個 task

Free Edition 同時最多 5 個 job tasks，且此 crawler 有來源限速與 quota 考量。第一版用單一 notebook task 依序執行最穩定：

1. 網路與 dependency preflight。
2. 日資料。
3. 到期才跑月資料。
4. 到期才跑季資料。
5. 快照類。
6. 寫 run log 與驗證結果。

Job 設定：

- `max_concurrent_runs = 1`
- queue enabled
- timeout 建議 2–3 小時；若財報另拆季度 Job，日 Job 可縮短
- retry 1 次，間隔至少 10–15 分鐘
- 任一核心日資料失敗時 Job 應標示 failed，不要只 print 後繼續當成功

### 9.2 UI 設定步驟

1. 左側進入 **Jobs & Pipelines**。
2. **Create job**。
3. 新增 serverless Python notebook task，指向 orchestrator notebook。
4. 在 Job details 選 **Add trigger**。
5. Trigger type 選 **Scheduled**。
6. Schedule type 選 **Advanced**。
7. Time zone 選 `Asia/Taipei`。
8. 每天建議在 `20:00` 執行，避開收盤後資料尚未完全發布的時間。
9. 將 maximum concurrent runs 設為 1。
10. 先按 Run now，連續成功 3 次後才解除 schedule pause。

Quartz cron 可用：

```text
0 0 20 * * ?
```

這代表每天台北時間 20:00。雖然日價資料只在交易日更新，仍可每天觸發，由 orchestrator 對空資料與假日做安全處理，符合「每天跑一次」的需求。官方排程步驟與時區說明：[Run jobs on a schedule](https://docs.databricks.com/aws/en/jobs/scheduled)。

### 9.3 程式化部署（第二階段）

UI 版穩定後，再使用 Declarative Automation Bundles 保存 job 設定。概念設定如下：

```yaml
resources:
  jobs:
    finlab_daily_update:
      name: finlab-daily-update
      max_concurrent_runs: 1
      queue:
        enabled: true
      schedule:
        quartz_cron_expression: "0 0 20 * * ?"
        timezone_id: "Asia/Taipei"
        pause_status: "UNPAUSED"
      tasks:
        - task_key: crawl_and_merge
          notebook_task:
            notebook_path: ../src/finlab_daily_update.ipynb
          timeout_seconds: 10800
          max_retries: 1
```

Bundles 的正常流程是 `validate -> deploy -> run`。參考：[Develop a job with Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial) 與 [Bundles job resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)。

## 10. 你提供的兩個 ID 能做什麼

### Workspace ID

`7474649614053494` 可識別 workspace，但本身不是 CLI host，也不是登入憑證。CLI、SDK 或 Bundle 部署仍需要瀏覽器網址列中的完整 Workspace URL，例如：

```text
https://<workspace-instance>.cloud.databricks.com
```

官方說明 Workspace URL 與 ID 的差異：[Get identifiers for workspace objects](https://docs.databricks.com/aws/en/workspace/workspace-details)。

### Open Sharing ID

`aws:us-east-2:77425890-e9d9-4b60-80bf-7724fcd90025` 是 Delta Sharing/Open Sharing 的識別資訊，不是 workspace API endpoint、access token，也不能用來上傳檔案或代替 OAuth。它在未來要把 Delta tables 分享給外部 recipient 時才有用途。

### 真正開始部署前仍需要

- 完整 Workspace URL。
- 目前登入使用者在 catalog/schema 的建立權限。
- Free Edition notebook 對上述四個主要資料來源網域的 smoke-test 結果。

不要把 Google 密碼、OAuth token 或 PAT 寫入 notebook、Markdown 或 Git。CLI 優先用瀏覽器 OAuth U2M。

## 11. 驗收與切換

每張表至少驗證：

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT STRUCT(stock_id, date)) AS distinct_keys,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM finlab.bronze.price;
```

驗收清單：

- `rows = distinct_keys`。
- min/max date 與原 pickle 一致。
- 抽樣 20 個股票、20 個日期，比對所有欄位。
- benchmark 同一天保有多個 timestamp。
- 月營收日期語意沒有整月偏移。
- 四張財報表最新季度一致，且 cumulative/single-quarter 沒有混淆。
- 同一天重跑 Job，總 row count 不會無故增加，這是 idempotency 測試。
- 人為讓一個 endpoint 失敗，state 不應被誤標為 success。
- Job run log 能指出是哪個 dataset 失敗。

建議雙軌跑 7–14 天：本機 pickle 更新暫時保留，Databricks 每日同步更新並比對。確認穩定後才停止 `auto_update.py`。本機歷史檔不要立即刪除，至少保留一份唯讀備份到切換驗收完成。

## 12. 建議實施順序

### Phase 0：可行性（半天）

- 取得 Workspace URL。
- 執行網路 smoke test。
- 確認能建立 schema、Delta table、managed Volume 與 scheduled Job。
- 以 `monthly_report` 做第一個端到端測試。

### Phase 1：小表歷史搬遷（1 天）

- 建立 catalog/schema/state/run log。
- 將四張 divide/cap 表與 monthly_report 轉 Parquet、上傳、建立 Delta。
- 實作通用 MERGE 與驗證。

### Phase 2：日資料增量（1–2 天）

- 搬 `pe`、`bargin_report`、`price`。
- 完成 daily orchestrator、retry、logging、7 日 lookback。
- 設定每天 20:00 排程，但先 pause；Run now 驗證後再啟用。

### Phase 3：benchmark（1 天）

- 先調查 3.22 GiB 是否含異常重複或不必要欄位。
- 確保 key 使用 timestamp。
- 分開觀察其儲存與每日增量成本。

### Phase 4：財報（2–4 天）

- 把「下載/解析當季」與「合併全歷史」拆開。
- 只將新季度四表 MERGE，不再全歷史重建。
- 中間 ZIP/HTML 放暫存 Volume，成功後保留最近一期或清除。

### Phase 5：正式切換

- 雙軌驗證 7–14 天。
- 改寫 `finlab.data.Data.get()`，讓分析端讀 Delta/Databricks SQL，或匯出所需小範圍資料至本機 cache。
- 停止本機 `auto_update.py`。
- 保留 pickle 唯讀備份；確認回復演練後再另行決定歸檔/刪除。

## 13. 最終建議

最先做的不是上傳 43 GiB，而是用 `monthly_report` 完成一條最小可行路徑：

1. Free Edition serverless 能否連 TWSE/TPEx/MOPS。
2. crawler 能否在沒有 ipywidgets 與 `history` 相對路徑依賴下執行。
3. 一批 pandas DataFrame 能否正確 MERGE 到 Delta。
4. Job 能否每天排程且重跑不重複。

四項都通過，再依小表到大表搬遷。若第 1 項失敗，結論不是 Databricks 不能存放，而是 Free Edition 不適合直接擔任 crawler 執行器；仍可使用外部 runner 爬取，讓 Databricks 專注於 Delta 儲存、資料治理、查詢與後續模型開發。

## 14. Forecast model 訓練與打包

NN、LightGBM、Random Forest 的時間切分、train-only preprocessing、評估、artifact
bundle、MLflow/Unity Catalog 註冊與 Databricks Python script Job 步驟，已獨立整理在
`docs/forecast_model_training_and_packaging_zh-TW.md`。模型訓練不併入每日 crawler
Job；先 materialize 月頻 gold feature table，再由獨立 training Job 建立 Candidate。
