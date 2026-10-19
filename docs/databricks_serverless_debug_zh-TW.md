# Databricks Free Edition serverless 相容性除錯

本文件記錄 FinLab 歷史資料匯入過程遇到的 serverless 限制、判斷方式與修正。

## `PERSIST TABLE is not supported on serverless compute`

### 症狀

`load_historical_parquet` 可成功讀取 Parquet，但呼叫 `DeltaWriter.merge_spark()` 後失敗：

```text
[NOT_SUPPORTED_WITH_SERVERLESS]
PERSIST TABLE is not supported on serverless compute
SQLSTATE: 0A000
```

### 根因

`delta_writer.py` 曾在去重後呼叫：

```python
prepared = prepared.cache()
```

Databricks serverless 使用 Spark Connect。`DataFrame.cache()`、`persist()`、
`unpersist()`、`checkpoint()` 以及 SQL `CACHE TABLE` 都不受 serverless 支援。
Spark Connect 將這個 cache 請求送到服務端後，錯誤訊息呈現為不支援
`PERSIST TABLE`。

這個錯誤與 Parquet timestamp、Delta Column Mapping、Volume 權限或資料內容無關。

### 本專案修正

移除 `prepared.cache()`，也移除其所有對應的 `prepared.unpersist()`。去重、
非空 key 過濾、row count、首次建表以及後續 Delta MERGE 邏輯不變：

```python
prepared = (
    prepared
    .withColumn("_row_number", F.row_number().over(window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
)
input_rows = prepared.count()
```

因為沒有 cache，`count()` 和後續 write/MERGE 會各自計算一次 DataFrame lineage。
這會增加首次歷史匯入的計算時間，但符合 Free Edition serverless 限制，而且不會
改變輸出資料。若未來資料轉換昂貴到無法接受重算，官方建議把中間結果實體化成
受控的 Delta staging table，而不是改用 cache。

### 部署與重跑

在專案根目錄同步修正後的模組：

```powershell
$email = "hsiangju.lai.95316@gmail.com"
$profileName = "finlab-free"
$workspaceRoot = "/Users/$email/finlab_project"

databricks sync `
  ".\databricks_migrate\databricks_src" `
  "$workspaceRoot/databricks_src" `
  --exclude "*.pyc" `
  --exclude "__pycache__" `
  --profile $profileName
```

若目前 notebook session 已 import 舊版模組，執行：

```python
dbutils.library.restartPython()
```

然後重新 Run All，`datasets` 先設為 `monthly_report`。不需要重新轉換或上傳
Parquet；這個錯誤發生在 Parquet 讀取之後。

完成後驗證：

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT STRUCT(stock_id, date)) AS distinct_keys,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM finlab.bronze.monthly_report;
```

`monthly_report` 的歷史來源預期為 381,839 筆；`rows` 應等於
`distinct_keys`。

## 參考文件

- [Databricks serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
- [Migrate from classic compute to serverless compute](https://docs.databricks.com/aws/en/compute/serverless/migration)
- [Databricks DataFrame.persist reference](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/persist)

## `ModuleNotFoundError: No module named 'talib'`

### 症狀與根因

Daily Job 在 `price` 尚未開始抓資料前失敗。`crawler.py` 本身不使用 TA-Lib；
真正的 import chain 是：

```text
import finlab.crawler
  -> 執行 finlab/__init__.py
  -> from .data import Data
  -> finlab/data.py import talib.abstract
```

因此這是 package 頂層的非必要依賴，不是 `crawl_price()` 的 runtime dependency。

### 本專案修正

`finlab/__init__.py` 改用 module `__getattr__` 延遲載入 `Data`，並在
`talib_all_stock()` 真正被呼叫時才 import `Data`。這樣可同時達成：

- crawler-only Job 不需要安裝 TA-Lib；
- 既有的 `from finlab import Data` API 保持可用；
- 真正執行 `Data.talib()`、`finlab.ml` 或其他技術指標程式時，仍明確要求
  TA-Lib，應在另外的 ML/feature environment 安裝。

修改後必須把 `finlab/` 重新同步到 Workspace，並重啟已經 import 舊 package
的 Python session。只同步 `databricks_src/` 不會帶入這項修正。

### 若執行環境確實需要 TA-Lib

在 serverless notebook 的 Environment side pane，把下列 PyPI requirement 加到
Dependencies 後按 Apply：

```text
TA-Lib==0.8.2
```

目前官方 TA-Lib Python 套件提供 Linux `x86_64` 與 `aarch64` binary wheels，
並將原生 C library 包含在 wheel 中，因此不需要 `apt`、Conda 或 init script。
正式 Lakeflow Job 若使用 Jobs Environment 而不是 Notebook Environment，也必須在
該 task 的 Environment and Libraries 加入相同 dependency。

安裝後驗證：

```python
import platform
import numpy as np
import talib

print("architecture:", platform.machine())
print("numpy:", np.__version__)
print("talib:", talib.__version__)
print(talib.SMA(np.arange(20, dtype="float64"), timeperiod=5)[-3:])
```

## `ChunkedEncodingError: Connection broken: IncompleteRead`

### 症狀與根因

TWSE 或 TPEx 回應採用 chunked transfer 時，來源站或中間網路偶爾會在完整 body
傳完前關閉連線。Requests 會拋出 `ChunkedEncodingError`，其中
`IncompleteRead(read, expected)` 分別表示已收到與尚缺少的 bytes。

舊版 `requests_get()` 只捕捉 `ConnectionError` 和 `ReadTimeout`，因此這個例外會
跳過底層 HTTP retry，直接讓該 dataset 失敗。

### 本專案修正

- 將 `ChunkedEncodingError` 納入暫時性 HTTP errors；
- 每次傳輸失敗後關閉舊 Session，避免重用已損壞的 pooled connection；
- 使用新的 Session 重試最多四次；
- connect/read timeout 分別為 10/30 秒；
- retry 等待採 5、10、20 秒指數退避並加少量 jitter；
- 最後一次仍失敗時重新拋出原始例外，讓 crawler run log 記錄真正原因。

外層 `invoke_crawler()` 仍保留 dataset 級兩次重試。失敗 run 的
`crawler_state.max_event_time` 不會前進；已成功 MERGE 的日期由
`stock_id + date` key 保證重跑不會新增重複資料。因此部署新版 `finlab/` 後可直接
重跑相同 `run_date`，不需要清空 Delta table。

