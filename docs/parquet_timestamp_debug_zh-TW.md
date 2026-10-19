# 歷史匯入：TIMESTAMP(NANOS) 除錯與修復

記錄日期：2026-10-19

## 錯誤與原因

`load_historical_parquet` 的 log：

```text
[monthly_report] reading /Volumes/finlab/ops/landing/monthly_report.parquet
[PARQUET_TYPE_ILLEGAL] Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
```

錯誤發生在 `spark.read.parquet()` 的 schema inference，還沒執行該 dataset 的 Delta
MERGE。現有 Parquet 檔內 `date` 的物理 logical type 為奈秒 timestamp，而目前使用的
Spark reader 不支援。`false` 代表時間不帶 UTC-adjusted 註記；本次問題是 `NANOS`
精度，不需要因此修改成 UTC 或把日期變成字串。

先前 notebook 只檢查 PyArrow 能讀回、欄名與列數相同，沒有檢查 Spark 的 timestamp
相容性。pandas datetime64[ns] 搭配 PyArrow 的預設格式可能直接產生奈秒 Parquet。

2026-10-19 本機 metadata 檢查：`export/parquet` 中 13 個檔案的 `date` 都是
`timestamp[ns]`；不是只影響 monthly_report。

## 已修改的程式

`databricks_migrate/pkl_to_parquet.ipynb` 的 writer 明確設定：

```python
df.to_parquet(
    target,
    index=False,
    engine="pyarrow",
    compression="snappy",
    version="2.4",
    coerce_timestamps="us",
    allow_truncated_timestamps=False,
    use_deprecated_int96_timestamps=False,
)
```

`us` 是微秒：一微秒等於 1,000 奈秒。這是日期的儲存精度轉換，不是時區平移。
`allow_truncated_timestamps=False` 會拒絕無法精確表示的 sub-microsecond 資料，避免
悄悄改掉日期鍵。另增加 physical schema 檢查，確定未留下 `TIMESTAMP(NANOS)`。

既有 Parquet 可使用 `databricks_migrate/repair_parquet_timestamps.py` 修復：

- 不重新讀 pkl，使用 Arrow 每批 100,000 列串流轉寫。
- 將 timestamp 欄位安全轉成微秒，其他欄位維持原型別、名稱和值。
- 寫入另一個目錄，拒絕原地覆寫與覆寫已存在的輸出。
- 比對列數與欄名，驗證 physical schema，再將暫存檔改為正式輸出名稱。
- 發生精度損失或其他錯誤時移除未完成暫存檔，原始輸入保留。

## monthly_report：可以直接使用的修復檔

已產生：

```text
databricks_migrate/export/parquet_spark/monthly_report.parquet
```

實測結果：381,839 列；physical schema 為 `TIMESTAMP(MICROS,false)`。原始資料
安全 cast 到微秒後，全部欄位與值和新檔完全相同。

在專案根目錄 PowerShell 執行以下指令，替換 Volume 中的舊格式檔案：

```powershell
databricks fs cp `
  ".\databricks_migrate\export\parquet_spark\monthly_report.parquet" `
  "dbfs:/Volumes/finlab/ops/landing/monthly_report.parquet" `
  --overwrite `
  --profile finlab-free
```

回到 Databricks，先執行讀取驗證：

```python
df = spark.read.parquet("/Volumes/finlab/ops/landing/monthly_report.parquet")
df.printSchema()
print(df.count())  # 381839
```

成功後重新執行 `load_historical_parquet`，widgets 設：

```text
catalog       = finlab
bronze_schema = bronze
ops_schema    = ops
landing_volume = landing
datasets      = monthly_report
```

`landing_volume` 填 Volume 的名字 `landing`，程式會組合完整 `/Volumes/...` 路徑。
這次失敗不需要刪除 state 或 Delta table；成功重跑會記錄新的 success。

## 修復其他歷史檔案

在使用原本能成功轉檔的 Python 環境中執行。為保留上面已修復的 monthly_report，
此處選另一個新輸出目錄：

```powershell
python .\databricks_migrate\repair_parquet_timestamps.py `
  --datasets all `
  --output-dir .\databricks_migrate\export\parquet_spark_all
```

它會逐檔處理，也會重新輸出一份 monthly_report。大型 price/benchmark 仍需要讀寫
完整檔案，但不會一次把整張表載入 pandas。若輸出已存在，請指定另一個新目錄；
程式不會自行覆蓋它。

修復後上傳（會覆蓋 landing 中同名檔案；本機原始檔仍保留）：

```powershell
databricks fs cp `
  ".\databricks_migrate\export\parquet_spark_all" `
  "dbfs:/Volumes/finlab/ops/landing/" `
  --recursive --overwrite --profile finlab-free
```

對新轉檔 notebook 不必只為這次錯誤重新跑所有 pkl；修復工具可以直接利用既有
Parquet。之後若從 pkl 重新產生檔案，則使用修正後的 writer 選項。

## 驗證與參考文件

```powershell
python -B -m unittest databricks_migrate.tests.test_parquet_timestamps -v
```

三個回歸測試：跨 batch 保留字串代碼、日期、null 與數值；拒絕真實奈秒精度損失；
拒絕覆寫輸入或已存在的輸出。本機均通過。Databricks Spark 讀取與 Delta MERGE
仍需在重傳檔案後由 workspace 實際執行；本次未連線修改雲端檔案。

- [Databricks 官方 KB：Illegal Parquet type / timestamp nanoseconds](https://kb.databricks.com/en_US/python/illegal-parquet-type-exception-when-reading-a-parquet-file-with-timestamp-column-of-datatype-int-nanoseconds)：對應本次錯誤及 reader schema inference 失敗的原因。該頁也提供明列 schema、把奈秒讀為 long 的替代方法；這份實作選擇在來源改為微秒，讓全部後續 consumers 可直接讀取。
- [Apache Arrow：write_table](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html)：Parquet version、`coerce_timestamps`、`allow_truncated_timestamps` 與 INT96 選項。
- [Apache Arrow：ParquetWriter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html)：逐批寫入 Parquet。
