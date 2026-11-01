# Forecast feature table 與模型調參實作紀錄

日期：2026-10-23（Asia/Taipei）

範圍：`finlab.gold.forecast_training_features`、walk-forward 自動調參、正式模型訓練參數串接

## 1. 需求與完成條件

本次需求分為兩部分：

1. 建立產生 `finlab.gold.forecast_training_features` 的 Databricks notebook。
2. 整理並實作一套能自動尋找較佳模型參數、保留未見 test set、最後交由正式訓練程式打包模型的方法。

完成條件定義為：

- Gold table 有明確的一列一股票一期資料契約及唯一主鍵。
- feature 只使用 observation date 當下已知資料。
- label、validation 與 test 不會回流到 feature preprocessing 或超參數選擇。
- 調參結果能以檔案直接傳給正式訓練程式，不需人工複製參數。
- 既有 NN、LightGBM、Random Forest 訓練／打包流程保持可執行。
- 補上單元測試、smoke test 與操作文件。

## 2. 現況盤點

開始實作前檢查了下列既有內容：

- `LL_updated_forecast.ipynb`：舊版特徵公式、模型組合及 top-20 評估方式。
- `finlab/ml.py`：`add_profit_prediction()`、`add_rank_prediction()` 與技術指標工具。
- `finlab/data.py`：`Data.get_adj()` 的除權息／減資還原邏輯。
- `databricks_migrate/databricks_src/crawler_registry.py`：bronze dataset 名稱及更新週期。
- `databricks_migrate/databricks_src/delta_writer.py`：bronze table 的 `stock_id + date` 契約。
- `databricks_migrate/databricks_src/train_forecast_models.py`：既有時間切分、train-only preprocessing、三模型訓練、評估及 bundle 打包骨架。
- `docs/forecast_model_training_and_packaging_zh-TW.md`：既有模型訓練規格。

盤點後確認資料流應拆成三個生命週期：

```text
每日 crawler / bronze refresh
            |
            v
月頻 gold feature materialization
            |
            v
季度或按需 tuning + candidate training
```

因此沒有把 feature engineering 或 tuning 塞進每日 crawler job。

## 3. Gold notebook 實作過程

### 3.1 來源資料

Notebook 使用：

- `finlab.bronze.price`
- `finlab.bronze.pe`
- `finlab.bronze.monthly_report`
- 可用時納入 `otc_cap_reduction`、`twse_cap_reduction`、`otc_divide_ratio`、`twse_divide_ratio`

所有 table name 都由受限的 identifier widgets 組合，避免把任意字串直接插入 SQL/table identifier。

### 3.2 價格還原

舊版 `Data.get_adj()` 會把四種 corporate-action ratio 按股票累乘到價格。本次以 Spark timeline 重作：

1. 四張 ratio table 正規化成 `stock_id / date / adjustment_ratio`。
2. 同股票、同日期有多筆事件時先取乘積。
3. ratio event 與 daily price union 成同一條 timeline。
4. 依 `date, event_order` 計算累積乘積；同日事件排在 price 前。
5. 對 open/high/low/close 套用 adjustment factor。

事件落在非交易日時，會在下一個 price row 前先進入累積值，與舊邏輯的效果一致。若來源 ratio table 不存在，notebook 會明確警告並使用 raw price。

### 3.3 特徵

沿用舊 forecast notebook 的主要訊號：

- `mom1...mom9`
- `pe`、`pb`
- `bias5/10/20/60/120/240`
- `acc5/10/20/60/120/240`
- `rsv5/10/20/60/120/240`

技術指標以每支股票的 daily ordered window 計算。`acc240` 需要讀取前 480 個交易列，因此價格在正式 `start_date` 前額外讀取 800 個日曆日，保留週末、假日及停牌的緩衝。

### 3.4 Point-in-time 對齊

月營收的 source date 預設加五天，對齊舊 notebook 的每月 15 日 checkpoint。接著把 daily feature rows 與 monthly observation rows union 成 timeline，以 `last(..., ignorenulls=True)` 只向前帶入當日或更早的值。

沒有使用「找最近日期後雙向補值」，以免把 observation date 之後的 PE、PB 或價格技術指標帶回當期。

### 3.5 Label

每個 observation 的進場價格定義為該日期之後第一個交易日的 adjusted open；下一期同樣取 observation 後第一個交易日 open：

```text
return(t) = next_entry_open(t + 1) / entry_open(t)
```

同一期股票再以 `percent_rank()` 產生 `[0, 1]` 的 `rank`。最後一個月份因下一期價格未知而自然排除。

### 3.6 發布前品質門檻

寫入前檢查：

- rows > 0；
- `COUNT(*) = COUNT(DISTINCT date, stock_id)`；
- period count 達到 `minimum_periods`；
- `rank` 落在 `[0, 1]`；
- return 不為 null、NaN，且為正值。

通過後才以 overwrite + overwriteSchema 完整重建 Delta table。同一份 bronze snapshot 重跑會得到相同業務資料；`_materialized_at` 只記錄本次產製時間。

完成檔案：

- `databricks_migrate/notebooks/forecast_training_features.py`

## 4. 自動調參實作過程

### 4.1 為何不用隨機 K-fold

股票月頻 label 使用未來一期價格，隨機切列會讓同一時期股票散落在 train/validation，並可能讓未來期間進入 train。因此實作 expanding-window validation，且所有切分以完整 `date` 為單位。

### 4.2 切分設計

預設概念為：

```text
expanding train | purge | validation fold 1
       expanding train | purge | validation fold 2
              expanding train | purge | validation fold 3
                                               | purge | untouched test
```

- `label_horizon_periods=1` 時，每個 train/validation 邊界排除一期。
- 最後 `test_periods=24` 完全不提供給 Optuna objective。
- 每個 fold 各自 fit median imputer；NN 另外只用 fold train fit MinMaxScaler。

### 4.3 Objective 與搜尋範圍

預設 objective 是各 validation 月份橫斷面 Spearman Rank IC 的平均；可切換為 MSE。

- LightGBM：estimators、learning rate、leaves、depth、min child、row/column sampling、L1/L2。
- Random Forest：estimators、depth、min leaf、max features。
- Neural Network：hidden layers、dropout、learning rate、batch size；因成本較高，不列入預設 models。

LightGBM 每個 fold 使用 early stopping，最後輸出的 `lgb_estimators` 採最佳 trial 各 fold `best_iteration` 中位數。NN 若有執行，`nn_epochs` 同樣取各 fold 最佳 epoch 中位數。

### 4.4 輸出與恢復

每個 model 產生：

- `trials_<model>.csv`：完整 trial 記錄。
- `best_params.json`：最佳 trial、fold scores、CV 日期範圍、untouched test 範圍，以及 `training_config_overrides`。

可用 `--storage` 指定 Optuna storage 並以相同 study name 恢復。Volume 上的 SQLite 只建議單程序使用；多 worker 應改外部 RDB。

完成檔案：

- `databricks_migrate/databricks_src/tune_forecast_models.py`
- `databricks_migrate/tests/test_tune_forecast_models.py`

## 5. 正式訓練串接

為避免人工從 tuning log 複製參數，正式訓練程式新增 `--params-file`。

合併順序：

```text
TrainConfig defaults < best_params.json < explicit CLI arguments
```

JSON 只接受白名單內的模型參數；未知 key 直接失敗，避免錯字被靜默忽略。新增可設定項目涵蓋 NN hidden units/learning rate、LightGBM tree/regularization 參數及 RF max features。

正式訓練仍使用既有流程：chronological train/validation/test、train-only preprocessing、validation early stopping、untouched test 評估、artifact reload smoke test、checksums，以及選配 MLflow/Unity Catalog registration。

異動檔案：

- `databricks_migrate/databricks_src/train_forecast_models.py`
- `databricks_migrate/tests/test_train_forecast_models.py`
- `databricks_migrate/requirements-ml.txt`（加入 `optuna`）

## 6. 實際驗證紀錄

### 6.1 語法與 CLI

執行：

```powershell
python -m py_compile `
  databricks_migrate\databricks_src\train_forecast_models.py `
  databricks_migrate\databricks_src\tune_forecast_models.py `
  databricks_migrate\notebooks\forecast_training_features.py
```

結果：通過。

另檢查兩支 script 的 `--help`，`--params-file`、新增 LightGBM/RF/NN 參數與 tuner options 均可正常解析。

### 6.2 單元測試

系統預設 Python 3.13 沒有 numpy/pandas，因此第一次載入測試時得到 dependency import error；這不是程式 assertion failure。之後改用已存在且具有 numpy/pandas/scikit-learn 的 Anaconda base，不新增或修改使用者套件。

執行：

```powershell
C:\Users\poppi\anaconda3\python.exe -m unittest `
  databricks_migrate.tests.test_train_forecast_models `
  databricks_migrate.tests.test_tune_forecast_models -v
```

結果：9/9 通過。涵蓋：

- feature inference 與 reserved columns；
- duplicate key／rank range fail-fast；
- chronological split 與兩側 purge；
- tuning JSON whitelist 與 CLI precedence；
- expanding folds 與 untouched test；
- Rank IC scoring；
- model name validation。

完整 migration suite：21/21 通過。

### 6.3 模型 train/save/reload smoke test

使用既有 `finlab` Conda environment 執行：

```powershell
C:\Users\poppi\anaconda3\envs\finlab\python.exe `
  -m databricks_migrate.tests._smoke_training
```

結果：

```text
FULL_SMOKE_OK features=['mom1', 'pe', 'bias20'] output_shape=(3, 4)
```

NN、LightGBM、Random Forest 均完成訓練、保存、重載與 ensemble predict。TensorFlow 顯示本機缺少 CUDA DLL，但自動使用 CPU，smoke test 成功。

### 6.4 靜態品質檢查

- `git diff --check`：通過。
- 新增 Python 檔案無 trailing whitespace。
- 沒有執行破壞性 git 或資料刪除操作。

## 7. 尚待 Databricks 驗證

本機沒有同時具備 `dbutils + Spark + Delta + bronze tables` 的 runtime，因此以下項目必須在 Databricks serverless 完成：

- 三張必要 bronze tables 的實際中文欄位名稱是否完全符合候選清單；
- 四張 corporate-action table 的 ratio 欄位與事件日對齊結果；
- notebook 的 Spark execution plan、shuffle／memory 用量；
- overwrite `finlab.gold.forecast_training_features` 的 Unity Catalog 權限；
- Gold table row count、period count、缺值率與抽樣 label 人工核對；
- 安裝 `requirements-ml.txt` 後的 Optuna 實際 trial；
- Volume 上的 tuning artifacts、bundle 與 MLflow registration。

這些是 runtime／資料整合驗證，不影響目前已通過的本機程式與模型打包測試。

## 8. 建議實際執行順序

1. Sync `notebooks/`、`databricks_src/` 與 requirements 到 Workspace。
2. 以較近 `start_date`、較低 `minimum_periods` 跑 feature notebook smoke test。
3. 查詢 row/key/date/rank/feature coverage，抽樣核對數支股票。
4. 恢復完整歷史設定，正式產生 gold table。
5. 以 3 trials、2 folds 執行 tuner smoke test。
6. 確認 `trials_*.csv`、`best_params.json` 和 untouched test 日期。
7. 執行正式 30–50 trials；NN 視資源另開較小 study。
8. 將同一 run 的 `best_params.json` 傳給 `train_forecast_models.py --params-file`。
9. 檢查 validation/test metrics、reload smoke test、manifest 與 checksums。
10. 只註冊 Candidate；完成交易成本與容量檢查後才考慮 Champion alias。

詳細操作指令與參數說明見：

- `docs/forecast_feature_and_tuning_automation_zh-TW.md`
- `docs/forecast_model_training_and_packaging_zh-TW.md`

## 9. Gold 部署與 FinLab Agent 契約增補

後續檢視 `finlab_agent/docs/architecture.md`、`README.md`、repository、Pydantic schemas、
三個 tools 與 App resources 後，確認原本只有模型 training Gold table，尚不足以部署 Agent。
Agent 實際需要兩張獨立 consumer contracts：

- `finlab.gold.daily_prices`：OHLCV、固定 adjustment basis；
- `finlab.gold.stock_factors`：以 `available_at` 為準的 point-in-time 營收因子。

因此新增：

- `databricks_migrate/notebooks/materialize_agent_gold.py`；
- `docs/gold_data_deployment_zh-TW.md`；
- `finlab_agent/docs/data-contracts.md`。

Materialization notebook 支援：第一次 full rebuild、日常 14 日重疊 MERGE、定期 full
rebuild、四張 corporate-action source 嚴格檢查、adjustment basis 防混寫、精確前月／去年
同月 revenue join、quality gates、table properties 與 contract registry。

資料權限也明確分離：一般 Agent 只讀 `daily_prices`／`stock_factors`；含未來
`return/rank` label 的 `forecast_training_features` 只開放 training principal。未來新增
Tool 時，依 capability 建立新的資料產品與 repository protocol，不把所有欄位與方法持續
塞入單一 `PriceRepository` 或寬因子表。

部署文件已補齊：bronze 前置檢查、CLI sync、candidate smoke widgets、正式 full run、
驗收 SQL、日常 incremental Job、App resources/GRANT、端到端問題、回滾與新 Tool 上線順序。
