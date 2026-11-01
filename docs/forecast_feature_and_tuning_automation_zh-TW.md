# Forecast training features 與自動調參流程

最後更新：2026-10-23

本次實作的盤點、設計決策、修改過程、測試結果與待驗證項目，記錄於
`docs/forecast_feature_tuning_implementation_record_zh-TW.md`。

Gold schema、Agent 所需 `daily_prices`／`stock_factors`、權限與 Job 部署步驟見
`docs/gold_data_deployment_zh-TW.md`；正式資料契約見
`finlab_agent/docs/data-contracts.md`。

## 交付內容

- `databricks_migrate/notebooks/forecast_training_features.py`：由 bronze Delta tables 建立 `finlab.gold.forecast_training_features`。
- `databricks_migrate/databricks_src/tune_forecast_models.py`：Optuna + purged expanding-window 模型調參。
- `databricks_migrate/databricks_src/train_forecast_models.py`：可用 `--params-file` 讀取調參結果，再做正式 validation/test、打包與 MLflow 註冊。

流程如下：

```text
bronze price / pe / monthly_report / corporate-action ratios
                    |
                    v
       forecast_training_features notebook
                    |
                    v
 finlab.gold.forecast_training_features
                    |
          test 保留、不參與調參
                    v
       tune_forecast_models.py (walk-forward)
                    |
                    v
              best_params.json
                    |
                    v
       train_forecast_models.py --params-file
                    |
                    v
       untouched test + versioned model bundle
```

## 1. Gold notebook 的資料定義

一列為一個 observation month／股票，主鍵是 `date + stock_id`。預設只保留四碼股票。

特徵包括：

- `mom1` 到 `mom9`：月營收年序列中的歷史月增率訊號，沿用原 forecast notebook 的 shift 定義；
- `pe`、`pb`；
- `bias5...240`、`acc5...240`、`rsv5...240`；
- `return`：本期 observation 後第一個交易日開盤，到下期 observation 後第一個交易日開盤的倍數；
- `rank`：同一期 `return` 的 percentile rank。

為避免 look-ahead：

1. 月營收日期預設再延後五天，與舊 notebook 的 15 日 checkpoint 一致。
2. 日頻特徵只用 observation date 當日或之前的最後已知值。
3. label 使用 observation date 之後的開盤價，最後一個尚無下一期價格的月份不寫入。
4. 價格預設以四張除權息／減資 ratio table 還原；來源缺表時 notebook 會明確警告。

Notebook 寫入前會檢查：非空、主鍵唯一、期間數量門檻，以及 `rank` 位於 `[0, 1]`。通過後以完整重算／overwrite 發布，因此同一批 bronze 資料重跑是冪等的。

### 執行

把 notebook sync 到 Workspace，建立 notebook task，預設 widgets 即會寫入：

```text
finlab.gold.forecast_training_features
```

第一次可把 `start_date` 設近一點、`minimum_periods` 降低做 smoke test；正式執行再恢復完整歷史。若改變公式，請同步更新 `_feature_version`，不要在同一版本靜默改義。

## 2. 自動調參方法

調參腳本預設最佳化每一期橫斷面 Spearman Rank IC 的平均，而不是只看 row-level MSE。這比較貼近「每月選出相對較好的股票」的目標；也可以用 `--objective mse` 切換。

切分方式：

```text
expanding train | purge | validation 1
       expanding train | purge | validation 2
              expanding train | purge | validation 3
                                               | purge | untouched test
```

- test 預設最後 24 期，Optuna 完全不讀取。
- 每個 fold 的 imputer/scaler 都只 fit 該 fold 的 train。
- `label_horizon_periods=1` 時，每個 train/validation 邊界 purge 一期。
- LightGBM 另外在各 fold early stop，輸出的 `lgb_estimators` 是最佳 trial 各 fold `best_iteration` 的中位數。
- NN 是昂貴選項，預設只調 `lgb,rf`；需要時明確加入 `--models lgb,rf,nn`。

### Smoke run

```powershell
python .\databricks_migrate\databricks_src\tune_forecast_models.py `
  --input-path .\data\forecast_training_features.parquet `
  --output-dir .\forecastModel\tuning\smoke `
  --models lgb,rf `
  --n-trials 3 `
  --cv-splits 2 `
  --validation-periods 3 `
  --test-periods 6
```

### Databricks candidate run

```text
python tune_forecast_models.py
  --input-table finlab.gold.forecast_training_features
  --output-dir /Volumes/finlab/ml/tuning/stock_rank/2026-q4
  --models lgb,rf
  --n-trials 50
  --cv-splits 4
  --validation-periods 6
  --test-periods 24
  --storage sqlite:////Volumes/finlab/ml/tuning/stock_rank/2026-q4/optuna.db
```

SQLite storage 只適合 `--n-jobs 1` 的單程序恢復執行。若要多 worker 並行，應使用 Optuna 支援的外部 RDB storage；不要讓多個 worker 同時寫同一個 Volume SQLite 檔。

每個 model 會輸出完整 `trials_<model>.csv`，總結寫入 `best_params.json`。同一組 candidate 正式執行時要固定 gold table snapshot、feature 欄位順序和 test window。

## 3. 用最佳參數做正式訓練

```powershell
python .\databricks_migrate\databricks_src\train_forecast_models.py `
  --input-table finlab.gold.forecast_training_features `
  --params-file /Volumes/finlab/ml/tuning/stock_rank/2026-q4/best_params.json `
  --output-dir /Volumes/finlab/ml/model_artifacts/stock_rank `
  --experiment-name /Shared/finlab-stock-rank `
  --register-model-name finlab.ml.stock_rank_ensemble
```

參數優先順序為：程式預設值 < `best_params.json` < 明列 CLI 參數。因此可以用同一份結果，只以 `--rf-estimators 100` 覆寫做快速驗證。

`best_model` 代表 walk-forward validation 勝出者，不等於可以直接升 Champion。正式 train script 仍會：

1. 用獨立 validation 做 early stopping；
2. 對 untouched test 只評估一次；
3. 保存 dataset fingerprint、完整 config、predictions、metrics 與 checksums；
4. 只建立 Candidate。另行通過交易成本、換手、容量與資料品質門檻後才升 Champion。

## 4. 建議自動化排程

季度執行一個三 task workflow：

1. `materialize_features`：執行 gold notebook。
2. `tune_models`：依賴 1，執行 tuner，寫入帶季度或 run ID 的 Volume 目錄。
3. `train_candidate`：依賴 2，以該 run 的 `best_params.json` 訓練與註冊 Candidate。

每日 crawler 不應觸發模型調參。若資料品質 gate 或任一 fold 失敗，整條 candidate workflow 應停止，不沿用一份無法對應當前 dataset fingerprint 的舊參數檔。

## 5. 驗證指令

不需 Spark、TensorFlow 或 LightGBM 的結構測試：

```powershell
python -m unittest `
  databricks_migrate.tests.test_train_forecast_models `
  databricks_migrate.tests.test_tune_forecast_models -v
```

Notebook 的 Spark execution 與 Delta write 需在 Databricks serverless runtime 驗證。
