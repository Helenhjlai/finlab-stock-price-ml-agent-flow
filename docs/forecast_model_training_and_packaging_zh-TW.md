# Forecast model 訓練、打包與 Databricks 執行指南

最後更新：2026-10-17

實作檔案：

- `databricks_migrate/notebooks/forecast_training_features.py`
- `databricks_migrate/databricks_src/tune_forecast_models.py`
- `databricks_migrate/databricks_src/train_forecast_models.py`
- `databricks_migrate/requirements-ml.txt`
- `databricks_migrate/tests/test_train_forecast_models.py`

Gold table 產製、purged walk-forward 自動調參及 `--params-file` 用法另見
`docs/forecast_feature_and_tuning_automation_zh-TW.md`。

## 1. 這支程式處理到哪裡

這支程式負責「已產生好的模型特徵資料」到「可部署模型包」的完整流程：

```text
finlab.gold.forecast_training_features
              │
              │ 載入並驗證 date / stock_id / rank / features
              v
     chronological train / validation / test
              │
              │ 交界 purge 一個 label horizon
              v
    preprocessing 只在 train 執行 fit
              │
              ├── imputer + MinMaxScaler ──> Neural Network
              │
              └── imputer ────────────────> LightGBM / Random Forest
                                               │
                                               v
                              validation + untouched test 評估
                                               │
                                               v
                              versioned artifact bundle
                                               │
                         ┌─────────────────────┴───────────────────┐
                         v                                         v
                 Unity Catalog Volume                    MLflow registered model
                                                              （可選）
```

它刻意不在訓練期間重新爬資料，也不直接從 `finlab.bronze.price` 等原始表臨時計算全部特徵。上游應先把某一版本的 feature/label materialize 成 `finlab.gold.forecast_training_features`。原因是：

1. crawler 每日更新、feature table 月頻更新、模型候選訓練是三個不同生命週期。
2. 固定的 training table 能留下可查證的資料版本，避免一次訓練中途資料改變。
3. `toPandas()` 只收斂後的月頻訓練列，而不是數十 GB 的日資料。

因此這裡的「從頭到尾」是指模型訓練流程的開始到模型打包完成；bronze 到 gold 的 feature engineering 應是前一個獨立 Job/task。

## 2. 輸入資料契約

一列必須代表一個月份／一支股票：

| 欄位 | 必要 | 說明 |
|---|---:|---|
| `date` | 是 | 預測時點，會轉成 timestamp |
| `stock_id` | 是 | 股票代碼，固定視為字串 |
| `rank` | 是 | 下一期報酬在同期間股票中的 percentile rank，舊程式由 `add_rank_prediction(..., n=1)` 產生 |
| feature columns | 是 | 例如 `mom1`、`pe`、`pb`、`bias60`、KD/RSI/MACD 等 |
| `return` | 否 | 若有，額外計算每期 top-K 的平均報酬與相對全體平均差異 |

主鍵是 `date + stock_id`。若有重複，程式直接失敗，不自行猜測應保留哪筆。

建議正式執行時明列 feature 順序：

```text
--feature-cols mom1,mom2,mom3,mom4,mom5,mom6,mom7,mom8,mom9,pe,pb,...
```

不指定時，程式會：

- 排除 `date`、`stock_id`、`rank`、`return`、舊預測結果和 `_...` metadata。
- 只採用至少 95% 非空值可轉為數字的欄位。
- 將正負無限值轉成 missing，再由 train-only median imputer 處理。

自動推斷適合 smoke test；production 建議明列 feature，避免上游新增數值欄後無意改變模型輸入。

## 3. 為什麼先切資料，再 normalize

正確順序：

```python
split = chronological_split(data)

nn_preprocessor.fit(split.train[features])
train_x = nn_preprocessor.transform(split.train[features])
validation_x = nn_preprocessor.transform(split.validation[features])
test_x = nn_preprocessor.transform(split.test[features])
```

train、validation、test 都使用同一個由 train 學到的 min/max，因此尺度定義一致；test 值落到 `[0, 1]` 之外是合理現象，代表未來值超出過去範圍，不應回頭用未來資料更新 scaler。

若先對全資料執行 `fit_transform()`，test 的最小值、最大值與缺值中位數會流入訓練流程，使 test 評估偏樂觀。scikit-learn 官方的 data leakage 指南同樣要求先切分，且 `fit`/`fit_transform` 只使用 train，再對其他資料使用 `transform`。

本實作保存兩個 preprocessor：

- `nn_preprocessor.joblib`：median imputer + MinMaxScaler。
- `tree_preprocessor.joblib`：median imputer；不縮放 LightGBM/RF 的特徵。

## 4. 時間切分與 purge 邏輯

預設參數：

```text
validation_periods = 12
test_periods = 24
label_horizon_periods = 1
```

切割單位是完整的 `date`，同一日期的股票不會散落在不同 split。概念如下：

```text
train | purge 1 period | validation 12 periods | purge 1 period | test 24 periods
```

舊 `rank(t)` 使用 `t` 到 `t+1` 的報酬。若 train 的最後一筆緊貼 validation，最後一個 train label 實際會用到 validation 期間的價格。`label_horizon_periods=1` 因此在兩個邊界各排除一個日期。

test 只用於最後一次候選模型報告，不用於：

- scaler/imputer fitting；
- NN early stopping；
- LightGBM early stopping；
- 超參數選擇；
- ensemble 權重選擇。

若未來改成 `add_rank_prediction(..., n=3)`，也應把 `--label-horizon-periods` 改為 `3`。

## 5. 三個模型的實際邏輯

### Neural Network

沿用舊 notebook 的主要形狀：

```text
features -> Dense(100, relu) -> Dense(100, relu)
         -> Dropout(0.7) -> Dense(1, sigmoid)
```

- loss：MSE。
- optimizer：Adam。
- validation：真正的後段時間資料，不使用 `validation_split=0.1` 隨機抽列。
- early stopping：監控 `val_loss`，並恢復最佳權重。
- 輸出使用 `.keras` 格式。

### LightGBM

- 輸入只有 train-fit median imputation，不做 MinMax scaling。
- 使用 validation set early stopping。
- 保存 native Booster 文字檔 `lightgbm.txt`。

### Random Forest

- 輸入只有 train-fit median imputation。
- 預設 `max_depth=18`、`min_samples_leaf=5`、`max_features=sqrt`，避免重現過去接近 1 GB 的無限制森林。
- 保存 `random_forest.joblib`。

RF 的 joblib/pickle 類 artifact 只能載入自己建立並信任的檔案，不應載入未知來源檔案。

### Ensemble

目前為簡單且可解釋的等權平均：

```python
ensemble = (nn + lightgbm + random_forest) / 3
```

不要用 test 最佳化權重。之後若要改權重，只能用 validation 或 rolling walk-forward 結果，再對 test 做一次最終驗證。

## 6. 評估指標

每個模型及 ensemble 都會產生：

- MSE、MAE；
- 全資料 Spearman rank correlation；
- 每一期橫斷面 Spearman Rank IC 的平均；
- IC information ratio（平均 IC / IC 標準差）；
- 若有 `return`，每期 top-K 平均報酬與相對當期全體平均的差。

`top_K_mean_return` 是快速診斷，不是完整回測；尚未處理換手、成交量限制、漲跌停、滑價、交易稅與手續費，不能直接當成策略績效。

## 7. 模型包內容

每次執行會建立新目錄，不覆蓋舊版本：

```text
/Volumes/finlab/ml/model_artifacts/stock_rank/<run_name>/
├─ manifest.json
├─ checksums.sha256
├─ models/
│  ├─ nn.keras
│  ├─ lightgbm.txt
│  └─ random_forest.joblib
├─ preprocessing/
│  ├─ nn_preprocessor.joblib
│  └─ tree_preprocessor.joblib
└─ evaluation/
   ├─ validation_metrics.json
   ├─ test_metrics.json
   ├─ validation_predictions.csv.gz
   ├─ test_predictions.csv.gz
   ├─ nn_history.csv
   ├─ feature_importance.csv
   └─ reload_smoke_test.json
```

`manifest.json` 記錄：

- feature 名稱及順序；
- train/validation/test 日期與列數；
- purge dates；
- 所有訓練參數；
- dataset key、feature values 與 label 的 fingerprint；
- 套件版本；
- validation/test metrics；
- ensemble 權重與各 artifact 相對路徑。

打包完成前，程式會從磁碟重新載入 NN、LightGBM、RF 與 preprocessors，跑一批 test sample。只有輸出列數正確且全部為有限值才建立最終目錄；最後再產生 SHA-256 checksums。

## 8. 本機執行

先安裝 ML requirements：

```powershell
python -m pip install -r .\databricks_migrate\requirements-ml.txt
```

用小參數 smoke test：

```powershell
python .\databricks_migrate\databricks_src\train_forecast_models.py `
  --input-path .\data\forecast_training_features.parquet `
  --output-dir .\forecastModel\training_runs `
  --run-name smoke-001 `
  --nn-epochs 3 `
  --lgb-estimators 20 `
  --rf-estimators 10
```

正式訓練時移除三個縮小模型的參數，讓程式使用預設值。若想額外產生 tar.gz，加入 `--archive`。

基本程式測試不會真的訓練模型：

```powershell
python -m unittest databricks_migrate.tests.test_train_forecast_models -v
```

若本機已裝好 TensorFlow、LightGBM 與 scikit-learn，可用 synthetic data 驗證三個
模型的 train/save/reload/predict 完整鏈：

```powershell
python -m databricks_migrate.tests._smoke_training
```

## 9. Databricks Free Edition 執行步驟

### 9.1 建立 schema 與 Volume

在 Databricks SQL editor 或 notebook 執行：

```sql
CREATE SCHEMA IF NOT EXISTS finlab.gold;
CREATE SCHEMA IF NOT EXISTS finlab.ml;
CREATE VOLUME IF NOT EXISTS finlab.ml.model_artifacts;
```

訓練資料目標表：

```text
finlab.gold.forecast_training_features
```

模型 bundle 路徑：

```text
/Volumes/finlab/ml/model_artifacts/stock_rank/<run_name>/
```

### 9.2 同步 script 與 requirements

本機 PowerShell：

```powershell
$email = "your.name@gmail.com"
$profileName = "finlab-free"
$workspaceRoot = "/Users/$email/finlab_project"

databricks sync ".\databricks_migrate\databricks_src" `
  "$workspaceRoot/databricks_src" `
  --exclude "*.pyc" --exclude "__pycache__" `
  --profile $profileName

databricks workspace import `
  ".\databricks_migrate\requirements-ml.txt" `
  "$workspaceRoot/requirements-ml.txt" `
  --format AUTO --overwrite `
  --profile $profileName
```

### 9.3 建立一次性的 training Job

Databricks UI：

1. 左側選 `Jobs & Pipelines`，建立 Job。
2. Task type 選 `Python script`。
3. Source 選 `Workspace`。
4. Path 選 `/Workspace/Users/<email>/finlab_project/databricks_src/train_forecast_models.py`。
5. Compute 使用 serverless；若可選 base environment，優先選 `ML`。
6. Environment dependencies 加入：

   ```text
   -r /Workspace/Users/<email>/finlab_project/requirements-ml.txt
   ```

7. Python script task 的 Parameters 是 JSON 字串陣列，不是 key/value。輸入：

   ```json
   [
     "--input-table", "finlab.gold.forecast_training_features",
     "--output-dir", "/Volumes/finlab/ml/model_artifacts/stock_rank",
     "--experiment-name", "/Shared/finlab-stock-rank",
     "--register-model-name", "finlab.ml.stock_rank_ensemble"
   ]
   ```

8. 第一次先追加 smoke-test 參數：

   ```json
   ["--nn-epochs", "3", "--lgb-estimators", "20", "--rf-estimators", "10"]
   ```

   實際 UI 中需將它們合併成同一個 JSON array。
9. Run now。檢查 task log、Volume bundle、MLflow experiment 與 Catalog Explorer 中的新 model version。
10. smoke test 成功後移除縮小參數再正式跑。

Serverless Python script task 需要設定 environment；不要額外安裝 PySpark，Databricks 官方明確指出這會破壞 serverless Spark 環境。

### 9.4 Free Edition 資源考量

Free Edition 是 quota-limited、serverless-only、CPU-only。NN 可跑但不會有 GPU；RF 也可能是最耗記憶體和時間的模型。因此：

- 先用 `3/20/10` 小參數確認流程。
- 正式 RF 保留深度與 leaf 限制。
- input table 超過預設 2,000,000 列時程式拒絕 `toPandas()`；應先縮成月頻 gold table，不要調大限制來硬載 bronze。
- 訓練不應排每天；建議季度產生 Candidate，只有評估勝過 Champion 才提升。

## 10. MLflow 與模型提升

提供 `--experiment-name` 時，程式會：

1. log 訓練參數與 test metrics；
2. 把整個 bundle 當作 custom PyFunc model artifact；
3. 建立包含四個欄位的統一 `predict()`：NN、LGB、RF、ensemble；
4. 若同時給 `--register-model-name`，建立 Unity Catalog model version。

程式只註冊 Candidate，不自動把它設為 production。通過獨立 walk-forward、交易成本與資料品質檢查後，才設定 `Champion` alias：

```python
from mlflow import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(
    name="finlab.ml.stock_rank_ensemble",
    alias="Champion",
    version="<validated version>",
)
```

推論端只依 alias：

```python
import mlflow.pyfunc

model = mlflow.pyfunc.load_model(
    "models:/finlab.ml.stock_rank_ensemble@Champion"
)
predictions = model.predict(feature_frame)
```

這讓「訓練出新版本」與「正式模型切換」成為兩個獨立、可稽核的動作。

## 11. 驗收清單

- [ ] `date + stock_id` 無重複。
- [ ] feature list 與預期完全一致，順序也一致。
- [ ] train、validation、test 日期不重疊。
- [ ] split 交界具有與 label horizon 相同的 purge periods。
- [ ] manifest 的 preprocessing `fit_scope` 是 `train_only`。
- [ ] `reload_smoke_test.json` 是 `passed`。
- [ ] `checksums.sha256` 已產生。
- [ ] test metrics 沒有參與調參或 model selection。
- [ ] Candidate 經獨立檢查後才更新 Champion alias。

## 12. Reference docs

以下是這份設計可查證的主要官方文件：

- [scikit-learn：Common pitfalls and recommended practices](https://scikit-learn.org/stable/common_pitfalls.html)：先 split，preprocessing 只能在 train 上 fit，再對其他 split transform。
- [scikit-learn：Pipeline and composite estimators](https://scikit-learn.org/stable/modules/compose.html)：使用 Pipeline 維持一致的 preprocessing 並降低 leakage 風險。
- [TensorFlow：EarlyStopping](https://www.tensorflow.org/api_docs/python/tf/keras/callbacks/EarlyStopping)：validation early stopping 與 `restore_best_weights`。
- [LightGBM Python API](https://lightgbm.readthedocs.io/en/stable/Python-API.html)：`LGBMRegressor` 與 `early_stopping` callback。
- [MLflow：PythonModel / custom PyFunc](https://mlflow.org/docs/latest/ml/model/python_model/)：將自訂推論邏輯與多個 artifacts 包成同一個 MLflow model。
- [MLflow `pyfunc` API](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html)：`log_model`、artifacts、signature 與 `load_model`。
- [Databricks：Manage model lifecycle in Unity Catalog](https://docs.databricks.com/aws/en/machine-learning/manage-model-lifecycle/)：三層 model 名稱、Model Registry、aliases 與 Champion 載入方式。
- [Databricks：Python script task parameters](https://docs.databricks.com/aws/en/jobs/task-parameters)：Python script task 使用 JSON string array 傳遞 CLI arguments。
- [Databricks：Run Lakeflow Jobs with serverless compute](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs)：serverless 支援 Python script tasks。
- [Databricks：Configure the serverless environment](https://docs.databricks.com/aws/en/compute/serverless/dependencies)：Job environment dependencies、requirements file 與不得自行安裝 PySpark 的限制。
- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)：serverless-only、配額與 job concurrency 等限制。

