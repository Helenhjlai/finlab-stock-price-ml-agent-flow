# Databricks 部署指南

## 0. 前置條件

若目前只要預覽 UI，不需要先建立 endpoint；使用
[`model-providers-and-demo.md`](model-providers-and-demo.md) 的 `demo` 模式。本部署檔的
`app.yaml` 已明確設定 `FINLAB_MODEL_BACKEND=databricks`，正式 App 才會呼叫模型資源。

- Databricks workspace 與可用的 SQL warehouse。
- 一個支援 tool/function calling 的 chat model endpoint。
- 兩張符合 [正式資料契約](data-contracts.md) 的 Unity Catalog Gold 表。
- Databricks CLI 已登入；本機僅部署者需要 CLI 認證。

建議先在 SQL Editor 驗證：

```sql
SELECT date, stock_id, open, high, low, close, volume
FROM finlab.gold.daily_prices
ORDER BY date DESC LIMIT 10;

SELECT date, stock_id, revenue_yoy
FROM finlab.gold.stock_factors
ORDER BY date DESC LIMIT 10;
```

資料必須先從現有 `finlab.bronze.*` 正規化到 Gold。實際中文欄名依你的 crawler
schema 而異，因此不要盲目貼固定 SQL；先 `DESCRIBE TABLE`，再將欄位映射成契約名稱。
完整 materialization、widget、品質 SQL、增量更新與回滾步驟見
[Gold Data 部署指南](../../docs/gold_data_deployment_zh-TW.md)。

## 1. 本機驗證

```powershell
cd finlab_agent
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
pytest
```

`requirements-dev.txt` 是可在 Python 3.13 執行的核心測試集合；要在本機啟動完整 App，
請使用 Python 3.10–3.12 並安裝 `requirements.txt`。Databricks 部署亦應選用相同範圍，
避免 `databricks-connect` 的 Python 版本衝突。

所有非 live 測試應通過；live routing eval 需要模型端點認證：

```powershell
$env:RUN_LIVE_AGENT_EVAL = "true"
pytest tests/test_tool_selection_live.py -q
```

## 2. 建立 Databricks App

在 Workspace 的 **Apps → Create app → Custom app** 建立 App。加入四個資源，資源 key
必須與 `app.yaml` 一致：

|資源 key|類型|最小權限|用途|
|---|---|---|---|
|`sql-warehouse`|SQL warehouse|CAN USE|執行查詢|
|`price-table`|UC table|SELECT|日 OHLCV|
|`factor-table`|UC table|SELECT|營收年增率|
|`chat-model`|Model serving endpoint|CAN QUERY|Agent 推理與 tool calling|

Databricks Apps 會為 App 建立 service principal。除此之外，確認它對兩張表所在位置有
`USE CATALOG`、`USE SCHEMA`，不要給 `MODIFY` 或 `OWN`。

## 3. 部署程式

### 選項 A：Git（正式環境建議）

把 repo 連到 App，source code path 設成 `finlab_agent`，選定 tag 或 commit 後 Deploy。
每次正式版用 tag，回滾時重新部署上一個 tag。

### 選項 B：Workspace source（教學／開發）

```powershell
databricks sync . /Workspace/Users/<email>/finlab-agent
databricks apps deploy <app-name> `
  --source-code-path /Workspace/Users/<email>/finlab-agent
```

部署系統會安裝 `requirements.txt`，再依 `app.yaml` 執行 Streamlit。不要同步 `.venv`、
測試快取或 token 檔案。

## 4. 第一次驗收

依序詢問：

1. `請告訴我 2330 在 2024-05-20 的收盤價、MA20、MA60 與價格動能。`
2. `截至 2024-05-20，找出營收年增率大於 15% 且價格高於 MA60 的股票。`
3. `用 tw_ma20_60 回測 2024-01-02 到 2024-05-20。`

在 App logs 確認沒有權限或 schema 錯誤；在 MLflow experiment 檢查模型呼叫、工具名稱、
延遲與錯誤率。不要把完整敏感資料列或憑證寫入 log。

## 5. 發版閘門

- `pytest` 通過。
- live routing eval 通過，且人工抽查工具參數。
- 固定測試資料的數值基準沒有未解釋漂移。
- Gold table freshness 與空值檢查通過。
- 權限仍符合最小權限。
- 風險說明包含：非投資建議、未來不保證、前視偏誤、成本、流動性／市場衝擊、
  存活者偏誤。

## 常見問題

- **查不到資料**：確認資源 key、`USE CATALOG/SCHEMA`、表 `SELECT` 與 warehouse
  `CAN USE`。
- **模型不呼叫工具**：確認 endpoint 支援 tool calling，執行 live routing eval，改善工具
  docstring 與案例，勿把金融數值塞進 prompt。
- **MA 與 FinLab 不同**：確認價格是否同為還原價、截止日、休市日、缺值補法與視窗。
- **回測太好**：先查訊號是否延後、因子公告日、歷史股票池、成本與流動性。
- **升級後失敗**：Agent 套件變動快；先在分支更新 lock、跑完整 eval，再部署新 tag。

