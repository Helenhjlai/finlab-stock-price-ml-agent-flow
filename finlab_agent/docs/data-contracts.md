# FinLab Agent 資料契約與 Tool 擴充規格

最後更新：2026-10-23

這份文件是 Agent 消費 Unity Catalog 資料的正式契約。部署操作見
[`../../docs/gold_data_deployment_zh-TW.md`](../../docs/gold_data_deployment_zh-TW.md)。
可供測試與自動化讀取的 manifest 位於
[`../contracts/data_products.v1.json`](../contracts/data_products.v1.json)；部署後 registry 是其
Unity Catalog 投影。文件、manifest、producer table properties 三者必須維持一致。

## 1. 設計原則

資料契約不是「目前 SQL 剛好查得到哪些欄位」，而是 producer 與 tool consumer 之間可測試、可版本化的介面。

每個資料產品都必須定義：

- 完整 UC table name 與 owner；
- grain 與 primary key；
- 欄位名稱、型別、單位、nullability；
- event time、available time 與 point-in-time 規則；
- adjustment basis／計算定義；
- freshness SLA、回補與 revision 政策；
- contract version、相容性與棄用方式；
- quality gates 與 consumer tools。

LLM 不直接查任意 table、欄位或 SQL。每個 tool 經過 Pydantic input、固定 repository query、確定性 calculation 及 output validation。

## 2. 共通欄位與時間語意

|概念|規則|
|---|---|
|`stock_id`|大寫、trim 後的官方完整證券代號；價格 v2 支援四碼普通股與 `00` 開頭 ETF|
|`date`|現有 Agent repository 的相容欄位；語意必須由各契約明定|
|event time|經濟事件發生或資料所描述的期間|
|available time|市場／Agent 最早可以安全使用資料的日期或時間|
|`_contract_version`|資料列所遵循的契約，例如 `daily_prices/v2`|
|`_materialized_at`|Gold materialization 的 UTC/Timestamp 執行時間，不代表資料可用時間|

因子、財報、模型預測等非同步資料必須同時保留 event time 與 available time。回測及 `as_of` 查詢只能使用 `available_at <= as_of`，不可用報表期間結束日代替公告日。

## 3. 現行 v1 資料產品

### 3.1 `finlab.gold.daily_prices` — `daily_prices/v2`

用途：`get_stock_indicators`、`screen_stocks`、`run_strategy_backtest`。

Grain／primary key：一個交易日、一支股票；`date + stock_id`。

|欄位|型別|null|語意|
|---|---|---:|---|
|`date`|DATE|否|交易日；同時是 event/available date|
|`stock_id`|STRING|否|官方完整證券代號；普通股為四碼數字，ETF 可含官方英文字尾|
|`stock_id_numeric`|STRING|否|代號的純數字 stem；不可單獨當跨商品唯一鍵|
|`name`|STRING|可|從該交易日 crawler compound ID 拆出的證券名稱；正式名稱有效期間未來由 `security_master/v1` 管理|
|`is_etf`|BOOLEAN|否|官方代號是否以 `00` 開頭|
|`etf_type_code`|STRING|可|ETF 為 `PLAIN/A/B/C/D/K/L/M/R/S/T/U/V`；普通股為 null|
|`etf_type`|STRING|可|ETF 類型的可讀中文標籤；普通股為 null|
|`open`|DOUBLE|否|開盤價|
|`high`|DOUBLE|否|最高價|
|`low`|DOUBLE|否|最低價|
|`close`|DOUBLE|否|收盤價|
|`volume`|DOUBLE|可|成交量，單位沿用 crawler 的成交股數|
|`adjustment_basis`|STRING|否|`corporate_action_adjusted_v1` 或 `raw`|
|`_contract_version`|STRING|否|固定 `daily_prices/v2`|
|`_materialized_at`|TIMESTAMP|否|產製時間|

約束：

- OHLC 都是有限數值且大於 0；`high >= low`。
- 同一張 table 不可混合不同 adjustment basis。
- 只保留四碼普通股，或符合 `00` 開頭 ETF 官方格式的代號；排除指數與其他非支援商品。
- `stock_id` 是 join／primary key 使用的官方完整代號；不可用 `stock_id_numeric` 取代，
  否則可能把帶字尾 ETF 與其他代號錯誤合併。
- `name` 暫時來自 price crawler；建立 `security_master/v1` 後應改由該產品管理名稱有效期間。
- OHLC 使用相同 corporate-action adjustment factor；volume v1 不反向調整。
- 新資料 SLA：交易日 T+1，Asia/Taipei 20:30 前；來源未發布或 crawler 失敗時 freshness gate 應失敗。

### 3.2 `finlab.gold.stock_factors` — `stock_factors/v1`

用途：`screen_stocks`；亦可作為未來 factor tools 的 point-in-time 基礎。

Grain／primary key：一個因子可用日期、一支股票；`date + stock_id`。v1 每月一列。

|欄位|型別|null|語意|
|---|---|---:|---|
|`date`|DATE|否|相容欄位，等於 `available_at`|
|`stock_id`|STRING|否|證券代號|
|`available_at`|DATE|否|因子最早可供 tool 使用日期|
|`source_release_date`|DATE|否|bronze monthly report 的發布 checkpoint|
|`revenue_period_end`|DATE|否|營收所屬月份月底|
|`revenue`|DOUBLE|否|當月營收，來源貨幣單位不在 v1 換算|
|`revenue_mom`|DOUBLE|可|`revenue / lag(revenue, 1) - 1`|
|`revenue_yoy`|DOUBLE|可|`revenue / lag(revenue, 12) - 1`；15% 為 `0.15`|
|`_contract_version`|STRING|否|固定 `stock_factors/v1`|
|`_materialized_at`|TIMESTAMP|否|產製時間|

約束：

- `date = available_at >= source_release_date > revenue_period_end`。
- lag 不足的初期 `revenue_mom`／`revenue_yoy` 可以為 null；不可用未來值回填。
- 修訂舊營收時允許同 key 覆寫，但需由 Delta history 保留可追溯版本。

### 3.3 `finlab.gold.forecast_training_features` — `forecast_training_features/v1`

用途：模型 tuning/training，不由現行三個 Agent tools 直接查詢。

Grain／primary key：一個 observation date、一支股票；`date + stock_id`。

必要欄位：

- `date`, `stock_id`；
- `mom1...mom9`, `pe`, `pb`, `bias*`, `acc*`, `rsv*`；
- `return`：下一 observation 的開盤報酬倍數；
- `rank`：同一期橫斷面 percentile rank，範圍 `[0, 1]`；
- `_feature_version`, `_materialized_at`。

這張表含未來 label，不可授權一般研究 Agent 查詢，避免 tool 無意洩漏未來資料。只有 training job principal 需要 `SELECT`。

### 3.4 `finlab.gold.data_contract_registry` — `data_contract_registry/v1`

用途：部署驗證、catalog discovery、契約稽核；不是讓 LLM 任意選 table 的入口。

Primary key：`product_name`。

欄位包含 contract version、table name、grain、primary key JSON、event/available time 欄、freshness SLA、owner、status、schema JSON 及更新時間。

## 4. 契約版本規則

|變更|處理方式|
|---|---|
|新增 nullable 欄位|同一 major version，可更新 registry|
|放寬 description、增加 metadata|同一 major version|
|欄位改名／刪除／型別改變|建立 `v2` table 或相容 view，不原地破壞|
|grain、primary key 或時間語意改變|一定升 major version|
|公式或 adjustment basis 改變|一定升 major version|

Consumer migration 順序：先發布 v2 → producer/consumer 雙跑 → contract tests 與 tool eval 通過 → Agent resource 切換 → 保留 v1 至公告期限後再移除。

## 5. Tool 與資料產品依賴

|Tool|目前資料產品|計算位置|
|---|---|---|
|`get_stock_indicators`|`daily_prices/v2`|Pandas deterministic calculation|
|`screen_stocks`|`daily_prices/v2`, `stock_factors/v1`|Pandas deterministic calculation|
|`run_strategy_backtest`|`daily_prices/v2`|Pandas deterministic calculation|

每個 tool output 的 `provenance` 建議逐步補齊：

```json
{
  "source_tables": ["finlab.gold.daily_prices"],
  "contract_versions": {"daily_prices": "v2"},
  "effective_as_of": "2026-10-22",
  "materialized_at": "2026-10-23T12:00:00Z",
  "calculation_version": "indicators/v1",
  "rows_read": 120
}
```

## 6. 未來新增 Tool 的擴充模式

不要繼續把所有能力加到 `PriceRepository`，也不要建立一張無限變寬的 `stock_factors`。依 capability 拆 repository protocol 與資料產品：

```text
ToolDefinition
 ├─ Pydantic input/output schema
 ├─ required capabilities
 ├─ resource/query limits
 ├─ deterministic calculation
 └─ required warnings/provenance
          |
          +--> MarketDataRepository   -> daily_prices
          +--> FactorRepository       -> stock_factors / fundamentals_pti
          +--> UniverseRepository     -> universe_membership
          +--> ForecastRepository     -> model_forecasts
          +--> FlowRepository         -> institutional_flows
          +--> EventRepository        -> corporate/news events
```

建議的下一批資料契約：

|資料產品|Grain/key|必要時間欄|可能 Tool|
|---|---|---|---|
|`security_master`|一證券／有效版本|`valid_from`, `valid_to`|代號查詢、產業分類|
|`universe_membership`|指數／股票／有效期間|`valid_from`, `valid_to`|無存活者偏誤篩選與回測|
|`fundamentals_point_in_time`|股票／報告項目／可用時間|`period_end`, `available_at`, `revision_id`|財務比率、基本面比較|
|`institutional_flows`|交易日／股票／法人類別|`date`, `available_at`|法人動向工具|
|`model_forecasts`|模型版本／as-of／股票／horizon|`feature_as_of`, `predicted_for`, `generated_at`|`get_model_forecast`|
|`corporate_events`|事件 ID／股票|`event_date`, `announced_at`|除權息、重大事件查詢|

### 新 Tool 上線檢查順序

1. 先定義使用案例、grain、時間語意與禁止事項。
2. 新增／版本化 Gold data product，建立 producer quality gates。
3. 新增專用 repository protocol/adapter；只暴露參數化查詢。
4. 新增 Pydantic input 與具體 output model，不只使用無限制 `dict`。
5. 在 tool registry 登錄 tool、capabilities、timeout、row limit、warnings。
6. 更新 App resources 與 service-principal 最小 `SELECT` 權限。
7. 補 calculation tests、contract tests、routing eval、prompt-injection cases。
8. 先 shadow/canary，再加入 production agent 的可選 tool list。

## 7. Model forecast Tool 的額外契約

`get_model_forecast` 不應直接讀 training feature table，也不應讓 LLM 指定 model URI。建議 `model_forecasts/v1` 至少包含：

|欄位|語意|
|---|---|
|`feature_as_of`|特徵資料截止時間|
|`predicted_for`|預測對應期間|
|`stock_id`|股票代號|
|`horizon`|預測期長|
|`score`|原始模型分數|
|`rank`|當期橫斷面 rank|
|`model_name`, `model_version`|固定 UC model identity|
|`generated_at`|推論時間|
|`feature_contract_version`|使用的 feature contract|

Serving endpoint、model name、允許的 horizon 由 deployment config 白名單決定；tool output 必須揭露模型版本、feature as-of、訓練／推論限制，不提供「保證報酬」措辭。

## 8. 明確禁止的模式

- 讓 LLM 傳入任意 table、column、SQL、Python 或 model URI。
- 以財報期間結束日取代公告／available time。
- 在同一價格表混用 raw 與 adjusted OHLC。
- 為新 tool 直接更改既有欄位語意而不升版。
- 將含 label 的 training table 授權給一般 Agent。
- 為方便而授予 App 整個 catalog 的 `SELECT` 或 `MODIFY`。
- tool 無日期／股票池／row limit，直接把大表送進 Pandas 或 prompt。

## 9. Capability Catalog 與資料契約的關係

Runtime capability manifest 位於
[`../contracts/capabilities.v1.json`](../contracts/capabilities.v1.json)。它不複製資料
schema，而以 `required_data_products` 引用本文件的版本化產品；實際工具 input schema
由 Pydantic 在啟動時產生。

部署時只有同時符合下列條件的能力才能啟用：

1. manifest 的 `enabled=true`；
2. implementation tool 已由應用程式明確注入；
3. 所有 required data products 均為 `agent_read`；
4. contract version、freshness 與 quality gates 通過；
5. App service principal 具有依賴資源的最小權限。

Agent 先搜尋 capability，再用短效 token 執行；這個 token 只限制「可以執行哪些已註冊
能力」，不增加 Unity Catalog 權限。完整流程與 Gold roadmap 見
[`planner-capability-architecture.md`](planner-capability-architecture.md)。
