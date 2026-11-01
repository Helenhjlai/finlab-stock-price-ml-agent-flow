# 架構與設計決策

## 一次請求的生命週期

1. Streamlit 把最近 12 則對話與新問題交給 structured Planner。
2. Planner 產生目標、子問題、必要輸入與步驟依賴，不預先知道工具名稱。
3. LangChain executor 針對每個步驟呼叫 `search_capabilities`。
4. Capability Catalog 回傳已啟用能力、實際 input schema、Gold 契約依賴與短效 token。
5. `execute_capability` 只允許執行該次搜尋結果中的 allow-listed Python tool。
6. Pydantic 驗證參數；Repository 以參數化 SQL 讀 Unity Catalog 標準化長表。
7. Pandas 計算指標、篩選或回測；模型不參與金融數值計算。
8. `validate_tool_result` 正規化結果並強制檢查回測風險文字。
9. Agent 整合已驗證證據；MLflow 記錄 plan、discovery、execution 與回答 trace。

完整 Planner／Capability 設計見
[`planner-capability-architecture.md`](planner-capability-architecture.md)。

## 為何不直接讓 LLM 寫策略 Python

金融資料含不可信內容，而且回測程式可以消耗大量資源、讀取機密或產生難以重現的
結果。本 MVP 採「策略登錄表」：公開工具只收 `strategy_id`。策略定義、股票池、
成本與再平衡頻率都在 Git 中，能 code review、測試、重播與版本化。

## 資料契約

正式、版本化的欄位定義、時間語意、相容性規則與未來 Tool 擴充方式見
[`data-contracts.md`](data-contracts.md)。Gold tables 的建立與驗收步驟見
[`../../docs/gold_data_deployment_zh-TW.md`](../../docs/gold_data_deployment_zh-TW.md)。

`FINLAB_PRICE_TABLE` 必須是 Unity Catalog 三層名稱，且包含：

|欄位|型別|說明|
|---|---|---|
|`date`|DATE/TIMESTAMP|交易日|
|`stock_id`|STRING|證券代號|
|`open/high/low/close`|DOUBLE|同一調整基準的 OHLC；不可混用還原／未還原價|
|`volume`|DOUBLE|成交量|

`FINLAB_FACTOR_TABLE` 必須包含：

|欄位|型別|說明|
|---|---|---|
|`date`|DATE/TIMESTAMP|資訊可使用日期，不應只放財報期間結束日|
|`stock_id`|STRING|證券代號|
|`revenue_yoy`|DOUBLE|營收年增率，小數比例，例如 15% = 0.15|

重要：因子表的 `date` 應代表市場當時「已知」的日期。若把未來才公告的營收放回月份
結束日，篩選與回測會產生前視偏誤。

## 回測假設

- 收盤後形成訊號，持倉向後位移一個交易日。
- 等權重；`tw_ma20_60` 與 `tw_momentum_top2` 每月最後交易日重算。
- 費用與滑價按週轉率扣除。
- 未模擬漲跌停、成交量限制、市場衝擊、借券、股利現金流與稅制細節。
- Sharpe 使用日報酬、252 日年化、無風險利率 0。
- `trade_count` 是個別資產權重發生變動的次數，不等同券商成交回報筆數。

## 安全界線

- Unity Catalog 表名須通過 `catalog.schema.table` 白名單格式。
- 值條件使用 SQL 參數，不串接使用者輸入。
- 工具沒有 `eval`、`exec`、shell、任意 SQL 或任意 Python 入口。
- Catalog manifest 不能動態 import；只有啟動時注入的 Python allow-list 可以執行。
- Discovery token 有效期與 capability scope，不能用一次搜尋授權未出現的能力。
- App service principal 只授予 warehouse `CAN USE`、模型 `CAN QUERY`、兩表 `SELECT`。
- 對話歷史限制 12 則；正式上線前仍應制定 PII、保留期與稽核政策。

## 下一階段

1. 將現有 bronze 表轉為上述 Gold 契約，並建立資料品質檢查。
2. 把策略股票池改成歷史時點成分表，處理存活者偏誤。
3. 若要使用既有訓練模型，新增 `get_model_forecast`，只呼叫固定的 UC Model
   Serving endpoint，並回傳模型版本、特徵時間與信賴區間。
4. 將 routing case 擴充到拒答、模糊日期、錯誤代號與提示注入案例。
5. Tool 增加時依 capability 拆分 repository 與 Gold data product，不持續擴大單一
   `PriceRepository` 或建立無限變寬的 factor table。
6. 多 replica 時把目前 in-memory discovery grant 改為 signed token 或共享短效儲存。

