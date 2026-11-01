# 模型選擇與無 LLM Demo

最後更新：2026-10-23

資料放在 Databricks，不代表 LLM 也必須由 Databricks 提供。本專案把兩者分開：

|設定|用途|
|---|---|
|`FINLAB_DATA_BACKEND`|`sample` 或 `databricks`，決定金融資料來源|
|`FINLAB_MODEL_BACKEND`|`demo` 或 `databricks`，決定是否呼叫 LLM|

因此可以先用 sample data + demo router 看 UI；也可以使用 Databricks Gold data + 外部模型
adapter。不要把資料層與模型層綁成同一個選擇。

## 1. 最快：完全不使用 LLM

PowerShell：

```powershell
cd C:\D\finlab_ml\finlab_ml_course\finlab_practice\finlab_agent
$env:PYTHONPATH = "src"
$env:FINLAB_DATA_BACKEND = "sample"
$env:FINLAB_MODEL_BACKEND = "demo"
.\.venv311\Scripts\python.exe -m streamlit run app.py
```

打開 Streamlit 顯示的本機網址，左側有四個 Demo 按鈕。Demo 會實際執行 capability
registry、Pydantic schema、Pandas calculation 與結果驗證，但使用確定性路由，不發送任何
prompt，也不需要 API key。

Demo 不是模型品質測試；它只用來確認 UI、工具、資料與風險說明。

## 2. 在 Databricks Free Edition 找 endpoint

1. 登入 workspace。
2. 左側開啟 **AI Gateway**。
3. 選擇 **Models**。
4. 找 system-provided model service；點進模型確認 endpoint name、權限及可用狀態。
5. 優先選明確支援 function calling 的 chat model；本專案 Planner 最好也支援 structured
   output。實際清單依 workspace region、Free Edition 限制及當下配額為準。

舊版介面可能顯示在 **Serving → Endpoints**，或可先由 **Playground** 的模型下拉選單測試。
不要只照文件抄某個 endpoint 名稱；以自己 workspace 顯示的名稱為準。

Free Edition 沒有 provisioned throughput 或 GPU serving，部分模型不可用，而且 App 每次
啟動／部署後最多執行 24 小時便會自動停止。它適合學習和 PoC，不適合承諾 production SLA。

## 3. 先單獨測試 Databricks 模型

先以 CLI 登入，避免把 token 寫進 repo：

```powershell
$workspaceHost = "https://<your-workspace-host>"
databricks auth login --host $workspaceHost --profile finlab-free
$env:DATABRICKS_CONFIG_PROFILE = "finlab-free"
```

設定你在 Models 畫面看到的 endpoint：

```powershell
$env:DATABRICKS_MODEL_ENDPOINT = "<endpoint-name>"
$env:PYTHONPATH = "src"
```

最小 smoke test：

```powershell
.\.venv311\Scripts\python.exe -c "from databricks_langchain import ChatDatabricks; import os; m=ChatDatabricks(endpoint=os.environ['DATABRICKS_MODEL_ENDPOINT'], temperature=0); print(m.invoke('請只回答：連線成功').content)"
```

如果這一步失敗，先不要測 Agent。依序檢查：endpoint name、workspace region、模型 entitlement、
`CAN QUERY`／`EXECUTE` 權限、CLI profile 和 Free Edition quota。

## 4. 讓本機 App 使用 Databricks 模型

金融資料仍可先使用 sample CSV，只把模型切到 Databricks：

```powershell
$env:FINLAB_DATA_BACKEND = "sample"
$env:FINLAB_MODEL_BACKEND = "databricks"
$env:DATABRICKS_MODEL_ENDPOINT = "<endpoint-name>"
$env:DATABRICKS_CONFIG_PROFILE = "finlab-free"
$env:PYTHONPATH = "src"
.\.venv311\Scripts\python.exe -m streamlit run app.py
```

這是最容易除錯的順序：先確認 LLM planning／tool calling，再把
`FINLAB_DATA_BACKEND` 切成 `databricks`。

## 5. 其他模型方式

`build_agent(settings, repository, model=...)` 已允許注入任何相容的 LangChain chat model，
因此可選：

- OpenAI／Azure OpenAI；
- Anthropic Claude；
- Google Gemini；
- Ollama 或 LM Studio 本機模型；
- 任一 OpenAI-compatible endpoint；
- Databricks External Model endpoint：模型在外部，但認證、限流與治理仍經 Databricks。

模型至少要可靠支援 tool calling。若不支援 structured output，Planner 會使用單步安全
fallback；若連 tool calling 都不支援，就不適合這個 Agent executor。

正式把新 provider 加入 App 時，應新增 `ModelFactory` adapter 與 provider-specific smoke
test，不要在 `agent.py` 堆疊大量環境變數判斷。API key 應放在本機環境、Databricks Secret
或 App resource，不可提交 Git。
