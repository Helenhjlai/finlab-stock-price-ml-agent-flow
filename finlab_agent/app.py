from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


APP_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(APP_ROOT / "src"))

from finlab_agent.agent import build_agent  # noqa: E402
from finlab_agent.config import Settings  # noqa: E402
from finlab_agent.repository import build_repository  # noqa: E402
from finlab_agent.service import ask_agent  # noqa: E402


st.set_page_config(page_title="FinLab 量化研究 Agent", page_icon="📈", layout="centered")
st.title("📈 FinLab 量化研究 Agent")
st.caption("需求規劃 × 能力發現 × Databricks 資料／模型 × 確定性運算")

display_settings = Settings.from_env()
demo_question = None


@st.cache_resource
def get_runtime():
    settings = Settings.from_env()
    repository = build_repository(settings)
    return settings, build_agent(settings, repository)


with st.sidebar:
    st.subheader("執行模式")
    if display_settings.model_backend == "demo":
        st.success("Demo：不呼叫 LLM")
        st.caption("使用本機測試資料與確定性路由，適合先預覽介面。")
        st.subheader("Demo 範例")
        if st.button("2330 技術指標", use_container_width=True):
            demo_question = "請告訴我 2330 在 2024-05-20 的收盤價、MA20、MA60 與價格動能。"
        if st.button("營收＋均線選股", use_container_width=True):
            demo_question = (
                "截至 2024-05-20，從 2330、2454、0050 找出營收年增率大於 15%"
                "且價格高於 MA60 的股票，依營收成長由高到低排序。"
            )
        if st.button("均線策略回測", use_container_width=True):
            demo_question = (
                "用 tw_ma20_60 回測 2024-01-02 到 2024-05-20，"
                "告訴我年化報酬、最大回撤、Sharpe 與交易數。"
            )
        if st.button("多步驟研究", use_container_width=True):
            demo_question = (
                "先查 2330 截至 2024-05-20 的均線與動能，再用 tw_ma20_60 "
                "回測 2024-01-02 到 2024-05-20，綜合說明技術狀態與策略風險。"
            )
    else:
        st.info("Databricks LLM")
        st.caption(f"Endpoint：{display_settings.model_endpoint}")
    st.subheader("使用提醒")
    st.write("研究結果僅供教學與研究，不構成投資建議。")
    st.write("回測會納入指定手續費與滑價，但不等於實際成交結果。")
    if st.button("清除對話"):
        st.session_state.messages = []
        st.rerun()

if "messages" not in st.session_state:
    st.session_state.messages = []

for item in st.session_state.messages:
    with st.chat_message(item["role"]):
        st.markdown(item["content"])
        if item.get("capabilities"):
            st.caption("執行能力：" + "、".join(item["capabilities"]))
        if item.get("plan"):
            with st.expander("研究計畫"):
                st.json(item["plan"])

question = demo_question or st.chat_input(
    "例如：用 tw_ma20_60 回測 2024-01-02 到 2024-05-20"
)
if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        try:
            _, agent = get_runtime()
            history = [
                {"role": item["role"], "content": item["content"]}
                for item in st.session_state.messages[:-1]
            ]
            with st.spinner("讀取資料並執行確定性計算…"):
                result = ask_agent(agent, question, history)
            st.markdown(result.answer)
            if result.capabilities_used:
                st.caption("執行能力：" + "、".join(result.capabilities_used))
            if result.research_plan:
                with st.expander("研究計畫"):
                    st.json(result.research_plan)
                    if result.planning_mode == "fallback":
                        st.warning("本次模型未輸出結構化計畫，已採單步安全 fallback。")
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": result.answer,
                    "capabilities": result.capabilities_used,
                    "plan": result.research_plan,
                }
            )
        except Exception as exc:
            st.error(f"無法完成查詢：{exc}")
            st.info("請檢查 Databricks App 資源、資料表 schema 與模型端點權限。")

