from langchain_core.messages import AIMessage

from finlab_agent.agent import PlanningResearchAgent
from finlab_agent.schemas import ResearchPlan


class _StructuredModel:
    def __init__(self, plan):
        self.plan = plan

    def with_structured_output(self, _schema):
        return self

    def invoke(self, _messages):
        return self.plan


class _NoStructuredOutputModel:
    def with_structured_output(self, _schema):
        raise NotImplementedError


class _Executor:
    def __init__(self):
        self.state = None

    def invoke(self, state):
        self.state = state
        return {"messages": [*state["messages"], AIMessage(content="完成")]}


def test_planning_runtime_passes_structured_plan_to_executor(price_repository):
    plan = ResearchPlan.model_validate(
        {
            "goal": "分析 2330 技術狀態",
            "steps": [
                {
                    "step_id": "step_1",
                    "question": "2330 的均線與動能為何？",
                    "capability_query": "股票均線與動能",
                    "required_inputs": ["stock_id", "as_of"],
                    "depends_on": [],
                }
            ],
        }
    )
    executor = _Executor()
    runtime = PlanningResearchAgent(
        model=_StructuredModel(plan), executor=executor, registry=None
    )
    result = runtime.invoke(
        {"messages": [{"role": "user", "content": "分析 2330 技術狀態"}]}
    )
    assert result["planning_mode"] == "model"
    assert result["research_plan"]["steps"][0]["capability_query"] == "股票均線與動能"
    assert "內部研究計畫" in executor.state["messages"][-1].content


def test_planning_runtime_uses_safe_fallback_when_structured_output_unavailable():
    executor = _Executor()
    runtime = PlanningResearchAgent(
        model=_NoStructuredOutputModel(), executor=executor, registry=None
    )
    result = runtime.invoke(
        {"messages": [{"role": "user", "content": "找出適合的研究能力"}]}
    )
    assert result["planning_mode"] == "fallback"
    assert result["research_plan"]["steps"][0]["capability_query"] == "找出適合的研究能力"


def test_planning_runtime_stops_for_material_clarification():
    plan = {
        "goal": "執行策略回測",
        "steps": [
            {
                "step_id": "step_1",
                "question": "回測哪一段期間？",
                "capability_query": "策略回測",
                "required_inputs": ["start_date", "end_date"],
                "depends_on": [],
            }
        ],
        "needs_clarification": True,
        "clarification_question": "請提供回測開始與結束日期。",
    }
    executor = _Executor()
    runtime = PlanningResearchAgent(
        model=_StructuredModel(plan), executor=executor, registry=None
    )
    result = runtime.invoke(
        {"messages": [{"role": "user", "content": "幫我回測均線策略"}]}
    )
    assert result["messages"][-1].content == "請提供回測開始與結束日期。"
    assert executor.state is None
