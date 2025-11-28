# tests/test_planner.py
import pytest
from src.agents.planner import PlannerAgent

def test_interpret_query_intent_roas():
    p = PlannerAgent()
    info = p.interpret_query("Analyze ROAS drop")
    assert info["intent"] == "analyze_roas"

def test_generate_plan_structure():
    p = PlannerAgent()
    plan = p.generate_plan("Analyze ROAS drop", dataset_meta={"rows": 100})
    assert "tasks" in plan
    assert len(plan["tasks"]) >= 3
    ids = [t["id"] for t in plan["tasks"]]
    assert "T1" in ids and "T2" in ids and "T3" in ids

def test_task_dependencies():
    p = PlannerAgent()
    plan = p.generate_plan("Analyze ROAS drop")
    task_by_id = {t["id"]: t for t in plan["tasks"]}
    assert task_by_id["T2"]["depends_on"] == ["T1"]
    assert "T3" in task_by_id
    assert "T4" in task_by_id
