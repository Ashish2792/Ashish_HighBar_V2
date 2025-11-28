# tests/test_creative_evaluator.py

import pytest

from src.agents.creative_evaluator import CreativeEvaluatorAgent

def _fake_data_summary_for_chs():
    return {
        "campaign_daily": [
            {"campaign_name":"Camp A","date":"2025-01-01","spend":100,"impressions":10000,"clicks":300,"purchases":10,"revenue":400,"ctr":0.03,"roas":4.0},
            {"campaign_name":"Camp A","date":"2025-01-02","spend":100,"impressions":9000,"clicks":250,"purchases":9,"revenue":350,"ctr":0.0278,"roas":3.5},
            {"campaign_name":"Camp A","date":"2025-01-03","spend":100,"impressions":11000,"clicks":250,"purchases":8,"revenue":280,"ctr":0.0227,"roas":2.8},
            {"campaign_name":"Camp A","date":"2025-01-04","spend":100,"impressions":10500,"clicks":220,"purchases":7,"revenue":250,"ctr":0.0210,"roas":2.5},
        ],
        "creative_repetition": [
            {
                "campaign_name":"Camp A",
                "total_impressions":40500,
                "unique_creatives":2,
                "impression_share_of_top_creative":0.8
            }
        ],
        "text_terms": {
            "Camp A": [
                {"term":"seamless","count":10},
                {"term":"comfort","count":15},
                {"term":"breathable","count":5},
                {"term":"sale","count":3}
            ]
        }
    }

def test_creative_evaluator_adds_chs_and_confidence():
    data_summary = _fake_data_summary_for_chs()
    agent = CreativeEvaluatorAgent(config={
        "recent_window_days": 2,
        "previous_window_days": 2,
        "behavior_weight": 0.5,
        "text_weight": 0.3,
        "fatigue_weight": 0.2,
        "min_impressions_for_stats": 0
    })

    hypotheses = [
        {
            "id": "HYP-001",
            "scope": "campaign",
            "campaign_name": "Camp A",
            "driver_type": "creative",
            "hypothesis": "Creative fatigue is driving ROAS drop.",
            "rationale": "Dummy.",
            "metrics_snapshot": {},
            "required_evidence": ["metric_significance", "chs_trend"],
            "initial_confidence": 0.5
        }
    ]

    result = agent.run_creative_evaluation(hypotheses, data_summary)
    chs_summary = result["chs_summary"]
    evaluated = result["evaluated_hypotheses"]

    assert "Camp A" in chs_summary
    rec = chs_summary["Camp A"]
    assert "chs_prev" in rec and "chs_recent" in rec

    assert len(evaluated) == 1
    h = evaluated[0]
    assert "creative_confidence" in h
    assert "chs_prev" in h and "chs_recent" in h
    assert h["campaign_name"] == "Camp A"
