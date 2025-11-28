# tests/test_metric_evaluator.py

import pytest

from src.agents.metric_evaluator import MetricEvaluatorAgent

def _fake_data_summary():
    # minimal plausible structure to test logic
    return {
        "global_daily": [
            {"date": "2025-01-01", "spend": 100, "impressions": 10000, "clicks": 300, "purchases": 10, "revenue": 400, "ctr": 0.03,   "roas": 4.0},
            {"date": "2025-01-02", "spend": 100, "impressions": 9000,  "clicks": 250, "purchases": 9,  "revenue": 350, "ctr": 0.0278,"roas": 3.5},
            {"date": "2025-01-03", "spend": 100, "impressions": 11000, "clicks": 250, "purchases": 8,  "revenue": 280, "ctr": 0.0227,"roas": 2.8},
            {"date": "2025-01-04", "spend": 100, "impressions": 10500, "clicks": 220, "purchases": 7,  "revenue": 250, "ctr": 0.0210,"roas": 2.5},
        ],
        "campaign_daily": [
            {"campaign_name":"Test Campaign","date":"2025-01-01","spend":100,"impressions":10000,"clicks":300,"purchases":10,"revenue":400,"ctr":0.03,   "roas":4.0},
            {"campaign_name":"Test Campaign","date":"2025-01-02","spend":100,"impressions":9000,"clicks":250,"purchases":9,"revenue":350,"ctr":0.0278,"roas":3.5},
            {"campaign_name":"Test Campaign","date":"2025-01-03","spend":100,"impressions":11000,"clicks":250,"purchases":8,"revenue":280,"ctr":0.0227,"roas":2.8},
            {"campaign_name":"Test Campaign","date":"2025-01-04","spend":100,"impressions":10500,"clicks":220,"purchases":7,"revenue":250,"ctr":0.0210,"roas":2.5},
        ]
    }

def test_metric_evaluator_adds_fields():
    data_summary = _fake_data_summary()
    agent = MetricEvaluatorAgent(config={
        "recent_window_days": 2,
        "previous_window_days": 2,
        "p_value_threshold": 0.1,
        "bootstrap_iters": 200,
        "min_impressions_for_stats": 0
    })

    hypotheses = [
        {
            "id": "HYP-001",
            "scope": "campaign",
            "campaign_name": "Test Campaign",
            "driver_type": "creative",
            "hypothesis": "ROAS dropped for Test Campaign.",
            "rationale": "Dummy rationale.",
            "metrics_snapshot": {},
            "required_evidence": ["metric_significance"],
            "initial_confidence": 0.5
        }
    ]

    result = agent.run_metric_evaluation(hypotheses, data_summary)
    evaluated = result["evaluated_hypotheses"]
    assert len(evaluated) == 1
    h = evaluated[0]
    assert "metric_confidence" in h
    assert "validated" in h
    assert "metric_effect_size_pct" in h
    assert "metric_sample" in h
    assert isinstance(h["metric_confidence"], float)
