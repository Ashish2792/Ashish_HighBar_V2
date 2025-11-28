# tests/test_insight_agent.py

import pytest
from src.agents.insight_agent import InsightAgent

def _fake_data_summary():
    # minimal plausible structure to test logic
    return {
        "meta": {
            "n_rows": 4,
            "date_min": "2025-01-01",
            "date_max": "2025-01-04",
            "n_campaigns": 1,
            "n_adsets": 1,
            "n_creatives": 1
        },
        "global_daily": [
            {"date": "2025-01-01", "spend": 100, "impressions": 10000, "clicks": 300, "purchases": 10, "revenue": 400, "ctr": 0.03, "roas": 4.0},
            {"date": "2025-01-02", "spend": 100, "impressions": 9000,  "clicks": 250, "purchases": 9,  "revenue": 350, "ctr": 0.0278, "roas": 3.5},
            {"date": "2025-01-03", "spend": 100, "impressions": 11000, "clicks": 250, "purchases": 8,  "revenue": 280, "ctr": 0.0227, "roas": 2.8},
            {"date": "2025-01-04", "spend": 100, "impressions": 10500, "clicks": 220, "purchases": 7,  "revenue": 250, "ctr": 0.0210, "roas": 2.5},
        ],
        "campaign_daily": [
            {"campaign_name":"Test Campaign","date":"2025-01-01","spend":100,"impressions":10000,"clicks":300,"purchases":10,"revenue":400,"ctr":0.03,"roas":4.0},
            {"campaign_name":"Test Campaign","date":"2025-01-02","spend":100,"impressions":9000,"clicks":250,"purchases":9,"revenue":350,"ctr":0.0278,"roas":3.5},
            {"campaign_name":"Test Campaign","date":"2025-01-03","spend":100,"impressions":11000,"clicks":250,"purchases":8,"revenue":280,"ctr":0.0227,"roas":2.8},
            {"campaign_name":"Test Campaign","date":"2025-01-04","spend":100,"impressions":10500,"clicks":220,"purchases":7,"revenue":250,"ctr":0.0210,"roas":2.5},
        ],
        "campaign_summary": [
            {
                "campaign_name":"Test Campaign",
                "spend":400,"impressions":40500,"clicks":1020,"purchases":34,"revenue":1280,
                "ctr":0.0252,"cvr":0.0333,"cpc":0.392,"cpm":9.876,"roas":3.2
            }
        ],
        "creative_summary": [],
        "creative_repetition": [],
        "text_terms": {}
    }

def test_insight_agent_runs_and_returns_hypotheses():
    data_summary = _fake_data_summary()
    agent = InsightAgent(config={
        "recent_window_days": 2,
        "previous_window_days": 2,
        "roas_drop_threshold_pct": -10.0,
        "low_ctr_threshold": 0.02,
        "min_impressions_for_stats": 1000
    })
    result = agent.run_insight_generation(data_summary, intent="analyze_roas")
    assert "hypotheses" in result
    assert isinstance(result["hypotheses"], list)
    # in this toy example we expect at least one campaign-level hypothesis
    assert len(result["hypotheses"]) >= 1
    first = result["hypotheses"][0]
    assert "id" in first
    assert "driver_type" in first
    assert "initial_confidence" in first
