# tests/test_creative_generator.py

import pytest

from src.agents.creative_generator import CreativeGeneratorAgent

def _fake_data_summary_for_creatives():
    return {
        "campaign_summary": [
            {
                "campaign_name": "Camp A",
                "spend": 500,
                "impressions": 50000,
                "clicks": 900,
                "purchases": 40,
                "revenue": 2000,
                "ctr": 0.018,
                "cvr": 0.044,
                "cpc": 0.55,
                "cpm": 10.0,
                "roas": 4.0
            }
        ],
        "creative_summary": [
            {
                "campaign_name": "Camp A",
                "creative_message": "Soft, breathable everyday underwear for all-day comfort.",
                "spend": 300,
                "impressions": 30000,
                "clicks": 600,
                "purchases": 25,
                "revenue": 1200,
                "ctr": 0.02,
                "roas": 4.0
            }
        ],
        "text_terms": {
            "Camp A": [
                {"term": "comfort", "count": 15},
                {"term": "seamless", "count": 8},
                {"term": "breathable", "count": 5}
            ]
        }
    }

def _fake_chs_summary():
    return {
        "Camp A": {
            "campaign_name": "Camp A",
            "chs_prev": 70.0,
            "chs_recent": 50.0,
            "behavior_prev": 0.75,
            "behavior_recent": 0.55,
            "text_quality": 0.55,
            "fatigue_score": 0.40
        }
    }

def test_creative_generator_outputs_suggestions():
    data_summary = _fake_data_summary_for_creatives()
    chs_summary = _fake_chs_summary()
    agent = CreativeGeneratorAgent(config={
        "variants_per_type": 2,
        "low_ctr_threshold": 0.02,
        "chs_threshold": 60.0,
        "max_campaigns": 5
    })

    hypotheses = [
        {
            "id": "HYP-001",
            "scope": "campaign",
            "campaign_name": "Camp A",
            "driver_type": "creative",
            "hypothesis": "Creative fatigue causing CTR drop.",
            "rationale": "Dummy.",
            "metrics_snapshot": {},
            "required_evidence": ["metric_significance", "chs_trend"],
            "initial_confidence": 0.5,
            "creative_confidence": 0.7
        }
    ]

    result = agent.run_creative_generation(data_summary, chs_summary, hypotheses)
    creatives = result["creatives"]
    assert len(creatives) >= 1
    camp = creatives[0]
    assert camp["campaign_name"] == "Camp A"
    assert "suggestions" in camp
    assert len(camp["suggestions"]) > 0
    first = camp["suggestions"][0]
    for field in ["headline", "message", "cta", "variant_type", "targeted_weakness", "core_term", "overlap_score"]:
        assert field in first
