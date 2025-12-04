"""
tests/test_creative_generator_v2.py

Lightweight test for CreativeGeneratorV2.

Goal:
- Ensure the upgraded creative generator runs on a tiny synthetic input
  and returns a well-formed creatives payload (no crashes, correct keys).
"""

from src.agents.creative_generator_v2 import CreativeGeneratorV2


def test_creative_generator_v2_basic():
    # --- Minimal synthetic data_summary ---
    data_summary = {
        "campaign_summary": [
            {
                "campaign_name": "Test Campaign",
                "spend": 100.0,
                "impressions": 5000,
                "clicks": 150,
                "purchases": 10,
                "revenue": 300.0,
                "ctr": 0.03,
                "cvr": 10 / 150,
                "cpc": 100.0 / 150,
                "cpm": 100.0 / 5000 * 1000,
                "roas": 300.0 / 100.0,
            }
        ],
        "creative_summary": [
            {
                "campaign_name": "Test Campaign",
                "creative_message": "Ultra-soft briefs for everyday comfort.",
                "spend": 100.0,
                "impressions": 5000,
                "clicks": 150,
                "purchases": 10,
                "revenue": 300.0,
                "ctr": 0.03,
                "roas": 3.0,
            }
        ],
        "text_terms": {
            "Test Campaign": [
                {"term": "comfort", "count": 12},
                {"term": "soft", "count": 8},
                {"term": "breathable", "count": 5},
            ]
        },
    }

    # --- Minimal CHS summary: slightly weak text_quality ---
    chs_summary = {
        "Test Campaign": {
            "chs_prev": 70.0,
            "chs_recent": 55.0,
            "behavior_prev": 0.7,
            "behavior_recent": 0.6,
            "text_quality": 0.4,
            "fatigue_score": 0.6,
        }
    }

    # --- One synthetic creative-related hypothesis ---
    hypotheses = [
        {
            "id": "HYP-TEST-001",
            "scope": "campaign",
            "campaign_name": "Test Campaign",
            "driver_type": "creative",
            "initial_confidence": 0.5,
            "rationale": "Synthetic hypothesis for testing CreativeGeneratorV2.",
        }
    ]

    # --- Instantiate CreativeGeneratorV2 with deterministic seed ---
    gen = CreativeGeneratorV2(
        config={
            "variants_per_style": 2,
            "low_ctr_threshold": 0.05,   # our CTR is 0.03 -> considered low
            "chs_threshold": 60.0,       # CHS=55 -> considered weak
            "max_campaigns": 5,
            "seed": 123,
            "overlap_threshold": 0.9,
            "max_suggestions_per_campaign": 10,
        },
        run_id="test",
    )

    output = gen.run_creative_generation(
        data_summary=data_summary,
        chs_summary=chs_summary,
        hypotheses=hypotheses,
        params=None,
    )

    # ---- Basic shape assertions ----
    assert "creatives" in output, "Output should contain 'creatives' key"
    assert isinstance(output["creatives"], list), "'creatives' should be a list"
    assert len(output["creatives"]) >= 1, "Expected at least one campaign in creatives output"

    first = output["creatives"][0]
    assert first.get("campaign_name") == "Test Campaign"
    assert "suggestions" in first and isinstance(first["suggestions"], list)

    # At least one suggestion with required keys
    assert len(first["suggestions"]) >= 1
    s0 = first["suggestions"][0]
    for key in ["headline", "message", "cta", "variant_style", "core_term"]:
        assert key in s0, f"Suggestion missing key: {key}"
