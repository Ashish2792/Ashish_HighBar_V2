# tests/test_data_agent.py
import os
import pytest
from src.agents.data_agent import DataAgent

@pytest.mark.parametrize("sample_mode", ["auto", "off"])
def test_data_summary_keys(sample_mode):
    csv_path = "data/synthetic_fb_ads_undergarments.csv"
    if not os.path.exists(csv_path):
        pytest.skip("CSV not present at expected path.")
    agent = DataAgent(config={"sample_mode": False, "date_col": "date"})
    summary = agent.run_data_load_summary(csv_path, sample=sample_mode)
    for key in ["meta", "global_daily", "campaign_daily",
                "campaign_summary", "creative_summary",
                "creative_repetition", "text_terms"]:
        assert key in summary
