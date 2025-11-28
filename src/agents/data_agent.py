"""
src/agents/data_agent.py
Data Agent implementation.

Responsibilities:
- Load the Facebook Ads CSV.
- Validate required columns and basic types.
- Compute a compact data_summary used by downstream agents:
  - meta
  - global_daily
  - campaign_daily
  - campaign_summary
  - creative_summary
  - creative_repetition (for CHS fatigue component)
  - text_terms (for creative grounding)

This agent implements the "data_load_summary" task (T1 in the Planner's plan).
"""

from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
from collections import Counter
import re

REQUIRED_COLUMNS = [
    "campaign_name",
    "adset_name",
    "date",
    "spend",
    "impressions",
    "clicks",
    "purchases",
    "revenue",
    "creative_type",
    "creative_message",
    "audience_type",
    "platform",
    "country"
]

def _safe_pct(numerator: float, denominator: float) -> float:
    if denominator is None or denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)

class DataAgent:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.date_col = self.config.get("date_col", "date")

    # ----------- Public API -----------

    def run_data_load_summary(
        self,
        csv_path: str,
        sample: str = "auto"
    ) -> Dict[str, Any]:
        """
        Main entry point for the 'data_load_summary' task.
        Returns a JSON-serializable dict representing the data_summary.
        """
        df = self._load_csv(csv_path, sample_mode=sample)
        self._validate_columns(df)

        df = self._preprocess(df)
        summary = self._build_summary(df)
        return summary

    # ----------- Internal helpers -----------

    def _load_csv(self, path: str, sample_mode: str = "auto") -> pd.DataFrame:
        df = pd.read_csv(path)
        if self.date_col not in df.columns:
            raise ValueError(f"Expected date column '{self.date_col}' not found in CSV.")
        # Optional sampling for speed
        sample_flag = self.config.get("sample_mode", False)
        sample_frac = self.config.get("sample_frac", 0.5)
        if sample_mode != "off" and sample_flag and 0 < sample_frac < 1.0:
            df = df.sample(frac=sample_frac, random_state=42).reset_index(drop=True)
        return df

    def _validate_columns(self, df: pd.DataFrame) -> None:
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns in dataset: {missing}")

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # date parsing
        df[self.date_col] = pd.to_datetime(df[self.date_col], errors="coerce")

        numeric_cols = ["spend", "impressions", "clicks", "purchases", "revenue"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # drop rows with no date
        df = df.dropna(subset=[self.date_col])

        # derived metrics
        df["ctr"] = df.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        df["cvr"] = df.apply(lambda r: _safe_pct(r["purchases"], r["clicks"]), axis=1)
        df["cpc"] = df.apply(lambda r: _safe_pct(r["spend"], r["clicks"]), axis=1)
        df["cpm"] = df.apply(lambda r: _safe_pct(r["spend"], r["impressions"]) * 1000.0, axis=1)
        df["roas"] = df.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        return df

    def _build_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        meta = self._build_meta(df)
        global_daily = self._build_global_daily(df)
        campaign_daily = self._build_campaign_daily(df)
        campaign_summary = self._build_campaign_summary(df)
        creative_summary, creative_repetition = self._build_creative_summary(df)
        text_terms = self._build_text_terms(df)

        summary: Dict[str, Any] = {
            "meta": meta,
            "global_daily": global_daily,
            "campaign_daily": campaign_daily,
            "campaign_summary": campaign_summary,
            "creative_summary": creative_summary,
            "creative_repetition": creative_repetition,
            "text_terms": text_terms,
        }
        return summary

    def _build_meta(self, df: pd.DataFrame) -> Dict[str, Any]:
        date_min = df[self.date_col].min()
        date_max = df[self.date_col].max()
        return {
            "n_rows": int(df.shape[0]),
            "date_min": None if pd.isna(date_min) else date_min.strftime("%Y-%m-%d"),
            "date_max": None if pd.isna(date_max) else date_max.strftime("%Y-%m-%d"),
            "n_campaigns": int(df["campaign_name"].nunique()),
            "n_adsets": int(df["adset_name"].nunique()),
            "n_creatives": int(df["creative_message"].nunique())
        }

    def _build_global_daily(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        daily = (df
                 .groupby(self.date_col)
                 .agg({
                     "spend": "sum",
                     "impressions": "sum",
                     "clicks": "sum",
                     "purchases": "sum",
                     "revenue": "sum"
                 })
                 .reset_index())
        daily["ctr"] = daily.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        daily["roas"] = daily.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        out: List[Dict[str, Any]] = []
        for _, row in daily.sort_values(self.date_col).iterrows():
            out.append({
                "date": row[self.date_col].strftime("%Y-%m-%d"),
                "spend": float(row["spend"]),
                "impressions": int(row["impressions"]),
                "clicks": int(row["clicks"]),
                "purchases": int(row["purchases"]),
                "revenue": float(row["revenue"]),
                "ctr": float(row["ctr"]),
                "roas": float(row["roas"])
            })
        return out

    def _build_campaign_daily(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        grp = (df
               .groupby(["campaign_name", self.date_col])
               .agg({
                   "spend": "sum",
                   "impressions": "sum",
                   "clicks": "sum",
                   "purchases": "sum",
                   "revenue": "sum"
               })
               .reset_index())
        grp["ctr"] = grp.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        grp["roas"] = grp.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        out: List[Dict[str, Any]] = []
        for _, row in grp.sort_values([self.date_col, "campaign_name"]).iterrows():
            out.append({
                "campaign_name": row["campaign_name"],
                "date": row[self.date_col].strftime("%Y-%m-%d"),
                "spend": float(row["spend"]),
                "impressions": int(row["impressions"]),
                "clicks": int(row["clicks"]),
                "purchases": int(row["purchases"]),
                "revenue": float(row["revenue"]),
                "ctr": float(row["ctr"]),
                "roas": float(row["roas"])
            })
        return out

    def _build_campaign_summary(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        grp = (df
               .groupby("campaign_name")
               .agg({
                   "spend": "sum",
                   "impressions": "sum",
                   "clicks": "sum",
                   "purchases": "sum",
                   "revenue": "sum"
               })
               .reset_index())
        grp["ctr"] = grp.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        grp["cvr"] = grp.apply(lambda r: _safe_pct(r["purchases"], r["clicks"]), axis=1)
        grp["cpc"] = grp.apply(lambda r: _safe_pct(r["spend"], r["clicks"]), axis=1)
        grp["cpm"] = grp.apply(lambda r: _safe_pct(r["spend"], r["impressions"]) * 1000.0, axis=1)
        grp["roas"] = grp.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        out: List[Dict[str, Any]] = []
        for _, row in grp.sort_values("campaign_name").iterrows():
            out.append({
                "campaign_name": row["campaign_name"],
                "spend": float(row["spend"]),
                "impressions": int(row["impressions"]),
                "clicks": int(row["clicks"]),
                "purchases": int(row["purchases"]),
                "revenue": float(row["revenue"]),
                "ctr": float(row["ctr"]),
                "cvr": float(row["cvr"]),
                "cpc": float(row["cpc"]),
                "cpm": float(row["cpm"]),
                "roas": float(row["roas"])
            })
        return out

    def _build_creative_summary(self, df: pd.DataFrame):
        grp = (df
               .groupby(["campaign_name", "creative_message"])
               .agg({
                   "spend": "sum",
                   "impressions": "sum",
                   "clicks": "sum",
                   "purchases": "sum",
                   "revenue": "sum"
               })
               .reset_index())
        grp["ctr"] = grp.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        grp["roas"] = grp.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        creative_summary: List[Dict[str, Any]] = []
        for _, row in grp.iterrows():
            creative_summary.append({
                "campaign_name": row["campaign_name"],
                "creative_message": row["creative_message"],
                "spend": float(row["spend"]),
                "impressions": int(row["impressions"]),
                "clicks": int(row["clicks"]),
                "purchases": int(row["purchases"]),
                "revenue": float(row["revenue"]),
                "ctr": float(row["ctr"]),
                "roas": float(row["roas"])
            })

        # Creative repetition stats for fatigue / CHS
        repetition: List[Dict[str, Any]] = []
        for campaign, sub in grp.groupby("campaign_name"):
            total_impr = sub["impressions"].sum()
            if total_impr <= 0:
                share_top = 0.0
            else:
                share_top = float((sub["impressions"].max()) / total_impr)
            repetition.append({
                "campaign_name": campaign,
                "total_impressions": int(total_impr),
                "unique_creatives": int(sub["creative_message"].nunique()),
                "impression_share_of_top_creative": share_top
            })

        return creative_summary, repetition

    def _build_text_terms(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Build simple term frequencies per campaign.
        This is used later for creative grounding and CHS text component.
        """
        def tokenize(text: str) -> List[str]:
            text = text.lower()
            text = re.sub(r"[^a-z0-9\s]", " ", text)
            tokens = [t for t in text.split() if len(t) > 2]
            return tokens

        per_campaign: Dict[str, Counter] = {}
        for campaign, sub in df.groupby("campaign_name"):
            counter = Counter()
            for msg in sub["creative_message"].dropna().astype(str).tolist():
                counter.update(tokenize(msg))
            per_campaign[campaign] = counter

        # Convert to serializable top-k lists
        text_terms: Dict[str, Any] = {}
        for campaign, counter in per_campaign.items():
            text_terms[campaign] = [
                {"term": term, "count": int(count)}
                for term, count in counter.most_common(30)
            ]
        return text_terms
