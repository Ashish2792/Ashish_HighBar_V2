class DataAgent:
    """
    DataAgent

    Role:
        Implements T1: `data_load_summary`.
        Loads the raw FB Ads CSV, validates core columns, computes all
        aggregates that downstream agents rely on.

    Inputs:
        - csv_path: path to CSV.
        - config:
            - date_col (default: "date")
            - sample_mode + sample_frac (for downsampling large files)

    Outputs (data_summary dict):
        - meta: row counts, date range, campaign/adset/creative counts.
        - global_daily: list of daily aggregates (roas, ctr, spend, etc).
        - campaign_daily: per-campaign daily metrics.
        - campaign_summary: per-campaign aggregates.
        - creative_summary: per-creative aggregates.
        - creative_repetition: simple fatigue proxy per campaign.
        - text_terms: per-campaign token frequencies for grounding creatives.

    Assumptions:
        - CSV has at least the REQUIRED_COLUMNS defined at the top of this file.
        - `date_col` can be parsed into a datetime; rows with invalid dates are dropped.
        - Numeric columns with non-numeric values are coerced to NaN and then
          handled via aggregation functions that tolerate NaNs.
        - CTR/ROAS are derived metrics; downstream agents depend on them.
    """


from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
from collections import Counter
import re

# All columns we ideally expect in this assignment dataset
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
    "country",
]

# Columns that are truly critical for the rest of the pipeline to make sense.
HARD_REQUIRED = [
    "campaign_name",
    "spend",
    "impressions",
]

# Columns that we can safely synthesize with defaults if they are missing.
SOFT_NUMERIC_OPTIONAL = ["clicks", "purchases", "revenue"]
SOFT_CATEGORICAL_OPTIONAL = [
    "adset_name",
    "creative_type",
    "creative_message",
    "audience_type",
    "platform",
    "country",
]


def _safe_pct(numerator: float, denominator: float) -> float:
    if denominator is None or denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


class DataAgent:
    def __init__(self, config: Optional[Dict[str, Any]] = None, run_id: Optional[str] = None):
        """
        DataAgent
        - config: optional settings (sample behavior, preferred date_col)
        - run_id: ID for structured logging
        """
        from src.utils.logger import AgentLogger

        self.config = config or {}
        self.run_id = run_id
        self.logger = AgentLogger("DataAgent", run_id=self.run_id)

        # preferred fallback date column (may be overridden later)
        self.date_col = self.config.get("date_col", "date")

        self._sample_info: Dict[str, Any] = {}
        self.logger.debug("init", "DataAgent initialized", {"config": self.config})


    # ----------- Public API -----------

    def run_data_load_summary(
        self,
        csv_path: str,
        sample: str = "auto",
    ) -> Dict[str, Any]:
        """
        Main entry point for the 'data_load_summary' task.
        Returns a JSON-serializable dict representing the data_summary.
        """
        df = self._load_csv(csv_path, sample_mode=sample)
        schema_info = self._validate_and_patch_columns(df)

        df = self._preprocess(df)
        summary = self._build_summary(df, schema_info=schema_info)
        return summary

    # ----------- Internal helpers -----------

    def _infer_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Try to infer the date column from a set of candidates, case-insensitive.
        This makes the agent more robust if the upstream export changes slightly
        (e.g. 'date_start', 'Date', 'day').
        """
        # Map lowercased name -> original column name
        norm_map = {c.lower(): c for c in df.columns}

        candidates: List[str] = []
        if self.date_col:
            candidates.append(self.date_col)
        # Reasonable fallbacks for Meta exports
        candidates.extend(
            [
                "date",
                "date_start",
                "reporting_start",
                "reporting_date",
                "day",
            ]
        )

        for cand in candidates:
            key = cand.lower()
            if key in norm_map:
                return norm_map[key]

        return None

    def _load_csv(self, path: str, sample_mode: str = "auto") -> pd.DataFrame:
        df = pd.read_csv(path)
        original_rows = int(df.shape[0])

        inferred_date = self._infer_date_column(df)
        if inferred_date is None:
            raise ValueError(
                "Could not infer a date column. "
                f"Tried candidates like '{self.date_col}', 'date', 'date_start', 'day'. "
                f"Available columns: {list(df.columns)}"
            )

        # Update the internal date_col to the inferred one so downstream code is consistent
        self.date_col = inferred_date

        # Optional sampling for speed
        sample_flag = self.config.get("sample_mode", False)
        sample_frac = self.config.get("sample_frac", 0.5)

        if sample_mode != "off" and sample_flag and 0 < sample_frac < 1.0:
            df = df.sample(frac=sample_frac, random_state=42).reset_index(drop=True)
            sampled_rows = int(df.shape[0])
            self._sample_info = {
                "enabled": True,
                "original_rows": original_rows,
                "sampled_rows": sampled_rows,
                "sample_frac": float(sample_frac),
            }
        else:
            self._sample_info = {
                "enabled": False,
                "original_rows": original_rows,
                "sampled_rows": original_rows,
                "sample_frac": 1.0,
            }

        return df

    def _validate_and_patch_columns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Make the agent more adaptable:
        - Ensure hard-required columns exist (or fail fast with a clear error).
        - Soft-fill optional numeric and categorical columns when missing.
        - Record schema drift information to meta so evaluators / humans can see it.
        """
        schema_info: Dict[str, Any] = {
            "inferred_date_column": self.date_col,
            "hard_missing": [],
            "soft_filled_numeric": [],
            "soft_filled_categorical": [],
            "extra_columns": [],
            "missing_required_after_patch": [],
        }

        # 1) Hard-required columns (excluding date_col which is already inferred)
        hard_missing = [c for c in HARD_REQUIRED if c not in df.columns]
        if hard_missing:
            schema_info["hard_missing"] = hard_missing
            raise ValueError(
                "Dataset is missing critical columns required for analysis: "
                f"{hard_missing}. Columns present: {list(df.columns)}"
            )

        # 2) Soft optional numeric columns: create as zeros if missing
        for col in SOFT_NUMERIC_OPTIONAL:
            if col not in df.columns:
                df[col] = 0.0
                schema_info["soft_filled_numeric"].append(col)

        # 3) Soft optional categorical columns: create as 'UNKNOWN' if missing
        for col in SOFT_CATEGORICAL_OPTIONAL:
            if col not in df.columns:
                df[col] = "UNKNOWN"
                schema_info["soft_filled_categorical"].append(col)

        # 4) Track extra columns (schema drift in the other direction)
        extra_cols = [c for c in df.columns if c not in REQUIRED_COLUMNS]
        schema_info["extra_columns"] = extra_cols

        # 5) What required columns (from the original assignment schema) are still missing?
        missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        schema_info["missing_required_after_patch"] = missing_required

        return schema_info

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # date parsing
        df[self.date_col] = pd.to_datetime(df[self.date_col], errors="coerce")

        numeric_cols = ["spend", "impressions", "clicks", "purchases", "revenue"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                # Should not happen thanks to _validate_and_patch_columns,
                # but we keep a defensive fallback.
                df[col] = np.nan

        # drop rows with no valid date
        df = df.dropna(subset=[self.date_col])

        # derived metrics
        df["ctr"] = df.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        df["cvr"] = df.apply(lambda r: _safe_pct(r["purchases"], r["clicks"]), axis=1)
        df["cpc"] = df.apply(lambda r: _safe_pct(r["spend"], r["clicks"]), axis=1)
        df["cpm"] = df.apply(lambda r: _safe_pct(r["spend"], r["impressions"]) * 1000.0, axis=1)
        df["roas"] = df.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        return df

    def _build_summary(self, df: pd.DataFrame, schema_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        meta = self._build_meta(df, schema_info=schema_info)
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

    def _build_meta(self, df: pd.DataFrame, schema_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        date_min = df[self.date_col].min()
        date_max = df[self.date_col].max()

        meta: Dict[str, Any] = {
            "n_rows": int(df.shape[0]),
            "date_min": None if pd.isna(date_min) else date_min.strftime("%Y-%m-%d"),
            "date_max": None if pd.isna(date_max) else date_max.strftime("%Y-%m-%d"),
            "n_campaigns": int(df["campaign_name"].nunique()),
            "n_adsets": int(df["adset_name"].nunique()) if "adset_name" in df.columns else 0,
            "n_creatives": int(df["creative_message"].nunique()) if "creative_message" in df.columns else 0,
        }

        # Numeric null rates after preprocessing (useful to see data quality)
        numeric_cols = ["spend", "impressions", "clicks", "purchases", "revenue"]
        null_rates: Dict[str, float] = {}
        for col in numeric_cols:
            if col in df.columns:
                null_rates[col] = float(df[col].isna().mean())
        meta["numeric_null_rate"] = null_rates

        # Sampling info from _load_csv
        if self._sample_info:
            meta["sampling"] = self._sample_info

        # Schema drift / patch info
        if schema_info is not None:
            meta["schema"] = schema_info

        return meta

    def _build_global_daily(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        daily = (
            df.groupby(self.date_col)
            .agg(
                {
                    "spend": "sum",
                    "impressions": "sum",
                    "clicks": "sum",
                    "purchases": "sum",
                    "revenue": "sum",
                }
            )
            .reset_index()
        )
        daily["ctr"] = daily.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        daily["roas"] = daily.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        out: List[Dict[str, Any]] = []
        for _, row in daily.sort_values(self.date_col).iterrows():
            out.append(
                {
                    "date": row[self.date_col].strftime("%Y-%m-%d"),
                    "spend": float(row["spend"]),
                    "impressions": int(row["impressions"]),
                    "clicks": int(row["clicks"]),
                    "purchases": int(row["purchases"]),
                    "revenue": float(row["revenue"]),
                    "ctr": float(row["ctr"]),
                    "roas": float(row["roas"]),
                }
            )
        return out

    def _build_campaign_daily(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        grp = (
            df.groupby(["campaign_name", self.date_col])
            .agg(
                {
                    "spend": "sum",
                    "impressions": "sum",
                    "clicks": "sum",
                    "purchases": "sum",
                    "revenue": "sum",
                }
            )
            .reset_index()
        )
        grp["ctr"] = grp.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        grp["roas"] = grp.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        out: List[Dict[str, Any]] = []
        for _, row in grp.sort_values([self.date_col, "campaign_name"]).iterrows():
            out.append(
                {
                    "campaign_name": row["campaign_name"],
                    "date": row[self.date_col].strftime("%Y-%m-%d"),
                    "spend": float(row["spend"]),
                    "impressions": int(row["impressions"]),
                    "clicks": int(row["clicks"]),
                    "purchases": int(row["purchases"]),
                    "revenue": float(row["revenue"]),
                    "ctr": float(row["ctr"]),
                    "roas": float(row["roas"]),
                }
            )
        return out

    def _build_campaign_summary(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        grp = (
            df.groupby("campaign_name")
            .agg(
                {
                    "spend": "sum",
                    "impressions": "sum",
                    "clicks": "sum",
                    "purchases": "sum",
                    "revenue": "sum",
                }
            )
            .reset_index()
        )
        grp["ctr"] = grp.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        grp["cvr"] = grp.apply(lambda r: _safe_pct(r["purchases"], r["clicks"]), axis=1)
        grp["cpc"] = grp.apply(lambda r: _safe_pct(r["spend"], r["clicks"]), axis=1)
        grp["cpm"] = grp.apply(lambda r: _safe_pct(r["spend"], r["impressions"]) * 1000.0, axis=1)
        grp["roas"] = grp.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        out: List[Dict[str, Any]] = []
        for _, row in grp.sort_values("campaign_name").iterrows():
            out.append(
                {
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
                    "roas": float(row["roas"]),
                }
            )
        return out

    def _build_creative_summary(self, df: pd.DataFrame):
        grp = (
            df.groupby(["campaign_name", "creative_message"])
            .agg(
                {
                    "spend": "sum",
                    "impressions": "sum",
                    "clicks": "sum",
                    "purchases": "sum",
                    "revenue": "sum",
                }
            )
            .reset_index()
        )
        grp["ctr"] = grp.apply(lambda r: _safe_pct(r["clicks"], r["impressions"]), axis=1)
        grp["roas"] = grp.apply(lambda r: _safe_pct(r["revenue"], r["spend"]), axis=1)

        creative_summary: List[Dict[str, Any]] = []
        for _, row in grp.iterrows():
            creative_summary.append(
                {
                    "campaign_name": row["campaign_name"],
                    "creative_message": row["creative_message"],
                    "spend": float(row["spend"]),
                    "impressions": int(row["impressions"]),
                    "clicks": int(row["clicks"]),
                    "purchases": int(row["purchases"]),
                    "revenue": float(row["revenue"]),
                    "ctr": float(row["ctr"]),
                    "roas": float(row["roas"]),
                }
            )

        # Creative repetition stats for fatigue / CHS
        repetition: List[Dict[str, Any]] = []
        for campaign, sub in grp.groupby("campaign_name"):
            total_impr = sub["impressions"].sum()
            if total_impr <= 0:
                share_top = 0.0
            else:
                share_top = float((sub["impressions"].max()) / total_impr)
            repetition.append(
                {
                    "campaign_name": campaign,
                    "total_impressions": int(total_impr),
                    "unique_creatives": int(sub["creative_message"].nunique()),
                    "impression_share_of_top_creative": share_top,
                }
            )

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
