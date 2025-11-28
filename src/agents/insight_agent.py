"""
src/agents/insight_agent.py
Insight Agent implementation.

Responsibilities:
- Take summarized data from DataAgent (data_summary).
- Detect patterns at overall and campaign level (ROAS/CTR changes).
- Generate structured hypotheses with rationale and initial_confidence.
- Tag hypotheses with driver_type (creative / funnel / audience / mixed / overall).
- Specify required_evidence so Evaluator + Creative Evaluator know what to compute.
"""

from typing import Dict, Any, List, Optional
import datetime
from collections import defaultdict
from statistics import mean

import math


class InsightAgent:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # default thresholds; will usually be overridden by Planner's params
        self.config = {
            "recent_window_days": 14,
            "previous_window_days": 14,
            "roas_drop_threshold_pct": -20.0,
            "low_ctr_threshold": 0.02,
            "min_impressions_for_stats": 1000,
        }
        if config:
            self.config.update(config)

    # ---------- Public API ----------

    def run_insight_generation(
        self,
        data_summary: Dict[str, Any],
        intent: str,
        params: Optional[Dict[str, Any]] = None,
        campaign_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Entry point for T2: 'insight_generation'.

        data_summary: output of DataAgent.run_data_load_summary
        intent: string from Planner (e.g. 'analyze_roas', 'analyze_ctr')
        params: extra overrides from Planner (thresholds, windows)
        """
        if params:
            # merge Planner params into config
            self.config.update({
                k: v for k, v in params.items()
                if k in self.config
            })

        meta = data_summary.get("meta", {})
        global_daily = data_summary.get("global_daily", [])
        campaign_daily = data_summary.get("campaign_daily", [])
        campaign_summary = data_summary.get("campaign_summary", [])

        # Build index by campaign for daily stats
        daily_by_campaign = defaultdict(list)
        for row in campaign_daily:
            daily_by_campaign[row["campaign_name"]].append(row)

        # 1) Overall hypothesis (account-level ROAS/CTR change)
        overall_hypotheses = self._build_overall_hypotheses(global_daily, intent)

        # 2) Campaign-level hypotheses (drivers of ROAS change)
        campaign_hypotheses = self._build_campaign_hypotheses(
            campaign_summary,
            daily_by_campaign,
            intent,
            campaign_filter
        )

        hypotheses = overall_hypotheses + campaign_hypotheses

        return {
            "hypotheses": hypotheses,
            "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "config_used": self.config
        }

    # ---------- Internal helpers ----------

    def _parse_date(self, datestr: str) -> datetime.date:
        return datetime.datetime.strptime(datestr, "%Y-%m-%d").date()

    def _split_windows(
        self,
        series: List[Dict[str, Any]],
        recent_window_days: int,
        previous_window_days: int
    ):
        """
        Given a list of daily dicts with 'date' (YYYY-MM-DD), split into previous and recent windows
        based on the max date in the series.
        """
        if not series:
            return [], []

        dates = [self._parse_date(r["date"]) for r in series]
        max_date = max(dates)
        recent_cutoff = max_date
        prev_end = max_date - datetime.timedelta(days=recent_window_days)
        prev_start = prev_end - datetime.timedelta(days=previous_window_days)

        prev = []
        recent = []
        for row, d in zip(series, dates):
            if prev_start < d <= prev_end:
                prev.append(row)
            elif prev_end < d <= recent_cutoff:
                recent.append(row)

        return prev, recent

    def _avg_metric(self, rows: List[Dict[str, Any]], metric: str) -> Optional[float]:
        vals = [r.get(metric) for r in rows if r.get(metric) is not None]
        if not vals:
            return None
        return float(mean(vals))

    def _pct_change(self, prev: Optional[float], recent: Optional[float]) -> Optional[float]:
        if prev is None or prev == 0 or recent is None:
            return None
        return float((recent - prev) / prev * 100.0)

    # ---------- Overall hypotheses ----------

    def _build_overall_hypotheses(
        self,
        global_daily: List[Dict[str, Any]],
        intent: str
    ) -> List[Dict[str, Any]]:
        if not global_daily:
            return []

        prev, recent = self._split_windows(
            global_daily,
            self.config["recent_window_days"],
            self.config["previous_window_days"]
        )

        if not prev or not recent:
            return []

        prev_roas = self._avg_metric(prev, "roas")
        recent_roas = self._avg_metric(recent, "roas")
        prev_ctr = self._avg_metric(prev, "ctr")
        recent_ctr = self._avg_metric(recent, "ctr")

        roas_change = self._pct_change(prev_roas, recent_roas)
        ctr_change = self._pct_change(prev_ctr, recent_ctr)

        hypotheses: List[Dict[str, Any]] = []

        # Only create hypothesis if something changed meaningfully
        if roas_change is not None and abs(roas_change) > 5:
            if roas_change < 0:
                hypothesis_text = "Overall ROAS has decreased in the recent period."
            else:
                hypothesis_text = "Overall ROAS has increased in the recent period."

            rationale = f"ROAS changed by {roas_change:.1f}% (prev={prev_roas:.2f}, recent={recent_roas:.2f})."
            if ctr_change is not None:
                rationale += f" CTR changed by {ctr_change:.1f}% (prev={prev_ctr:.4f}, recent={recent_ctr:.4f})."

            # initial confidence based on magnitude of change
            magnitude = min(1.0, abs(roas_change) / 50.0)  # 50% change caps contribution
            initial_confidence = 0.4 + 0.4 * magnitude  # between 0.4 and 0.8 approx

            hypotheses.append({
                "id": "HYP-OVERALL-ROAS",
                "scope": "overall",
                "campaign_name": None,
                "driver_type": "overall",
                "hypothesis": hypothesis_text,
                "rationale": rationale,
                "metrics_snapshot": {
                    "prev": {"roas": prev_roas, "ctr": prev_ctr},
                    "recent": {"roas": recent_roas, "ctr": recent_ctr},
                    "pct_change": {"roas": roas_change, "ctr": ctr_change}
                },
                "required_evidence": ["metric_significance"],
                "initial_confidence": float(initial_confidence)
            })

        return hypotheses

    # ---------- Campaign-level hypotheses ----------

    def _build_campaign_hypotheses(
        self,
        campaign_summary: List[Dict[str, Any]],
        daily_by_campaign,
        intent: str,
        campaign_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        roas_thresh = self.config["roas_drop_threshold_pct"]
        low_ctr_thresh = self.config["low_ctr_threshold"]
        min_impr = self.config["min_impressions_for_stats"]

        hypotheses: List[Dict[str, Any]] = []
        counter = 1

        for cs in campaign_summary:
            cname = cs["campaign_name"]
            if campaign_filter and cname != campaign_filter:
                continue

            daily = sorted(daily_by_campaign.get(cname, []), key=lambda r: r["date"])
            if not daily:
                continue

            prev, recent = self._split_windows(
                daily,
                self.config["recent_window_days"],
                self.config["previous_window_days"]
            )
            if not prev or not recent:
                continue

            # aggregate metrics
            prev_roas = self._avg_metric(prev, "roas")
            recent_roas = self._avg_metric(recent, "roas")
            prev_ctr = self._avg_metric(prev, "ctr")
            recent_ctr = self._avg_metric(recent, "ctr")

            prev_impr = sum(r.get("impressions", 0) for r in prev)
            recent_impr = sum(r.get("impressions", 0) for r in recent)

            roas_change = self._pct_change(prev_roas, recent_roas)
            ctr_change = self._pct_change(prev_ctr, recent_ctr)

            # Skip low-volume campaigns
            if prev_impr < min_impr and recent_impr < min_impr:
                continue

            # Determine patterns
            if roas_change is None and ctr_change is None:
                continue

            # We'll build hypotheses mostly where ROAS drops
            if roas_change is not None and roas_change <= roas_thresh:
                driver_type, hypo_text = self._classify_driver(roas_change, ctr_change)
                rationale = (
                    f"Campaign '{cname}' ROAS changed by {roas_change:.1f}% "
                    f"(prev={prev_roas:.2f}, recent={recent_roas:.2f}). "
                )
                if ctr_change is not None:
                    rationale += (
                        f"CTR changed by {ctr_change:.1f}% "
                        f"(prev={prev_ctr:.4f}, recent={recent_ctr:.4f}). "
                    )
                rationale += f"Impressions prev={prev_impr}, recent={recent_impr}."

                # Initial confidence: based on magnitude of roas_change and volume
                mag = min(1.0, abs(roas_change) / 50.0)
                vol_factor = min(1.0, math.log10(max(prev_impr + recent_impr, 10)) / 5.0)
                initial_confidence = 0.4 + 0.3 * mag + 0.2 * vol_factor

                hyp_id = f"HYP-{counter:03d}"
                counter += 1

                required_evidence = ["metric_significance"]
                if driver_type == "creative":
                    required_evidence.append("chs_trend")   # ask Creative Evaluator
                elif driver_type in ("funnel", "audience", "mixed"):
                    required_evidence.append("segment_breakdown")

                hypotheses.append({
                    "id": hyp_id,
                    "scope": "campaign",
                    "campaign_name": cname,
                    "driver_type": driver_type,
                    "hypothesis": hypo_text,
                    "rationale": rationale,
                    "metrics_snapshot": {
                        "prev": {
                            "roas": prev_roas,
                            "ctr": prev_ctr,
                            "impressions": prev_impr
                        },
                        "recent": {
                            "roas": recent_roas,
                            "ctr": recent_ctr,
                            "impressions": recent_impr
                        },
                        "pct_change": {
                            "roas": roas_change,
                            "ctr": ctr_change
                        }
                    },
                    "required_evidence": required_evidence,
                    "initial_confidence": float(initial_confidence)
                })

            # Optionally: insights for low CTR campaigns even if ROAS not too bad
            # (useful for creative generation later)
            if prev_ctr is not None and recent_ctr is not None:
                recent_ctr_is_low = recent_ctr < low_ctr_thresh
                if recent_ctr_is_low and (roas_change is None or roas_change > roas_thresh):
                    # creative performance concern, not necessarily ROAS crash
                    hypo_text = (
                        f"CTR is structurally low for campaign '{cname}', "
                        f"likely indicating weak ad creative or mismatch with audience."
                    )
                    rationale = (
                        f"Recent CTR={recent_ctr:.4f} below threshold {low_ctr_thresh:.4f} "
                        f"(prev CTR={prev_ctr:.4f}). Impressions prev={prev_impr}, recent={recent_impr}."
                    )
                    mag = min(1.0, abs((recent_ctr - low_ctr_thresh) / low_ctr_thresh)) if low_ctr_thresh > 0 else 0.5
                    vol_factor = min(1.0, math.log10(max(prev_impr + recent_impr, 10)) / 5.0)
                    initial_confidence = 0.4 + 0.3 * mag + 0.2 * vol_factor

                    hyp_id = f"HYP-{counter:03d}"
                    counter += 1

                    hypotheses.append({
                        "id": hyp_id,
                            "scope": "campaign",
                            "campaign_name": cname,
                            "driver_type": "creative",
                            "hypothesis": hypo_text,
                            "rationale": rationale,
                            "metrics_snapshot": {
                                "prev": {
                                    "roas": prev_roas,
                                    "ctr": prev_ctr,
                                    "impressions": prev_impr
                                },
                                "recent": {
                                    "roas": recent_roas,
                                    "ctr": recent_ctr,
                                    "impressions": recent_impr
                                },
                                "pct_change": {
                                    "roas": roas_change,
                                    "ctr": ctr_change
                                }
                            },
                            "required_evidence": ["metric_significance", "chs_trend"],
                            "initial_confidence": float(initial_confidence)
                        })

        return hypotheses

    def _classify_driver(
        self,
        roas_change: Optional[float],
        ctr_change: Optional[float]
    ):
        """
        Very simple rule-based classification of driver_type based on ROAS and CTR changes.
        """
        if roas_change is None:
            return "mixed", "ROAS change is unclear but campaign performance looks unstable."

        if ctr_change is None:
            # can't distinguish creative vs funnel, default to mixed
            return "mixed", "ROAS dropped; unclear if driven by click-through or conversion."

        # heuristics:
        # ROAS ↓, CTR ↓  => creative/upper-funnel
        # ROAS ↓, CTR ~0 => funnel/conversion
        # ROAS ↓, CTR ↑  => audience/low-intent clicks or funnel
        if roas_change < 0:
            if ctr_change < -5:
                driver_type = "creative"
                hypo_text = "ROAS and CTR both dropped; likely creative fatigue or weaker ad messaging."
            elif abs(ctr_change) <= 5:
                driver_type = "funnel"
                hypo_text = "ROAS dropped while CTR is stable; likely a post-click or pricing/funnel issue."
            else:  # ctr_change > 5
                driver_type = "audience"
                hypo_text = (
                    "ROAS dropped while CTR increased; likely attracting low-intent clicks "
                    "or a mismatch between audience and product value."
                )
        else:
            driver_type = "mixed"
            hypo_text = "ROAS improved; campaign is performing better overall, but deeper drivers need evaluation."

        return driver_type, hypo_text
