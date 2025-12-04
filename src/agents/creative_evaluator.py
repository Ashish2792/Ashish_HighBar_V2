class CreativeEvaluatorAgent:
    """
    CreativeEvaluatorAgent (CHS)

    Role:
        Implements T4: `creative_evaluation`.
        Computes a Creative Health Score (CHS) per campaign and enriches
        creative-related hypotheses with CHS trend evidence.

    Inputs:
        - hypotheses: from InsightAgent (some with driver_type="creative").
        - data_summary:
            - campaign_summary
            - creative_repetition
            - text_terms (for text quality)
        - config/params:
            - behavior_weight, text_weight, fatigue_weight
            - recent_window_days, previous_window_days
            - min_impressions_for_stats

    Outputs:
        - {
            "chs_summary": { campaign_name -> component scores },
            "evaluated_hypotheses": [ ... ],
            "config_used": {...},
            "evaluated_at": ISO timestamp
          }

        For creative hypotheses we attach:
            - chs_prev, chs_recent, chs_delta
            - chs_components (behavior/text/fatigue)
            - creative_confidence: how strongly CHS supports the hypothesis.

    Assumptions:
        - Behavior_score is relative: we compare a campaign vs others.
        - Fatigue_score is derived from concentration of impressions in top creatives.
        - Text_quality_score is heuristic and based on patterns in text_terms
          (benefits, urgency, social proof phrases).
        - If CHS cannot be computed for a campaign, we keep the hypothesis
          but assign a conservative (low) creative_confidence.
    """


from typing import Dict, Any, List, Optional
import datetime
import math
import traceback

from src.utils.logger import AgentLogger
from src.utils.errors import CreativeEvaluatorError, wrap_exc

class CreativeEvaluatorAgent:
    def __init__(self, config: Optional[Dict[str, Any]] = None, run_id: Optional[str] = None):
        # Default weights; can be overridden by Planner's params
        self.config = {
            "recent_window_days": 14,
            "previous_window_days": 14,
            "behavior_weight": 0.5,
            "text_weight": 0.3,
            "fatigue_weight": 0.2,
            "min_impressions_for_stats": 1000,
        }
        if config:
            self.config.update(config)
        self.run_id = run_id
        self.logger = AgentLogger("CreativeEvaluator", run_id=self.run_id)
        self.logger.debug("init", "CreativeEvaluator initialized", {"config": self.config})

    # ---------- Public API ----------

    def run_creative_evaluation(
        self,
        hypotheses: List[Dict[str, Any]],
        data_summary: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Entry point for T4: 'creative_evaluation'.

        - hypotheses: from InsightAgent (some with driver_type="creative" and required_evidence including "chs_trend")
        - data_summary: from DataAgent
        - params: optional overrides (e.g., chs_weights, window sizes)
        """
        self.logger.info("start", "run_creative_evaluation start", {"n_hypotheses": len(hypotheses)})
        try:
            if params:
                # Merge in CHS-related params
                chs_weights = params.get("chs_weights", {})
                if chs_weights:
                    self.config["behavior_weight"] = chs_weights.get("behavior", self.config["behavior_weight"])
                    self.config["text_weight"] = chs_weights.get("text", self.config["text_weight"])
                    self.config["fatigue_weight"] = chs_weights.get("fatigue", self.config["fatigue_weight"])

                for k in ["recent_window_days", "previous_window_days", "min_impressions_for_stats"]:
                    if k in params:
                        self.config[k] = params[k]
                self.logger.debug("config_update", "Updated CreativeEvaluator config", {"updated_keys": list(params.keys())})

            # Compute CHS per campaign (prev vs recent)
            chs_summary = self._build_chs_summary(data_summary)
            self.logger.info("chs_computed", "Computed CHS for campaigns", {"n_chs": len(chs_summary)})

            evaluated_hypotheses: List[Dict[str, Any]] = []
            enriched_count = 0
            skipped_count = 0

            for hyp in hypotheses:
                h = dict(hyp)
                required = h.get("required_evidence", [])
                driver_type = h.get("driver_type")
                campaign_name = h.get("campaign_name")

                # Only enrich creative-related hypotheses that explicitly want CHS
                if "chs_trend" not in required or driver_type != "creative" or not campaign_name:
                    evaluated_hypotheses.append(h)
                    continue

                chs_info = chs_summary.get(campaign_name)
                if not chs_info:
                    # No CHS info; leave hypothesis unchanged but low creative_confidence
                    h["creative_confidence"] = 0.3
                    evaluated_hypotheses.append(h)
                    skipped_count += 1
                    continue
                chs_prev = chs_info["chs_prev"]
                chs_recent = chs_info["chs_recent"]
                chs_delta = chs_recent - chs_prev if chs_prev is not None and chs_recent is not None else None

            # Compute creative_confidence:
            # - bigger CHS drop => higher confidence that creative is a problem
            # - if CHS went up, treat creative hypothesis as weak
                if chs_delta is None:
                    base_conf = 0.4
                    creative_confidence = base_conf
                    trend_text = "CHS not available; falling back to neutral creative confidence."
                elif chs_delta < 0:
                    drop = -chs_delta  # positive number
                    drop_factor = min(1.0, drop / 30.0)  # 30-point drop => saturate
                    base_conf = 0.4
                    creative_confidence = base_conf + 0.4 * drop_factor  # up to ~0.8
                    trend_text = f"CHS dropped by {drop:.1f} points, suggesting creative performance is weakening."
                else:
                # CHS improved or stable; creative not likely the main issue
                    base_conf = 0.2
                    creative_confidence = base_conf
                    trend_text = f"CHS increased by {chs_delta:.1f} points, so creative is unlikely to be the primary issue."

                h["chs_prev"] = chs_prev
                h["chs_recent"] = chs_recent
                h["chs_delta"] = chs_delta
                h["chs_components"] = {
                    "behavior_prev": chs_info["behavior_prev"],
                    "behavior_recent": chs_info["behavior_recent"],
                    "text_quality": chs_info["text_quality"],
                    "fatigue": chs_info["fatigue_score"],
                }

            # Expose components + explanation for CHS-based confidence
                h["creative_confidence_components"] = {
                    "base": float(base_conf),
                    "chs_delta": chs_delta,
                    "behavior_prev": chs_info["behavior_prev"],
                    "behavior_recent": chs_info["behavior_recent"],
                    "text_quality": chs_info["text_quality"],
                    "fatigue": chs_info["fatigue_score"],
                }
                h["creative_confidence_explanation"] = (
                    f"Creative confidence {creative_confidence:.2f} driven by CHS trend. {trend_text}"
                )

                h["creative_confidence"] = float(creative_confidence)

                evaluated_hypotheses.append(h)
                enriched_count += 1

            self.logger.info("success", "run_creative_evaluation completed", {"enriched": enriched_count, "skipped": skipped_count})
            return {
                "chs_summary": chs_summary,
                "evaluated_hypotheses": evaluated_hypotheses,
                "config_used": self.config,
                "evaluated_at": datetime.datetime.utcnow().isoformat() + "Z"
            }
        except CreativeEvaluatorError:
            # Already typed; ensure logged and re-raised
            self.logger.error("creative_eval_error", "CreativeEvaluator validation error", {"trace": traceback.format_exc()})
            raise
        except Exception as e:
            self.logger.error("exception", "Unhandled exception in CreativeEvaluator", {"trace": traceback.format_exc()})
            raise wrap_exc("CreativeEvaluator failed during run_creative_evaluation", e, CreativeEvaluatorError)

    # ---------- CHS computation ----------

    def _build_chs_summary(self, data_summary: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        Build CHS components and final CHS per campaign.

        Uses:
        - campaign_daily: for prev/recent ROAS & CTR (behavior).
        - creative_repetition: for fatigue_score.
        - text_terms: for text_quality_score.
        """
        try:
            campaign_daily = data_summary.get("campaign_daily", [])
            creative_repetition = data_summary.get("creative_repetition", [])
            text_terms = data_summary.get("text_terms", {})

            # 1) Organize daily metrics by campaign
            daily_by_campaign: Dict[str, List[Dict[str, Any]]] = {}
            for row in campaign_daily:
                cname = row.get("campaign_name")
                if not cname:
                    continue
                daily_by_campaign.setdefault(cname, []).append(row)

            # 2) Compute prev/recent metrics (ROAS, CTR) per campaign
            campaign_stats: Dict[str, Dict[str, Any]] = {}
            for cname, series in daily_by_campaign.items():
                prev, recent = self._split_windows(
                    sorted(series, key=lambda r: r["date"]),
                    self.config["recent_window_days"],
                    self.config["previous_window_days"]
                )
                if not prev or not recent:
                    self.logger.debug("skip_campaign", "Insufficient windows for campaign", {"campaign": cname, "prev_days": len(prev), "recent_days": len(recent)})
                    continue

                prev_roas = self._avg([r.get("roas") for r in prev])
                recent_roas = self._avg([r.get("roas") for r in recent])
                prev_ctr = self._avg([r.get("ctr") for r in prev])
                recent_ctr = self._avg([r.get("ctr") for r in recent])

                prev_impr = sum(r.get("impressions", 0) for r in prev)
                recent_impr = sum(r.get("impressions", 0) for r in recent)
                if (prev_impr + recent_impr) < self.config.get("min_impressions_for_stats", 0):
                    self.logger.debug("low_volume", "Skipping campaign due to low volume", {"campaign": cname, "total_impr": prev_impr + recent_impr})
                    continue

                campaign_stats[cname] = {
                    "prev_roas": prev_roas,
                    "recent_roas": recent_roas,
                    "prev_ctr": prev_ctr,
                    "recent_ctr": recent_ctr,
                    "prev_impr": prev_impr,
                    "recent_impr": recent_impr,
                }

            self.logger.debug("campaign_stats_ready", "Prepared campaign stats for CHS computation", {"n_campaigns": len(campaign_stats)})

            # 3) Compute behavior scores via percentiles across campaigns
            behavior_scores = self._compute_behavior_scores(campaign_stats)

            # 4) Compute text quality & fatigue for each campaign
            chs_summary: Dict[str, Dict[str, Any]] = {}
            fatigue_by_campaign = {r.get("campaign_name"): r for r in creative_repetition} if creative_repetition else {}

            for cname, stats in campaign_stats.items():
                behavior_prev = behavior_scores.get(cname, {}).get("behavior_prev", 0.5)
                behavior_recent = behavior_scores.get(cname, {}).get("behavior_recent", 0.5)

                text_quality = self._compute_text_quality(cname, text_terms.get(cname, []))
                fatigue_info = fatigue_by_campaign.get(cname, None)
                if fatigue_info:
                    top_share = fatigue_info.get("impression_share_of_top_creative", 0.0)
                    fatigue_score = max(0.0, min(1.0, 1.0 - float(top_share)))
                else:
                    fatigue_score = 0.5  # neutral if we don't know

                # 5) Combine into CHS
                bw = self.config.get("behavior_weight", 0.5)
                tw = self.config.get("text_weight", 0.3)
                fw = self.config.get("fatigue_weight", 0.2)
                # normalize weights
                total_w = bw + tw + fw
                if total_w <= 0:
                    bw = 0.5
                    tw = 0.3
                    fw = 0.2
                    total_w = 1.0

                bw_norm = bw / total_w
                tw_norm = tw / total_w
                fw_norm = fw / total_w

                chs_prev = 100.0 * (bw_norm * behavior_prev + tw_norm * text_quality + fw_norm * fatigue_score)
                chs_recent = 100.0 * (bw_norm * behavior_recent + tw_norm * text_quality + fw_norm * fatigue_score)

                chs_summary[cname] = {
                    "campaign_name": cname,
                    "chs_prev": float(chs_prev),
                    "chs_recent": float(chs_recent),
                    "behavior_prev": float(behavior_prev),
                    "behavior_recent": float(behavior_recent),
                    "text_quality": float(text_quality),
                    "fatigue_score": float(fatigue_score),
                }

            return chs_summary
        except Exception as e:
            self.logger.error("chs_failure", "Failed to compute CHS summary", {"trace": traceback.format_exc()})
            raise wrap_exc("CreativeEvaluator failed building CHS summary", e, CreativeEvaluatorError)

    # ---------- helpers ----------

    def _parse_date(self, datestr: str) -> datetime.date:
        return datetime.datetime.strptime(datestr, "%Y-%m-%d").date()

    def _split_windows(
        self,
        series: List[Dict[str, Any]],
        recent_window_days: int,
        previous_window_days: int
    ):
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

    def _avg(self, vals: List[Optional[float]]) -> Optional[float]:
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        return float(sum(vals) / len(vals))

    def _compute_behavior_scores(self, campaign_stats: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
        """
        Convert prev/recent ROAS & CTR into behavior scores in [0,1]
        using simple percentiles across campaigns.
        """
        prev_roas_vals = [s["prev_roas"] for s in campaign_stats.values() if s.get("prev_roas") is not None]
        recent_roas_vals = [s["recent_roas"] for s in campaign_stats.values() if s.get("recent_roas") is not None]
        prev_ctr_vals = [s["prev_ctr"] for s in campaign_stats.values() if s.get("prev_ctr") is not None]
        recent_ctr_vals = [s["recent_ctr"] for s in campaign_stats.values() if s.get("recent_ctr") is not None]

        def percentile(val: Optional[float], arr: List[float]) -> float:
            if val is None or not arr:
                return 0.5
            arr_sorted = sorted(arr)
            count_le = sum(1 for x in arr_sorted if x <= val)
            return count_le / len(arr_sorted)

        behavior_scores: Dict[str, Dict[str, float]] = {}
        for cname, s in campaign_stats.items():
            roas_prev_pct = percentile(s.get("prev_roas"), prev_roas_vals)
            roas_recent_pct = percentile(s.get("recent_roas"), recent_roas_vals)
            ctr_prev_pct = percentile(s.get("prev_ctr"), prev_ctr_vals)
            ctr_recent_pct = percentile(s.get("recent_ctr"), recent_ctr_vals)

            behavior_prev = (roas_prev_pct + ctr_prev_pct) / 2.0
            behavior_recent = (roas_recent_pct + ctr_recent_pct) / 2.0

            behavior_scores[cname] = {
                "behavior_prev": float(behavior_prev),
                "behavior_recent": float(behavior_recent),
            }
        return behavior_scores

    def _compute_text_quality(self, campaign_name: str, terms: List[Dict[str, Any]]) -> float:
        """
        Compute a text quality score in [0,1] based on presence of:
        - benefit words
        - urgency words
        - social proof words
        using the top tokens in text_terms[campaign].
        """
        if not terms:
            return 0.5  # neutral

        # Simple keyword dictionaries
        benefit_words = {
            "comfort", "comfortable", "soft", "seamless", "breathable", "support",
            "fit", "stretch", "lightweight", "invisible", "smooth"
        }
        urgency_words = {
            "today", "now", "limited", "last", "sale", "deal", "offer", "hurry"
        }
        social_words = {
            "rated", "reviews", "bestseller", "favorite", "loved", "customers"
        }

        total_count = sum(t.get("count", 0) for t in terms)
        if total_count <= 0:
            return 0.5

        benefit_count = sum(t.get("count", 0) for t in terms if t.get("term") in benefit_words)
        urgency_count = sum(t.get("count", 0) for t in terms if t.get("term") in urgency_words)
        social_count = sum(t.get("count", 0) for t in terms if t.get("term") in social_words)

        benefit_ratio = benefit_count / total_count
        urgency_ratio = urgency_count / total_count
        social_ratio = social_count / total_count

        # Weighted combination, with a baseline
        score = 0.3 + 0.4 * benefit_ratio + 0.2 * urgency_ratio + 0.1 * social_ratio
        score = max(0.0, min(1.0, score))
        return float(score)
