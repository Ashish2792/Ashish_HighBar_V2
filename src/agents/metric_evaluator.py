"""
src/agents/metric_evaluator.py
Metric Evaluator Agent implementation.

Responsibilities:
- Take hypotheses from InsightAgent + data_summary from DataAgent.
- Validate hypotheses quantitatively:
  * measure ROAS and CTR changes between previous and recent windows.
  * run simple statistical tests:
      - bootstrap p-value for ROAS difference
      - z-test for CTR difference
- Compute metric_confidence and validated flag.
"""

from typing import Dict, Any, List, Optional
import datetime
import math
import random
from statistics import mean


class MetricEvaluatorAgent:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = {
            "recent_window_days": 14,
            "previous_window_days": 14,
            "p_value_threshold": 0.05,
            "bootstrap_iters": 1000,
            "min_impressions_for_stats": 1000,
        }
        if config:
            self.config.update(config)

        # Seed can be controlled externally if needed
        random.seed(42)

    # ---------- Public API ----------

    def run_metric_evaluation(
        self,
        hypotheses: List[Dict[str, Any]],
        data_summary: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Evaluate hypotheses using numeric metrics.

        Returns:
        {
          "evaluated_hypotheses": [...],
          "config_used": {...}
        }
        """
        if params:
            for k in ["recent_window_days", "previous_window_days", "p_value_threshold", "bootstrap_iters"]:
                if k in params:
                    self.config[k] = params[k]

        global_daily = data_summary.get("global_daily", [])
        campaign_daily = data_summary.get("campaign_daily", [])

        # Build index by campaign for daily stats
        daily_by_campaign: Dict[str, List[Dict[str, Any]]] = {}
        for row in campaign_daily:
            cname = row["campaign_name"]
            daily_by_campaign.setdefault(cname, []).append(row)

        evaluated: List[Dict[str, Any]] = []
        for hyp in hypotheses:
            # Copy to avoid mutating original
            h = dict(hyp)

            # Only evaluate if it requests metric evidence
            required = h.get("required_evidence", [])
            if "metric_significance" not in required:
                # still pass it through unchanged
                evaluated.append(h)
                continue

            scope = h.get("scope")
            cname = h.get("campaign_name")

            if scope == "overall":
                series = global_daily
            elif scope == "campaign" and cname in daily_by_campaign:
                # sort by date
                series = sorted(daily_by_campaign[cname], key=lambda r: r["date"])
            else:
                # can't evaluate
                h["metric_confidence"] = 0.0
                h["validated"] = False
                evaluated.append(h)
                continue

            prev, recent = self._split_windows(
                series,
                self.config["recent_window_days"],
                self.config["previous_window_days"]
            )

            if not prev or not recent:
                h["metric_confidence"] = 0.0
                h["validated"] = False
                evaluated.append(h)
                continue

            # Collect daily metrics
            prev_roas_vals = [row.get("roas") for row in prev if row.get("roas") is not None]
            recent_roas_vals = [row.get("roas") for row in recent if row.get("roas") is not None]

            prev_ctr_vals = [row.get("ctr") for row in prev if row.get("ctr") is not None]
            recent_ctr_vals = [row.get("ctr") for row in recent if row.get("ctr") is not None]

            prev_impr = sum(row.get("impressions", 0) for row in prev)
            recent_impr = sum(row.get("impressions", 0) for row in recent)
            prev_clicks = sum(row.get("clicks", 0) for row in prev)
            recent_clicks = sum(row.get("clicks", 0) for row in recent)

            prev_roas = self._avg(prev_roas_vals)
            recent_roas = self._avg(recent_roas_vals)
            prev_ctr = self._avg(prev_ctr_vals)
            recent_ctr = self._avg(recent_ctr_vals)

            # Effect size: default to ROAS pct change; fallback to CTR if ROAS missing
            effect_roas_pct = self._pct_change(prev_roas, recent_roas)
            effect_ctr_pct = self._pct_change(prev_ctr, recent_ctr)
            effect_size_pct = effect_roas_pct if effect_roas_pct is not None else effect_ctr_pct

            # p-values
            p_roas = None
            if prev_roas_vals and recent_roas_vals and len(prev_roas_vals) >= 2 and len(recent_roas_vals) >= 2:
                p_roas = self._bootstrap_p_value(prev_roas_vals, recent_roas_vals, self.config["bootstrap_iters"])

            p_ctr = None
            if prev_impr > 0 and recent_impr > 0 and prev_clicks >= 0 and recent_clicks >= 0:
                p_ctr = self._proportion_ztest(
                    prev_clicks, prev_impr,
                    recent_clicks, recent_impr
                )

            total_impr = prev_impr + recent_impr
            n_days_prev = len(prev)
            n_days_recent = len(recent)

            # Confidence components
            base = 0.5
            volume_factor = self._volume_factor(total_impr)
            p_for_conf = p_roas if p_roas is not None else p_ctr
            significance_factor = self._significance_factor(p_for_conf, self.config["p_value_threshold"])
            stability_factor = self._stability_factor(n_days_prev + n_days_recent)

            metric_confidence = base * volume_factor * significance_factor * stability_factor

            validated = False
            if effect_size_pct is not None:
                if abs(effect_size_pct) >= 5 and metric_confidence >= 0.5:
                    validated = True

            # Attach evaluation details
            h["metric_confidence"] = float(metric_confidence)
            h["validated"] = bool(validated)
            h["metric_effect_size_pct"] = effect_size_pct
            h["metric_p_value_roas"] = p_roas
            h["metric_p_value_ctr"] = p_ctr
            h["metric_sample"] = {
                "prev_days": n_days_prev,
                "recent_days": n_days_recent,
                "prev_impressions": int(prev_impr),
                "recent_impressions": int(recent_impr),
                "prev_clicks": int(prev_clicks),
                "recent_clicks": int(recent_clicks),
            }

            evaluated.append(h)

        return {
            "evaluated_hypotheses": evaluated,
            "config_used": self.config,
            "evaluated_at": datetime.datetime.utcnow().isoformat() + "Z"
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
        Same logic as InsightAgent _split_windows.
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

    def _avg(self, vals: List[float]) -> Optional[float]:
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        return float(mean(vals))

    def _pct_change(self, prev: Optional[float], recent: Optional[float]) -> Optional[float]:
        if prev is None or prev == 0 or recent is None:
            return None
        return float((recent - prev) / prev * 100.0)

    def _bootstrap_p_value(self, prev_vals: List[float], recent_vals: List[float], iters: int) -> float:
        """
        Simple two-sample bootstrap p-value for difference in means under null that distributions are equal.
        """
        combined = prev_vals + recent_vals
        n1 = len(prev_vals)
        n2 = len(recent_vals)
        observed_diff = mean(recent_vals) - mean(prev_vals)
        count_extreme = 0

        for _ in range(iters):
            sample1 = [random.choice(combined) for _ in range(n1)]
            sample2 = [random.choice(combined) for _ in range(n2)]
            diff = mean(sample2) - mean(sample1)
            if abs(diff) >= abs(observed_diff):
                count_extreme += 1

        p_value = count_extreme / float(iters)
        return p_value

    def _proportion_ztest(self, k1: int, n1: int, k2: int, n2: int) -> Optional[float]:
        """
        Two-proportion z-test approximate p-value for CTR difference.
        Returns two-sided p-value.
        """
        if n1 <= 0 or n2 <= 0:
            return None
        p1 = k1 / n1
        p2 = k2 / n2
        p_pool = (k1 + k2) / (n1 + n2)
        if p_pool in (0.0, 1.0):
            return None
        denom = math.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
        if denom == 0:
            return None
        z = (p1 - p2) / denom
        p_value = 2 * (1 - self._normal_cdf(abs(z)))
        return p_value

    def _normal_cdf(self, z: float) -> float:
        return 0.5 * (1 + math.erf(z / math.sqrt(2)))

    def _volume_factor(self, total_impressions: int) -> float:
        # scale log10 impressions into [0,1] roughly; 10^5 impressions => ~1
        if total_impressions <= 0:
            return 0.3
        return min(1.0, math.log10(total_impressions) / 5.0)

    def _significance_factor(self, p_value: Optional[float], threshold: float) -> float:
        if p_value is None:
            return 0.5
        if p_value <= threshold:
            return 1.0
        return max(0.3, 1 - p_value)

    def _stability_factor(self, n_days: int) -> float:
        # more days => more stable; 7+ days ≈ 1.0
        return min(1.0, n_days / 7.0)
