"""
src/orchestrator/aggregator.py
Final aggregation and report writing.

Responsibilities:
- Combine evaluated hypotheses (metric + creative) into final_confidence.
- Serialize insights.json and creatives.json.
- Generate a human-readable report.md.
"""

from typing import Dict, Any, List
from pathlib import Path
import json
import datetime


class Aggregator:
    def __init__(self):
        pass

    def aggregate_and_write(
        self,
        plan: Dict[str, Any],
        data_summary: Dict[str, Any],
        hypotheses: List[Dict[str, Any]],
        creative_output: Dict[str, Any],
        outdir: Path,
    ) -> Dict[str, Any]:
        """
        Merge metric + creative evidence, compute final_confidence,
        and write artifacts to disk.
        """
        outdir.mkdir(parents=True, exist_ok=True)

        # 1) Compute final_confidence per hypothesis
        enriched_hyps = self._compute_final_confidence(hypotheses)

        # 2) Write insights.json  (JSON is ASCII-safe but we still set utf-8)
        insights_path = outdir / "insights.json"
        with open(insights_path, "w", encoding="utf-8") as f:
            json.dump({"hypotheses": enriched_hyps}, f, indent=2)

        # 3) Write creatives.json
        creatives_path = outdir / "creatives.json"
        with open(creatives_path, "w", encoding="utf-8") as f:
            json.dump(creative_output, f, indent=2)

        # 4) Write report.md  🔧 force UTF-8
        report_path = outdir / "report.md"
        report_md = self._build_report_md(
            plan=plan,
            data_summary=data_summary,
            hypotheses=enriched_hyps,
            creative_output=creative_output,
        )
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_md)

        return {
            "insights_path": str(insights_path),
            "creatives_path": str(creatives_path),
            "report_path": str(report_path),
        }


    # ---------- confidence combination ----------

    def _compute_final_confidence(self, hypotheses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for h in hypotheses:
            h2 = dict(h)
            metric_conf = h2.get("metric_confidence")
            creative_conf = h2.get("creative_confidence")
            initial_conf = h2.get("initial_confidence", 0.4)
            driver_type = h2.get("driver_type", "overall")

            if metric_conf is None and creative_conf is None:
                final_conf = initial_conf
            elif driver_type == "creative":
                # combine both where available, bias slightly toward metric evidence
                m = metric_conf if metric_conf is not None else initial_conf
                c = creative_conf if creative_conf is not None else 0.4
                final_conf = 0.6 * m + 0.4 * c
            else:
                # non-creative hypotheses: rely mostly on metric evidence
                if metric_conf is not None:
                    final_conf = metric_conf
                else:
                    final_conf = initial_conf

            # clamp to [0,1]
            final_conf = max(0.0, min(1.0, final_conf))
            h2["final_confidence"] = float(final_conf)
            out.append(h2)
        return out

    # ---------- report generation ----------

    def _build_report_md(
        self,
        plan: Dict[str, Any],
        data_summary: Dict[str, Any],
        hypotheses: List[Dict[str, Any]],
        creative_output: Dict[str, Any],
    ) -> str:
        lines = []
        ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        query_info = plan.get("query_info", {})
        meta = data_summary.get("meta", {})

        # Header
        lines.append(f"# Facebook Performance Analysis Report\n")
        lines.append(f"_Generated at {ts}_  \n")
        lines.append(f"**Query:** {query_info.get('raw_query', '')}\n")
        lines.append("---\n")

        # Meta section
        lines.append("## Dataset Overview\n")
        lines.append(f"- Rows: **{meta.get('n_rows', 'NA')}**")
        lines.append(f"- Campaigns: **{meta.get('n_campaigns', 'NA')}**")
        lines.append(f"- Ad sets: **{meta.get('n_adsets', 'NA')}**")
        lines.append(f"- Creatives: **{meta.get('n_creatives', 'NA')}**")
        lines.append(f"- Date range: **{meta.get('date_min', 'NA')} → {meta.get('date_max', 'NA')}**\n")

        # Top hypotheses
        lines.append("## Top Insights\n")
        if not hypotheses:
            lines.append("_No hypotheses were generated._\n")
        else:
            # sort by final_confidence desc
            top = sorted(hypotheses, key=lambda h: h.get("final_confidence", 0.0), reverse=True)[:10]
            for h in top:
                cid = h.get("id")
                scope = h.get("scope")
                cname = h.get("campaign_name") or "Overall"
                driver = h.get("driver_type")
                conf = h.get("final_confidence", 0.0)
                hypothesis_text = h.get("hypothesis", "")
                rationale = h.get("rationale", "")
                lines.append(f"### {cid} — {cname} ({scope}, driver: {driver}, confidence: {conf:.2f})\n")
                lines.append(f"- **Hypothesis:** {hypothesis_text}")
                lines.append(f"- **Rationale:** {rationale}\n")

        # Creative recommendations
        lines.append("## Creative Recommendations\n")
        creatives = creative_output.get("creatives", [])
        if not creatives:
            lines.append("_No creative recommendations generated._\n")
        else:
            for block in creatives:
                cname = block.get("campaign_name", "Unknown Campaign")
                chs = block.get("chs_current")
                weak = block.get("weak_components", [])
                lines.append(f"### Campaign: {cname}\n")
                if chs is not None:
                    lines.append(f"- **Current CHS:** {chs:.1f}")
                if weak:
                    lines.append(f"- **Weak components:** {', '.join(weak)}")
                test_plan = block.get("test_plan", {})
                if test_plan:
                    lines.append(f"- **Suggested test split:** {test_plan}\n")

                # Show top 3 suggestions
                suggestions = block.get("suggestions", [])[:3]
                for s in suggestions:
                    lines.append(f"- **Variant ({s['variant_type']}):**")
                    lines.append(f"  - Headline: {s['headline']}")
                    lines.append(f"  - Message: {s['message']}")
                    lines.append(f"  - CTA: {s['cta']}\n")

        # Checklist
        lines.append("## Action Checklist\n")
        lines.append("- [ ] Pause or reduce budget on campaigns with low-confidence but severely negative ROAS.")
        lines.append("- [ ] Prioritize creative tests in campaigns flagged with low CHS and low CTR.")
        lines.append("- [ ] For funnel-driven hypotheses, inspect landing page, pricing, and checkout analytics.")
        lines.append("- [ ] Re-run this agent after at least 7 days of new data to validate changes.\n")

        return "\n".join(lines)
