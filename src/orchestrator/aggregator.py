# src/orchestrator/aggregator.py
"""
Aggregator: collects outputs from agents and writes final artifacts:
 - reports/insights.json
 - reports/creatives.json
 - reports/report.md

This version is tolerant to both 'variant_type' (old generator) and 'variant_style' (V2),
and will not raise if those keys are missing. It also gracefully handles missing fields.
"""

from typing import Dict, Any, List, Optional
import os
import json
import datetime
from pathlib import Path

class Aggregator:
    def __init__(self):
        pass

    def aggregate_and_write(
        self,
        plan: Dict[str, Any],
        data_summary: Dict[str, Any],
        hypotheses: List[Dict[str, Any]],
        creative_output: Dict[str, Any],
        outdir: Path
    ) -> Dict[str, str]:
        """
        Aggregates inputs and writes outputs to outdir.
        Returns a dict with filesystem paths for the written artifacts.
        """
        outdir.mkdir(parents=True, exist_ok=True)

        insights = {
            "plan": plan,
            "data_summary_meta": data_summary.get("meta", {}),
            "hypotheses": hypotheses,
            "generated_at": datetime.datetime.utcnow().isoformat() + "Z"
        }

        creatives = creative_output or {"creatives": []}

        # write JSON artifacts
        insights_path = outdir / "insights.json"
        creatives_path = outdir / "creatives.json"
        report_path = outdir / "report.md"

        with open(insights_path, "w", encoding="utf-8") as f:
            json.dump(insights, f, indent=2, ensure_ascii=False)

        with open(creatives_path, "w", encoding="utf-8") as f:
            json.dump(creatives, f, indent=2, ensure_ascii=False)

        # write a human-readable markdown report
        report_md = self._build_report_md(plan, data_summary, hypotheses, creatives)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_md)

        return {
            "insights_path": str(insights_path),
            "creatives_path": str(creatives_path),
            "report_path": str(report_path)
        }

    def _safe_get(self, d: Dict[str, Any], key: str, default=None):
        if not isinstance(d, dict):
            return default
        return d.get(key, default)

    def _build_report_md(
        self,
        plan: Dict[str, Any],
        data_summary: Dict[str, Any],
        hypotheses: List[Dict[str, Any]],
        creatives: Dict[str, Any]
    ) -> str:
        """
        Build a markdown report summarizing:
         - plan and config
         - top hypotheses (validated)
         - creative suggestions (campaign-level)
        """
        lines: List[str] = []
        now = datetime.datetime.utcnow().isoformat() + "Z"
        lines.append(f"# Kasparro Agentic FB-Analyst Report\n")
        lines.append(f"_Generated at: {now}_\n")
        lines.append("## 1) Plan summary\n")
        q = plan.get("query_info", {}).get("raw_query", "N/A")
        lines.append(f"- Query: **{q}**\n")
        lines.append(f"- Tasks: {len(plan.get('tasks', []))}\n")
        lines.append("\n## 2) Dataset meta\n")
        meta = data_summary.get("meta", {})
        lines.append(f"- Rows: {meta.get('n_rows', 'N/A')}")
        lines.append(f"- Date range: {meta.get('date_min', 'N/A')} → {meta.get('date_max', 'N/A')}")
        lines.append(f"- Campaigns: {meta.get('n_campaigns', 'N/A')}")
        lines.append("")

        # Hypotheses: show top validated ones first
        lines.append("## 3) Hypotheses (top validated first)\n")
        if not hypotheses:
            lines.append("_No hypotheses generated._\n")
        else:
            # sort by final_confidence or metric_confidence/initial_confidence
            def hyp_score(h):
                return float(h.get("final_confidence") or h.get("metric_confidence") or h.get("initial_confidence") or 0.0)
            sorted_h = sorted(hypotheses, key=hyp_score, reverse=True)
            for h in sorted_h[:20]:
                hid = h.get("id", "N/A")
                scope = h.get("scope", "N/A")
                cname = h.get("campaign_name", "N/A")
                driver = h.get("driver_type", "N/A")
                conf = hyp_score(h)
                lines.append(f"- **{hid}** | scope: _{scope}_ | campaign: _{cname}_ | driver: _{driver}_ | confidence: **{conf:.2f}**")
                # short rationale
                if h.get("rationale"):
                    lines.append(f"  - Rationale: {h.get('rationale')}")
                # metrics snapshot if present
                ms = h.get("metrics_snapshot")
                if isinstance(ms, dict):
                    prev = ms.get("prev", {})
                    recent = ms.get("recent", {})
                    lines.append(f"  - Metrics (prev → recent): ROAS {prev.get('roas','N/A')} → {recent.get('roas','N/A')}, CTR {prev.get('ctr','N/A')} → {recent.get('ctr','N/A')}")
                lines.append("")

        # Creatives: campaign-level sections
        lines.append("## 4) Creative suggestions\n")
        creatives_list = creatives.get("creatives", []) if isinstance(creatives, dict) else []

        if not creatives_list:
            lines.append("_No creative suggestions generated._\n")
        else:
            for c in creatives_list:
                cname = c.get("campaign_name", "N/A")
                chs = c.get("chs_current", None)
                weak = c.get("weak_components", [])
                suggestions = c.get("suggestions", [])
                lines.append(f"### Campaign: **{cname}**  ")
                lines.append(f"- CHS (recent): {chs}  ")
                lines.append(f"- Weak components: {', '.join(weak) if weak else 'N/A'}  ")
                lines.append(f"- Suggestions: {len(suggestions)}  ")
                lines.append("")

                if not suggestions:
                    lines.append("_No suggestions available_\n")
                    continue

                # enumerate suggestions with tolerant keys
                for idx, s in enumerate(suggestions, start=1):
                    # support both variant keys
                    variant = s.get("variant_type") or s.get("variant_style") or s.get("variant") or "variant"
                    headline = s.get("headline") or s.get("title") or ""
                    message = s.get("message") or s.get("body") or ""
                    cta = s.get("cta") or s.get("cta_text") or ""
                    overlap = s.get("overlap_score", None)
                    risk = s.get("risk_level", None)
                    reasoning = s.get("reasoning_chain", [])
                    chs_targets = s.get("chs_targets", s.get("targeted_weakness", []))

                    lines.append(f"- **Suggestion {idx}** ({variant})")
                    if headline:
                        lines.append(f"  - Headline: {headline}")
                    if message:
                        # keep message short in report (first 200 chars)
                        msg_short = message if len(message) <= 200 else message[:197] + "..."
                        lines.append(f"  - Message: {msg_short}")
                    if cta:
                        lines.append(f"  - CTA: {cta}")
                    if overlap is not None:
                        lines.append(f"  - Overlap score vs existing creatives: {overlap:.2f}")
                    if risk:
                        lines.append(f"  - Risk level: {risk}")
                    if chs_targets:
                        lines.append(f"  - CHS targets: {', '.join(chs_targets)}")
                    if reasoning:
                        # include top 2 reasoning bullets
                        for r in (reasoning[:2] if isinstance(reasoning, list) else [reasoning]):
                            lines.append(f"  - Reason: {r}")
                    lines.append("")

        # Footer: run metadata
        lines.append("---")
        lines.append("Generated by Kasparro Agentic FB-Analyst")
        lines.append("")

        return "\n".join(lines)
