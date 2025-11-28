"""
src/agents/planner.py
Planner Agent implementation.

Responsibilities:
- Interpret user query and classify intent.
- Produce an ordered task plan (JSON-serializable dict) with tasks including:
  id, type, agent, params, depends_on, description.
- Expose simple reflection/retry logic to widen analysis if evaluations come back low-confidence.
"""

from typing import Optional, Dict, Any, List
import datetime
import json

DEFAULT_CONFIG = {
    "recent_window_days": 14,
    "previous_window_days": 14,
    "roas_drop_threshold_pct": -20,
    "low_ctr_threshold": 0.02,
    "min_impressions_for_stats": 1000,
    "max_retries": 2,
    "reflection_confidence_thresh": 0.4,
}

TASK_TEMPLATE = {
    "id": None,
    "type": None,
    "agent": None,
    "params": {},
    "depends_on": [],
    "description": "",
}


class PlannerAgent:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)

    def interpret_query(self, query: str) -> Dict[str, Any]:
        """
        Classifies query intent and returns a small metadata dict.
        Keep this simple and deterministic (keyword-based).
        """
        q = query.lower().strip()
        if "roas" in q or "roas drop" in q or "revenue" in q:
            intent = "analyze_roas"
        elif "ctr" in q or "low ctr" in q or "click-through" in q:
            intent = "analyze_ctr"
        elif "creative" in q or "ads copy" in q or "creative ideas" in q:
            intent = "creative_optimize"
        else:
            intent = "general_diagnosis"
        return {
            "raw_query": query,
            "intent": intent,
            "created_at": datetime.datetime.utcnow().isoformat() + "Z",
        }

    def _new_task(
        self,
        tid: str,
        ttype: str,
        agent: str,
        params: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
        desc: str = "",
    ) -> Dict[str, Any]:
        """
        Helper to create a task dict with consistent structure.
        """
        t = TASK_TEMPLATE.copy()
        t["id"] = tid
        t["type"] = ttype
        t["agent"] = agent
        t["params"] = params or {}
        t["depends_on"] = depends_on or []
        t["description"] = desc
        return t

    def generate_plan(
        self,
        query: str,
        dataset_meta: Optional[Dict[str, Any]] = None,
        campaign_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Return a plan (dict) containing:
        - query_info
        - dataset_meta
        - campaign_filter
        - tasks: ordered list of tasks
        - plan_created_at
        - config
        """
        info = self.interpret_query(query)
        intent = info["intent"]
        tasks: List[Dict[str, Any]] = []

        # 1. Data load + summary
        tasks.append(
            self._new_task(
                "T1",
                "data_load_summary",
                "data_agent",
                params={"sample": "auto"},
                desc="Load CSV, validate columns, compute core summary aggregates (daily, campaign, creative).",
            )
        )

        # 2. Initial insight generation
        tasks.append(
            self._new_task(
                "T2",
                "insight_generation",
                "insight_agent",
                params={
                    "intent": intent,
                    "initial_scope": "account_and_campaign",
                    "roas_drop_threshold_pct": self.config["roas_drop_threshold_pct"],
                    "low_ctr_threshold": self.config["low_ctr_threshold"],
                    "min_impressions_for_stats": self.config["min_impressions_for_stats"],
                    "recent_window_days": self.config["recent_window_days"],
                    "previous_window_days": self.config["previous_window_days"],
                },
                depends_on=["T1"],
                desc="Generate hypotheses using summarized data (Think → Analyze → Conclude).",
            )
        )

        # 3. Metric evaluation (statistical checks)
        tasks.append(
            self._new_task(
                "T3",
                "metric_evaluation",
                "metric_evaluator",
                params={
                    "recent_window_days": self.config["recent_window_days"],
                    "previous_window_days": self.config["previous_window_days"],
                },
                depends_on=["T2"],
                desc=(
                    "Validate numeric hypotheses using tests (bootstrap/proportion tests), "
                    "compute metric_confidence."
                ),
            )
        )

        # 4. Creative evaluation (CHS)
        tasks.append(
            self._new_task(
                "T4",
                "creative_evaluation",
                "creative_evaluator",
                params={"chs_weights": {"behavior": 0.5, "text": 0.3, "fatigue": 0.2}},
                depends_on=["T1"],
                desc="Compute Creative Health Score (CHS) per campaign and component deltas.",
            )
        )

        # 5. Creative generation for flagged campaigns
        tasks.append(
            self._new_task(
                "T5",
                "creative_generation",
                "creative_generator",
                params={"variants_per_type": 3, "target": "low_ctr_or_low_chs"},
                depends_on=["T3", "T4"],
                desc=(
                    "Produce candidate creatives for low-CTR or low-CHS campaigns; "
                    "include meta tags for targeted CHS component."
                ),
            )
        )

        # 6. Final aggregation
        tasks.append(
            self._new_task(
                "T6",
                "final_aggregation",
                "orchestrator",
                params={"outputs": ["insights.json", "creatives.json", "report.md"]},
                depends_on=["T3", "T4", "T5"],
                desc="Aggregate evaluated hypotheses, compute final_confidences, create outputs and logs.",
            )
        )

        plan = {
            "query_info": info,
            "dataset_meta": dataset_meta or {},
            "campaign_filter": campaign_filter,
            "tasks": tasks,
            "plan_created_at": datetime.datetime.utcnow().isoformat() + "Z",
            "config": self.config,
        }
        return plan

    def reflect_and_retry(self, evaluation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Simple reflection logic:
        - If no hypothesis reaches reflection_confidence_thresh, propose a retry plan.
        - Return a dict containing retry flag and suggested new tasks.
        """
        conf_thresh = self.config.get("reflection_confidence_thresh", 0.4)
        validated_high = [h for h in evaluation_results if h.get("final_confidence", 0) >= conf_thresh]

        action: Dict[str, Any] = {"retry": False, "reason": None, "new_tasks": []}
        if len(validated_high) == 0 and len(evaluation_results) > 0:
            action["retry"] = True
            action["reason"] = (
                "No high-confidence hypotheses; suggest widening time windows "
                "and requesting segment-level analysis."
            )
            new_task = self._new_task(
                "T2b",
                "insight_generation",
                "insight_agent",
                params={
                    "intent": "wider_analysis",
                    "recent_window_days": self.config["recent_window_days"] * 2,
                },
                depends_on=["T1"],
                desc="Retry insight generation with wider window and deeper segmentation.",
            )
            action["new_tasks"].append(new_task)
        return action


def serialize_plan(plan: Dict[str, Any]) -> str:
    """Utility to pretty-print a plan as JSON."""
    return json.dumps(plan, indent=2)
