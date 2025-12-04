class PlannerAgent:
    """
    PlannerAgent

    Role:
        Turn a high-level user query (e.g. "Analyze ROAS drop") into
        a concrete multi-step plan (T1–T6) for the rest of the agents.

    Inputs:
        - query: raw user question as a string.
        - config: window sizes, thresholds, retry behaviour.

    Outputs:
        - plan dict with:
            - query_info: interpreted intent + metadata
            - tasks: ordered list of task dicts (id, type, agent, params, depends_on)
            - dataset_meta: later filled in by DataAgent
            - config: thresholds used when planning

    Assumptions:
        - Dates/metrics are handled by downstream agents; Planner only
          reasons on intent and which tasks to schedule.
        - Uses simple keyword-based intent detection, so behaviour is
          deterministic and easy to debug.
    """


from typing import Optional, Dict, Any, List
import datetime
import json
import traceback

from src.utils.logger import AgentLogger
from src.utils.errors import wrap_exc

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
    def __init__(self, config: Optional[Dict[str, Any]] = None, run_id: Optional[str] = None):
        """
        PlannerAgent
        - config: planner-related thresholds and window sizes.
        - run_id: optional string to correlate logs with a particular run.
        """
        self.config = DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)
        self.run_id = run_id
        self.logger = AgentLogger("PlannerAgent", run_id=self.run_id)
        self.logger.debug("init", "PlannerAgent initialized", {"config": self.config})

    # ------------------------------------------------------------------
    # Query interpretation / adaptability
    # ------------------------------------------------------------------
    def interpret_query(self, query: str) -> Dict[str, Any]:
        """
        Classifies query intent and returns a small metadata dict.

        Goals for P1:
        - Be more adaptable to different phrasings.
        - Capture not just a single intent, but also:
            * metrics_focus: ["roas", "ctr", ...]
            * include_creative_analysis (bool)
            * include_audience_analysis (bool)
            * include_spend_analysis (bool)
        - Attach a simple intent_confidence + notes for observability.

        Deterministic, keyword-based on purpose.
        """
        q = (query or "").lower().strip()

        # --- Metric focus detection ---
        metrics_focus: List[str] = []
        if any(k in q for k in ["roas", "return on ad", "revenue", "sales"]):
            metrics_focus.append("roas")
        if any(k in q for k in ["ctr", "click-through", "click through", "clicks"]):
            metrics_focus.append("ctr")
        if any(k in q for k in ["cpa", "cac", "cost per", "cost-per"]):
            metrics_focus.append("cpa")
        if any(k in q for k in ["spend", "budget", "scaling", "scale"]):
            metrics_focus.append("spend")

        # default: if nothing explicit, we look at both ROAS and CTR
        if not metrics_focus:
            metrics_focus = ["roas", "ctr"]

        # --- Analysis focus flags ---
        include_creative = any(
            k in q
            for k in [
                "creative",
                "ad copy",
                "copy",
                "headline",
                "image",
                "video",
                "thumbnail",
                "ad text",
            ]
        )
        include_audience = any(
            k in q
            for k in [
                "audience",
                "targeting",
                "age",
                "gender",
                "location",
                "interest",
                "segment",
            ]
        )
        include_spend = any(
            k in q
            for k in [
                "budget",
                "spend",
                "scaling",
                "scale",
                "bid",
            ]
        )

        # --- High-level intent classification ---
        if "why" in q or "diagnose" in q or "what is happening" in q:
            intent = "general_diagnosis"
            intent_conf = 0.8
        elif any(k in q for k in ["roas", "return on ad", "revenue", "sales"]):
            intent = "analyze_roas"
            intent_conf = 0.9
        elif any(k in q for k in ["ctr", "click-through", "click through", "clicks"]):
            intent = "analyze_ctr"
            intent_conf = 0.9
        elif include_creative:
            intent = "creative_optimize"
            intent_conf = 0.8
        else:
            # very generic query
            intent = "general_diagnosis"
            intent_conf = 0.6

        # --- Initial scope guess (overall vs campaign) ---
        if "account" in q or "overall" in q:
            initial_scope = "account"
        elif "campaign" in q or "per campaign" in q:
            initial_scope = "campaign"
        else:
            initial_scope = "account_and_campaign"

        notes = []
        if "sample" in q:
            notes.append("User mentioned sampling; data agent may downsample.")
        if "no creatives" in q or "skip creatives" in q:
            notes.append("User suggested creatives are not a focus; creative generation can be de-prioritized.")

        info = {
            "raw_query": query,
            "intent": intent,
            "intent_confidence": intent_conf,
            "metrics_focus": metrics_focus,
            "include_creative_analysis": include_creative or True,  # default: keep creatives on
            "include_audience_analysis": include_audience,
            "include_spend_analysis": include_spend,
            "initial_scope": initial_scope,
            "notes": notes,
            "created_at": datetime.datetime.utcnow().isoformat() + "Z",
        }
        self.logger.info("interpret_query", "Interpreted query", info)
        return info

    # ------------------------------------------------------------------
    # Task helpers / plan construction
    # ------------------------------------------------------------------
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

        Wrapped with logging + error handling for robustness.
        """
        self.logger.info("start", "Planner.generate_plan starting", {"query": query, "campaign_filter": campaign_filter})
        try:
            info = self.interpret_query(query)
            intent = info["intent"]
            initial_scope = info.get("initial_scope", "account_and_campaign")

            tasks: List[Dict[str, Any]] = []

            # 1. Data load + summary
            tasks.append(
                self._new_task(
                    "T1",
                    "data_load_summary",
                    "data_agent",
                    params={
                        "sample": "auto",
                    },
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
                        "initial_scope": initial_scope,
                        "metrics_focus": info.get("metrics_focus", ["roas", "ctr"]),
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
                    params={
                        "chs_weights": {"behavior": 0.5, "text": 0.3, "fatigue": 0.2},
                        "requested_by_query": info.get("include_creative_analysis", True),
                    },
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
                    params={
                        "variants_per_type": 3,
                        "target": "low_ctr_or_low_chs",
                        "metrics_focus": info.get("metrics_focus", ["roas", "ctr"]),
                    },
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
            self.logger.info("success", "Planner.generate_plan completed", {"n_tasks": len(tasks)})
            return plan
        except Exception as e:
            self.logger.error("exception", "Planner.generate_plan failed", {"trace": traceback.format_exc()})
            raise wrap_exc("PlannerAgent failed to generate plan", e)

    # ------------------------------------------------------------------
    # Reflection / retry logic
    # ------------------------------------------------------------------
    def reflect_and_retry(self, evaluation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Simple reflection logic:
        - If no hypothesis reaches reflection_confidence_thresh, propose a retry plan.
        - Return a dict containing retry flag and suggested new tasks.
        """
        self.logger.info("start", "Planner.reflect_and_retry starting", {"n_results": len(evaluation_results)})
        try:
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
            self.logger.info("success", "Planner.reflect_and_retry completed", {"action": action})
            return action
        except Exception as e:
            self.logger.error("exception", "Planner.reflect_and_retry failed", {"trace": traceback.format_exc()})
            raise wrap_exc("PlannerAgent reflect_and_retry failed", e)


def serialize_plan(plan: Dict[str, Any]) -> str:
    """Utility to pretty-print a plan as JSON."""
    return json.dumps(plan, indent=2)
