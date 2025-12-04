#!/usr/bin/env python
"""
run.py
Orchestrator CLI for the Agentic Facebook Performance Analyst.

Pipeline:
 1) Load config
 2) PlannerAgent -> generate task plan
 3) DataAgent -> data_summary
 4) InsightAgent -> hypotheses
 5) MetricEvaluatorAgent -> metric_confidence
 6) CreativeEvaluatorAgent (CHS) -> chs_summary + creative_confidence
 7) CreativeGeneratorAgent -> creatives
 8) Aggregator -> insights.json, creatives.json, report.md, logs

This V2 changes:
- Generate a single run_id and pass it to all agents so logs correlate.
- Prefer CreativeGeneratorV2 if available.
- Default outdir changed to 'reports/' to match submission spec.
- Run-level log file includes run_id.
"""

import argparse
import json
from pathlib import Path
import datetime
import importlib
import sys
import os

import yaml

from src.agents import (
    PlannerAgent,
    DataAgent,
    InsightAgent,
    MetricEvaluatorAgent,
    CreativeEvaluatorAgent,
    CreativeGeneratorAgent,
)
from src.orchestrator.aggregator import Aggregator


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def build_agent_configs(cfg: dict) -> dict:
    """
    Split the global config.yaml into per-agent configs.
    """
    data_cfg = cfg.get("data", {})
    analysis_cfg = cfg.get("analysis", {})
    evaluator_cfg = cfg.get("evaluator", {})
    planner_cfg = cfg.get("planner", {})
    chs_cfg = cfg.get("chs", {})

    # Planner expects thresholds & windows
    planner_agent_cfg = {
        "recent_window_days": analysis_cfg.get("recent_window_days", 14),
        "previous_window_days": analysis_cfg.get("previous_window_days", 14),
        "roas_drop_threshold_pct": analysis_cfg.get("roas_drop_threshold_pct", -20),
        "low_ctr_threshold": analysis_cfg.get("low_ctr_threshold", 0.02),
        "min_impressions_for_stats": analysis_cfg.get("min_impressions_for_stats", 1000),
        "max_retries": planner_cfg.get("max_retries", 2),
        "reflection_confidence_thresh": planner_cfg.get("reflection_confidence_thresh", 0.4),
    }

    insight_cfg = {
        "recent_window_days": analysis_cfg.get("recent_window_days", 14),
        "previous_window_days": analysis_cfg.get("previous_window_days", 14),
        "roas_drop_threshold_pct": analysis_cfg.get("roas_drop_threshold_pct", -20),
        "low_ctr_threshold": analysis_cfg.get("low_ctr_threshold", 0.02),
        "min_impressions_for_stats": analysis_cfg.get("min_impressions_for_stats", 1000),
    }

    metric_eval_cfg = {
        "recent_window_days": analysis_cfg.get("recent_window_days", 14),
        "previous_window_days": analysis_cfg.get("previous_window_days", 14),
        "p_value_threshold": evaluator_cfg.get("p_value_threshold", 0.05),
        "bootstrap_iters": evaluator_cfg.get("bootstrap_iters", 2000),
        "min_impressions_for_stats": analysis_cfg.get("min_impressions_for_stats", 1000),
    }

    creative_eval_cfg = {
        "recent_window_days": analysis_cfg.get("recent_window_days", 14),
        "previous_window_days": analysis_cfg.get("previous_window_days", 14),
        "behavior_weight": chs_cfg.get("behavior_weight", 0.5),
        "text_weight": chs_cfg.get("text_weight", 0.3),
        "fatigue_weight": chs_cfg.get("fatigue_weight", 0.2),
        "min_impressions_for_stats": analysis_cfg.get("min_impressions_for_stats", 1000),
    }

    creative_gen_cfg = {
        "variants_per_type": 3,
        "low_ctr_threshold": analysis_cfg.get("low_ctr_threshold", 0.02),
        "chs_threshold": 60.0,
        "max_campaigns": 10,
    }

    return {
        "data": data_cfg,
        "planner": planner_agent_cfg,
        "insight": insight_cfg,
        "metric": metric_eval_cfg,
        "creative_eval": creative_eval_cfg,
        "creative_gen": creative_gen_cfg,
        "logging": cfg.get("logging", {}),
    }


def _select_creative_generator_class():
    """
    Prefer CreativeGeneratorV2 if available in src.agents; otherwise use CreativeGeneratorAgent.
    """
    try:
        # Try to import CreativeGeneratorV2 from src.agents (it's exported by __init__ if present)
        mod = importlib.import_module("src.agents")
        cls = getattr(mod, "CreativeGeneratorV2", None)
        if cls:
            return cls
    except Exception:
        pass
    # fallback
    return CreativeGeneratorAgent


def execute_plan(
    query: str,
    cfg: dict,
    data_path: str,
    outdir: str,
) -> None:
    """
    Execute the full pipeline using the existing agents.
    """
    agent_cfgs = build_agent_configs(cfg)
    outdir_path = Path(outdir)
    outdir_path.mkdir(parents=True, exist_ok=True)

    # create a run id to correlate logs across agents
    run_id = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    # allow logging dir override via env var or config
    logs_dir = cfg.get("logging", {}).get("outdir", os.environ.get("KASPARRO_LOG_DIR", "logs"))
    os.environ["KASPARRO_LOG_DIR"] = logs_dir  # let AgentLogger pick this up if it reads env
    Path(logs_dir).mkdir(parents=True, exist_ok=True)

    # Instantiate agents with run_id to allow per-agent log files to be correlated
    planner = PlannerAgent(config=agent_cfgs["planner"], run_id=run_id)
    data_agent = DataAgent(config={
        "sample_mode": cfg["data"].get("sample_mode", True),
        "sample_frac": cfg["data"].get("sample_frac", 0.5),
        "date_col": cfg["data"].get("date_col", "date")
    }, run_id=run_id)
    insight_agent = InsightAgent(config=agent_cfgs["insight"], run_id=run_id)
    metric_evaluator = MetricEvaluatorAgent(config=agent_cfgs["metric"], run_id=run_id)
    creative_evaluator = CreativeEvaluatorAgent(config=agent_cfgs["creative_eval"], run_id=run_id)

    # pick creative generator implementation
    CreativeGenCls = _select_creative_generator_class()
    creative_generator = CreativeGenCls(config=agent_cfgs["creative_gen"], run_id=run_id)

    aggregator = Aggregator()

    # 1) Planner: generate plan
    plan = planner.generate_plan(query, dataset_meta={}, campaign_filter=None)

    context = {
        "plan": plan,
        "data_summary": None,
        "hypotheses": None,
        "metric_eval": None,
        "creative_eval": None,
        "creative_output": None,
    }

    # 2) Execute tasks in the order given by the plan
    for task in plan["tasks"]:
        ttype = task["type"]
        params = task.get("params", {}) or {}

        if ttype == "data_load_summary":
            # DataAgent
            ds = data_agent.run_data_load_summary(data_path, sample=params.get("sample", "auto"))
            context["data_summary"] = ds
            # Update dataset_meta in plan from data_summary.meta
            plan["dataset_meta"] = ds.get("meta", {})

        elif ttype == "insight_generation":
            if context["data_summary"] is None:
                raise RuntimeError("data_summary missing before insight_generation")

            intent = plan["query_info"]["intent"]
            res = insight_agent.run_insight_generation(
                data_summary=context["data_summary"],
                intent=intent,
                params=params,
                campaign_filter=plan.get("campaign_filter"),
            )
            context["hypotheses"] = res["hypotheses"]

        elif ttype == "metric_evaluation":
            if context["data_summary"] is None or context["hypotheses"] is None:
                raise RuntimeError("Missing data_summary or hypotheses before metric_evaluation")
            res = metric_evaluator.run_metric_evaluation(
                hypotheses=context["hypotheses"],
                data_summary=context["data_summary"],
                params=params,
            )
            context["metric_eval"] = res
            # Feed updated hypotheses forward
            context["hypotheses"] = res["evaluated_hypotheses"]

        elif ttype == "creative_evaluation":
            if context["data_summary"] is None or context["hypotheses"] is None:
                raise RuntimeError("Missing data_summary or hypotheses before creative_evaluation")
            res = creative_evaluator.run_creative_evaluation(
                hypotheses=context["hypotheses"],
                data_summary=context["data_summary"],
                params=params,
            )
            context["creative_eval"] = res
            context["hypotheses"] = res["evaluated_hypotheses"]

        elif ttype == "creative_generation":
            if context["data_summary"] is None or context["creative_eval"] is None:
                raise RuntimeError("Missing data_summary or creative_eval before creative_generation")
            chs_summary = context["creative_eval"]["chs_summary"]
            res = creative_generator.run_creative_generation(
                data_summary=context["data_summary"],
                chs_summary=chs_summary,
                hypotheses=context["hypotheses"],
                params=params,
            )
            context["creative_output"] = res

        elif ttype == "final_aggregation":
            # Will be handled after loop by Aggregator
            continue

        else:
            # Unknown task type; skip or log
            continue

    # Planner reflection (just compute, we don't rerun for now)
    reflection = planner.reflect_and_retry(context["hypotheses"] or [])

    # 3) Final aggregation: insights.json, creatives.json, report.md, logs
    agg_result = aggregator.aggregate_and_write(
        plan=plan,
        data_summary=context["data_summary"],
        hypotheses=context["hypotheses"] or [],
        creative_output=context["creative_output"] or {"creatives": []},
        outdir=outdir_path,
    )

    # 4) Write simple run-level JSON log (includes run_id)
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_log_dir = Path(cfg.get("logging", {}).get("outdir", os.environ.get("KASPARRO_LOG_DIR", "logs")))
    run_log_dir.mkdir(parents=True, exist_ok=True)
    run_log_path = run_log_dir / f"run_{run_id}.json"

    log_payload = {
        "run_id": run_id,
        "timestamp": ts,
        "query": query,
        "plan": plan,
        "reflection": reflection,
        "outputs": agg_result,
    }
    with open(run_log_path, "w") as f:
        json.dump(log_payload, f, indent=2)

    print(f"Run complete. run_id={run_id}")
    print(f"  Insights:  {agg_result['insights_path']}")
    print(f"  Creatives: {agg_result['creatives_path']}")
    print(f"  Report:    {agg_result['report_path']}")
    print(f"  Run log:   {run_log_path}")


def main():
    parser = argparse.ArgumentParser(description="Agentic FB Performance Analyst")
    parser.add_argument(
        "query",
        nargs="?",
        default="Analyze ROAS drop",
        help="High-level question, e.g. 'Analyze ROAS drop' or 'Diagnose low CTR'.",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to YAML config file.",
    )
    parser.add_argument(
        "--data-path",
        default=None,
        help="Override dataset path from config.",
    )
    parser.add_argument(
        "--outdir",
        default="reports/",
        help="Directory to write insights.json, creatives.json, and report.md.",
    )

    args = parser.parse_args()

    cfg = load_config(args.config)
    data_path = args.data_path or cfg["data"]["path"]

    execute_plan(
        query=args.query,
        cfg=cfg,
        data_path=data_path,
        outdir=args.outdir,
    )


if __name__ == "__main__":
    main()
