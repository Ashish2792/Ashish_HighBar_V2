# src/agents/__init__.py
# original file reference: :contentReference[oaicite:3]{index=3}

from .planner import PlannerAgent
from .data_agent import DataAgent
from .insight_agent import InsightAgent
from .metric_evaluator import MetricEvaluatorAgent
from .creative_evaluator import CreativeEvaluatorAgent

# keep original creative generator export; also expose V2 if present
from .creative_generator import CreativeGeneratorAgent
try:
    from .creative_generator_v2 import CreativeGeneratorV2
    __all_extra = ["CreativeGeneratorV2"]
except Exception:
    # creative_generator_v2 may be absent in some branches; ignore import error
    __all_extra = []

__all__ = [
    "PlannerAgent",
    "DataAgent",
    "InsightAgent",
    "MetricEvaluatorAgent",
    "CreativeEvaluatorAgent",
    "CreativeGeneratorAgent",
] + __all_extra
