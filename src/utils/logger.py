# src/utils/logger.py
import json
import os
import datetime
from typing import Any, Dict, Optional

LOGS_DIR = os.environ.get("KASPARRO_LOG_DIR", "logs")

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _now_iso() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

class AgentLogger:
    """
    Simple JSONL logger per agent, per run timestamp.
    Usage:
        lg = AgentLogger("DataAgent", run_id="20251201_120000")
        lg.info("load_start", {"path": "data.csv"})
    This writes lines to: logs/DataAgent_20251201_120000.jsonl
    """
    def __init__(self, agent_name: str, run_id: Optional[str] = None):
        _ensure_dir(LOGS_DIR)
        ts = run_id or datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_agent = agent_name.replace(" ", "_")
        filename = f"{safe_agent}_{ts}.jsonl"
        self.path = os.path.join(LOGS_DIR, filename)
        # warm file
        open(self.path, "a").close()

    def _emit(self, level: str, event: str, message: str, metadata: Optional[Dict[str, Any]] = None):
        entry = {
            "ts": _now_iso(),
            "level": level,
            "agent": os.path.basename(self.path).split("_")[0],
            "event": event,
            "message": message,
            "metadata": metadata or {}
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str, ensure_ascii=False) + "\n")

    def info(self, event: str, message: str, metadata: Optional[Dict[str, Any]] = None):
        self._emit("INFO", event, message, metadata)

    def warn(self, event: str, message: str, metadata: Optional[Dict[str, Any]] = None):
        self._emit("WARN", event, message, metadata)

    def error(self, event: str, message: str, metadata: Optional[Dict[str, Any]] = None):
        self._emit("ERROR", event, message, metadata)

    def debug(self, event: str, message: str, metadata: Optional[Dict[str, Any]] = None):
        self._emit("DEBUG", event, message, metadata)
