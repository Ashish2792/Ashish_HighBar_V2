# src/utils/errors.py
from typing import Optional

class AgentError(Exception):
    """Base for all agent-related errors."""
    def __init__(self, message: str, *, original: Optional[Exception] = None):
        super().__init__(message)
        self.original = original

class DataAgentError(AgentError):
    pass

class InsightAgentError(AgentError):
    pass

class MetricEvaluatorError(AgentError):
    pass

class CreativeEvaluatorError(AgentError):
    pass

class CreativeGeneratorError(AgentError):
    pass

# helper builder
def wrap_exc(msg: str, exc: Exception, exc_type=AgentError) -> AgentError:
    # return a typed exception while attaching original
    return exc_type(msg, original=exc)
