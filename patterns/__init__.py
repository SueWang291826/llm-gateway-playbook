"""Reference implementations distilled from a production LLM gateway.

Each module is dependency-free (standard library only) and can be copied
into a project as a single file:

* :mod:`patterns.circuit_breaker` -- cross-process SQLite circuit breaker
  (closed -> open -> half-open, atomic probe claim).
* :mod:`patterns.hedged_request` -- asyncio hedged calls: preferred-first
  within a budget, fallback guarantees an answer.
* :mod:`patterns.staged_budget` -- per-stage deadlines plus the adaptive
  "wave budget" formula for fan-out stages.
"""

from .circuit_breaker import CircuitBreaker, CircuitOpen
from .hedged_request import HedgedError, HedgedResult, hedged_call
from .staged_budget import BudgetExceeded, StageClock, wave_budget

__all__ = [
    "CircuitBreaker",
    "CircuitOpen",
    "HedgedError",
    "HedgedResult",
    "hedged_call",
    "BudgetExceeded",
    "StageClock",
    "wave_budget",
]
