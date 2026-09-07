"""Staged deadlines: a hung stage may only burn its own budget, never the pipeline's.

Two pieces:

:func:`wave_budget` -- the adaptive budget formula for fan-out stages. When
``N`` items run with concurrency ``C`` and per-item timeout ``T``, the stage
needs about ``ceil(N / C)`` waves, so::

    stage_budget = max(floor, ceil(N / C) * T + setup_slack)

The floor keeps tiny fan-outs from being starved; the slack absorbs
scheduling jitter. Crucially the formula scales the stage's budget *with*
the amount of work, while a single hung item still costs at most one ``T``.

:class:`StageClock` -- per-stage elapsed/remaining tracking with an
``ensure_within`` pre-flight check ("is there still room for X more seconds
in this stage?") used to skip work instead of starting it and abandoning it
halfway.

Why this exists (the incident behind it): a fan-out pipeline reused the
global 600s execution timeout for every stage. One hung item held the
stage -- and a conversation lock -- for the full 600s; every subsequent
request hit a 409 lock timeout. Per-stage deadlines turned "one hung item
blocks everything" into "one hung item wastes exactly its own slot".
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

__all__ = ["wave_budget", "StageClock", "BudgetExceeded"]


def wave_budget(
    item_count: int,
    concurrency: int,
    per_item_timeout: float,
    *,
    floor: float = 300.0,
    setup_slack: float = 30.0,
) -> float:
    """Budget for a stage that runs ``item_count`` items at ``concurrency``.

    >>> wave_budget(24, 8, 90.0)
    300.0
    >>> wave_budget(48, 8, 90.0)
    570.0
    """
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")
    if per_item_timeout < 0:
        raise ValueError("per_item_timeout must be >= 0")
    if item_count <= 0:
        return 0.0
    waves = math.ceil(item_count / concurrency)
    return max(floor, waves * per_item_timeout + setup_slack)


class BudgetExceeded(RuntimeError):
    """A stage blew past its deadline."""

    def __init__(self, stage: str, elapsed: float, budget: float):
        self.stage = stage
        self.elapsed = elapsed
        self.budget = budget
        super().__init__(
            "stage %r exceeded its budget: %.1fs elapsed > %.1fs allowed"
            % (stage, elapsed, budget)
        )


@dataclass
class StageClock:
    """Track per-stage deadlines across one pipeline run.

    ``clock`` is injectable for tests; defaults to ``time.monotonic``
    (this object tracks a single in-process run, unlike a shared
    circuit breaker, so monotonic is the right clock here).
    """

    budgets: Dict[str, float]
    clock: Callable[[], float] = time.monotonic
    _starts: Dict[str, float] = field(default_factory=dict)

    def start(self, stage: str) -> None:
        if stage not in self.budgets:
            raise KeyError("no budget declared for stage %r" % stage)
        self._starts[stage] = self.clock()

    def elapsed(self, stage: str) -> float:
        if stage not in self._starts:
            return 0.0
        return self.clock() - self._starts[stage]

    def remaining(self, stage: str) -> float:
        return max(0.0, self.budgets[stage] - self.elapsed(stage))

    def check(self, stage: str) -> None:
        """Raise :class:`BudgetExceeded` if the stage is past its deadline."""
        elapsed = self.elapsed(stage)
        if elapsed > self.budgets[stage]:
            raise BudgetExceeded(stage, elapsed, self.budgets[stage])

    def ensure_within(self, stage: str, seconds_needed: float) -> None:
        """Pre-flight: can this stage still absorb ``seconds_needed`` more?

        Use this before starting a unit of work, so a stage that is already
        out of time *skips* the unit instead of starting it and abandoning
        it halfway (the unit's own timeout does the in-flight protection).
        """
        left = self.remaining(stage)
        if left < seconds_needed:
            raise BudgetExceeded(stage, self.elapsed(stage), self.budgets[stage])

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        """Observability: elapsed/remaining per started stage."""
        return {
            stage: {
                "elapsed": self.elapsed(stage),
                "remaining": self.remaining(stage),
                "budget": self.budgets[stage],
            }
            for stage in self._starts
        }
