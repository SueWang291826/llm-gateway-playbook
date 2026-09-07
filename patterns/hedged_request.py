"""Asyncio hedged requests: preferred-first within a budget, fallback guarantees an answer.

This is a local adaptation of hedged requests (Google, "The Tail at Scale",
CACM 2013; also gRPC hedging). Pure tail-latency hedging keeps whichever
reply arrives first. Here the semantics are deliberately different::

    launch preferred + fallbacks simultaneously
      |
      |-- preferred succeeds before the budget expires
      |       -> use PREFERRED (quality first: the strong-but-queued model
      |          must win whenever it can make it in time)
      |
      |-- preferred fails, or the budget expires with it still running
              -> use the first FALLBACK that succeeded, waiting until the
                 budget expires if necessary (stability: the fallback lane,
                 which does not queue, is near-guaranteed to land)
              -> if nothing succeeded, raise HedgedError with per-attempt details.

Wall time never exceeds ``budget`` (plus a small cleanup delay while pending
calls are cancelled). Compared to the serial alternative -- run preferred
until its timeout, then re-run the fallback -- you get the same worst case
with none of the wasted first leg.

Usage sketch::

    result = await hedged_call(
        [
            ("primary", lambda: call_model(PREFERRED)),   # strong, may queue
            ("fallback", lambda: call_model(FALLBACK)),   # fast lane
        ],
        budget=150.0,
        preferred="primary",
    )
    print(result.used, result.value, result.attempts["primary"].status)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

__all__ = ["hedged_call", "HedgedResult", "HedgedError", "Attempt"]

Factory = Callable[[], Awaitable[Any]]

# attempt.status values
PENDING = "pending"
OK = "ok"
ERROR = "error"
CANCELLED = "cancelled"


@dataclass
class Attempt:
    """Outcome of one hedged call, for logging / tracing / tests."""

    name: str
    status: str = PENDING
    elapsed: Optional[float] = None       # completion time - start, when known
    error: Optional[BaseException] = None  # set when status == ERROR
    finished_at: Optional[float] = None    # internal: completion timestamp


@dataclass
class HedgedResult:
    used: str
    value: Any
    elapsed: float
    attempts: Dict[str, Attempt] = field(default_factory=dict)


class HedgedError(RuntimeError):
    """All hedged calls failed or did not finish within the budget."""

    def __init__(self, attempts: Dict[str, Attempt]):
        self.attempts = attempts
        detail = ", ".join(
            "%s=%s" % (a.name, a.status) for a in attempts.values()
        )
        super().__init__("all hedged calls failed or timed out (%s)" % detail)


def _finalize(attempts: Dict[str, Attempt], tasks: Dict[str, "asyncio.Task"],
              started_at: float, now: float) -> None:
    """Fill in final statuses for every attempt; safe to call more than once."""
    for name, task in tasks.items():
        att = attempts[name]
        if att.status != PENDING:
            continue
        if task.cancelled():
            att.status = CANCELLED
        elif task.done():
            exc = task.exception()  # also marks the exception as retrieved
            if exc is None:
                att.status = OK
            else:
                att.status = ERROR
                att.error = exc
        else:
            att.status = PENDING
        finished = att.finished_at if att.finished_at is not None else now
        att.elapsed = finished - started_at


async def hedged_call(
    calls: Sequence[Tuple[str, Factory]],
    *,
    budget: float,
    preferred: Optional[str] = None,
) -> HedgedResult:
    """Run several async calls concurrently under one shared budget.

    :param calls: ``(name, factory)`` pairs; factories are zero-argument
        coroutines functions, started together.
    :param budget: seconds. The preferred call gets the whole budget; the
        total wall time never exceeds it.
    :param preferred: which name wins if it succeeds in time (default: the
        first call). Other calls are tie-broken by completion order.
    :raises HedgedError: if no call succeeds within the budget.
    """
    if not calls:
        raise ValueError("hedged_call needs at least one call")
    if budget <= 0:
        raise ValueError("budget must be positive")
    names = [name for name, _ in calls]
    if len(set(names)) != len(names):
        raise ValueError("call names must be unique")
    if preferred is None:
        preferred = names[0]
    if preferred not in names:
        raise ValueError("preferred=%r is not among the calls" % preferred)

    loop = asyncio.get_running_loop()
    started_at = loop.time()
    deadline = started_at + budget

    attempts = {name: Attempt(name) for name in names}
    tasks: Dict[str, "asyncio.Task"] = {}
    task_to_name: Dict["asyncio.Task", str] = {}

    for name, factory in calls:
        task = asyncio.ensure_future(factory())
        tasks[name] = task
        task_to_name[task] = name

        def _mark_done(t: "asyncio.Task", _name: str = name) -> None:
            attempts[_name].finished_at = loop.time()

        task.add_done_callback(_mark_done)

    def _result(used: str, task: "asyncio.Task") -> HedgedResult:
        _finalize(attempts, tasks, started_at, loop.time())
        return HedgedResult(used=used, value=task.result(),
                            elapsed=loop.time() - started_at, attempts=attempts)

    try:
        # ---- Phase 1: give the preferred call the full budget. ------------
        preferred_task = tasks[preferred]
        await asyncio.wait({preferred_task}, timeout=budget)
        if preferred_task.done() and not preferred_task.cancelled() \
                and preferred_task.exception() is None:
            return _result(preferred, preferred_task)

        # ---- Phase 2: preferred failed or ran out of clock. ---------------
        # Harvest an already-successful fallback first (the common case when
        # the fallback lane is fast), otherwise wait for one until deadline.
        others = [(name, tasks[name]) for name in names if name != preferred]
        while True:
            winners = [
                (name, task) for name, task in others
                if task.done() and not task.cancelled() and task.exception() is None
            ]
            if winners:
                # tie-break by completion time, then by declaration order
                winners.sort(key=lambda pair: (
                    attempts[pair[0]].finished_at
                    if attempts[pair[0]].finished_at is not None else loop.time(),
                    names.index(pair[0]),
                ))
                name, task = winners[0]
                return _result(name, task)

            pending = [task for _, task in others if not task.done()]
            remaining = deadline - loop.time()
            if not pending or remaining <= 0:
                break
            await asyncio.wait(set(pending), timeout=remaining,
                               return_when=asyncio.FIRST_COMPLETED)

        _finalize(attempts, tasks, started_at, loop.time())
        raise HedgedError(attempts)
    finally:
        # Cancel whatever is still running (success return, error raise, or
        # cancellation from outside) and reap it so nothing warns/leaks.
        stragglers = [t for t in tasks.values() if not t.done()]
        for task in stragglers:
            task.cancel()
        if stragglers:
            await asyncio.gather(*stragglers, return_exceptions=True)
        # Reap done: refresh statuses so cancelled stragglers are reported as
        # CANCELLED rather than PENDING (the attempts dict is shared with any
        # already-returned HedgedResult, which is intentional -- callers see
        # the final truth).
        _finalize(attempts, tasks, started_at, loop.time())
