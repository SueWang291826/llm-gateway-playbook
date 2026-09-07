"""Tests for the pattern reference implementations.

No third-party dependencies: asyncio tests use plain ``asyncio.run`` inside
sync test functions, so plain pytest (no asyncio plugin) is enough.
"""

from __future__ import annotations

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from patterns.circuit_breaker import CircuitBreaker, OPEN, CLOSED, HALF_OPEN
from patterns.hedged_request import Attempt, HedgedError, hedged_call
from patterns.staged_budget import BudgetExceeded, StageClock, wave_budget


# --------------------------------------------------------------------------- helpers
class FakeClock:
    def __init__(self, start: float = 1_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def run(coro):
    return asyncio.run(coro)


# ----------------------------------------------------------------- circuit breaker
def test_breaker_opens_after_threshold(tmp_path):
    breaker = CircuitBreaker(str(tmp_path / "b.db"), threshold=3, cooldown=60.0)
    assert breaker.allow("model-a") is True
    assert breaker.record_failure("model-a") == CLOSED
    assert breaker.record_failure("model-a") == CLOSED
    assert breaker.record_failure("model-a") == OPEN
    assert breaker.allow("model-a") is False
    snap = breaker.snapshot("model-a")
    assert snap["state"] == OPEN and snap["fail_count"] == 3


def test_success_resets_consecutive_counter(tmp_path):
    breaker = CircuitBreaker(str(tmp_path / "b.db"), threshold=3, cooldown=60.0)
    breaker.record_failure("model-a")
    breaker.record_failure("model-a")
    breaker.record_success("model-a")
    assert breaker.record_failure("model-a") == CLOSED  # counter restarted
    assert breaker.allow("model-a") is True


def test_open_blocks_until_cooldown_then_single_probe_claim(tmp_path):
    clock = FakeClock()
    breaker = CircuitBreaker(str(tmp_path / "b.db"), threshold=2, cooldown=60.0, clock=clock)
    breaker.record_failure("model-a")
    breaker.record_failure("model-a")  # -> OPEN
    clock.advance(10.0)
    assert breaker.allow("model-a") is False          # still cooling down
    clock.advance(50.0)                               # 60s total
    assert breaker.allow("model-a") is True           # THIS process claims the probe
    assert breaker.snapshot("model-a")["state"] == HALF_OPEN
    assert breaker.allow("model-a") is False          # second caller: probe already in flight


def test_probe_success_closes(tmp_path):
    clock = FakeClock()
    breaker = CircuitBreaker(str(tmp_path / "b.db"), threshold=2, cooldown=60.0, clock=clock)
    breaker.record_failure("model-a")
    breaker.record_failure("model-a")
    clock.advance(60.0)
    assert breaker.allow("model-a") is True           # claim probe
    breaker.record_success("model-a")
    assert breaker.snapshot("model-a")["state"] == CLOSED
    assert breaker.allow("model-a") is True


def test_probe_failure_reopens_with_fresh_cooldown(tmp_path):
    clock = FakeClock()
    breaker = CircuitBreaker(str(tmp_path / "b.db"), threshold=2, cooldown=60.0, clock=clock)
    breaker.record_failure("model-a")
    breaker.record_failure("model-a")
    clock.advance(60.0)
    assert breaker.allow("model-a") is True           # claim probe
    assert breaker.record_failure("model-a") == OPEN  # probe failed -> re-open NOW
    assert breaker.allow("model-a") is False          # fresh cooldown applies
    clock.advance(60.0)
    assert breaker.allow("model-a") is True           # and can claim again


def test_unknown_name_is_healthy_and_snapshot_none(tmp_path):
    breaker = CircuitBreaker(str(tmp_path / "b.db"))
    assert breaker.allow("never-seen") is True
    assert breaker.snapshot("never-seen") is None


def test_breakers_are_independent_per_name(tmp_path):
    breaker = CircuitBreaker(str(tmp_path / "b.db"), threshold=1, cooldown=60.0)
    assert breaker.record_failure("model-a") == OPEN
    assert breaker.allow("model-b") is True


# ----------------------------------------------------------------- hedged requests
async def _sleep_then(value, seconds, exception=None):
    await asyncio.sleep(seconds)
    if exception is not None:
        raise exception
    return value


def test_preferred_fast_wins():
    async def inner():
        return await hedged_call(
            [("primary", lambda: _sleep_then("P", 0.05)),
             ("fallback", lambda: _sleep_then("F", 0.02))],
            budget=1.0, preferred="primary",
        )
    res = run(inner())
    assert res.used == "primary" and res.value == "P"
    # the fallback finished fine (even earlier) but was simply not chosen:
    # an unused-but-successful attempt reports "ok", which is the honest status
    assert res.attempts["fallback"].status == "ok"


def test_quality_first_within_budget():
    """The preferred call wins even when the fallback finishes earlier -- as
    long as the preferred makes it inside the budget."""
    async def inner():
        return await hedged_call(
            [("primary", lambda: _sleep_then("P", 0.15)),
             ("fallback", lambda: _sleep_then("F", 0.02))],
            budget=1.0, preferred="primary",
        )
    res = run(inner())
    assert res.used == "primary"
    assert res.elapsed >= 0.15
    assert res.attempts["fallback"].status == "ok"   # landed at 0.02, unused


def test_slow_fallback_is_cancelled_when_preferred_wins():
    async def inner():
        return await hedged_call(
            [("primary", lambda: _sleep_then("P", 0.05)),
             ("fallback", lambda: _sleep_then("F", 5.0))],
            budget=1.0, preferred="primary",
        )
    res = run(inner())
    assert res.used == "primary" and res.value == "P"
    assert res.attempts["fallback"].status == "cancelled"
    assert res.elapsed < 1.0                         # did not wait for the straggler


def test_fallback_rescues_slow_preferred():
    """The incident case: preferred lane queues past the budget, the fast
    fallback lane lands, total time stays within ~1x budget."""
    async def inner():
        return await hedged_call(
            [("primary", lambda: _sleep_then("P", 5.0)),
             ("fallback", lambda: _sleep_then("F", 0.05))],
            budget=0.3, preferred="primary",
        )
    res = run(inner())
    assert res.used == "fallback" and res.value == "F"
    assert res.elapsed < 0.6                       # ~budget, not 5s
    assert res.attempts["primary"].status == "cancelled"


def test_preferred_error_falls_back():
    async def inner():
        return await hedged_call(
            [("primary", lambda: _sleep_then(None, 0.02, exception=RuntimeError("boom"))),
             ("fallback", lambda: _sleep_then("F", 0.10))],
            budget=1.0, preferred="primary",
        )
    res = run(inner())
    assert res.used == "fallback"
    assert res.attempts["primary"].status == "error"
    assert isinstance(res.attempts["primary"].error, RuntimeError)


def test_all_fail_raises_with_attempt_details():
    async def inner():
        return await hedged_call(
            [("primary", lambda: _sleep_then(None, 0.02, exception=RuntimeError("a"))),
             ("fallback", lambda: _sleep_then(None, 0.02, exception=RuntimeError("b")))],
            budget=1.0,
        )
    try:
        run(inner())
        assert False, "expected HedgedError"
    except HedgedError as e:
        assert {a.status for a in e.attempts.values()} == {"error"}


def test_budget_expiry_with_nothing_landing_raises():
    async def inner():
        return await hedged_call(
            [("primary", lambda: _sleep_then("P", 5.0)),
             ("fallback", lambda: _sleep_then("F", 5.0))],
            budget=0.2,
        )
    try:
        run(inner())
        assert False, "expected HedgedError"
    except HedgedError as e:
        # both stragglers were cancelled when the budget expired
        assert set(a.status for a in e.attempts.values()) == {"cancelled"}


def test_single_call_is_enough():
    async def inner():
        return await hedged_call([("only", lambda: _sleep_then("OK", 0.01))], budget=1.0)
    res = run(inner())
    assert res.used == "only" and res.value == "OK"


def test_hedged_input_validation():
    async def inner_empty():
        await hedged_call([], budget=1.0)
    async def inner_dup():
        await hedged_call([("a", lambda: _sleep_then(1, 0.01)),
                           ("a", lambda: _sleep_then(2, 0.01))], budget=1.0)
    async def inner_unknown_preferred():
        await hedged_call([("a", lambda: _sleep_then(1, 0.01))], budget=1.0, preferred="zzz")
    for maker in (inner_empty, inner_dup, inner_unknown_preferred):
        try:
            run(maker())
            assert False, "expected ValueError"
        except ValueError:
            pass


# ----------------------------------------------------------------- staged budgets
def test_wave_budget_formula():
    assert wave_budget(24, 8, 90.0) == 300.0        # 3 waves -> 300 -> floor ties
    assert wave_budget(48, 8, 90.0) == 570.0        # 6 waves -> 570
    assert wave_budget(8, 8, 90.0) == 300.0         # 1 wave -> 120, floored at 300
    assert wave_budget(1, 8, 90.0) == 300.0
    assert wave_budget(9, 8, 90.0) == 300.0         # 2 waves -> 210, still floored
    assert wave_budget(0, 8, 90.0) == 0.0           # no work, no budget


def test_wave_budget_validation():
    try:
        wave_budget(10, 0, 90.0)
        assert False
    except ValueError:
        pass


def test_stage_clock_check_and_ensure_within():
    clock = FakeClock(start=0.0)
    sc = StageClock({"slice": 10.0, "map": 300.0}, clock=clock)
    sc.start("slice")
    clock.advance(5.0)
    sc.check("slice")                    # 5s of 10s: fine
    sc.ensure_within("slice", 4.0)       # 5s left, need 4: fine
    try:
        sc.ensure_within("slice", 6.0)   # need 6, only 5 left
        assert False
    except BudgetExceeded:
        pass
    clock.advance(6.0)                   # now 11s elapsed
    try:
        sc.check("slice")
        assert False
    except BudgetExceeded as e:
        assert e.stage == "slice" and e.elapsed == 11.0 and e.budget == 10.0
    # a stage that was never started reports zero elapsed and full remaining
    assert sc.elapsed("map") == 0.0 and sc.remaining("map") == 300.0


def test_stage_clock_snapshot():
    clock = FakeClock(start=0.0)
    sc = StageClock({"map": 300.0}, clock=clock)
    sc.start("map")
    clock.advance(120.0)
    snap = sc.snapshot()
    assert snap["map"]["elapsed"] == 120.0
    assert snap["map"]["remaining"] == 180.0
    assert snap["map"]["budget"] == 300.0
