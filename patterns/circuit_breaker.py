"""Cross-process circuit breaker backed by SQLite.

State machine per resource name (a model, an endpoint, a downstream ...):

    CLOSED    --(consecutive failures >= threshold)-->  OPEN
    OPEN      --(cooldown elapsed, ATOMIC claim)------>  HALF_OPEN
    HALF_OPEN --(probe succeeds)---------------------->  CLOSED
    HALF_OPEN --(probe fails)------------------------->  OPEN

What differs from a textbook single-process breaker, and why:

* State lives in SQLite (WAL) so N worker processes share ONE view.
  In-memory counters per process multiply the effective threshold by the
  worker count and desynchronize recovery.
* The OPEN -> HALF_OPEN transition is granted atomically by a conditional
  UPDATE (``WHERE state='open' AND opened_at <= now - cooldown``): when the
  cooldown expires, exactly ONE process gets to fire the probe.
* "Slow" is not "down": this module deliberately does NOT decide what counts
  as a failure. The caller does. In an LLM-gateway setting you usually want
  to record gateway-level errors (5xx, auth, connection refused) but NOT
  timeouts -- tripping a breaker on a busy-but-healthy model makes things
  worse.

Usage sketch::

    breaker = CircuitBreaker("breaker.db", threshold=3, cooldown=60.0)

    if not breaker.allow("model-a"):
        # open (or half-open probe owned by another process): fail fast /
        # fall back without paying for a doomed call
        ...
    try:
        result = call_model_a()
        breaker.record_success("model-a")
    except GatewayError:          # NOT timeout!
        state = breaker.record_failure("model-a")
        ...

Notes:
* Use a real file path. ``":memory:"`` gives every short-lived connection its
  own private database, which silently breaks sharing.
* Every method opens a short-lived connection; operations are cheap
  (microseconds) and safe across processes thanks to WAL + busy_timeout.
"""

from __future__ import annotations

import contextlib
import sqlite3
import time
from typing import Callable, Dict, Optional, Tuple

__all__ = ["CircuitBreaker", "CircuitOpen", "CLOSED", "OPEN", "HALF_OPEN"]

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS breaker (
    name        TEXT PRIMARY KEY,
    state       TEXT NOT NULL DEFAULT 'closed',
    fail_count  INTEGER NOT NULL DEFAULT 0,
    opened_at   REAL,
    updated_at  REAL
);
"""


class CircuitOpen(RuntimeError):
    """Raised by :meth:`CircuitBreaker.guard` when the breaker is open."""


class CircuitBreaker:
    def __init__(
        self,
        path: str,
        *,
        threshold: int = 3,
        cooldown: float = 60.0,
        clock: Callable[[], float] = time.time,
        busy_timeout_ms: int = 3000,
    ) -> None:
        # wall clock, not time.monotonic(): the state is shared across
        # processes and each process' monotonic origin differs.
        self.path = path
        self.threshold = max(1, int(threshold))
        self.cooldown = float(cooldown)
        self._clock = clock
        self._busy_timeout_ms = busy_timeout_ms
        with self._connect() as conn:
            conn.execute(_SCHEMA)

    # ------------------------------------------------------------------ plumbing
    @contextlib.contextmanager
    def _connect(self):
        conn = sqlite3.connect(
            self.path, timeout=self._busy_timeout_ms / 1000.0, isolation_level=None
        )
        try:
            conn.execute("PRAGMA busy_timeout=%d" % self._busy_timeout_ms)
            conn.execute("PRAGMA journal_mode=WAL")
            yield conn
        finally:
            conn.close()

    def _row(self, conn: sqlite3.Connection, name: str) -> Optional[Tuple[str, int, Optional[float]]]:
        cur = conn.execute(
            "SELECT state, fail_count, opened_at FROM breaker WHERE name=?", (name,)
        )
        return cur.fetchone()

    # ------------------------------------------------------------------- public
    def allow(self, name: str) -> bool:
        """True if a call to ``name`` may proceed.

        When the breaker is OPEN and the cooldown has elapsed, this call
        atomically claims the single half-open probe slot: the first caller
        gets True, everyone else keeps getting False until the probe settles.
        """
        now = self._clock()
        with self._connect() as conn:
            row = self._row(conn, name)
            if row is None:
                return True  # never seen: assume healthy
            state, _fail, opened_at = row
            if state == CLOSED:
                return True
            if state == OPEN:
                if opened_at is not None and now - opened_at >= self.cooldown:
                    cur = conn.execute(
                        "UPDATE breaker SET state=?, updated_at=? "
                        "WHERE name=? AND state=? AND opened_at<=?",
                        (HALF_OPEN, now, name, OPEN, now - self.cooldown),
                    )
                    return cur.rowcount == 1  # exactly one process claims the probe
                return False
            return False  # HALF_OPEN: a probe is in flight somewhere else

    def record_success(self, name: str) -> None:
        """Record a successful call: close the breaker, reset the counter."""
        now = self._clock()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO breaker(name,state,fail_count,opened_at,updated_at) "
                "VALUES(?,?,0,NULL,?) "
                "ON CONFLICT(name) DO UPDATE SET state=?, fail_count=0, opened_at=NULL, updated_at=?",
                (name, CLOSED, now, CLOSED, now),
            )

    def record_failure(self, name: str) -> str:
        """Record a failed call; returns the resulting state.

        A failure while HALF_OPEN immediately re-opens the breaker (the probe
        was the cooldown's one chance). A failure while CLOSED just increments
        the consecutive counter, opening the breaker at ``threshold``.
        """
        now = self._clock()
        with self._connect() as conn:
            row = self._row(conn, name)
            if row is None:
                fail = 1
                new_state = OPEN if fail >= self.threshold else CLOSED
                conn.execute(
                    "INSERT INTO breaker(name,state,fail_count,opened_at,updated_at) VALUES(?,?,?,?,?)",
                    (name, new_state, fail, now if new_state == OPEN else None, now),
                )
                return new_state
            state, fail, opened_at = row
            fail += 1
            if state == HALF_OPEN:
                new_state = OPEN
                opened_at = now
            elif fail >= self.threshold:
                new_state = OPEN
                opened_at = now
            else:
                new_state = CLOSED
            conn.execute(
                "UPDATE breaker SET state=?, fail_count=?, opened_at=?, updated_at=? WHERE name=?",
                (new_state, fail, opened_at, now, name),
            )
            return new_state

    def snapshot(self, name: str) -> Optional[Dict]:
        """Observability: current persisted state, or None if never seen."""
        with self._connect() as conn:
            row = self._row(conn, name)
        if row is None:
            return None
        state, fail, opened_at = row
        return {"state": state, "fail_count": fail, "opened_at": opened_at}
