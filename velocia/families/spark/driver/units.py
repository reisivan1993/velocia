"""Timing loop without Spark: tags, warm-up, per-unit timeout, statuses.

Spark is reached only through the callables passed in, so this module is tested without it.
"""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

TAG_FORMAT = "velocia:{run_id}:{unit_id}:{rep}"
WARMUP_REP = 0


@dataclass(frozen=True)
class Unit:
    id: str
    sql: str | None = None  # a SQL unit
    job: str | None = None  # or a job unit (jobs.JOBS)


def tag(run_id: str, unit_id: str, rep: int) -> str:
    return TAG_FORMAT.format(run_id=run_id, unit_id=unit_id, rep=rep)


def guarded(
    unit_tag: str, timeout_s: float, fn: Callable[[], Any], cancel: Callable[[str], None]
) -> tuple[str, str | None, Any]:
    """Run ``fn`` with a watchdog that cancels the tag's jobs after ``timeout_s``.

    Returns (status, error, value) with status ``ok``, ``failed`` or ``timeout``.
    """
    timed_out = threading.Event()

    def on_timeout() -> None:
        timed_out.set()
        cancel(unit_tag)

    timer = threading.Timer(timeout_s, on_timeout)
    timer.daemon = True
    timer.start()
    value: Any = None
    error: str | None = None
    try:
        value = fn()
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        timer.cancel()
    if timed_out.is_set():
        return "timeout", error, None
    return ("failed" if error else "ok"), error, value


def run_units(
    units: list[Unit],
    run_id: str,
    reps: list[int],
    warmup: bool,
    timeout_s: float,
    execute: Callable[[Unit, str], None],
    cancel: Callable[[str], None],
    on_result: Callable[[dict[str, Any]], None],
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Run every unit once per pass; passes are the warm-up (rep 0, discarded) then ``reps``.

    ``execute(unit, tag)`` runs the unit with every Spark job tagged; ``cancel(tag)`` cancels
    those jobs. A unit that times out or fails is reported and the loop moves on.
    """
    passes = ([WARMUP_REP] if warmup else []) + list(reps)
    for rep in passes:
        for unit in units:
            unit_tag = tag(run_id, unit.id, rep)
            started_at = time.time()
            start = clock()
            status, error, _ = guarded(
                unit_tag, timeout_s, partial(execute, unit, unit_tag), cancel
            )
            wall_ms = (clock() - start) * 1000.0
            on_result(
                {
                    "unit_id": unit.id,
                    "rep": rep,
                    "tag": unit_tag,
                    "discarded": rep == WARMUP_REP,
                    "status": status,
                    "wall_ms": round(wall_ms, 3) if status == "ok" else None,
                    "started_at": round(started_at, 3),
                    "error": error if status != "ok" else None,
                }
            )
