"""Bring-your-own Python jobs (docs/03-spec.md §1.2).

The experiment names a Python file or zip (``artifact``), a function to run (``entrypoint``,
``module:function`` or ``function``) and optionally a validator. The function is called as
``fn(spark, tables, scratch_dir)``.

Inside the function, ``with velocia.unit("name"):`` tags and times one unit, like Velocia's
own jobs. A function that never uses the helper is one opaque unit, timed around the call.
``import velocia`` resolves to this helper only inside a Velocia run (the engine image has no
Velocia package); outside one, ``velocia.unit`` does nothing, so the same job runs anywhere.
"""

import importlib
import os
import sys
import time
import types
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from .units import WARMUP_REP, guarded, tag

OPAQUE_UNIT = "app"


class _Run:
    def __init__(self, run_id: str, rep: int, sc: Any) -> None:
        self.run_id, self.rep, self.sc = run_id, rep, sc
        self.results: list[dict[str, Any]] = []


_current: _Run | None = None


@contextmanager
def unit(name: str) -> Iterator[None]:
    """Tag and time one unit of a bring-your-own job (no-op outside a Velocia run)."""
    run = _current
    if run is None:
        yield
        return
    unit_tag = tag(run.run_id, name, run.rep)
    outer = run.sc.getLocalProperty("spark.jobGroup.id")
    run.sc.setJobGroup(unit_tag, unit_tag, interruptOnCancel=True)
    started_at = time.time()
    start = time.monotonic()
    status, error = "ok", None
    try:
        yield
    except Exception as exc:
        status, error = "failed", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        run.results.append(
            {
                "unit_id": name,
                "rep": run.rep,
                "tag": unit_tag,
                "discarded": run.rep == WARMUP_REP,
                "status": status,
                "wall_ms": round((time.monotonic() - start) * 1000, 3),
                "started_at": started_at,
                "error": error,
            }
        )
        if outer:
            run.sc.setJobGroup(outer, outer, interruptOnCancel=True)


def install_helper() -> None:
    """Make ``import velocia`` / ``velocia.unit`` work for user code in this process."""
    if "velocia" not in sys.modules:
        module = types.ModuleType("velocia", "Velocia for Apache Spark job helper")
        module.unit = unit  # type: ignore[attr-defined]
        sys.modules["velocia"] = module


def load(path: str, target: str) -> Callable[..., Any]:
    """``module:function`` (or ``function`` from the artifact's own module) from ``path``."""
    folder = path if path.endswith(".zip") else os.path.dirname(path)
    if folder not in sys.path:
        sys.path.insert(0, folder)
    module_name, _, function = target.rpartition(":")
    if not module_name:
        module_name = os.path.splitext(os.path.basename(path))[0]
    return getattr(importlib.import_module(module_name), function)  # type: ignore[no-any-return]


def run_job(
    fn: Callable[..., Any],
    spark: Any,
    tables: dict[str, str],
    scratch: str,
    run_id: str,
    rep: int,
    timeout_s: float,
) -> list[dict[str, Any]]:
    """One call of the job; its units' results (helper units, or one opaque unit)."""
    global _current
    sc = spark.sparkContext
    app_tag = tag(run_id, OPAQUE_UNIT, rep)
    sc.setJobGroup(app_tag, app_tag, interruptOnCancel=True)
    _current = _Run(run_id, rep, sc)
    started_at = time.time()
    start = time.monotonic()
    try:
        status, error, _ = guarded(
            app_tag, timeout_s, lambda: fn(spark, tables, scratch), _cancel(sc)
        )
    finally:
        results, _current = _current.results, None
        sc.setLocalProperty("spark.jobGroup.id", None)
        sc.setLocalProperty("spark.job.description", None)
    if results:
        # A cancelled unit raised inside the helper and was recorded as failed: it timed out.
        if status == "timeout" and results[-1]["status"] == "failed":
            results[-1] = {**results[-1], "status": "timeout", "wall_ms": None}
        return results
    return [
        {
            "unit_id": OPAQUE_UNIT,
            "rep": rep,
            "tag": app_tag,
            "discarded": rep == WARMUP_REP,
            "status": status,
            "wall_ms": round((time.monotonic() - start) * 1000, 3) if status == "ok" else None,
            "started_at": started_at,
            "error": error,
        }
    ]


def _cancel(sc: Any) -> Callable[[str], None]:
    def cancel(_: str) -> None:
        sc.cancelAllJobs()  # the job may have moved on to a unit's own job group

    return cancel
