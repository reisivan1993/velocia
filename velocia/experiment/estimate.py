"""Dry-run estimates: run count and a rough duration (docs/03-spec.md §3).

There is no history yet, so the duration comes from fixed per-suite costs scaled by data size
and cluster cores. It is meant to tell minutes from hours, nothing finer; the worst case at the
per-unit timeout is reported next to it.
"""

from dataclasses import dataclass
from datetime import timedelta

from velocia.config.models import Experiment, Mode, Suite
from velocia.experiment.expander import Cell
from velocia.workloads import get_workload

APP_OVERHEAD_S = 15.0  # container or pod start plus Spark session start, per run
MIN_UNIT_S = 1.0
# TPC-H: vanilla Apache Spark 3.5 at SF1 took ~0.9 s per query on 4 cores (2026-10-21 demo);
# rounded up because accelerators are faster and larger clusters scale imperfectly.
CORE_SECONDS_PER_UNIT_PER_SF = {Suite.tpch: 5.0, Suite.tpcds: 5.0}
CORE_SECONDS_PER_MILLION_ROWS = {
    Suite.tera: 6.0,
    Suite.text: 4.0,
    Suite.etl: 10.0,
    Suite.graph: 30.0,
    Suite.kmeans: 20.0,
}


@dataclass(frozen=True)
class Estimate:
    cells: int
    runs: int  # timed Spark applications: cells x repetitions
    correctness_passes: int  # one untimed pass per cell
    units: int | None  # per run; None when the suite's unit list is not known yet
    duration: timedelta | None
    worst_case: timedelta | None  # every unit hitting the per-unit timeout


def unit_count(experiment: Experiment) -> int | None:
    units = experiment.workload.units
    if isinstance(units, list):
        return len(units)
    workload = get_workload(experiment.workload.suite)
    return None if workload is None else len(workload.units())


def estimate(experiment: Experiment, cells: list[Cell]) -> Estimate:
    units = unit_count(experiment)
    runs = len(cells) * experiment.repetitions
    unit_s = _unit_seconds(experiment)
    duration = worst = None
    if units is not None:
        timeout_s = experiment.timeout.total_seconds()
        worst_s = 0.0
        total_s = 0.0
        for cell in cells:
            # Warm mode runs every unit once more as a discarded warm-up.
            passes = units * (2 if cell.mode is Mode.warm else 1)
            cores = cell.cluster.executors * cell.cluster.executor_cores
            apps = experiment.repetitions + 1  # + the correctness pass
            worst_s += apps * (APP_OVERHEAD_S + passes * timeout_s)
            if unit_s is not None:
                per_unit = min(timeout_s, max(MIN_UNIT_S, unit_s / cores))
                total_s += apps * (APP_OVERHEAD_S + passes * per_unit)
        worst = timedelta(seconds=worst_s)
        duration = timedelta(seconds=round(total_s)) if unit_s is not None else None
    return Estimate(
        cells=len(cells),
        runs=runs,
        correctness_passes=len(cells),
        units=units,
        duration=duration,
        worst_case=worst,
    )


def _unit_seconds(experiment: Experiment) -> float | None:
    """Core-seconds for one unit, or None when there is no cost model for the suite."""
    workload = experiment.workload
    if workload.suite in CORE_SECONDS_PER_UNIT_PER_SF and workload.scale_factor is not None:
        return CORE_SECONDS_PER_UNIT_PER_SF[workload.suite] * workload.scale_factor
    if workload.suite in CORE_SECONDS_PER_MILLION_ROWS and workload.rows is not None:
        return CORE_SECONDS_PER_MILLION_ROWS[workload.suite] * workload.rows / 1e6
    return None


def format_duration(value: timedelta) -> str:
    minutes = round(value.total_seconds() / 60)
    if minutes < 1:
        return "< 1m"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m" if hours else f"{minutes}m"
