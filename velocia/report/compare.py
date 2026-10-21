"""``velocia compare <exp_a> <exp_b>``: is a rerun consistent with the first run? (D-3)

Cells are matched by ``cell_key`` (same engine, cluster, config and mode). For every cell,
unit and measure: the first run's median with a seeded bootstrap 95% CI, and whether the
second run's median falls inside it. For every candidate engine: whether its ratio CI moved
from better to worse than the reference (or back). The v0.1 reproducibility criterion: at
least 90% of medians inside, and no such move.
"""

import random
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from velocia.stats.summary import percentile
from velocia.store.catalog import Catalog

CRITERION_SHARE = 0.90
RESAMPLES = 2000


class CompareError(RuntimeError):
    pass


@dataclass
class Comparison:
    a: str
    b: str
    checked: int = 0
    inside: int = 0
    outside: list[str] = field(default_factory=list)
    flips: list[str] = field(default_factory=list)
    unmatched_cells: int = 0

    @property
    def share_inside(self) -> float:
        return self.inside / self.checked if self.checked else 0.0

    @property
    def reproduces(self) -> bool:
        return self.checked > 0 and self.share_inside >= CRITERION_SHARE and not self.flips


def median_ci(values: list[float], seed: int, resamples: int = RESAMPLES) -> tuple[float, float]:
    rng = random.Random(seed)
    medians = [statistics.median(rng.choices(values, k=len(values))) for _ in range(resamples)]
    return percentile(medians, 0.025), percentile(medians, 0.975)


def _values(catalog: Catalog, experiment_id: str) -> dict[tuple[str, str, str], list[float]]:
    rows = catalog.query(
        "SELECT r.cell_key, m.unit_id, m.measure, m.value FROM measure_value m "
        "JOIN run r USING (run_id) WHERE r.experiment_id = ? AND m.value IS NOT NULL",
        [experiment_id],
    )
    out: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for row in rows:
        out[(row["cell_key"], row["unit_id"], row["measure"])].append(row["value"])
    return out


def _side(ratio: dict[str, Any]) -> str:
    if ratio["ci_low"] is not None and ratio["ci_low"] > 1:
        return "better"
    if ratio["ci_high"] is not None and ratio["ci_high"] < 1:
        return "worse"
    return "unclear"


def compare(catalog: Catalog, a: str, b: str, force: bool = False) -> Comparison:
    for experiment_id in (a, b):
        if catalog.experiment(experiment_id) is None:
            raise CompareError(f"unknown experiment {experiment_id!r}")

    def hardware(experiment_id: str) -> set[str]:
        rows = catalog.query(
            "SELECT DISTINCT hardware_manifest_hash AS h FROM cell WHERE experiment_id = ?",
            [experiment_id],
        )
        return {r["h"] for r in rows}

    if hardware(a) != hardware(b) and not force:
        raise CompareError(
            "the experiments ran on different hardware manifests; use --force to compare anyway"
        )

    result = Comparison(a, b)
    first, second = _values(catalog, a), _values(catalog, b)
    cells_a = {k[0] for k in first}
    result.unmatched_cells = len(cells_a ^ {k[0] for k in second})
    for index, (key, values) in enumerate(sorted(first.items())):
        rerun = second.get(key)
        if not rerun or len(values) < 2:
            continue
        lo, hi = median_ci(values, seed=index)
        result.checked += 1
        median_b = statistics.median(rerun)
        if lo <= median_b <= hi:
            result.inside += 1
        else:
            cell, unit, measure = key
            result.outside.append(
                f"{cell} {unit} {measure}: rerun median {median_b:.4g} outside [{lo:.4g}, {hi:.4g}]"
            )

    def ratios(experiment_id: str) -> dict[tuple[str, str, str], dict[str, Any]]:
        rows = catalog.query(
            "SELECT * FROM result WHERE experiment_id = ? AND ratio IS NOT NULL",
            [experiment_id],
        )
        return {(r["cell_key"], r["unit_id"], r["measure"]): r for r in rows}

    ratios_b = ratios(b)
    for key, ra in ratios(a).items():
        rb = ratios_b.get(key)
        if rb is None:
            continue
        sides = {_side(ra), _side(rb)}
        if sides == {"better", "worse"}:
            result.flips.append(
                f"{ra['engine']} {ra['unit_id']} {ra['measure']}: {_side(ra)} in {a}, "
                f"{_side(rb)} in {b}"
            )
    return result
