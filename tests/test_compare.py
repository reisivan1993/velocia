import subprocess
import sys
import time
from pathlib import Path

import pytest

from velocia.report.compare import CompareError, compare
from velocia.store.catalog import Catalog


def _experiment(
    cat: Catalog,
    exp: str,
    walls: dict[str, list[float]],
    ratio: tuple[float, float],
    hw: str = "hw1",
) -> None:
    cat.insert("experiment", {"experiment_id": exp, "bootstrap_seed": 1})
    for cell, values in walls.items():
        cat.insert("cell", {"experiment_id": exp, "cell_key": cell, "hardware_manifest_hash": hw})
        for rep, value in enumerate(values, 1):
            run_id = f"{exp}-{cell}-{rep}"
            cat.insert("run", {"run_id": run_id, "experiment_id": exp, "cell_key": cell})
            cat.insert_many(
                "measure_value",
                [
                    {
                        "run_id": run_id,
                        "unit_id": "q1",
                        "rep": rep,
                        "measure": "wall_time",
                        "value": value,
                    }
                ],
            )
    cat.insert_many(
        "result",
        [
            {
                "experiment_id": exp,
                "cell_key": "comet",
                "engine": "spark-comet",
                "unit_id": "q1",
                "measure": "wall_time",
                "ratio": 1.0,
                "ci_low": ratio[0],
                "ci_high": ratio[1],
            }
        ],
    )


def test_rerun_reproduces(tmp_path: Path) -> None:
    with Catalog(tmp_path) as cat:
        base = {"vanilla": [10.0, 10.2, 9.9, 10.1, 10.0], "comet": [5.0, 5.1, 4.9, 5.0, 5.05]}
        _experiment(cat, "a", base, (1.9, 2.1))
        _experiment(cat, "b", {"vanilla": [10.05] * 5, "comet": [5.02] * 5}, (1.95, 2.05))
        result = compare(cat, "a", "b")
    assert (result.checked, result.inside, result.flips) == (2, 2, [])
    assert result.reproduces


def test_rerun_outside_ci_and_ratio_flip(tmp_path: Path) -> None:
    with Catalog(tmp_path) as cat:
        _experiment(cat, "a", {"vanilla": [10.0, 10.1, 9.9], "comet": [5.0, 5.1, 4.9]}, (1.9, 2.1))
        _experiment(cat, "b", {"vanilla": [10.0] * 3, "comet": [20.0] * 3}, (0.4, 0.6))
        result = compare(cat, "a", "b")
        assert result.inside == 1 and len(result.outside) == 1
        assert result.flips and "better in a, worse in b" in result.flips[0]
        assert not result.reproduces
        _experiment(cat, "c", {"vanilla": [10.0] * 3}, (1.0, 1.0), hw="hw2")
        with pytest.raises(CompareError, match="different hardware"):
            compare(cat, "a", "c")
        assert compare(cat, "a", "c", force=True).checked == 1
        with pytest.raises(CompareError, match="unknown experiment"):
            compare(cat, "a", "zzz")


HOLD_LOCK = (
    "import duckdb, sys, time; c = duckdb.connect(sys.argv[1]); "
    "print('locked', flush=True); time.sleep(1.5); c.close()"
)


def test_catalog_waits_for_another_process_lock(tmp_path: Path) -> None:
    Catalog(tmp_path)  # create the schema
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            HOLD_LOCK,
            str(tmp_path / "catalog.duckdb"),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert holder.stdout is not None and holder.stdout.readline().strip() == "locked"
    started = time.monotonic()
    rows = Catalog(tmp_path).query("SELECT count(*) AS n FROM experiment")
    assert rows == [{"n": 0}] and time.monotonic() - started > 0.5
    holder.wait(timeout=10)
