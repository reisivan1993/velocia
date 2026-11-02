"""Seeded edge-case data (T33): reproducible, and the distributions asked for."""

import json
import math
import shutil
from pathlib import Path

import duckdb
import pytest
import yaml
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.config.models import DataFormat, Experiment, Suite, SynthSpec
from velocia.datagen.manifest import DatasetManifest
from velocia.datagen.tera import generate_spark_suite, synth_dataset_id

FMT = DataFormat.parquet_zstd


def test_synth_spec_and_dataset_id() -> None:
    spec = SynthSpec(key_dist="zipf", zipf_s=1.2)
    assert synth_dataset_id(spec, FMT) == synth_dataset_id(
        SynthSpec(key_dist="zipf", zipf_s=1.2), FMT
    )
    assert synth_dataset_id(spec, FMT) != synth_dataset_id(SynthSpec(key_dist="zipf"), FMT)
    assert synth_dataset_id(spec, FMT).startswith("synth-")
    with pytest.raises(ValueError, match="unknown synth parameter"):
        Experiment.model_validate(
            {
                "name": "x",
                "workload": {"kind": "sql", "suite": "synth", "sweep": {"spark.x": [1, 2]}},
                "engines": ["spark-vanilla"],
                "reference_engine": "spark-vanilla",
                "cluster": {
                    "runner": "local",
                    "executors": 1,
                    "executor_cores": 1,
                    "executor_memory": "1g",
                },
            }
        )


def zipf_exponent(fact: str, top: int = 1000) -> float:
    """Least-squares slope of log(count) over log(rank) for the ``top`` most frequent keys."""
    counts = [
        c
        for (c,) in duckdb.sql(
            f"SELECT count(*) AS c FROM '{fact}' GROUP BY key ORDER BY c DESC LIMIT {top}"
        ).fetchall()
    ]
    xs = [math.log(r) for r in range(1, len(counts) + 1)]
    ys = [math.log(c) for c in counts]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / sum(
        (x - mx) ** 2 for x in xs
    )
    return -slope


def _generate(spec: SynthSpec, out: Path) -> DatasetManifest:
    manifest, _ = generate_spark_suite(
        Suite.synth, spec.rows, FMT, out, out / "drivers", cores=4, synth=spec
    )
    return manifest


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_synth_is_reproducible_and_shaped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    zipf = SynthSpec(rows=2_000_000, keys=10_000, key_dist="zipf", zipf_s=1.2, files=4)
    a = _generate(zipf, tmp_path / "a")
    b = _generate(zipf, tmp_path / "b")
    assert a.checksum == b.checksum and a.dataset_id == b.dataset_id
    assert {t.name: t.rows for t in a.tables} == {"fact": 2_000_000, "dim": 10_000}
    fact = f"{a.location}/fact/*.parquet"
    measured = zipf_exponent(fact)
    assert abs(measured - 1.2) / 1.2 < 0.05, measured

    # Hot key, nulls and file count, through the CLI with a spec file.
    monkeypatch.chdir(tmp_path)
    (tmp_path / "synth.yaml").write_text(
        yaml.safe_dump(
            {
                "rows": 500_000,
                "keys": 1000,
                "key_dist": "hotkey",
                "hot_fraction": 0.3,
                "null_fraction": 0.2,
                "payload_columns": 3,
                "files": 5,
                "string_length": 40,
            }
        )
    )
    result = CliRunner().invoke(app, ["datagen", "--suite", "synth", "--spec", "synth.yaml"])
    assert result.exit_code == 0, result.output
    location = next((tmp_path / "velocia-results" / "data").glob("synth-*"))
    fact = f"{location}/fact/*.parquet"
    hot, nulls, length = duckdb.sql(
        f"SELECT avg((key = 0)::INT), avg((p1 IS NULL)::INT), avg(length(s)) FROM '{fact}'"
    ).fetchone()
    assert abs(hot - (0.3 + 0.7 / 1000)) < 0.01 and abs(nulls - 0.2) < 0.01
    assert 35 < length < 45
    assert len(list((location / "fact").glob("*.parquet"))) == 5


SWEEP = """
name: sweep
workload:
  kind: sql
  suite: synth
  synth: {rows: 200000, keys: 5000, key_dist: zipf, files: 2}
  sweep: {zipf_s: [0, 1.2]}
engines: [spark-vanilla, spark-comet]
reference_engine: spark-vanilla
cluster: {runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}
measures: [wall_time]
repetitions: 2
modes: [cold]
timeout_per_unit: 5m
"""


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_synth_sweep_runs_one_cell_per_point(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "exp.yaml").write_text(SWEEP)
    result = CliRunner().invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    assert result.output.count("Generating synth dataset synth-") == 2

    results = tmp_path / "velocia-results"
    report = json.loads(next((results / "reports").iterdir()).joinpath("report.json").read_text())
    points = sorted({(c["engine"], c["config"]["zipf_s"]) for c in report["cells"]})
    assert points == [
        ("spark-comet", "0"),
        ("spark-comet", "1.2"),
        ("spark-vanilla", "0"),
        ("spark-vanilla", "1.2"),
    ]
    units = {r["unit_id"] for r in report["results"]}
    assert {"join", "aggregate", "exploding_join", "sort"} <= units
    assert {r["correctness"] for r in report["results"]} == {"pass"}
    # Each sweep point ran on its own dataset.
    datasets = {
        json.loads(m.read_text())["dataset_id"] for m in (results / "runs").glob("*/manifest.json")
    }
    assert len(datasets) == 2


def test_query_parameters_render_sql_but_share_data() -> None:
    from velocia.workloads.synth import SQL, render_sql

    a, b = SynthSpec(selectivity=0.01), SynthSpec(selectivity=0.5, window_frame=3)
    assert synth_dataset_id(a, FMT) == synth_dataset_id(b, FMT)  # same data
    assert "< 10000" in render_sql(SQL["scan_filter"], a)
    assert "< 500000" in render_sql(SQL["scan_filter"], b)
    assert "BETWEEN 3 PRECEDING" in render_sql(SQL["window"], b)
    assert synth_dataset_id(SynthSpec(nest_fanout=8), FMT) != synth_dataset_id(a, FMT)


ALL_UNITS = """
name: all-units
workload:
  kind: sql
  suite: synth
  synth: {rows: 100000, keys: 2000, null_fraction: 0.1, files: 3, string_length: 24}
  sweep: {selectivity: [0.01, 0.5]}
engines: [spark-vanilla, spark-comet]
reference_engine: spark-vanilla
cluster: {runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}
measures: [wall_time]
repetitions: 1
modes: [cold]
timeout_per_unit: 5m
"""


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_every_synth_unit_on_vanilla_and_comet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "exp.yaml").write_text(ALL_UNITS)
    result = CliRunner().invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    # selectivity is a query parameter: both sweep points share one dataset.
    assert result.output.count("Generating synth dataset synth-") == 1
    results = tmp_path / "velocia-results"
    report = json.loads(next((results / "reports").iterdir()).joinpath("report.json").read_text())
    verdicts = {
        (r["engine"], r["unit_id"], r["config_hash"]): r["correctness"]
        for r in report["results"]
        if r["unit_id"] != "_total"
    }
    from velocia.workloads.synth import SQL

    assert {u for _, u, _ in verdicts} == set(SQL)
    assert {v for v in verdicts.values()} == {"pass"}, {
        k: v for k, v in verdicts.items() if v != "pass"
    }
