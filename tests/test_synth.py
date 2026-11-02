"""Seeded edge-case data (T33): reproducible, and the distributions asked for."""

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
