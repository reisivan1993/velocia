"""ETL pipeline job (T31): five tagged units, each validated against the reference."""

import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.config.models import Suite
from velocia.workloads import get_workload

STEPS = ["read", "clean", "join", "aggregate", "write"]


def test_etl_workload() -> None:
    etl = get_workload(Suite.etl)
    assert etl is not None
    assert [(u.id, u.job) for u in etl.units()] == [(s, f"etl_{s}") for s in STEPS]


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_etl_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    gen = cli.invoke(app, ["datagen", "--suite", "etl", "--rows", "200000"])
    assert gen.exit_code == 0, gen.output
    (tmp_path / "exp.yaml").write_text(
        "name: etl\nworkload: {kind: job, suite: etl, rows: 200000}\n"
        "engines: [spark-vanilla, spark-comet]\nreference_engine: spark-vanilla\n"
        "cluster: {runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}\n"
        "measures: [wall_time]\nrepetitions: 2\nmodes: [cold, warm]\n"
    )
    result = cli.invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    results = tmp_path / "velocia-results"
    report = json.loads(next((results / "reports").iterdir()).joinpath("report.json").read_text())
    verdicts = {(r["engine"], r["mode"], r["unit_id"]): r for r in report["results"]}
    for engine in ("spark-vanilla", "spark-comet"):
        for mode in ("cold", "warm"):
            for step in STEPS:
                r = verdicts[(engine, mode, step)]
                assert r["correctness"] == "pass" and r["n"] == 2, (engine, mode, step, r)

    # Every step is its own tagged unit in the event log.
    for run in (results / "runs").glob("*-r1"):
        tags = set()
        for log in (run / "eventlog").iterdir():
            for line in log.open():
                event = json.loads(line)
                tag = (event.get("Properties") or {}).get("spark.job.description", "")
                if event.get("Event") == "SparkListenerJobStart" and tag:
                    tags.add(tag.split(":")[2])
        assert set(STEPS) <= tags
    # Scratch output lived in the application's work dir and is gone.
    assert not list(results.rglob("scratch"))
