"""Bring-your-own Python jobs (T32): per-unit with the velocia.unit helper, opaque without."""

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.orchestrator.run import RunError, prepare

JOB = """
import velocia
from pyspark.sql import functions as F


def run(spark, tables, scratch):
    with velocia.unit("count"):
        spark.read.parquet(tables["lineitem"]).count()
    with velocia.unit("priorities"):
        spark.read.parquet(tables["orders"]).groupBy("o_orderpriority").count().write.mode(
            "overwrite"
        ).parquet(scratch + "/priorities")


def validate(spark, tables, scratch):
    out = spark.read.parquet(scratch + "/priorities")
    total = out.agg(F.sum("count")).first()[0]
    orders = spark.read.parquet(tables["orders"]).count()
    return {
        "count": {"rows": 1, "checksum": str(spark.read.parquet(tables["lineitem"]).count())},
        "priorities": {
            "rows": out.count(),
            "checksum": str(total),
            "problem": None if total == orders else f"{total} != {orders} orders",
        },
    }


def opaque(spark, tables, scratch):
    spark.read.parquet(tables["lineitem"]).groupBy("l_returnflag").count().collect()
"""

EXPERIMENT = """
name: {name}
workload:
  kind: job
  suite: custom
  dataset_id: tpch-sf0.01-parquet-zstd
  units: {units}
  job: {{artifact: jobs/my_job.py, entrypoint: "{entrypoint}"{validator}}}
engines: [spark-vanilla, spark-comet]
reference_engine: spark-vanilla
cluster: {{runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}}
measures: [wall_time, task_cpu_time]
repetitions: 2
modes: [cold]
"""


def _write(tmp_path: Path, name: str, units: str, entrypoint: str, validator: str = "") -> Path:
    (tmp_path / "jobs").mkdir(exist_ok=True)
    (tmp_path / "jobs" / "my_job.py").write_text(JOB)
    path = tmp_path / f"{name}.yaml"
    path.write_text(
        EXPERIMENT.format(name=name, units=units, entrypoint=entrypoint, validator=validator)
    )
    return path


def _report(results: Path, name: str) -> dict[str, Any]:
    report_dir = next((results / "reports").glob(f"{name}-*"))
    return json.loads((report_dir / "report.json").read_text())  # type: ignore[no-any-return]


def test_custom_job_needs_a_python_artifact(tmp_path: Path) -> None:
    from velocia.config import VelociaConfig

    path = _write(tmp_path, "x", "all", "my_job:run")
    (tmp_path / "jobs" / "my_job.py").unlink()
    with pytest.raises(RunError, match="not found"):
        prepare(path, VelociaConfig(results_dir=str(tmp_path / "r")))


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_custom_job_with_helper_and_opaque(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    assert cli.invoke(app, ["datagen", "--suite", "tpch", "--sf", "0.01"]).exit_code == 0
    results = tmp_path / "velocia-results"

    _write(
        tmp_path, "helper", "[count, priorities]", "my_job:run", ', validator: "my_job:validate"'
    )
    result = cli.invoke(app, ["run", "helper.yaml"])
    assert result.exit_code == 0, result.output
    report = json.loads(
        next((results / "reports").glob("helper-*")).joinpath("report.json").read_text()
    )
    rows = {(r["engine"], r["unit_id"], r["measure"]): r for r in report["results"]}
    for engine in ("spark-vanilla", "spark-comet"):
        for unit in ("count", "priorities"):
            r = rows[(engine, unit, "wall_time")]
            assert r["n"] == 2 and r["correctness"] == "pass", (engine, unit, r)
        assert rows[(engine, "priorities", "task_cpu_time")]["n"] == 2  # tagged in the event log

    _write(tmp_path, "opaque", "all", "my_job:opaque")
    result = cli.invoke(app, ["run", "opaque.yaml"])
    assert result.exit_code == 0, result.output
    report = json.loads(
        next((results / "reports").glob("opaque-*")).joinpath("report.json").read_text()
    )
    units = {r["unit_id"] for r in report["results"]}
    assert units == {"app", "_total"}
    app_rows = [
        r for r in report["results"] if r["unit_id"] == "app" and r["measure"] == "wall_time"
    ]
    assert all(r["n"] == 2 and r["correctness"] == "not_checked" for r in app_rows)
