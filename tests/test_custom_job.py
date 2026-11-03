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


JAVA = """
import org.apache.spark.sql.SparkSession;

public class VelociaJarJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        String run = spark.conf().get("spark.velocia.run_id", "");
        String rep = spark.conf().get("spark.velocia.rep", "1");
        boolean tag = args.length > 1 && args[1].equals("tagged");
        String[] units = {"count", "flags"};
        for (String unit : units) {
            if (tag) {
                spark.sparkContext().setJobDescription("velocia:" + run + ":" + unit + ":" + rep);
            }
            if (unit.equals("count")) {
                spark.read().parquet(args[0]).count();
            } else {
                spark.read().parquet(args[0]).groupBy("l_returnflag").count().collect();
            }
        }
        spark.stop();
    }
}
"""


def _build_jar(dest: Path) -> Path:
    import subprocess

    src = dest / "src"
    src.mkdir(parents=True)
    (src / "VelociaJarJob.java").write_text(JAVA)
    script = (
        "cd /src && javac -cp '/opt/spark/jars/*' -d /src VelociaJarJob.java "
        "&& jar cf /src/job.jar VelociaJarJob.class"
    )
    subprocess.run(
        ["docker", "run", "--rm", "--user", "0", "-v", f"{src}:/src", "--entrypoint", "sh",
         "velocia/engine-spark-vanilla:3.5.9", "-c", script],
        check=True,
        capture_output=True,
    )  # fmt: skip
    jobs = dest / "jobs"
    jobs.mkdir(exist_ok=True)
    (jobs / "job.jar").write_bytes((src / "job.jar").read_bytes())
    return jobs / "job.jar"


JAR_EXPERIMENT = """
name: {name}
workload:
  kind: job
  suite: custom
  dataset_id: tpch-sf0.01-parquet-zstd
  units: {units}
  job: {{artifact: jobs/job.jar, entrypoint: VelociaJarJob,
         args: ["${{table:lineitem}}", "{mode}"]}}
engines: [spark-vanilla, spark-comet]
reference_engine: spark-vanilla
cluster: {{runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}}
measures: [wall_time, task_cpu_time]
repetitions: 2
modes: [cold]
"""


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_jar_job_tagged_and_opaque(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_jar(tmp_path)
    cli = CliRunner()
    assert cli.invoke(app, ["datagen", "--suite", "tpch", "--sf", "0.01"]).exit_code == 0
    results = tmp_path / "velocia-results"

    (tmp_path / "tagged.yaml").write_text(
        JAR_EXPERIMENT.format(name="tagged", units="[count, flags]", mode="tagged")
    )
    result = cli.invoke(app, ["run", "tagged.yaml"])
    assert result.exit_code == 0, result.output
    rows = {
        (r["engine"], r["unit_id"], r["measure"]): r for r in _report(results, "tagged")["results"]
    }
    for engine in ("spark-vanilla", "spark-comet"):
        for unit in ("count", "flags"):
            wall = rows[(engine, unit, "wall_time")]
            assert wall["n"] == 2 and wall["correctness"] == "not_checked", (engine, unit, wall)
            assert rows[(engine, unit, "task_cpu_time")]["n"] == 2

    (tmp_path / "opaque.yaml").write_text(
        JAR_EXPERIMENT.format(name="opaque", units="all", mode="plain")
    )
    result = cli.invoke(app, ["run", "opaque.yaml"])
    assert result.exit_code == 0, result.output
    report = _report(results, "opaque")
    assert {r["unit_id"] for r in report["results"]} == {"app", "_total"}
    wall = [r for r in report["results"] if r["unit_id"] == "app" and r["measure"] == "wall_time"]
    assert all(r["n"] == 2 and r["median"] > 0 for r in wall)
    assert not list((results / "runs").glob("opaque-*-check")), "jar jobs have no check pass"


def test_jar_command_and_rules(tmp_path: Path) -> None:
    from velocia.config import VelociaConfig
    from velocia.families.spark.package import build_driver
    from velocia.runners.base import AppSpec
    from velocia.runners.local import program

    (tmp_path / "jobs").mkdir()
    (tmp_path / "jobs" / "job.jar").write_bytes(b"PK")
    app_spec = AppSpec(
        run_id="e-c001-r2",
        image="img",
        spark_conf={},
        driver=build_driver(tmp_path / "d"),
        task="time",
        mode="cold",
        reps=[2],
        warmup=False,
        units=[],
        dataset_location=tmp_path,
        tables=["lineitem"],
        timeout_per_unit_s=60,
        app_timeout_s=600,
        custom={
            "artifact": str(tmp_path / "jobs" / "job.jar"),
            "entrypoint": "com.example.Main",
            "validator": None,
            "args": '["${table:lineitem}", "x", "${table:nope}"]',
        },
    )
    assert program(app_spec, "/d", "/c", "/w", "/data") == [
        "--conf", "spark.velocia.run_id=e-c001-r2", "--conf", "spark.velocia.rep=2",
        "--class", "com.example.Main", "/c/job.jar", "/data/lineitem", "x", "${table:nope}",
    ]  # fmt: skip

    config = VelociaConfig(results_dir=str(tmp_path / "r"))
    exp = tmp_path / "exp.yaml"
    base = (
        "name: j\nworkload: {{kind: job, suite: custom, dataset_id: d, job: {{artifact: "
        "jobs/job.jar, entrypoint: Main{validator}}}}}\nengines: [spark-vanilla]\n"
        "reference_engine: spark-vanilla\ncluster: {{runner: local, executors: 1, "
        "executor_cores: 1, executor_memory: 1g}}\nmodes: [{modes}]\n"
    )
    exp.write_text(base.format(validator=", validator: x:y", modes="cold"))
    with pytest.raises(RunError, match="only Python jobs take a validator"):
        prepare(exp, config)
    exp.write_text(base.format(validator="", modes="cold, warm"))
    with pytest.raises(RunError, match="cold mode only"):
        prepare(exp, config)
