"""Driver entry point.

``spark-submit --py-files velocia-driver-<hash>.zip velocia_launch.py --spec run.json``.

``run.json`` (written by Velocia, version 1)::

    {"version": 1, "run_id": "...", "task": "time" | "check" | "teragen",
     "mode": "cold" | "warm", "reps": [1], "warmup": false, "timeout_s": 1800,
     "float_round_decimals": 4,
     "units": [{"id": "q1", "sql": "..."} | {"id": "terasort", "job": "terasort"}],
     "tables": {"lineitem": "/data/.../lineitem"}, "output_dir": "/velocia/out",
     "scratch_dir": "/velocia/work/scratch", "teragen": {"rows", "seed", "partitions", "output"},
     "stage": {...}, "eventlog_fetch": {"dir": "s3a://...", "dest": "/velocia/work/eventlog"},
     "cleanup": ["s3a://.../_scratch/<run_id>"], "executor_wait_s": 300}

``time`` writes ``timings.json`` (rewritten after every unit, so a crash keeps what finished);
``check`` writes ``checks.json`` with one row count and checksum per unit (untimed, ADR-5);
job units write into ``scratch_dir`` and their validator's verdict goes into ``checks.json``.
``teragen`` generates job input data (``generator``: ``tera``, ``text`` or ``synth``) into
``<output>/<table>`` and writes ``teragen.json``.
``stage`` copies a dataset to the cluster's storage and verifies it there (``stage.json``).

On clusters Velocia doesn't run (the ``external`` runner) the optional ``eventlog_fetch``
copies this application's finished event log back to a local directory after ``stop()``, and
``cleanup`` removes scratch output on the cluster's storage; both report into ``fetch.json``.
"""

import argparse
import json
import os
import sys
import time
from functools import partial
from typing import Any

from .fs import Fs, join
from .jobs import GENERATORS, JOBS, VALIDATORS, synthgen
from .units import Unit, guarded, run_units, tag

TIMINGS_FORMAT = "velocia-timings"
CHECKS_FORMAT = "velocia-checks"
SETUP_UNIT = "_setup"  # not a unit: table registration before the first unit


def _write_json(path: str, data: dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _spark_info(spark: Any) -> dict[str, Any]:
    sc = spark.sparkContext
    try:
        executors = len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1  # minus the driver
    except Exception:
        executors = None
    conf = sc.getConf()
    return {
        "version": spark.version,
        "master": sc.master,
        "app_id": sc.applicationId,
        "app_name": sc.appName,
        "default_parallelism": sc.defaultParallelism,
        "executors": executors,
        "executor_cores": conf.get("spark.executor.cores", None),
        "plugins": conf.get("spark.plugins", None),
        "python": sys.version.split()[0],
    }


def _time(spark: Any, spec: dict[str, Any], units: list[Unit], out_path: str) -> None:
    sc = spark.sparkContext
    doc: dict[str, Any] = {
        "format": TIMINGS_FORMAT,
        "version": 1,
        "run_id": spec["run_id"],
        "mode": spec["mode"],
        "started_at": time.time(),
        "finished_at": None,
        "spark": _spark_info(spark),
        "units": [],
    }
    _write_json(out_path, doc)

    def execute(unit: Unit, unit_tag: str) -> None:
        # The job group carries the tag as job description into every job and SQL execution.
        sc.setJobGroup(unit_tag, unit_tag, interruptOnCancel=True)
        try:
            if unit.job is not None:
                JOBS[unit.job](spark, spec["tables"], _scratch(spec, unit))
            else:
                spark.sql(unit.sql).write.format("noop").mode("overwrite").save()
        finally:
            sc.setLocalProperty("spark.jobGroup.id", None)
            sc.setLocalProperty("spark.job.description", None)

    def on_result(result: dict[str, Any]) -> None:
        doc["units"].append(result)
        _write_json(out_path, doc)

    run_units(
        units,
        run_id=spec["run_id"],
        reps=list(spec["reps"]),
        warmup=bool(spec["warmup"]),
        timeout_s=float(spec["timeout_s"]),
        execute=execute,
        cancel=sc.cancelJobGroup,
        on_result=on_result,
    )
    doc["finished_at"] = time.time()
    doc["spark"] = _spark_info(spark)  # executors have registered by now
    _write_json(out_path, doc)


def checksum(spark: Any, sql: str, decimals: int) -> dict[str, Any]:
    """Row count and order-independent checksum of a query result (docs/06-design.md §5.3).

    Floating and decimal columns are rounded to ``decimals`` and hashed as doubles, because
    engines sum floats in different orders. Hashes are summed as exact decimals.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, DoubleType, FloatType

    df = spark.sql(sql)
    df = df.toDF(*[f"c{i}" for i in range(len(df.columns))])  # result names may repeat
    cols = []
    for field in df.schema.fields:
        col = F.col(field.name)
        if isinstance(field.dataType, (FloatType, DoubleType, DecimalType)):
            col = F.round(col, decimals).cast("double")
        cols.append(col)
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("checksum"),
    ).first()
    return {"rows": int(row["rows"]), "checksum": str(row["checksum"] or 0)}


def _scratch(spec: dict[str, Any], unit: Unit) -> str:
    return os.path.join(spec["scratch_dir"], unit.id)


def _check_job(spark: Any, spec: dict[str, Any], unit: Unit) -> dict[str, Any]:
    """Run the job once more, untimed, and let its validator judge the output."""
    assert unit.job is not None
    output = _scratch(spec, unit)
    JOBS[unit.job](spark, spec["tables"], output)
    return VALIDATORS[unit.job](spark, spec["tables"], output)


def _teragen(spark: Any, spec: dict[str, Any], out_path: str) -> None:
    """Generate a job dataset (``generator``: tera, text or synth) from a seed.

    Each table goes to ``<output>/<table>``.
    """
    params = spec["teragen"]
    generator = params.get("generator", "tera")
    if generator == "synth":
        rows = synthgen(spark, params["synth"], params["output"])
    else:
        rows = {
            generator: GENERATORS[generator](
                spark,
                int(params["rows"]),
                int(params["seed"]),
                int(params["partitions"]),
                f"{params['output']}/{generator}",
            )
        }
    _write_json(
        out_path,
        {"format": "velocia-teragen", "version": 1, "rows": rows, "spark": _spark_info(spark)},
    )


def _check(spark: Any, spec: dict[str, Any], units: list[Unit], out_path: str) -> None:
    sc = spark.sparkContext
    decimals = int(spec.get("float_round_decimals", 4))
    timeout_s = float(spec["timeout_s"])
    results = []
    for unit in units:
        unit_tag = tag(spec["run_id"], unit.id, -1)  # -1: correctness pass, never timed
        sc.setJobGroup(unit_tag, unit_tag, interruptOnCancel=True)
        if unit.job is not None:
            work = partial(_check_job, spark, spec, unit)
        else:
            work = partial(checksum, spark, unit.sql or "", decimals)
        status, error, value = guarded(unit_tag, timeout_s, work, sc.cancelJobGroup)
        problem = value.pop("problem", None) if value is not None else None
        if problem:
            status, error = "invalid", problem  # the job ran but its output is wrong
        entry: dict[str, Any] = {
            "unit_id": unit.id,
            "tag": unit_tag,
            "status": status,
            "error": error,
            "rows": None,
            "checksum": None,
        }
        if value is not None:
            entry.update(value)
        results.append(entry)
    _write_json(
        out_path,
        {
            "format": CHECKS_FORMAT,
            "version": 1,
            "run_id": spec["run_id"],
            "float_round_decimals": decimals,
            "spark": _spark_info(spark),
            "units": results,
        },
    )


STAGED_MARKER = "_velocia_staged.json"


def _verify(spark: Any, fs: Fs, stage: dict[str, Any]) -> list[str]:
    """Every file's size, then each table's row count read back by Spark."""
    remote = stage["remote_dir"]
    problems = []
    for rel, expected in sorted(stage["files"].items()):
        size = fs.size(join(remote, rel))
        if size != expected:
            problems.append(f"{rel}: {size} bytes on the cluster, expected {expected}")
    if problems:
        return problems
    for table, expected in sorted(stage["tables"].items()):
        rows = spark.read.parquet(join(remote, table)).count()
        if rows != expected:
            problems.append(f"{table}: {rows} rows on the cluster, expected {expected}")
    return problems


def _stage(spark: Any, fs: Fs, spec: dict[str, Any], out_path: str) -> None:
    """Make ``remote_dir`` hold exactly this dataset, verified in place.

    A copy marked with the same dataset checksum is reused when it verifies; anything else
    (no marker, an interrupted copy, the same data written with other file names) is deleted
    and copied again. The marker is written last.
    """
    stage = spec["stage"]
    remote = stage["remote_dir"]
    marker = join(remote, STAGED_MARKER)
    previous = fs.read_text(marker)
    reused = previous is not None and json.loads(previous).get("checksum") == stage["checksum"]
    problems = _verify(spark, fs, stage) if reused else ["not staged"]
    uploaded = 0
    if problems:
        reused = False
        if fs.exists(remote):
            fs.delete(remote)
        for rel in stage["files"]:
            fs.upload(os.path.join(stage["local_dir"], rel), join(remote, rel))
            uploaded += 1
        problems = _verify(spark, fs, stage)
        if not problems:
            fs.write_text(marker, json.dumps({"checksum": stage["checksum"]}))
    _write_json(
        out_path,
        {
            "format": "velocia-stage",
            "version": 1,
            "remote_dir": remote,
            "reused": reused,
            "uploaded_files": uploaded,
            "problems": problems,
            "spark": _spark_info(spark),
        },
    )


def _fetch_eventlog(fs: Fs, fetch: dict[str, Any], app_id: str) -> str | None:
    """Copy this application's event log (file or rolling directory) to ``dest``."""
    names = [
        name
        for name, _, _ in fs.list(fetch["dir"])
        if name.startswith(app_id) or name == f"eventlog_v2_{app_id}"
    ]
    if not names:
        return f"no event log for {app_id} in {fetch['dir']}"
    os.makedirs(fetch["dest"], exist_ok=True)
    for name in names:
        fs.download(join(fetch["dir"], name), os.path.join(fetch["dest"], name))
    return None


def _after_stop(fs: Fs, spec: dict[str, Any], app_id: str) -> None:
    report: dict[str, Any] = {"format": "velocia-fetch", "version": 1, "eventlog": None}
    fetch = spec.get("eventlog_fetch")
    if fetch:
        try:
            report["eventlog"] = _fetch_eventlog(fs, fetch, app_id) or "ok"
        except Exception as exc:  # reported; the timings still count
            report["eventlog"] = f"{type(exc).__name__}: {exc}"
    problems = []
    for uri in spec.get("cleanup") or []:
        try:
            fs.delete(uri)
        except Exception as exc:
            problems.append(f"{uri}: {type(exc).__name__}: {exc}")
    report["cleanup_problems"] = problems
    _write_json(os.path.join(spec["output_dir"], "fetch.json"), report)


def _wait_for_executors(spark: Any, timeout_s: float) -> None:
    """On a cluster, fail clearly when no executor registers (e.g. it can't reach the driver)."""
    tracker = spark.sparkContext._jsc.sc().statusTracker()
    deadline = time.monotonic() + timeout_s
    while len(tracker.getExecutorInfos()) < 2:  # the driver is listed too
        if time.monotonic() > deadline:
            raise SystemExit(
                f"no executor registered within {timeout_s:.0f}s; check that executors can "
                "reach the driver (network, spark.driver.host) and that the cluster has free "
                "cores and memory"
            )
        time.sleep(1)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="velocia-driver")
    parser.add_argument("--spec", required=True, help="path to run.json")
    args = parser.parse_args(argv)
    with open(args.spec, encoding="utf-8") as f:
        spec = json.load(f)
    if spec.get("version") != 1:
        raise SystemExit(f"unsupported run.json version {spec.get('version')!r}")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"velocia-{spec['run_id']}").getOrCreate()
    fs = Fs(spark)
    app_id = spark.sparkContext.applicationId
    try:
        if spec.get("executor_wait_s"):
            _wait_for_executors(spark, float(spec["executor_wait_s"]))
        # Reading Parquet footers runs Spark jobs too; tag them so every job has a tag.
        setup_tag = tag(spec["run_id"], SETUP_UNIT, 0)
        spark.sparkContext.setJobGroup(setup_tag, setup_tag)
        for name, path in spec["tables"].items():
            spark.read.parquet(path).createOrReplaceTempView(name)
        units = [Unit(u["id"], u.get("sql"), u.get("job")) for u in spec["units"]]
        os.makedirs(spec["output_dir"], exist_ok=True)
        if spec["task"] == "teragen":
            _teragen(spark, spec, os.path.join(spec["output_dir"], "teragen.json"))
        elif spec["task"] == "time":
            _time(spark, spec, units, os.path.join(spec["output_dir"], "timings.json"))
        elif spec["task"] == "check":
            _check(spark, spec, units, os.path.join(spec["output_dir"], "checks.json"))
        elif spec["task"] == "stage":
            _stage(spark, fs, spec, os.path.join(spec["output_dir"], "stage.json"))
        else:
            raise SystemExit(f"unknown task {spec['task']!r}")
    finally:
        spark.stop()  # closes the event log; the JVM and ``fs`` stay usable
        if spec.get("eventlog_fetch") or spec.get("cleanup"):
            _after_stop(fs, spec, app_id)
