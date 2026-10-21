"""Driver entry point.

``spark-submit --py-files velocia-driver-<hash>.zip velocia_launch.py --spec run.json``.

``run.json`` (written by Velocia, version 1)::

    {"version": 1, "run_id": "...", "task": "time" | "check", "mode": "cold" | "warm",
     "reps": [1], "warmup": false, "timeout_s": 1800, "float_round_decimals": 4,
     "units": [{"id": "q1", "sql": "..."}], "tables": {"lineitem": "/data/.../lineitem"},
     "output_dir": "/velocia/out"}

``time`` writes ``timings.json`` (rewritten after every unit, so a crash keeps what finished);
``check`` writes ``checks.json`` with one row count and checksum per unit (untimed, ADR-5).
"""

import argparse
import json
import os
import sys
import time
from functools import partial
from typing import Any

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


def _check(spark: Any, spec: dict[str, Any], units: list[Unit], out_path: str) -> None:
    sc = spark.sparkContext
    decimals = int(spec.get("float_round_decimals", 4))
    timeout_s = float(spec["timeout_s"])
    results = []
    for unit in units:
        unit_tag = tag(spec["run_id"], unit.id, -1)  # -1: correctness pass, never timed
        sc.setJobGroup(unit_tag, unit_tag, interruptOnCancel=True)
        status, error, value = guarded(
            unit_tag, timeout_s, partial(checksum, spark, unit.sql, decimals), sc.cancelJobGroup
        )
        entry: dict[str, Any] = {"unit_id": unit.id, "tag": unit_tag, "status": status,
                                 "error": error, "rows": None, "checksum": None}  # fmt: skip
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
    try:
        # Reading Parquet footers runs Spark jobs too; tag them so every job has a tag.
        setup_tag = tag(spec["run_id"], SETUP_UNIT, 0)
        spark.sparkContext.setJobGroup(setup_tag, setup_tag)
        for name, path in spec["tables"].items():
            spark.read.parquet(path).createOrReplaceTempView(name)
        units = [Unit(u["id"], u["sql"]) for u in spec["units"]]
        os.makedirs(spec["output_dir"], exist_ok=True)
        if spec["task"] == "time":
            _time(spark, spec, units, os.path.join(spec["output_dir"], "timings.json"))
        elif spec["task"] == "check":
            _check(spark, spec, units, os.path.join(spec["output_dir"], "checks.json"))
        else:
            raise SystemExit(f"unknown task {spec['task']!r}")
    finally:
        spark.stop()
