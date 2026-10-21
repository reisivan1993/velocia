"""Unit tests for the run pipeline pieces that need no Docker."""

import json
from pathlib import Path
from typing import Any

import pytest
from conftest import PlantedSecrets

from velocia.collect.collector import collect
from velocia.collect.redaction import Redactor
from velocia.config.models import HardwareProfile, LocalCluster
from velocia.families.spark.package import build_driver
from velocia.hardware.probe import hardware_manifest_hash
from velocia.measures.builtins import BUILTINS, ClusterInputs, UnitInputs
from velocia.report.build import ratio_text, render_markdown
from velocia.runners.base import AppResult, AppSpec
from velocia.runners.local import DATA_MOUNT, docker_command, driver_memory, driver_spec
from velocia.store.catalog import Catalog

CLUSTER = LocalCluster(runner="local", executors=2, executor_cores=3, executor_memory="2g")


def _app(tmp_path: Path, **kw: Any) -> AppSpec:
    defaults: dict[str, Any] = dict(
        run_id="exp-c001-r1", image="velocia/engine-spark-vanilla:3.5.9",
        spark_conf={"spark.sql.adaptive.enabled": "true"}, driver=build_driver(tmp_path / "d"),
        task="time", mode="cold", reps=[1], warmup=False, units=[{"id": "q1", "sql": "select 1"}],
        dataset_location=Path("relative/data"), tables=["lineitem"], timeout_per_unit_s=60,
        app_timeout_s=600,
    )  # fmt: skip
    return AppSpec(**{**defaults, **kw})


# --- local runner -------------------------------------------------------------------------


def test_docker_command(tmp_path: Path) -> None:
    app = _app(tmp_path)
    cmd = docker_command(app, CLUSTER, tmp_path / "work")
    joined = " ".join(cmd)
    assert cmd[:3] == ["docker", "run", "--rm"]
    assert "--name velocia-exp-c001-r1" in joined
    assert "--label io.velocia.run_id=exp-c001-r1" in joined
    assert "--cpus 6" in joined and "--master local[6]" in joined
    # Mounts are absolute even when the dataset path is relative.
    assert f"{Path('relative/data').resolve()}:{DATA_MOUNT}:ro" in cmd
    assert "spark.sql.adaptive.enabled=true" in cmd
    assert "spark.eventLog.enabled=true" in cmd
    assert "spark.driver.memory=4096m" in cmd
    assert cmd[-2:] == ["--spec", "/velocia/work/run.json"]
    assert driver_memory(CLUSTER) == "4096m"


def test_driver_spec(tmp_path: Path) -> None:
    spec = driver_spec(_app(tmp_path, mode="warm", reps=[1, 2], warmup=True))
    assert spec["version"] == 1 and spec["warmup"] is True and spec["reps"] == [1, 2]
    assert spec["tables"] == {"lineitem": f"{DATA_MOUNT}/lineitem"}


# --- collector: planted secrets never reach the results directory --------------------------


def test_collector_redacts_everything_it_writes(
    tmp_path: Path, planted_secrets: PlantedSecrets
) -> None:
    work = tmp_path / "work"
    (work / "out").mkdir(parents=True)
    (work / "eventlog").mkdir()
    timings = {"units": [{"unit_id": "q1", "error": "conn password=PLANTED-SECRET-x failed"}]}
    (work / "out" / "timings.json").write_text(json.dumps(timings))
    job_start = {"Event": "SparkListenerJobStart", "Properties": planted_secrets.spark_conf}
    lines = [
        planted_secrets.event_log_line,
        json.dumps(job_start),
        "not json TOKEN=PLANTED-SECRET-y",
    ]
    (work / "eventlog" / "local-1").write_text("\n".join(lines) + "\n")
    result = AppResult("finished", 0, work, f"--conf a.secret={'PLANTED-SECRET-z'}", 0, 1, False)
    run_dir = tmp_path / "results" / "runs" / "r1"
    output = collect(result, run_dir, Redactor(planted_secrets.user_patterns))
    planted_secrets.assert_absent(tmp_path / "results")
    assert not work.exists()  # raw outputs are removed
    assert output["units"][0]["unit_id"] == "q1"
    assert (run_dir / "eventlog" / "local-1").read_text().count("\n") == 3


# --- measures ------------------------------------------------------------------------------


def test_builtin_measures() -> None:
    profile = HardwareProfile(
        name="apu", price_per_node_hour_usd=3.6, watts_per_node=500, accelerators_per_node=2
    )
    c = ClusterInputs(nodes=2, executors=2, executor_cores=4, profile=profile,
                      default_node_hour_usd=0.4)  # fmt: skip
    u = UnitInputs(wall_s=1800)
    assert BUILTINS["wall_time"].compute(u, c) == (1800, None)
    assert BUILTINS["node_hours"].compute(u, c) == (1.0, None)
    assert BUILTINS["accelerator_hours"].compute(u, c) == (2.0, None)
    assert BUILTINS["cost_usd"].compute(u, c) == (pytest.approx(3.6), None)
    assert BUILTINS["energy_kwh"].compute(u, c) == (0.5, None)
    value, reason = BUILTINS["task_cpu_time"].compute(u, c)
    assert value is None and reason and "not available" in reason
    no_profile = ClusterInputs(1, 1, 4, None, 0.4)
    assert BUILTINS["cost_usd"].compute(u, no_profile)[0] == pytest.approx(0.2)
    assert BUILTINS["accelerator_hours"].compute(u, no_profile)[0] is None
    assert BUILTINS["wall_time"].compute(UnitInputs(None), c) == (None, "unit did not finish")


# --- hardware hash ---------------------------------------------------------------------------


def _hw(**host: Any) -> dict[str, Any]:
    base = {"cpu_model": "M4", "arch": "arm64", "cores": 14, "memory_gib": 48}
    node = {
        "name": "local",
        "count": 1,
        "host": {**base, **host},
        "docker": {"cpus": 14, "memory_gib": 8, "server_version": "29"},
        "disk_type": None,
        "accelerators": None,
        "source": "detected",
    }
    return {"node_types": [node]}


def test_hardware_hash_ignores_irrelevant_facts() -> None:
    assert hardware_manifest_hash(_hw()) == hardware_manifest_hash(_hw(hostname="other"))
    changed = _hw()
    changed["node_types"][0]["docker"]["server_version"] = "30"
    assert hardware_manifest_hash(changed) == hardware_manifest_hash(_hw())
    assert hardware_manifest_hash(_hw(cpu_model="Xeon")) != hardware_manifest_hash(_hw())


# --- catalog and report -------------------------------------------------------------------


def test_catalog_round_trip_and_forget(tmp_path: Path) -> None:
    with Catalog(tmp_path) as cat:
        cat.insert("run", {"run_id": "r1", "experiment_id": "e", "cell_key": "c", "status": "x"})
        cat.insert_many("unit_run", [{"run_id": "r1", "unit_id": "q1", "rep": 1}])
        cat.update("run", {"run_id": "r1"}, {"status": "finished"})
        assert cat.query("SELECT status FROM run")[0]["status"] == "finished"
        cat.forget_cell("e", "c")
        assert cat.query("SELECT count(*) AS n FROM unit_run")[0]["n"] == 0
        cat.forget_cell("e", "nothing")  # no runs: no error


def _result(**kw: Any) -> dict[str, Any]:
    base = {"ratio": 1.4, "ci_low": 1.3, "ci_high": 1.5, "correctness": "pass"}
    return {**base, **kw}


def test_ratio_text_never_bare() -> None:
    assert ratio_text(_result()) == "1.40x [95% CI 1.30-1.50]"
    assert ratio_text(_result(correctness="fail")) == "FAILED (correctness)"
    assert ratio_text(_result(ci_low=None)) == "1.40x (CI n/a)"
    assert ratio_text(_result(ratio=None)) == "n/a"


def test_markdown_marks_failed_units_and_labels_tpc() -> None:
    row = {"config_hash": "h", "mode": "cold", "measure": "wall_time", "measure_unit": "s",
           "direction": "lower", "n": 5, "median": 1.0, "cv": 0.01, "cv_flag": False}  # fmt: skip
    report = {
        "experiment": {
            "id": "e1",
            "reference_engine": "v",
            "repetitions": 5,
            "bootstrap_seed": 1,
        },
        "label": "Data derived from TPC-H; not an official TPC result.",
        "cross_hardware": False,
        "cells": [
            {"cell_key": "a", "engine": "v", "mode_label": "cold", "config": {}},
            {"cell_key": "b", "engine": "c", "mode_label": "cold", "config": {}},
        ],
        "measures": ["wall_time"],
        "results": [
            {
                **row,
                **_result(ratio=None, ci_low=None, ci_high=None),
                "cell_key": "a",
                "engine": "v",
                "unit_id": "q1",
            },
            {**row, **_result(correctness="fail"), "cell_key": "b", "engine": "c", "unit_id": "q1"},
            {**row, **_result(), "cell_key": "b", "engine": "c", "unit_id": "_total"},
            {
                **row,
                **_result(ratio=None, ci_low=None, ci_high=None),
                "cell_key": "a",
                "engine": "v",
                "unit_id": "_total",
            },
        ],
        "flags": [],
        "hardware_manifests": {},
    }
    md = render_markdown(report)
    assert "derived from TPC-H; not an official TPC result" in md
    assert "| q1 | 1.000 s | 1.000 s | FAILED (correctness) |" in md
    assert "speedup 1.40x [95% CI 1.30-1.50]" in md


# --- results computation --------------------------------------------------------------------


def test_compute_results_correctness_ratios_and_total(tmp_path: Path) -> None:
    from velocia.config import VelociaConfig
    from velocia.config.models import Experiment
    from velocia.experiment.expander import expand
    from velocia.orchestrator.run import Prepared, compute_results
    from velocia.workloads import Unit

    experiment = Experiment.model_validate({
        "name": "t", "workload": {"kind": "sql", "suite": "tpch", "scale_factor": 0.1},
        "engines": ["spark-vanilla", "spark-comet"], "reference_engine": "spark-vanilla",
        "cluster": {"runner": "local", "executors": 1, "executor_cores": 2,
                    "executor_memory": "1g"},
        "measures": ["wall_time"], "repetitions": 5, "modes": ["cold"],
    })  # fmt: skip
    cells = expand(experiment)
    ref, cand = cells
    prepared = Prepared(experiment, "", "h", {}, [Unit("q1", "sql"), Unit("q2", "sql")],  # type: ignore[arg-type]
                        None, [BUILTINS["wall_time"]], {"default": None})  # type: ignore[arg-type]  # fmt: skip
    with Catalog(tmp_path) as cat:
        cat.insert("experiment", {"experiment_id": "e", "bootstrap_seed": 1})
        for cell in cells:
            cat.insert("cell", {"experiment_id": "e", "cell_key": cell.cell_key,
                                "hardware_manifest_hash": "hw"})  # fmt: skip
        walls = {ref.cell_key: (2000, 1000), cand.cell_key: (1000, 500)}
        for cell in cells:
            for rep in range(1, 6):
                run_id = f"{cell.cell_key}-r{rep}"
                cat.insert("run", {"run_id": run_id, "experiment_id": "e",
                                   "cell_key": cell.cell_key})  # fmt: skip
                q1, q2 = walls[cell.cell_key]
                status_q2 = "timeout" if (cell is cand and rep == 5) else "ok"
                cat.insert_many("unit_run", [
                    {"run_id": run_id, "unit_id": "q1", "rep": rep, "wall_ms": q1,
                     "status": "ok", "discarded": False},
                    {"run_id": run_id, "unit_id": "q2", "rep": rep,
                     "wall_ms": q2 if status_q2 == "ok" else None, "status": status_q2,
                     "discarded": False},
                ])  # fmt: skip
            check_run = f"{cell.cell_key}-check"
            cat.insert("run", {"run_id": check_run, "experiment_id": "e",
                               "cell_key": cell.cell_key})  # fmt: skip
            wrong = cell is cand
            cat.insert_many("unit_check", [
                {"run_id": check_run, "cell_key": cell.cell_key, "unit_id": "q1", "rows": 4,
                 "checksum": "111", "status": "ok"},
                {"run_id": check_run, "cell_key": cell.cell_key, "unit_id": "q2", "rows": 4,
                 "checksum": "999" if wrong else "222", "status": "ok"},
            ])  # fmt: skip
        compute_results(cat, "e", prepared, cells, VelociaConfig())
        rows = {(r["engine"], r["unit_id"]): r for r in cat.query("SELECT * FROM result")}

    q1 = rows[("spark-comet", "q1")]
    assert q1["ratio"] == 2.0 and q1["correctness"] == "pass" and q1["n"] == 5
    assert q1["ci_low"] == 2.0 and q1["ci_high"] == 2.0  # no variation
    q2 = rows[("spark-comet", "q2")]
    assert q2["correctness"] == "fail" and q2["timeouts"] == 1 and q2["n"] == 4
    total = rows[("spark-comet", "_total")]
    assert total["correctness"] == "fail"
    assert total["n"] == 4  # the repetition with a timeout has no total
    assert total["ratio"] == 2.0
    assert rows[("spark-vanilla", "q1")]["correctness"] == "pass"
    assert rows[("spark-vanilla", "q1")]["ratio"] is None  # the reference has no ratio
