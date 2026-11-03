"""The ``k8s`` runner end to end (T20): kind + the Spark Operator, set up by CI (k8s.yml).

Runs only when ``VELOCIA_K8S_TEST=1`` (the workflow sets it after creating the cluster, loading
the engine image, installing the operator and creating the ``velocia`` volume claim).
"""

import json
import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app

pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(os.environ.get("VELOCIA_K8S_TEST") != "1", reason="needs a k8s cluster"),
]

EXPERIMENT = """
name: {name}
workload: {{kind: sql, suite: tpch, scale_factor: 0.01, units: [q1, q3, q6, q13]}}
engines: [spark-vanilla]
reference_engine: spark-vanilla
cluster: {cluster}
measures: [wall_time, task_cpu_time]
repetitions: 2
modes: [cold]
timeout_per_unit: 10m
"""
K8S = "{runner: k8s, executors: 1, executor_cores: 1, executor_memory: 1g, driver_memory: 1g}"
LOCAL = "{runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}"


def _checks(results: Path, name: str) -> dict[str, tuple[int, str]]:
    check = next((results / "runs").glob(f"{name}-*-check"))
    units = json.loads((check / "checks.json").read_text())["units"]
    return {u["unit_id"]: (u["rows"], u["checksum"]) for u in units}


def test_k8s_matches_local(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    assert cli.invoke(app, ["datagen", "--suite", "tpch", "--sf", "0.01"]).exit_code == 0
    for name, cluster in (("on-k8s", K8S), ("on-local", LOCAL)):
        (tmp_path / f"{name}.yaml").write_text(EXPERIMENT.format(name=name, cluster=cluster))
        result = cli.invoke(app, ["run", f"{name}.yaml"])
        assert result.exit_code == 0, result.output

    results = tmp_path / "velocia-results"
    k8s_runs = sorted((results / "runs").glob("on-k8s-*"))
    for run in k8s_runs:
        manifest = json.loads((run / "manifest.json").read_text())
        assert manifest["status"] == "finished", (run.name, (run / "driver.log").read_text())
        assert manifest["hardware"]["node_types"][0]["source"] == "kubernetes"
        expected = "cold" if manifest["task"] == "check" else "cold (cache not dropped)"
        assert manifest["mode_label"] == expected
        assert list((run / "eventlog").iterdir()), "event log copied out of the volume"
    report_dir = next((results / "reports").glob("on-k8s-*"))
    report = json.loads((report_dir / "report.json").read_text())
    cpu = [r for r in report["results"] if r["measure"] == "task_cpu_time"]
    assert cpu and all(r["n"] == 2 for r in cpu)
    # Same data, same engine: the same answers as on the local runner.
    assert _checks(results, "on-k8s") == _checks(results, "on-local")
