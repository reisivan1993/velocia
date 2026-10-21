"""End-to-end on Docker: datagen, run (vanilla vs Comet), report. Needs both engine images."""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import PlantedSecrets
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.runners.local import LABEL

pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available"),
]

EXPERIMENT = """
name: e2e
workload: {{kind: sql, suite: tpch, scale_factor: 0.01, units: [q1, q3, q6, q13]}}
engines: [spark-vanilla, spark-comet]
reference_engine: spark-vanilla
cluster: {{runner: local, executors: 1, executor_cores: 2, executor_memory: 2g,
          hardware_profile: laptop}}
spark_conf: {{spark.hadoop.fs.s3a.secret.key: "{secret}"}}
measures: [wall_time, task_cpu_time, throughput_rows, cost_usd, vendor_cu]
repetitions: {reps}
modes: {modes}
timeout_per_unit: 5m
"""


def _measure_and_profile(root: Path) -> None:
    (root / "measures").mkdir()
    (root / "measures" / "vendor_cu.yaml").write_text(
        "name: vendor_cu\nunit: CU\ndirection: lower\n"
        "formula: unit.wall_s / 3600 * cluster.nodes * hw.cu_per_node_hour\n"
    )
    (root / "hardware").mkdir()
    (root / "hardware" / "laptop.yaml").write_text(
        "name: laptop\nprice_per_node_hour_usd: 0.1\ncustom: {cu_per_node_hour: 2.0}\n"
    )


def _velocia_containers() -> list[str]:
    out = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"label={LABEL}", "--format", "{{.Names}}"],
        capture_output=True, text=True, check=True,
    )  # fmt: skip
    return out.stdout.split()


def test_full_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planted_secrets: PlantedSecrets
) -> None:
    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    assert cli.invoke(app, ["datagen", "--suite", "tpch", "--sf", "0.01"]).exit_code == 0
    (tmp_path / "exp.yaml").write_text(
        EXPERIMENT.format(secret="PLANTED-SECRET-e2e", reps=2, modes="[cold, warm]")
    )
    _measure_and_profile(tmp_path)
    result = cli.invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    assert "spark-comet vs spark-vanilla (cold (cache not dropped))" in result.output
    assert "correctness pass" in result.output
    assert "[95% CI" in result.output

    results = tmp_path / "velocia-results"
    planted_secrets.assert_absent(results)
    report_dir = next((results / "reports").iterdir())
    report = json.loads((report_dir / "report.json").read_text())
    assert report["format"] == "velocia-report" and report["version"] == 1
    assert report["label"] == "Data derived from TPC-H; not an official TPC result."
    comet_totals = [
        r for r in report["results"]
        if r["engine"] == "spark-comet" and r["unit_id"] == "_total" and r["measure"] == "wall_time"
    ]  # fmt: skip
    assert len(comet_totals) == 2  # cold and warm
    assert all(r["correctness"] == "pass" and r["n"] == 2 for r in comet_totals)

    by_key = {(r["engine"], r["mode"], r["unit_id"], r["measure"]): r for r in report["results"]}
    wall = by_key[("spark-comet", "cold", "q1", "wall_time")]
    cu = by_key[("spark-comet", "cold", "q1", "vendor_cu")]
    assert cu["n"] == 2 and cu["median"] == pytest.approx(wall["median"] / 3600 * 2.0)
    assert cu["ci_low"] is not None
    assert any(d["name"] == "vendor_cu" and d["formula"] for d in report["measure_definitions"])
    cpu = [r for r in report["results"] if r["measure"] == "task_cpu_time"]
    assert cpu and all(r["n"] == 2 for r in cpu)
    markers = report["plan_markers"]
    by_engine = {c["engine"]: markers[c["cell_key"]] for c in report["cells"]}
    assert by_engine["spark-vanilla"]["accelerated"] == 0
    assert by_engine["spark-comet"]["accelerated"] > 0
    assert any("task_cpu_time counts Spark task threads only" in f for f in report["flags"])

    runs = sorted((results / "runs").iterdir())
    assert len(runs) == 2 * (2 + 1) + 2 * (1 + 1)  # cold: reps + check; warm: 1 app + check
    for run in runs:
        manifest = json.loads((run / "manifest.json").read_text())
        assert manifest["hardware_manifest_hash"].startswith("sha256:")
        assert manifest["engine"]["image_id"].startswith("sha256:")
        assert manifest["status"] == "finished"
    warm = next(r for r in runs if r.name.endswith("-warm"))
    timings = json.loads((warm / "timings.json").read_text())
    assert [u["rep"] for u in timings["units"]] == [0] * 4 + [1] * 4 + [2] * 4
    assert all(u["discarded"] == (u["rep"] == 0) for u in timings["units"])

    # Every Spark job in the event logs carries a Velocia tag.
    for run in runs:
        for log in (run / "eventlog").iterdir():
            for line in log.open():
                event = json.loads(line)
                if event["Event"] == "SparkListenerJobStart":
                    assert event["Properties"]["spark.job.description"].startswith("velocia:")

    shown = cli.invoke(app, ["report", report["experiment"]["id"]])
    assert shown.exit_code == 0 and "speedup" in shown.output
    assert not _velocia_containers()


def test_interrupt_leaves_no_containers(tmp_path: Path) -> None:
    env = {**os.environ}
    subprocess.run(
        [sys.executable, "-m", "velocia.cli.app", "datagen", "--suite", "tpch", "--sf", "0.01"],
        cwd=tmp_path, check=True, capture_output=True, env=env,
    )  # fmt: skip
    (tmp_path / "exp.yaml").write_text(EXPERIMENT.format(secret="x", reps=5, modes="[cold]"))
    _measure_and_profile(tmp_path)
    proc = subprocess.Popen(
        [sys.executable, "-m", "velocia.cli.app", "run", "exp.yaml"],
        cwd=tmp_path, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )  # fmt: skip
    try:
        deadline = time.time() + 120
        while not _velocia_containers():
            assert time.time() < deadline, "no container started"
            assert proc.poll() is None, "velocia run exited early"
            time.sleep(0.5)
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()
    time.sleep(1)
    assert not _velocia_containers()
