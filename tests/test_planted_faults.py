"""Planted faults (plan T23, docs/05 §3): each is reported as specified.

Uses a test-only engine image (tests/fixtures/engines/spark-faulty) whose Python startup hook
misbehaves according to spark.velocia.test.fault. The cross-hardware fault is a unit test
(test_pipeline_units.py); the unsorted-job-output fault comes with TeraSort (T18).
"""

import json
import shutil
import subprocess
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app

pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available"),
]

IMAGE = "velocia/test-spark-faulty:1"
FIXTURE = Path(__file__).parent / "fixtures" / "engines" / "spark-faulty"
EXPERIMENT = """
name: faults
workload: {kind: sql, suite: tpch, scale_factor: 0.01, units: [q1, q6]}
engines: [spark-vanilla, spark-wrong, spark-noisy, spark-slow]
reference_engine: spark-vanilla
cluster: {runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}
measures: [wall_time]
repetitions: 3
modes: [cold]
timeout_per_unit: 8s
"""


def test_planted_faults_are_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    subprocess.run(["docker", "build", "-q", "-t", IMAGE, str(FIXTURE)], check=True,
                   capture_output=True)  # fmt: skip
    monkeypatch.chdir(tmp_path)
    (tmp_path / "engines").mkdir()
    for fault in ("wrong", "noisy", "slow"):
        (tmp_path / "engines" / f"spark-{fault}.yaml").write_text(
            f"name: spark-{fault}\nimage: {IMAGE}\nspark_versions: ['3.5']\n"
            f"spark_conf: {{spark.velocia.test.fault: {fault}}}\n"
        )
    (tmp_path / "exp.yaml").write_text(EXPERIMENT)
    cli = CliRunner()
    assert cli.invoke(app, ["datagen", "--suite", "tpch", "--sf", "0.01"]).exit_code == 0
    started = time.time()
    result = cli.invoke(app, ["run", "exp.yaml"])
    elapsed = time.time() - started
    assert result.exit_code == 0, result.output

    report_dir = next((tmp_path / "velocia-results" / "reports").iterdir())
    report = json.loads((report_dir / "report.json").read_text())
    rows = {(r["engine"], r["unit_id"]): r for r in report["results"]}
    flags = "\n".join(report["flags"])

    # 1. Wrong results: multi-row q1 fails correctness, single-row q6 still matches; the
    #    total fails, and no ratio is shown for failed units anywhere.
    assert rows[("spark-wrong", "q1")]["correctness"] == "fail"
    assert rows[("spark-wrong", "q6")]["correctness"] == "pass"
    assert rows[("spark-wrong", "_total")]["correctness"] == "fail"
    assert "correctness FAILED: spark-wrong cold q1" in flags
    assert (
        "spark-wrong vs spark-vanilla (cold (cache not dropped)): speedup FAILED" in result.output
    )
    html = (report_dir / "report.html").read_text()
    assert "FAILED (correctness)" in html

    # 2. Noisy cell: high variation is flagged.
    assert rows[("spark-noisy", "q6")]["cv_flag"] or rows[("spark-noisy", "q1")]["cv_flag"]
    assert "high variation (CV > threshold): spark-noisy cold" in flags

    # 3. Timeout: every unit of the slow engine times out, is cancelled, and is reported.
    for unit in ("q1", "q6"):
        assert rows[("spark-slow", unit)]["timeouts"] == 3
        assert rows[("spark-slow", unit)]["n"] == 0
        assert rows[("spark-slow", unit)]["correctness"] == "not_checked"
        assert f"3 timeout(s): spark-slow cold {unit}" in flags
    # Cancellation works: 8 timed-out units at 8 s each, not a run that never ends.
    assert elapsed < 15 * 60
