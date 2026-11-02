"""PageRank and KMeans (T29, T30): iterative floating-point jobs checked within a tolerance."""

import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.orchestrator.run import _correctness, within_tolerance


def test_tolerance_rule() -> None:
    assert within_tolerance([1.0, 1000.0], [1.0 + 1e-9, 1000.0 + 1e-4], 1e-6)
    assert not within_tolerance([1.0, 1000.0], [1.0, 1000.01], 1e-6)
    assert not within_tolerance([1.0], [1.0, 2.0], 1e-6)  # different shape
    ok = {"status": "ok", "rows": 8, "checksum": "iterations=7", "tolerance": 1e-6}
    ref = {**ok, "check_values": "[100.0, 2.0]"}
    checks = {
        "ref": {"k": ref},
        "close": {"k": {**ok, "check_values": "[100.00000001, 2.0]"}},
        "far": {"k": {**ok, "check_values": "[100.1, 2.0]"}},
        "iters": {"k": {**ok, "checksum": "iterations=9", "check_values": "[100.0, 2.0]"}},
    }
    assert _correctness(checks, "close", "ref", "k") == "pass"
    assert _correctness(checks, "far", "ref", "k") == "fail"
    assert _correctness(checks, "iters", "ref", "k") == "fail"  # exact parts stay exact


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
@pytest.mark.parametrize(
    ("suite", "rows", "unit"), [("graph", 200_000, "pagerank"), ("kmeans", 200_000, "kmeans-train")]
)
def test_iterative_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, suite: str, rows: int, unit: str
) -> None:
    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    gen = cli.invoke(app, ["datagen", "--suite", suite, "--rows", str(rows)])
    assert gen.exit_code == 0, gen.output
    (tmp_path / "exp.yaml").write_text(
        f"name: {suite}\nworkload: {{kind: job, suite: {suite}, rows: {rows}}}\n"
        "engines: [spark-vanilla, spark-comet]\nreference_engine: spark-vanilla\n"
        "cluster: {runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}\n"
        "measures: [wall_time]\nrepetitions: 1\nmodes: [cold]\ntimeout_per_unit: 10m\n"
    )
    result = cli.invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    results = tmp_path / "velocia-results"
    report = json.loads(next((results / "reports").iterdir()).joinpath("report.json").read_text())
    verdicts = {(r["engine"], r["unit_id"]): r["correctness"] for r in report["results"]}
    assert verdicts[("spark-vanilla", unit)] == "pass"
    assert verdicts[("spark-comet", unit)] == "pass"
    check = json.loads(
        next((results / "runs").glob("*-c001-check")).joinpath("checks.json").read_text()
    )
    entry = check["units"][0]
    assert entry["tolerance"] == 1e-6 and len(entry["values"]) > 1
