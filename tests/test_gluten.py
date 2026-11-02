"""Apache Gluten (Velox) as the second accelerated engine (T27).

Gluten's official build is linux/amd64 only, so this runs on x86 machines (CI), not on arm64
Docker (Apple silicon), where Velocia refuses the engine up front.
"""

import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.hardware.probe import docker_platform

pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available"),
    pytest.mark.skipif(
        shutil.which("docker") is not None and docker_platform() != "linux/amd64",
        reason="Gluten's native build needs linux/amd64 Docker",
    ),
]

EXPERIMENT = """
name: gluten-sf1
workload: {kind: sql, suite: tpch, scale_factor: 1}
engines: [spark-vanilla, spark-gluten]
reference_engine: spark-vanilla
cluster: {runner: local, executors: 1, executor_cores: 2, executor_memory: 3g}
measures: [wall_time]
repetitions: 1
modes: [cold]
timeout_per_unit: 10m
"""


def test_gluten_engine_check_and_tpch_sf1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    check = cli.invoke(app, ["engines", "check", "spark-gluten"])
    assert check.exit_code == 0, check.output

    assert cli.invoke(app, ["datagen", "--suite", "tpch", "--sf", "1"]).exit_code == 0
    (tmp_path / "exp.yaml").write_text(EXPERIMENT)
    result = cli.invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    report_dir = next((tmp_path / "velocia-results" / "reports").iterdir())
    report = json.loads((report_dir / "report.json").read_text())
    gluten = {
        r["unit_id"]: r["correctness"]
        for r in report["results"]
        if r["engine"] == "spark-gluten" and r["measure"] == "wall_time"
    }
    assert len(gluten) == 23  # 22 queries + total
    assert {u: c for u, c in gluten.items() if c != "pass"} == {}
    markers = {c["engine"]: report["plan_markers"][c["cell_key"]] for c in report["cells"]}
    assert markers["spark-gluten"]["accelerated"] > 0
