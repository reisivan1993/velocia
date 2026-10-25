"""The ``external`` runner against a Spark standalone cluster the test starts (not Velocia).

Master and worker run in their own containers on a private Docker network; Velocia submits
from the engine image on that network in client mode. Storage is a directory mounted at the
same path in the worker and the submitting container (``file://``), standing in for S3/HDFS.
"""

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app

pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available"),
]

IMAGE = "velocia/engine-spark-vanilla:3.5.9"
SHARED = "/velocia-shared"
TEST_LABEL = "io.velocia.test=external"


@dataclass
class Standalone:
    network: str
    master_url: str
    rest_url: str
    shared: Path


def _docker(*args: str) -> str:
    out = subprocess.run(["docker", *args], capture_output=True, text=True, check=True)
    return out.stdout.strip()


def _wait_for_worker(rest_url: str, timeout_s: float = 90) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{rest_url}/json/", timeout=5) as response:
                if json.load(response).get("aliveworkers", 0) >= 1:
                    return
        except OSError:
            pass
        time.sleep(1)
    raise TimeoutError("standalone worker did not register")


@pytest.fixture
def standalone(tmp_path: Path) -> Iterator[Standalone]:
    suffix = uuid.uuid4().hex[:8]
    network, master = f"velocia-ext-{suffix}", f"velocia-ext-master-{suffix}"
    shared = tmp_path / "shared"
    (shared / "eventlog").mkdir(parents=True)
    user = (
        ["--user", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp"]
        if sys.platform == "linux"
        else []
    )
    common = ["run", "-d", "--rm", "--label", TEST_LABEL, "--network", network, *user]
    _docker("network", "create", network)
    try:
        _docker(
            *common,
            "--name",
            master,
            "-p",
            "127.0.0.1::8080",
            IMAGE,
            "/opt/spark/bin/spark-class",
            "org.apache.spark.deploy.master.Master",
            "--host",
            master,
        )
        _docker(
            *common,
            "--name",
            f"velocia-ext-worker-{suffix}",
            "-e",
            "SPARK_WORKER_DIR=/tmp/spark-work",
            "-v",
            f"{shared}:{SHARED}",
            IMAGE,
            "/opt/spark/bin/spark-class",
            "org.apache.spark.deploy.worker.Worker",
            "--cores",
            "2",
            "--memory",
            "2g",
            f"spark://{master}:7077",
        )
        port = _docker("port", master, "8080").splitlines()[0].rsplit(":", 1)[1]
        rest_url = f"http://127.0.0.1:{port}"
        _wait_for_worker(rest_url)
        yield Standalone(network, f"spark://{master}:7077", rest_url, shared)
    finally:
        ids = _docker("ps", "-aq", "--filter", f"label={TEST_LABEL}").split()
        if ids:
            subprocess.run(["docker", "rm", "-f", *ids], capture_output=True, check=False)
        subprocess.run(["docker", "network", "rm", network], capture_output=True, check=False)


EXPERIMENT = """
name: ext
workload: {{kind: sql, suite: tpch, scale_factor: 0.01, units: [q1, q3, q6]}}
engines: [spark-vanilla]
reference_engine: spark-vanilla
cluster:
  runner: external
  master: "{master}"
  executors: 1
  executor_cores: 2
  executor_memory: 1g
  driver_memory: 1g
  hardware_profile: apu-node
  data_uri: "file://{shared}/data"
  eventlog_uri: "{eventlog_uri}"
  rest_url: "{rest_url}"
  docker_network: {network}
  docker_args: ["-v", "{host_shared}:{shared}"]
measures: [wall_time, task_cpu_time, node_hours]
repetitions: 2
modes: [cold]
timeout_per_unit: 5m
"""


def _experiment(cluster: Standalone, eventlog_uri: str, rest_url: str) -> str:
    return EXPERIMENT.format(
        master=cluster.master_url,
        shared=SHARED,
        eventlog_uri=eventlog_uri,
        rest_url=rest_url,
        network=cluster.network,
        host_shared=cluster.shared,
    )


def _engine_check(cli: CliRunner) -> None:
    args = ["engines", "check", "spark-vanilla", "--cluster", "default", "--experiment", "exp.yaml"]
    check = cli.invoke(app, args)
    assert check.exit_code == 0, check.output
    assert "spark-vanilla: ok" in check.output


def test_external_standalone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, standalone: Standalone
) -> None:
    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    assert cli.invoke(app, ["datagen", "--suite", "tpch", "--sf", "0.01"]).exit_code == 0
    (tmp_path / "hardware").mkdir()
    (tmp_path / "hardware" / "apu-node.yaml").write_text(
        "name: apu-node\naccelerators_per_node: 1\naccelerator_model: test-apu\n"
    )
    (tmp_path / "exp.yaml").write_text(
        _experiment(standalone, f"file://{SHARED}/eventlog", standalone.rest_url)
    )
    refused = cli.invoke(app, ["run", "exp.yaml"])
    assert refused.exit_code != 0 and "no passing smoke test" in refused.output
    _engine_check(cli)
    result = cli.invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    assert "Staging dataset tpch-sf0.01" in result.output
    assert "quiet check flagged" not in result.output

    # The data went to the cluster's storage and was verified; scratch is gone.
    data = standalone.shared / "data"
    staged = data / "tpch-sf0.01-parquet-zstd"
    assert (staged / "_velocia_staged.json").is_file()
    assert not (data / "_scratch").exists() or not any((data / "_scratch").iterdir())

    results = tmp_path / "velocia-results"
    runs = sorted((results / "runs").iterdir())
    assert len(runs) == 3  # two cold repetitions + correctness pass
    for run in runs:
        manifest = json.loads((run / "manifest.json").read_text())
        assert manifest["status"] == "finished"
        assert manifest["spark"]["master"] == standalone.master_url
        node = manifest["hardware"]["node_types"][0]
        assert node["source"] == "declared" and node["accelerators"]["model"] == "test-apu"
        assert manifest["notes"] == []
        assert list((run / "eventlog").iterdir()), "event log fetched from eventlog_uri"
    cold = json.loads(
        next(r for r in runs if r.name.endswith("-r1")).joinpath("manifest.json").read_text()
    )
    assert cold["mode_label"] == "cold (cache not dropped)"

    report = json.loads(next((results / "reports").iterdir()).joinpath("report.json").read_text())
    cpu = [r for r in report["results"] if r["measure"] == "task_cpu_time"]
    assert cpu and all(r["n"] == 2 and r["median"] > 0 for r in cpu)
    assert all(r["correctness"] != "fail" for r in report["results"])

    # Second run: the data is already staged (not copied again); the REST API is unreachable.
    (tmp_path / "exp.yaml").write_text(
        _experiment(standalone, f"file://{SHARED}/eventlog", "http://127.0.0.1:9")
    )
    marker_mtime = (staged / "_velocia_staged.json").stat().st_mtime
    refused = cli.invoke(app, ["run", "exp.yaml"])  # a changed cluster needs a new check
    assert refused.exit_code != 0 and "no passing smoke test" in refused.output
    _engine_check(cli)
    result = cli.invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    assert (staged / "_velocia_staged.json").stat().st_mtime == marker_mtime
    assert "quiet check skipped (http://127.0.0.1:9 unreachable" in result.output


TERA = """
name: ext-tera
workload: {{kind: job, suite: tera, rows: 100000}}
engines: [spark-vanilla]
reference_engine: spark-vanilla
cluster:
  runner: external
  master: "{master}"
  executors: 1
  executor_cores: 2
  executor_memory: 1g
  data_uri: "file://{shared}/data"
  eventlog_uri: "file://{shared}/eventlog"
  docker_network: {network}
  docker_args: ["-v", "{host_shared}:{shared}"]
measures: [wall_time]
repetitions: 1
modes: [cold]
timeout_per_unit: 5m
"""


def test_external_terasort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, standalone: Standalone
) -> None:
    """Job output goes to the cluster's storage, is validated there, then removed."""
    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    gen = cli.invoke(app, ["datagen", "--suite", "tera", "--rows", "100000"])
    assert gen.exit_code == 0, gen.output
    (tmp_path / "exp.yaml").write_text(
        TERA.format(
            master=standalone.master_url,
            shared=SHARED,
            network=standalone.network,
            host_shared=standalone.shared,
        )
    )
    _engine_check(cli)
    result = cli.invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output
    assert "correctness pass: 1/1 ok" in result.output
    report_dir = next((tmp_path / "velocia-results" / "reports").iterdir())
    report = json.loads((report_dir / "report.json").read_text())
    assert {r["correctness"] for r in report["results"]} == {"pass"}
    scratch = standalone.shared / "data" / "_scratch"
    assert not scratch.exists() or not any(scratch.iterdir())
