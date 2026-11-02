"""Nightly (T42): the external runner with S3 storage (s3a) on a cluster the test starts.

A Spark standalone master and worker plus SeaweedFS's S3 gateway (MinIO no longer publishes
images), all in Docker. Spark reaches S3 through hadoop-aws, fetched by spark-submit
(--packages) into a cache kept between runs. The S3 keys are in the cluster's submit_args,
so this also checks that they are redacted everywhere.
"""

import json
import os
import subprocess
import urllib.request
from pathlib import Path

import pytest
from conftest import SECRET_MARKER
from test_external import IMAGE, Standalone, _docker, _engine_check, standalone  # noqa: F401
from typer.testing import CliRunner

from velocia.cli.app import app

pytestmark = pytest.mark.slow  # nightly: downloads hadoop-aws and the AWS SDK on a cold cache

SEAWEEDFS = (
    "chrislusf/seaweedfs:4.45@sha256:"
    "fc9f76fa993ad69966ffeb2f65d0318fcae39c6f8e20cf68ef7b3a5cb97769e5"
)
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"  # matches Apache Spark 3.5's Hadoop client
IVY_CACHE = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "velocia-test-ivy"
SECRET = f"{SECRET_MARKER}-s3"

EXPERIMENT = """
name: ext-s3
workload: {{kind: sql, suite: tpch, scale_factor: 0.01, units: [q1, q6]}}
engines: [spark-vanilla]
reference_engine: spark-vanilla
cluster:
  runner: external
  master: "{master}"
  executors: 1
  executor_cores: 2
  executor_memory: 1g
  data_uri: "s3a://bench/data"
  eventlog_uri: "s3a://bench/eventlog"
  rest_url: "{rest_url}"
  docker_network: {network}
  docker_args: ["-v", "{ivy}:/tmp/ivy2"]
  # Storage settings belong to the cluster (the engine check needs them too).
  submit_args: ["--packages", "{hadoop_aws}",
                "--conf", "spark.hadoop.fs.s3a.endpoint=http://{s3}:8333",
                "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
                "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
                "--conf", "spark.hadoop.fs.s3a.access.key=velocia",
                "--conf", "spark.hadoop.fs.s3a.secret.key={secret}"]
measures: [wall_time, task_cpu_time]
repetitions: 2
modes: [cold]
timeout_per_unit: 10m
"""


def _put(url: str, data: bytes = b"") -> None:
    request = urllib.request.Request(url, data=data, method="PUT")
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status in (200, 204), response.status


@pytest.mark.docker
def test_external_with_s3(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    standalone: Standalone,  # noqa: F811
) -> None:
    name = f"velocia-ext-s3-{tmp_path.name[-8:].replace('_', '-')}"
    # Spark signs its requests, so the gateway needs the test's key; unsigned requests (the
    # test's own bucket setup) go to SeaweedFS's "anonymous" identity.
    s3_config = tmp_path / "s3.json"
    actions = ["Admin", "Read", "Write", "List", "Tagging"]
    s3_config.write_text(
        json.dumps(
            {
                "identities": [
                    {"name": "anonymous", "actions": actions},
                    {
                        "name": "velocia",
                        "credentials": [{"accessKey": "velocia", "secretKey": SECRET}],
                        "actions": actions,
                    },
                ]
            }
        )
    )
    _docker(
        "run", "-d", "--rm", "--label", "io.velocia.test=external", "--network",
        standalone.network, "--name", name, "-p", "127.0.0.1::8333",
        "-v", f"{s3_config}:/etc/seaweedfs/s3.json:ro", SEAWEEDFS,
        "server", "-dir=/data", "-s3", "-s3.port=8333", "-s3.config=/etc/seaweedfs/s3.json",
        "-master.volumeSizeLimitMB=64",
    )  # fmt: skip
    port = _docker("port", name, "8333").splitlines()[0].rsplit(":", 1)[1]
    endpoint = f"http://127.0.0.1:{port}"
    for _ in range(60):  # the S3 gateway starts after the master and volume server
        try:
            _put(f"{endpoint}/bench")
            break
        except OSError:
            subprocess.run(["sleep", "1"], check=True)
    _put(f"{endpoint}/bench/eventlog/")  # Spark wants the event-log "directory" to exist
    IVY_CACHE.mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(tmp_path)
    cli = CliRunner()
    assert cli.invoke(app, ["datagen", "--suite", "tpch", "--sf", "0.01"]).exit_code == 0
    (tmp_path / "exp.yaml").write_text(
        EXPERIMENT.format(
            master=standalone.master_url,
            rest_url=standalone.rest_url,
            network=standalone.network,
            ivy=IVY_CACHE,
            hadoop_aws=HADOOP_AWS,
            s3=name,
            secret=SECRET,
        )
    )
    _engine_check(cli)
    result = cli.invoke(app, ["run", "exp.yaml"])
    assert result.exit_code == 0, result.output

    results = tmp_path / "velocia-results"
    for run in sorted((results / "runs").iterdir()):
        manifest = json.loads((run / "manifest.json").read_text())
        assert manifest["status"] == "finished" and manifest["notes"] == []
        assert list((run / "eventlog").iterdir()), "event log fetched from s3a"
    report = json.loads(next((results / "reports").iterdir()).joinpath("report.json").read_text())
    cpu = [r for r in report["results"] if r["measure"] == "task_cpu_time"]
    assert cpu and all(r["n"] == 2 for r in cpu)
    for path in results.rglob("*"):
        if path.is_file() and path.suffix in (".json", ".md", ".html", ".log", ""):
            assert SECRET not in path.read_text(errors="replace"), path
