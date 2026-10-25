"""``external`` runner pieces that need no cluster: command, run.json, labels, hardware."""

import json
from pathlib import Path
from typing import Any

import pytest

from velocia.config import VelociaConfig, load_model
from velocia.config.models import Experiment, ExternalCluster, HardwareProfile
from velocia.engines import cluster_checks
from velocia.engines.registry import load_engines
from velocia.families.spark.package import build_driver
from velocia.hardware.probe import hardware_manifest_hash
from velocia.orchestrator.run import RunError, prepare
from velocia.runners.base import AppSpec
from velocia.runners.external import (
    ExternalRunner,
    container_name,
    declared_hardware,
    fetch_notes,
)


def _cluster(**kw: Any) -> ExternalCluster:
    defaults: dict[str, Any] = dict(
        runner="external",
        master="spark://apu-host:7077",
        executors=2,
        executor_cores=8,
        executor_memory="32g",
        data_uri="s3a://bench/velocia/",
        eventlog_uri="s3a://bench/velocia/eventlog/",
        submit_args=["--packages", "org.apache.hadoop:hadoop-aws:3.3.4"],
    )
    return ExternalCluster(**{**defaults, **kw})


def _app(tmp_path: Path, **kw: Any) -> AppSpec:
    defaults: dict[str, Any] = dict(
        run_id="exp-c002-r1",
        image="vendor/spark-apu:1.0",
        spark_conf={"spark.plugins": "com.vendor.ApuPlugin"},
        driver=build_driver(tmp_path / "d"),
        task="time",
        mode="cold",
        reps=[1],
        warmup=False,
        units=[{"id": "q1", "sql": "select 1"}],
        dataset_location=tmp_path / "data",
        tables=["lineitem"],
        timeout_per_unit_s=60,
        app_timeout_s=600,
        dataset_id="tpch-sf10-parquet-zstd",
    )
    return AppSpec(**{**defaults, **kw})


def test_submit_from_engine_image(tmp_path: Path) -> None:
    runner = ExternalRunner(_cluster())
    cmd = runner.submit_command(_app(tmp_path), tmp_path / "work", "velocia-x")
    joined = " ".join(cmd)
    assert cmd[:3] == ["docker", "run", "--rm"] and "--network host" in joined
    assert "vendor/spark-apu:1.0 /opt/spark/bin/spark-submit" in joined
    assert "--master spark://apu-host:7077 --deploy-mode client" in joined
    for conf in (
        "spark.plugins=com.vendor.ApuPlugin",
        "spark.executor.instances=2",
        "spark.cores.max=16",
        "spark.executor.memory=32g",
        "spark.eventLog.dir=s3a://bench/velocia/eventlog/",
        "spark.dynamicAllocation.enabled=false",
    ):
        assert conf in cmd
    assert "spark.driver.host" not in joined  # host network: the real hostname works
    assert "--packages org.apache.hadoop:hadoop-aws:3.3.4 --py-files" in joined
    assert "/velocia/data" not in joined  # the dataset is only mounted to stage it
    assert cmd[-2:] == ["--spec", "/velocia/work/run.json"]


def test_submit_on_docker_network_and_from_spark_home(tmp_path: Path) -> None:
    runner = ExternalRunner(_cluster(docker_network="bench", docker_args=["-v", "/a:/b"]))
    cmd = runner.submit_command(_app(tmp_path, task="stage"), tmp_path / "w", "velocia-x")
    assert "spark.driver.host=velocia-x" in cmd and "-v" in cmd and "/a:/b" in cmd
    assert f"{(tmp_path / 'data').resolve()}:/velocia/data:ro" in cmd

    home = tmp_path / "spark"
    runner = ExternalRunner(_cluster(spark_home=str(home)))
    assert runner.check() == [f"{home}/bin/spark-submit not found (spark_home)"]
    cmd = runner.submit_command(_app(tmp_path), tmp_path / "w", "velocia-x")
    assert cmd[0] == f"{home}/bin/spark-submit" and "docker" not in cmd
    assert cmd[-1] == f"{tmp_path / 'w'}/run.json"
    assert ExternalRunner(_cluster(deploy_mode="cluster")).check() == [
        "the external runner supports deploy_mode: client only so far"
    ]


def test_run_spec_points_at_cluster_storage(tmp_path: Path) -> None:
    spec = ExternalRunner(_cluster()).run_spec(_app(tmp_path), tmp_path / "w")
    assert spec["tables"] == {"lineitem": "s3a://bench/velocia/tpch-sf10-parquet-zstd/lineitem"}
    assert spec["output_dir"] == "/velocia/work/out"  # client mode: the driver is local
    assert spec["scratch_dir"] == "s3a://bench/velocia/_scratch/exp-c002-r1"
    assert spec["cleanup"] == [spec["scratch_dir"]]
    assert spec["eventlog_fetch"] == {
        "dir": "s3a://bench/velocia/eventlog/",
        "dest": "/velocia/work/eventlog",
    }
    staged = ExternalRunner(_cluster()).run_spec(
        _app(tmp_path, task="stage", stage={"checksum": "c", "files": {}, "tables": {}}),
        tmp_path / "w",
    )
    assert staged["stage"] == {
        "checksum": "c",
        "files": {},
        "tables": {},
        "local_dir": "/velocia/data",
        "remote_dir": "s3a://bench/velocia/tpch-sf10-parquet-zstd",
    }


def test_container_name_is_a_hostname() -> None:
    assert container_name("stage-tpch-sf0.01-parquet-zstd-1") == (
        "velocia-stage-tpch-sf0-01-parquet-zstd-1"
    )
    assert len(container_name("x" * 100)) <= 63


def test_quiet_check_without_rest_url() -> None:
    quiet = ExternalRunner(_cluster()).quiet_check()
    assert not quiet.ok and quiet.reasons == ["quiet check skipped (no rest_url)"]


@pytest.mark.parametrize(
    ("report", "notes"),
    [
        ({"eventlog": "ok", "cleanup_problems": []}, []),
        (
            {"eventlog": "no event log for app-1 in s3a://b/e", "cleanup_problems": ["s3a://x"]},
            [
                "event log not fetched (no event log for app-1 in s3a://b/e); driver timings "
                "only, event-log measures are null",
                "scratch output not removed: s3a://x",
            ],
        ),
    ],
)
def test_fetch_notes(tmp_path: Path, report: dict[str, Any], notes: list[str]) -> None:
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "fetch.json").write_text(json.dumps(report))
    assert fetch_notes(tmp_path) == notes


def test_declared_hardware() -> None:
    apu = HardwareProfile(name="apu-node", accelerators_per_node=2, accelerator_model="APU")
    hw = declared_hardware("apu", _cluster(), apu)
    node = hw["node_types"][0]
    assert node["source"] == "declared" and node["count"] == 2
    assert node["host"] == {"cores": 8, "memory_gib": 32.0}
    assert node["accelerators"] == {"model": "APU", "count": 2, "source": "declared"}
    other = declared_hardware("apu", _cluster(), HardwareProfile(name="cpu-node"))
    assert hardware_manifest_hash(hw) != hardware_manifest_hash(other)


GATED = """
name: gated
workload: {{kind: sql, suite: tpch, scale_factor: 0.01, units: [q1]}}
engines: [spark-vanilla]
reference_engine: spark-vanilla
cluster: {{runner: external, master: "spark://h:7077", executors: {executors}, executor_cores: 1,
          executor_memory: 1g, data_uri: "file:///d", eventlog_uri: "file:///e"}}
measures: [wall_time]
"""


def test_cluster_check_gate(tmp_path: Path) -> None:
    config = VelociaConfig(results_dir=str(tmp_path / "results"))
    exp = tmp_path / "exp.yaml"
    exp.write_text(GATED.format(executors=1))
    with pytest.raises(RunError, match="no passing smoke test on cluster 'default'"):
        prepare(exp, config)
    engine = load_engines(Path(config.engines_dir))["spark-vanilla"]
    cluster = load_model(exp, Experiment).named_clusters["default"]
    cluster_checks.record_pass(tmp_path / "results", engine, "default", cluster, "e1")
    with pytest.raises(RunError, match=r"dataset .* not found"):  # past the gate
        prepare(exp, config)
    exp.write_text(GATED.format(executors=2))  # changed cluster config: check again
    with pytest.raises(RunError, match="no passing smoke test"):
        prepare(exp, config)
    with pytest.raises(RunError, match=r"dataset .* not found"):
        prepare(exp, config, cluster_gate=False)
