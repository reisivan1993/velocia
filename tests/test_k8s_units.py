"""``k8s`` runner pieces that need no cluster: the SparkApplication, hardware, names."""

from pathlib import Path
from typing import Any

import pytest

from velocia.config.models import HardwareProfile, K8sCluster
from velocia.families.spark.package import build_driver
from velocia.runners.base import AppSpec
from velocia.runners.k8s import K8sError, application, application_state, k8s_name, node_hardware

CLUSTER = K8sCluster(
    runner="k8s",
    namespace="bench",
    executors=3,
    executor_cores=4,
    executor_memory="8g",
    node_selector={"pool": "cpu"},
    tolerations=[{"key": "dedicated", "operator": "Equal", "value": "spark"}],
)


def _app(tmp_path: Path, **kw: Any) -> AppSpec:
    defaults: dict[str, Any] = dict(
        run_id="exp-C001-r1",
        image="velocia/engine-spark-vanilla:3.5.9",
        spark_conf={"spark.sql.adaptive.enabled": "true"},
        driver=build_driver(tmp_path / "d"),
        task="time",
        mode="cold",
        reps=[1],
        warmup=False,
        units=[{"id": "q1", "sql": "select 1"}],
        dataset_location=tmp_path,
        tables=["lineitem"],
        timeout_per_unit_s=60,
        app_timeout_s=600,
        dataset_id="tpch-sf1",
    )
    return AppSpec(**{**defaults, **kw})


def test_python_application(tmp_path: Path) -> None:
    app = _app(tmp_path)
    doc = application(app, CLUSTER, "velocia-exp-c001-r1")
    assert doc["apiVersion"] == "sparkoperator.k8s.io/v1beta2"
    assert doc["metadata"] == {
        "name": "velocia-exp-c001-r1",
        "namespace": "bench",
        "labels": {"io.velocia.run_id": "exp-c001-r1"},
    }
    spec = doc["spec"]
    run = "/velocia/runs/exp-C001-r1"
    assert spec["type"] == "Python" and spec["mode"] == "cluster"
    assert spec["mainApplicationFile"] == f"local://{run}/{app.driver.launcher_path.name}"
    assert spec["deps"]["pyFiles"] == [f"local://{run}/{app.driver.zip_path.name}"]
    assert spec["arguments"] == ["--spec", f"{run}/run.json"]
    assert spec["sparkConf"]["spark.eventLog.dir"] == f"file://{run}/eventlog"
    assert spec["sparkConf"]["spark.sql.adaptive.enabled"] == "true"
    assert spec["volumes"][0]["persistentVolumeClaim"] == {"claimName": "velocia"}
    executor = spec["executor"]
    assert (executor["instances"], executor["cores"], executor["memory"]) == (3, 4, "8192m")
    assert executor["nodeSelector"] == {"pool": "cpu"} and executor["tolerations"]
    assert spec["driver"]["serviceAccount"] == "spark"
    assert spec["driver"]["volumeMounts"] == [{"name": "velocia", "mountPath": "/velocia"}]


def test_jar_application_and_devices(tmp_path: Path) -> None:
    (tmp_path / "job.jar").write_bytes(b"PK")
    app = _app(
        tmp_path,
        custom={
            "artifact": str(tmp_path / "job.jar"),
            "entrypoint": "com.example.Main",
            "validator": None,
            "args": '["${table:lineitem}"]',
        },
    )
    apu = CLUSTER.model_copy(update={"resources": {"vendor.com/apu": 1}})
    spec = application(app, apu, "j")["spec"]
    assert spec["type"] == "Java" and spec["mainClass"] == "com.example.Main"
    assert spec["mainApplicationFile"] == "local:///velocia/runs/exp-C001-r1/custom/job.jar"
    assert spec["arguments"] == ["/velocia/data/tpch-sf1/lineitem"]
    assert spec["sparkConf"]["spark.velocia.run_id"] == "exp-C001-r1"
    assert spec["executor"]["gpu"] == {"name": "vendor.com/apu", "quantity": 1}
    two = CLUSTER.model_copy(update={"resources": {"a/x": 1, "b/y": 1}})
    with pytest.raises(K8sError, match="one device type"):
        application(app, two, "j")


def test_node_hardware_and_helpers() -> None:
    def node(pool: str, arch: str = "amd64") -> dict[str, Any]:
        return {
            "metadata": {"labels": {"pool": pool}},
            "status": {
                "nodeInfo": {"architecture": arch},
                "capacity": {"cpu": "16", "memory": "65536000Ki"},
            },
        }

    hw = node_hardware("cpu", CLUSTER, [node("cpu"), node("cpu"), node("gpu")], None)
    assert hw["node_types"] == [
        {
            "name": "cpu",
            "count": 2,
            "profile": None,
            "host": {"arch": "amd64", "cores": 16, "memory_gib": 62},
            "docker": None,
            "disk_type": None,
            "accelerators": None,
            "source": "kubernetes",
        }
    ]
    apu = HardwareProfile(name="apu", accelerators_per_node=2, accelerator_model="APU")
    assert node_hardware("a", CLUSTER, [node("cpu")], apu)["node_types"][0]["accelerators"] == {
        "model": "APU",
        "count": 2,
        "source": "declared",
    }
    assert k8s_name("velocia-Exp_1.2-r1") == "velocia-exp-1-2-r1"
    assert application_state({"status": {"applicationState": {"state": "COMPLETED"}}}) == (
        "COMPLETED"
    )
    assert application_state({}) == ""
