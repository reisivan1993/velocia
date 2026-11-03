"""The ``k8s`` runner: one Spark Operator ``SparkApplication`` per run (docs/03-spec.md §4).

Driver and executors run as pods in cluster mode. Everything they share with Velocia goes
through one PersistentVolumeClaim (``volume_claim``) mounted at ``/velocia`` in every pod:

    /velocia/data/<dataset_id>/     the dataset, copied in once and marked as verified
    /velocia/runs/<run_id>/         driver program, run.json, out/ and eventlog/

Velocia reaches the volume through a helper pod it starts for the run (``kubectl cp`` in and
out) and always deletes, like the SparkApplications themselves: nothing stays running.
Hardware comes from the Kubernetes API (CPU, memory, architecture of the nodes the pods may
run on) plus the hardware profile; cold mode can't drop node page caches ("cache not
dropped").
"""

import json
import re
import shutil
import subprocess
import tempfile
import time
from collections import deque
from pathlib import Path
from typing import Any

from velocia.config.models import HardwareProfile, K8sCluster
from velocia.datagen.manifest import DatasetManifest
from velocia.families.spark.package import DriverPackage
from velocia.runners.base import AppResult, AppSpec, AppStatus, QuietCheck
from velocia.runners.local import LABEL, driver_spec, host_slept_s, program

MOUNT = "/velocia"
API_VERSION = "sparkoperator.k8s.io/v1beta2"
POLL_S = 3.0
STAGED_MARKER = "_velocia_staged.json"
HELPER_TIMEOUT_S = 120
GIB = 1024**3


class K8sError(RuntimeError):
    pass


def k8s_name(text: str) -> str:
    """A DNS-1123 name (lowercase, digits, '-', at most 63 characters)."""
    return re.sub(r"[^a-z0-9-]+", "-", text.lower()).strip("-")[:63].rstrip("-")


def _memory_mib(size: str) -> int:
    factor = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024}[size[-1].lower()]
    return int(int(size[:-1]) * factor)


def application(
    app: AppSpec, cluster: K8sCluster, name: str, spark_version: str = "3.5.9"
) -> dict[str, Any]:
    """The SparkApplication for one run."""
    run_dir = f"{MOUNT}/runs/{app.run_id}"
    tail = program(app, run_dir, f"{run_dir}/custom", run_dir, f"{MOUNT}/data/{app.dataset_id}")
    conf = {
        **app.spark_conf,
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": f"file://{run_dir}/eventlog",
        "spark.dynamicAllocation.enabled": "false",
        "spark.ui.enabled": "false",
    }
    mounts = [{"name": "velocia", "mountPath": MOUNT}]
    pods = {"volumeMounts": mounts, "labels": {LABEL: k8s_name(app.run_id)}}
    if cluster.node_selector:
        pods["nodeSelector"] = dict(cluster.node_selector)
    if cluster.tolerations:
        pods["tolerations"] = [dict(t) for t in cluster.tolerations]
    # Driver and executors: their own memory plus Spark's default 10% overhead, so the pod
    # requests match what Spark asks for.
    executor: dict[str, Any] = {
        **pods,
        "instances": cluster.executors,
        "cores": cluster.executor_cores,
        "memory": f"{_memory_mib(cluster.executor_memory)}m",
    }
    if len(cluster.resources) > 1:
        raise K8sError("resources: the Spark Operator supports one device type per executor")
    for resource, amount in cluster.resources.items():
        # The operator's executor "gpu" field is a resource limit for any device name.
        executor["gpu"] = {"name": resource, "quantity": amount}
    spec: dict[str, Any] = {
        "type": "Java" if tail[0] == "--conf" else "Python",
        "mode": "cluster",
        "image": app.image,
        "imagePullPolicy": "IfNotPresent",
        "sparkVersion": spark_version,
        "sparkConf": conf,
        "restartPolicy": {"type": "Never"},
        "volumes": [
            {"name": "velocia", "persistentVolumeClaim": {"claimName": cluster.volume_claim}}
        ],
        "driver": {
            **pods,
            "cores": 1,
            "memory": f"{_memory_mib(cluster.driver_memory)}m",
            "serviceAccount": cluster.service_account,
        },
        "executor": executor,
    }
    if spec["type"] == "Python":
        # tail: --py-files <zips> <launcher> --spec <run.json>
        spec["pythonVersion"] = "3"
        spec["deps"] = {"pyFiles": [f"local://{p}" for p in tail[1].split(",")]}
        spec["mainApplicationFile"] = f"local://{tail[2]}"
        spec["arguments"] = tail[3:]
    else:
        # tail: --conf run_id --conf rep --class <main> <jar> <args...>
        for i in (1, 3):
            key, _, value = tail[i].partition("=")
            conf[key] = value
        spec["mainClass"] = tail[5]
        spec["mainApplicationFile"] = f"local://{tail[6]}"
        spec["arguments"] = tail[7:]
    return {
        "apiVersion": API_VERSION,
        "kind": "SparkApplication",
        "metadata": {
            "name": name,
            "namespace": cluster.namespace,
            "labels": {LABEL: k8s_name(app.run_id)},
        },
        "spec": spec,
    }


def application_state(doc: dict[str, Any]) -> str:
    """The operator's application state, e.g. RUNNING, COMPLETED, FAILED (empty if none)."""
    return str(((doc.get("status") or {}).get("applicationState") or {}).get("state") or "")


TERMINAL = {"COMPLETED": "finished", "FAILED": "failed", "SUBMISSION_FAILED": "failed"}


class Kubectl:
    def __init__(self, cluster: K8sCluster) -> None:
        self.base = ["kubectl", *(["--context", cluster.context] if cluster.context else [])]
        self.namespace = cluster.namespace

    def __call__(
        self, *args: str, stdin: str | None = None, check: bool = True, timeout: float = 600
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [*self.base, "-n", self.namespace, *args],
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if check and result.returncode != 0:
            raise K8sError(f"kubectl {' '.join(args[:2])}: {result.stderr.strip()[:2000]}")
        return result


def node_hardware(
    name: str, cluster: K8sCluster, nodes: list[dict[str, Any]], profile: HardwareProfile | None
) -> dict[str, Any]:
    """The manifest's hardware: the nodes the pods may run on, from the Kubernetes API."""
    eligible = [
        n
        for n in nodes
        if all(
            (n.get("metadata", {}).get("labels") or {}).get(k) == v
            for k, v in cluster.node_selector.items()
        )
    ]
    kinds: dict[tuple[Any, ...], int] = {}
    for n in eligible:
        info = (n.get("status") or {}).get("nodeInfo") or {}
        capacity = (n.get("status") or {}).get("capacity") or {}
        memory = str(capacity.get("memory", "0Ki"))
        memory_gib = round(int(memory[:-2] or 0) * 1024 / GIB) if memory.endswith("Ki") else None
        key = (info.get("architecture"), capacity.get("cpu"), memory_gib)
        kinds[key] = kinds.get(key, 0) + 1
    accelerators = None
    if profile is not None and profile.accelerators_per_node:
        accelerators = {
            "model": profile.accelerator_model,
            "count": profile.accelerators_per_node,
            "source": "declared",
        }
    node_types = [
        {
            "name": name,
            "count": count,
            "profile": profile.name if profile else None,
            "host": {"arch": arch, "cores": int(cpu) if cpu else None, "memory_gib": mem},
            "docker": None,
            "disk_type": None,
            "accelerators": accelerators,
            "source": "kubernetes",
        }
        for (arch, cpu, mem), count in sorted(kinds.items(), key=str)
    ]
    return {
        "node_types": node_types,
        "profile": profile.model_dump(mode="json") if profile is not None else None,
    }


class K8sRunner:
    def __init__(self, cluster: K8sCluster) -> None:
        self.cluster = cluster
        self.kubectl = Kubectl(cluster)
        self._apps: set[str] = set()
        self._helper: str | None = None

    # --- checks -------------------------------------------------------------------------

    def check(self) -> list[str]:
        if shutil.which("kubectl") is None:
            return ["kubectl not found on PATH"]
        problems = []
        if self.kubectl(
            "get", "crd", "sparkapplications.sparkoperator.k8s.io", check=False
        ).returncode:
            problems.append(
                "the Spark Operator is not installed (no SparkApplication resource); install "
                "it, e.g. helm install spark-operator spark-operator/spark-operator"
            )
        if self.kubectl("get", "pvc", self.cluster.volume_claim, check=False).returncode:
            problems.append(
                f"no PersistentVolumeClaim {self.cluster.volume_claim!r} in namespace "
                f"{self.cluster.namespace!r} (volume_claim)"
            )
        return problems

    def image_present(self, image: str) -> bool:
        return True  # pulled (or preloaded) by the cluster; a missing image fails the pod

    def nodes(self) -> list[dict[str, Any]]:
        out = self.kubectl("get", "nodes", "-o", "json", check=False)
        if out.returncode:
            return []
        return list(json.loads(out.stdout).get("items") or [])

    def quiet_check(self) -> QuietCheck:
        out = self.kubectl("get", "sparkapplications", "-o", "json", check=False)
        if out.returncode:
            return QuietCheck(ok=False, reasons=["quiet check skipped (cannot list applications)"])
        running = [
            a["metadata"]["name"]
            for a in json.loads(out.stdout).get("items") or []
            if application_state(a) not in ("", *TERMINAL)
            and a["metadata"]["name"] not in self._apps
        ]
        if running:
            return QuietCheck(ok=False, reasons=[f"other Spark applications running: {running}"])
        return QuietCheck(ok=True, reasons=[])

    # --- the helper pod: Velocia's way into the volume -------------------------------------

    def helper(self, image: str) -> str:
        if self._helper is not None:
            return self._helper
        name = k8s_name(f"velocia-io-{int(time.time() * 1000)}")
        manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "labels": {LABEL: "io"}},
            "spec": {
                "restartPolicy": "Never",
                "securityContext": {"runAsUser": 0},
                "containers": [
                    {
                        "name": "io",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["sleep", "86400"],
                        "volumeMounts": [{"name": "velocia", "mountPath": MOUNT}],
                    }
                ],
                "volumes": [
                    {
                        "name": "velocia",
                        "persistentVolumeClaim": {"claimName": self.cluster.volume_claim},
                    }
                ],
            },
        }
        self.kubectl("apply", "-f", "-", stdin=json.dumps(manifest))
        self._helper = name
        self.kubectl(
            "wait", "--for=condition=Ready", f"pod/{name}", f"--timeout={HELPER_TIMEOUT_S}s",
            timeout=HELPER_TIMEOUT_S + 30,
        )  # fmt: skip
        # Spark's pods run as the image's user (185 in the official image).
        self.exec(f"mkdir -p {MOUNT}/data {MOUNT}/runs && chmod 777 {MOUNT}/data {MOUNT}/runs")
        return name

    def exec(self, script: str) -> str:
        assert self._helper is not None
        return self.kubectl("exec", self._helper, "--", "sh", "-c", script).stdout

    def copy_in(self, local: Path, remote: str) -> None:
        assert self._helper is not None
        self.kubectl("cp", str(local), f"{self.cluster.namespace}/{self._helper}:{remote}")

    def copy_out(self, remote: str, local: Path) -> bool:
        assert self._helper is not None
        result = self.kubectl(
            "cp", f"{self.cluster.namespace}/{self._helper}:{remote}", str(local), check=False
        )
        return result.returncode == 0

    # --- data and runs ----------------------------------------------------------------------

    def stage(
        self, dataset: DatasetManifest, driver: DriverPackage, image: str
    ) -> tuple[list[str], str]:
        """Copy the dataset into the volume once; later runs reuse it (marker with checksum)."""
        self.helper(image)
        remote = f"{MOUNT}/data/{dataset.dataset_id}"
        marker = json.dumps({"checksum": dataset.checksum})
        current = self.exec(f"cat {remote}/{STAGED_MARKER} 2>/dev/null || true").strip()
        if current != marker:
            self.exec(f"rm -rf {remote} && mkdir -p {remote}")
            for table in dataset.tables:
                self.copy_in(Path(dataset.location) / table.name, f"{remote}/{table.name}")
        problems = []
        for table in dataset.tables:
            for file in table.files:
                size = self.exec(f"stat -c %s {remote}/{file.path} 2>/dev/null || echo -1").strip()
                if int(size or -1) != file.bytes:
                    problems.append(
                        f"{file.path}: {size} bytes in the volume, expected {file.bytes}"
                    )
        if not problems:
            self.exec(f"echo '{marker}' > {remote}/{STAGED_MARKER} && chmod -R a+rX {remote}")
        return problems, ""

    def run(self, app: AppSpec) -> AppResult:
        self.helper(app.image)
        work_dir = Path(tempfile.mkdtemp(prefix=f"velocia-{app.run_id}-"))
        (work_dir / "eventlog").mkdir()
        (work_dir / "out").mkdir()
        run_dir = f"{MOUNT}/runs/{app.run_id}"
        spec = driver_spec(
            app,
            data_root=f"{MOUNT}/data/{app.dataset_id}",
            work_root=run_dir,
            custom_dir=f"{run_dir}/custom",
        )
        (work_dir / "run.json").write_text(json.dumps(spec), "utf-8")
        self.exec(f"rm -rf {run_dir} && mkdir -p {run_dir}/eventlog {run_dir}/out {run_dir}/custom")
        for path in (app.driver.zip_path, app.driver.launcher_path, work_dir / "run.json"):
            self.copy_in(path, f"{run_dir}/{path.name}")
        if app.custom:
            artifact = Path(str(app.custom["artifact"]))
            self.copy_in(artifact, f"{run_dir}/custom/{artifact.name}")
        self.exec(f"chmod -R 777 {run_dir}")

        name = k8s_name(f"velocia-{app.run_id}")
        tail: deque[str] = deque(maxlen=40)
        started, started_mono = time.time(), time.monotonic()
        self._apps.add(name)
        status: AppStatus = "timeout"
        try:
            self.kubectl("apply", "-f", "-", stdin=json.dumps(application(app, self.cluster, name)))
            deadline = time.monotonic() + app.app_timeout_s
            while time.monotonic() < deadline:
                doc = json.loads(self.kubectl("get", "sparkapplication", name, "-o", "json").stdout)
                state = application_state(doc)
                if state in TERMINAL:
                    status = TERMINAL[state]  # type: ignore[assignment]
                    break
                time.sleep(POLL_S)
            logs = self.kubectl("logs", f"{name}-driver", "--tail=40", check=False)
            tail.extend((logs.stdout or logs.stderr).splitlines())
        finally:
            self.kubectl("delete", "sparkapplication", name, "--ignore-not-found", check=False)
            self._apps.discard(name)
        for part in ("out", "eventlog"):  # kubectl cp nests into an existing directory
            (work_dir / part).rmdir()
            if not self.copy_out(f"{run_dir}/{part}", work_dir / part):
                (work_dir / part).mkdir(exist_ok=True)
        self.exec(f"rm -rf {run_dir}")
        ended = time.time()
        return AppResult(
            status=status,
            exit_code=0 if status == "finished" else 1,
            work_dir=work_dir,
            log_tail="\n".join(tail),
            started_at=started,
            ended_at=ended,
            cache_dropped=False,
            host_slept_s=host_slept_s(started, ended, started_mono, time.monotonic()),
            notes=[],
        )

    def teardown(self) -> None:
        for name in list(self._apps):
            self.kubectl("delete", "sparkapplication", name, "--ignore-not-found", check=False)
            self._apps.discard(name)
        if self._helper is not None:
            self.kubectl("delete", "pod", self._helper, "--wait=false", check=False)
            self._helper = None
