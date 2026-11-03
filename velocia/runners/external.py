"""The ``external`` runner: ``spark-submit`` to a Spark cluster Velocia doesn't create or manage.

Typically a vendor's accelerator servers that expose the regular Spark API (docs/03-spec.md §4).
The vendor's Spark build stays as is; Velocia ships its driver program with each submission.
Client deploy mode: the driver runs here, so its timings land in a local work directory, while
executors run on the cluster and read the dataset from ``data_uri``.

``spark-submit`` comes from the engine image in Docker (``docker_network``, default ``host``,
must let executors connect back to the driver) or from a Spark install here (``spark_home``).

What can't be done on someone else's cluster is labeled, never hidden: the OS page cache is not
dropped ("cold (cache not dropped)"), hardware is declared by the profile, not probed, the quiet
check needs ``rest_url`` ("quiet check skipped"), and when the event log can't be fetched from
``eventlog_uri`` the run keeps its driver timings and says so.
"""

import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from functools import partial
from pathlib import Path
from typing import Any

from velocia.config.models import ExternalCluster, HardwareProfile
from velocia.datagen.manifest import DatasetManifest
from velocia.families.spark.package import DriverPackage
from velocia.runners.base import AppResult, AppSpec, QuietCheck
from velocia.runners.local import (
    CUSTOM_MOUNT,
    DATA_MOUNT,
    DRIVER_MOUNT,
    LABEL,
    WORK_MOUNT,
    app_status,
    custom_mount,
    driver_spec,
    host_slept_s,
    linux_user,
    program,
    run_process,
)

REST_TIMEOUT_S = 10
STAGE_TIMEOUT_S = 6 * 3600
EXECUTOR_WAIT_S = 300  # fail clearly when no executor can reach the driver


def container_name(run_id: str) -> str:
    """Also the driver's hostname on a Docker network, so a valid DNS label (no dots)."""
    return re.sub(r"[^a-z0-9-]+", "-", f"velocia-{run_id}".lower())[:63].rstrip("-")


def uri(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *(p.strip("/") for p in parts)])


def _memory_gib(size: str) -> float | None:
    factor = {"k": 1 / 1024**2, "m": 1 / 1024, "g": 1, "t": 1024}.get(size[-1].lower())
    return round(int(size[:-1]) * factor, 1) if factor else None


def declared_hardware(
    name: str, cluster: ExternalCluster, profile: HardwareProfile | None
) -> dict[str, Any]:
    """The manifest's hardware for nodes Velocia can't probe: what the experiment declares."""
    accelerators = None
    if profile is not None and profile.accelerators_per_node:
        accelerators = {
            "model": profile.accelerator_model,
            "count": profile.accelerators_per_node,
            "source": "declared",
        }
    return {
        "node_types": [
            {
                "name": name,
                "count": cluster.executors,
                "profile": profile.name if profile else None,
                "host": {
                    "cores": cluster.executor_cores,
                    "memory_gib": _memory_gib(cluster.executor_memory),
                },
                "docker": None,
                "disk_type": None,
                "accelerators": accelerators,
                "source": "declared",
            }
        ],
        "profile": profile.model_dump(mode="json") if profile is not None else None,
    }


IVY_DIR = "/tmp/ivy2"  # the image user's home may not be writable; --packages needs a cache


def submit_conf(cluster: ExternalCluster, app: AppSpec, driver_host: str | None) -> dict[str, str]:
    conf = {
        **({"spark.jars.ivy": IVY_DIR} if cluster.spark_home is None else {}),
        **app.spark_conf,
        "spark.executor.instances": str(cluster.executors),
        "spark.executor.cores": str(cluster.executor_cores),
        "spark.cores.max": str(cluster.executors * cluster.executor_cores),  # standalone
        "spark.executor.memory": cluster.executor_memory,
        "spark.driver.memory": cluster.driver_memory,
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": cluster.eventlog_uri,
        "spark.eventLog.compress": "false",
        "spark.eventLog.rolling.enabled": "false",
        "spark.dynamicAllocation.enabled": "false",
    }
    if driver_host:
        conf["spark.driver.host"] = driver_host
        conf["spark.driver.bindAddress"] = "0.0.0.0"
    return conf


class ExternalRunner:
    def __init__(self, cluster: ExternalCluster) -> None:
        self.cluster = cluster
        self._containers: set[str] = set()

    @property
    def in_docker(self) -> bool:
        return self.cluster.spark_home is None

    def check(self) -> list[str]:
        if self.cluster.deploy_mode != "client":
            return ["the external runner supports deploy_mode: client only so far"]
        if not self.in_docker:
            submit = Path(self.cluster.spark_home or "") / "bin" / "spark-submit"
            return [] if submit.is_file() else [f"{submit} not found (spark_home)"]
        if shutil.which("docker") is None:
            return ["docker CLI not found on PATH"]
        info = subprocess.run(["docker", "info"], capture_output=True, text=True, check=False)
        return [] if info.returncode == 0 else ["Docker daemon is not reachable (docker info)"]

    def image_present(self, image: str) -> bool:
        if not self.in_docker:
            return True
        result = subprocess.run(
            ["docker", "image", "inspect", image], capture_output=True, check=False
        )
        return result.returncode == 0

    # --- quiet check ------------------------------------------------------------------

    def _get_json(self, url: str) -> Any:
        with urllib.request.urlopen(url, timeout=REST_TIMEOUT_S) as response:
            return json.load(response)

    def quiet_check(self) -> QuietCheck:
        """No other applications running, per the cluster's REST API (standalone or YARN)."""
        rest = self.cluster.rest_url
        if not rest:
            return QuietCheck(ok=False, reasons=["quiet check skipped (no rest_url)"])
        try:
            master = self._get_json(uri(rest, "json") + "/")  # standalone master
            running = [a.get("name") or a.get("id") for a in master.get("activeapps", [])]
        except (OSError, ValueError, urllib.error.URLError):
            try:
                yarn = self._get_json(uri(rest, "ws/v1/cluster/apps?states=RUNNING"))
                running = [a.get("name") for a in ((yarn.get("apps") or {}).get("app") or [])]
            except (OSError, ValueError, urllib.error.URLError) as exc:
                return QuietCheck(
                    ok=False, reasons=[f"quiet check skipped ({rest} unreachable: {exc})"]
                )
        if running:
            return QuietCheck(
                ok=False, reasons=[f"other applications running on the cluster: {running}"]
            )
        return QuietCheck(ok=True, reasons=[])

    # --- submission -------------------------------------------------------------------

    def submit_command(self, app: AppSpec, work_dir: Path, name: str) -> list[str]:
        cluster = self.cluster
        if self.in_docker:
            networked = cluster.docker_network != "host"
            driver_dir, work = DRIVER_MOUNT, WORK_MOUNT
            prefix = [
                "docker",
                "run",
                "--rm",
                "--name",
                name,
                "--label",
                f"{LABEL}={app.run_id}",
                "--network",
                cluster.docker_network,
                "-v",
                f"{app.driver.zip_path.parent.resolve()}:{DRIVER_MOUNT}:ro",
                "-v",
                f"{work_dir.resolve()}:{WORK_MOUNT}",
            ]
            if sys.platform == "linux":  # write the work dir as this user
                prefix += linux_user()
            if app.task == "stage":
                prefix += ["-v", f"{app.dataset_location.resolve()}:{DATA_MOUNT}:ro"]
            prefix += custom_mount(app)
            prefix += [*cluster.docker_args, app.image, "/opt/spark/bin/spark-submit"]
            driver_host = name if networked else None
        else:
            driver_dir, work = str(app.driver.zip_path.parent.resolve()), str(work_dir)
            prefix = [str(Path(cluster.spark_home or "") / "bin" / "spark-submit")]
            driver_host = None
        cmd = [*prefix, "--master", cluster.master, "--deploy-mode", "client"]
        for key, value in sorted(submit_conf(cluster, app, driver_host).items()):
            cmd += ["--conf", f"{key}={value}"]
        return [
            *cmd,
            *cluster.submit_args,
            *program(
                app,
                driver_dir,
                CUSTOM_MOUNT if self.in_docker else None,
                work,
                uri(cluster.data_uri, app.dataset_id),
            ),
        ]

    def run_spec(self, app: AppSpec, work_dir: Path) -> dict[str, object]:
        work = WORK_MOUNT if self.in_docker else str(work_dir)
        scratch = uri(self.cluster.data_uri, "_scratch", app.run_id)
        spec = driver_spec(
            app,
            data_root=uri(self.cluster.data_uri, app.dataset_id),
            work_root=work,
            scratch_root=scratch,
            custom_dir=CUSTOM_MOUNT if self.in_docker else None,
        )
        spec["eventlog_fetch"] = {"dir": self.cluster.eventlog_uri, "dest": f"{work}/eventlog"}
        spec["cleanup"] = [scratch]
        spec["executor_wait_s"] = EXECUTOR_WAIT_S
        if app.stage is not None:
            local = DATA_MOUNT if self.in_docker else str(app.dataset_location.resolve())
            spec["stage"] = {
                **app.stage,
                "local_dir": local,
                "remote_dir": uri(self.cluster.data_uri, app.dataset_id),
            }
        return spec

    def run(self, app: AppSpec) -> AppResult:
        work_dir = Path(tempfile.mkdtemp(prefix=f"velocia-{app.run_id}-"))
        (work_dir / "eventlog").mkdir()
        (work_dir / "out").mkdir()
        (work_dir / "run.json").write_text(json.dumps(self.run_spec(app, work_dir)), "utf-8")
        name = container_name(app.run_id)
        started = time.time()
        started_mono = time.monotonic()
        if self.in_docker:
            self._containers.add(name)
        try:
            exit_code, tail, timed_out = run_process(
                self.submit_command(app, work_dir, name),
                app.app_timeout_s,
                partial(self._remove, name) if self.in_docker else lambda: None,
            )
        finally:
            if self.in_docker:
                self._remove(name)
        ended = time.time()
        return AppResult(
            status=app_status(exit_code, timed_out),
            exit_code=exit_code,
            work_dir=work_dir,
            log_tail=tail,
            started_at=started,
            ended_at=ended,
            cache_dropped=False,  # not ours to drop (D-5)
            host_slept_s=host_slept_s(started, ended, started_mono, time.monotonic()),
            notes=fetch_notes(work_dir),
        )

    def stage(
        self, dataset: DatasetManifest, driver: DriverPackage, image: str
    ) -> tuple[list[str], str]:
        """Copy ``dataset`` to ``data_uri`` unless it is already there, and verify it.

        Returns problems (empty when the data on the cluster matches) and the driver log tail.
        """
        app = AppSpec(
            run_id=f"stage-{dataset.dataset_id}-{int(time.time())}",
            image=image,
            spark_conf={},
            driver=driver,
            task="stage",
            mode="cold",
            reps=[],
            warmup=False,
            units=[],
            dataset_location=Path(dataset.location),
            tables=[],
            timeout_per_unit_s=STAGE_TIMEOUT_S,
            app_timeout_s=STAGE_TIMEOUT_S,
            dataset_id=dataset.dataset_id,
            stage={
                "checksum": dataset.checksum,
                "files": {f.path: f.bytes for t in dataset.tables for f in t.files},
                "tables": {t.name: t.rows for t in dataset.tables},
            },
        )
        result = self.run(app)
        try:
            out = result.work_dir / "out" / "stage.json"
            if result.status != "finished" or not out.is_file():
                return [f"staging the dataset failed ({result.status})"], result.log_tail
            return list(json.loads(out.read_text("utf-8"))["problems"]), result.log_tail
        finally:
            shutil.rmtree(result.work_dir, ignore_errors=True)

    def _remove(self, name: str) -> None:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        self._containers.discard(name)

    def teardown(self) -> None:
        for name in list(self._containers):
            self._remove(name)


def fetch_notes(work_dir: Path) -> list[str]:
    """Notes from the driver's after-stop report: event log not fetched, scratch left over."""
    path = work_dir / "out" / "fetch.json"
    if not path.is_file():
        return []
    report = json.loads(path.read_text("utf-8"))
    notes = []
    if report.get("eventlog") not in (None, "ok"):
        notes.append(
            f"event log not fetched ({report['eventlog']}); driver timings only, "
            "event-log measures are null"
        )
    notes += [f"scratch output not removed: {p}" for p in report.get("cleanup_problems", [])]
    return notes
