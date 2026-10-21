"""The ``local`` runner: one Docker container per Spark application, Spark in ``local[N]`` mode.

Every container is labeled and named after its run, started with ``--rm`` and also removed in a
``finally`` path, so an interrupted ``velocia run`` leaves none behind (CLAUDE.md: ephemeral
per-run containers are not services). Raw outputs go to a temporary directory outside the
results directory; the collector redacts them on the way in.

Cold mode drops the OS page cache only on a Linux host (a privileged container writes
``/proc/sys/vm/drop_caches``). On macOS the files are cached by the host, which Docker can't
reach, so cold runs there are labeled "cache not dropped" (D-5).
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from pathlib import Path

from velocia.config.models import LocalCluster
from velocia.runners.base import AppResult, AppSpec, AppStatus, QuietCheck

LABEL = "io.velocia.run_id"
DATA_MOUNT = "/velocia/data"
DRIVER_MOUNT = "/velocia/driver"
WORK_MOUNT = "/velocia/work"
LOG_TAIL_LINES = 40
TERAGEN_OUTPUT = "teragen-data"  # inside the work dir


def _memory_mb(size: str) -> int:
    if size.isdigit():  # plain bytes
        return int(size) // (1024 * 1024)
    factor = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024}[size[-1]]
    return int(int(size[:-1]) * factor)


OOM_KILLED = 137  # SIGKILL, from the kernel's out-of-memory killer
JAVA_OOM = 52  # Apache Spark's exit code for an uncaught OutOfMemoryError
JVM_OVERHEAD_MB = 1024  # metaspace, threads, Python driver and native buffers


def container_memory_mb(cluster: LocalCluster, spark_conf: dict[str, str]) -> int:
    """Rough memory one application container needs: heap, off-heap, overhead."""
    total = _memory_mb(driver_memory(cluster)) + JVM_OVERHEAD_MB
    if spark_conf.get("spark.memory.offHeap.enabled", "false").lower() == "true":
        total += _memory_mb(spark_conf.get("spark.memory.offHeap.size", "0m").lower() or "0m")
    return total


def driver_memory(cluster: LocalCluster) -> str:
    """In local mode the driver JVM runs every task, so it gets all executor memory."""
    total = cluster.executors * _memory_mb(cluster.executor_memory)
    return f"{max(total, _memory_mb(cluster.driver_memory))}m"


def docker_command(app: AppSpec, cluster: LocalCluster, work_dir: Path) -> list[str]:
    cores = cluster.executors * cluster.executor_cores
    conf = {
        **app.spark_conf,
        "spark.driver.memory": driver_memory(cluster),
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": f"file://{WORK_MOUNT}/eventlog",
        "spark.dynamicAllocation.enabled": "false",
        "spark.ui.enabled": "false",
    }
    user = ["--user", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp"]
    cmd = [
        "docker",
        "run",
        "--rm",
        "--name",
        f"velocia-{app.run_id}",
        "--label",
        f"{LABEL}={app.run_id}",
        "--cpus",
        str(cores),
        *(user if sys.platform == "linux" else []),
        # Absolute paths: Docker reads a relative one as a named volume.
        "-v",
        f"{app.dataset_location.resolve()}:{DATA_MOUNT}:ro",
        "-v",
        f"{app.driver.zip_path.parent.resolve()}:{DRIVER_MOUNT}:ro",
        "-v",
        f"{work_dir.resolve()}:{WORK_MOUNT}",
        app.image,
        "/opt/spark/bin/spark-submit",
        "--master",
        f"local[{cores}]",
    ]
    for key, value in sorted(conf.items()):
        cmd += ["--conf", f"{key}={value}"]
    cmd += [
        "--py-files",
        f"{DRIVER_MOUNT}/{app.driver.zip_path.name}",
        f"{DRIVER_MOUNT}/{app.driver.launcher_path.name}",
        "--spec",
        f"{WORK_MOUNT}/run.json",
    ]
    return cmd


def driver_spec(app: AppSpec) -> dict[str, object]:
    return {
        "version": 1,
        "run_id": app.run_id,
        "task": app.task,
        "mode": app.mode,
        "reps": app.reps,
        "warmup": app.warmup,
        "timeout_s": app.timeout_per_unit_s,
        "float_round_decimals": app.float_round_decimals,
        "units": app.units,
        "tables": {t: f"{DATA_MOUNT}/{t}" for t in app.tables},
        "output_dir": f"{WORK_MOUNT}/out",
        "scratch_dir": f"{WORK_MOUNT}/scratch",  # job unit output, removed with the work dir
        "teragen": (
            {**app.teragen, "output": f"{WORK_MOUNT}/{TERAGEN_OUTPUT}"} if app.teragen else None
        ),
    }


SLEEP_TOLERANCE_S = 2.0


def host_slept_s(wall_start: float, wall_end: float, mono_start: float, mono_end: float) -> float:
    """Seconds the host slept: on macOS the monotonic clock stops during sleep, wall time not.

    On Linux the monotonic clock also stops in suspend, so the same check holds there.
    """
    gap = (wall_end - wall_start) - (mono_end - mono_start)
    return round(gap, 1) if gap > SLEEP_TOLERANCE_S else 0.0


def keep_awake() -> subprocess.Popen[bytes] | None:
    """Hold off idle sleep while Velocia runs (macOS ``caffeinate``); None elsewhere.

    A sleeping host pauses the Docker VM in the middle of timed units. Closing a laptop lid
    still sleeps it; that case is detected per application instead (``host_slept_s``).
    """
    if sys.platform != "darwin" or shutil.which("caffeinate") is None:
        return None
    return subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])


def cpu_busy_pct(interval_s: float = 1.0) -> float | None:
    """Share of all CPUs busy over ``interval_s``; None where it can't be measured."""
    if sys.platform == "linux":

        def sample() -> tuple[int, int]:
            with open("/proc/stat", encoding="utf-8") as f:
                values = [int(v) for v in f.readline().split()[1:]]
            idle = values[3] + (values[4] if len(values) > 4 else 0)  # idle + iowait
            return idle, sum(values)

        idle1, total1 = sample()
        time.sleep(interval_s)
        idle2, total2 = sample()
        return 100.0 * (1 - (idle2 - idle1) / max(total2 - total1, 1))
    if sys.platform == "darwin":
        out = subprocess.run(
            ["top", "-l", "2", "-s", str(max(1, round(interval_s))), "-n", "0"],
            capture_output=True,
            text=True,
            check=False,
        )
        idle = re.findall(r"CPU usage:.*?([\d.]+)% idle", out.stdout)
        return 100.0 - float(idle[-1]) if idle else None  # the second sample is the interval
    return None


class LocalRunner:
    def __init__(
        self, cluster: LocalCluster, max_cpu_pct: float = 10.0, settle_s: float = 30.0
    ) -> None:
        self.cluster = cluster
        self.max_cpu_pct = max_cpu_pct
        self.settle_s = settle_s
        self._containers: set[str] = set()

    def check(self) -> list[str]:
        if shutil.which("docker") is None:
            return ["docker CLI not found on PATH"]
        info = subprocess.run(["docker", "info"], capture_output=True, text=True, check=False)
        return [] if info.returncode == 0 else ["Docker daemon is not reachable (docker info)"]

    def image_present(self, image: str) -> bool:
        result = subprocess.run(
            ["docker", "image", "inspect", image], capture_output=True, check=False
        )
        return result.returncode == 0

    def quiet_check(self) -> QuietCheck:
        reasons = []
        ps = subprocess.run(
            ["docker", "ps", "--filter", f"label={LABEL}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        others = [n for n in ps.stdout.split() if n]
        if others:
            reasons.append(f"other Velocia containers running: {', '.join(others)}")
        # Give the previous application's load time to fade before judging the machine.
        busy = cpu_busy_pct()
        deadline = time.time() + self.settle_s
        while busy is not None and busy > self.max_cpu_pct and time.time() < deadline:
            busy = cpu_busy_pct()
        if busy is not None and busy > self.max_cpu_pct:
            reasons.append(f"host CPU {busy:.0f}% busy > {self.max_cpu_pct:.0f}%")
        return QuietCheck(ok=not reasons, reasons=reasons)

    def drop_page_cache(self, image: str) -> bool:
        if sys.platform != "linux":
            return False
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--privileged",
                "--user",
                "0",
                "--entrypoint",
                "sh",
                image,
                "-c",
                "sync && echo 3 > /proc/sys/vm/drop_caches",
            ],
            capture_output=True,
            check=False,
            timeout=120,
        )
        return result.returncode == 0

    def run(self, app: AppSpec) -> AppResult:
        work_dir = Path(tempfile.mkdtemp(prefix=f"velocia-{app.run_id}-"))
        (work_dir / "eventlog").mkdir()
        (work_dir / "out").mkdir()
        (work_dir / "run.json").write_text(json.dumps(driver_spec(app)), "utf-8")
        cache_dropped = (
            app.task == "time" and app.mode == "cold" and self.drop_page_cache(app.image)
        )
        name = f"velocia-{app.run_id}"
        tail: deque[str] = deque(maxlen=LOG_TAIL_LINES)
        timed_out = threading.Event()

        def on_timeout() -> None:
            timed_out.set()
            self._remove(name)

        timer = threading.Timer(app.app_timeout_s, on_timeout)
        timer.daemon = True
        started = time.time()
        started_mono = time.monotonic()
        self._containers.add(name)
        try:
            proc = subprocess.Popen(
                docker_command(app, self.cluster, work_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            timer.start()
            assert proc.stdout is not None
            for line in proc.stdout:
                tail.append(line.rstrip())
            exit_code = proc.wait()
        finally:
            timer.cancel()
            self._remove(name)
        status: AppStatus = "finished"
        notes: list[str] = []
        if timed_out.is_set():
            status = "timeout"
        elif exit_code != 0:
            status = "failed"
            if exit_code == OOM_KILLED:
                notes.append(
                    "killed (exit 137): out of memory; the container needed more than Docker "
                    "could give it. Give Docker more memory or lower executor_memory"
                )
            elif exit_code == JAVA_OOM:
                notes.append(
                    "Java heap out of memory (exit 52); raise executor_memory (and Docker's "
                    "memory if needed)"
                )
        ended = time.time()
        return AppResult(
            status=status,
            exit_code=exit_code,
            work_dir=work_dir,
            log_tail="\n".join(tail),
            started_at=started,
            ended_at=ended,
            cache_dropped=cache_dropped,
            host_slept_s=host_slept_s(started, ended, started_mono, time.monotonic()),
            notes=notes,
        )

    def _remove(self, name: str) -> None:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        self._containers.discard(name)

    def teardown(self) -> None:
        for name in list(self._containers):
            self._remove(name)
