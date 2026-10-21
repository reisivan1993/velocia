"""``velocia doctor`` and ``velocia engines check`` (docs/04-interfaces.md §1, plan T22)."""

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import duckdb
import yaml

from velocia.config import ConfigError, VelociaConfig
from velocia.config.models import DataFormat, Suite
from velocia.datagen.duckdb_tpc import DatagenError, generate
from velocia.engines.registry import get_engine, load_engines
from velocia.hardware.probe import probe_local
from velocia.orchestrator.run import Orchestrator, RunError, prepare
from velocia.report.build import TOTAL_UNIT, build_report
from velocia.store.catalog import Catalog

Status = Literal["ok", "warn", "fail", "skip"]
MIN_FREE_GIB = 5
MIN_DOCKER_MEMORY_GIB = 4
CHECK_UNITS = ["q1", "q3", "q6"]
REFERENCE = "spark-vanilla"


@dataclass(frozen=True)
class Check:
    name: str
    status: Status
    detail: str


def doctor(config: VelociaConfig) -> list[Check]:
    checks: list[Check] = []
    docker = shutil.which("docker")
    if docker is None:
        checks.append(Check("docker", "fail", "docker CLI not found; install Docker"))
    else:
        info = subprocess.run(["docker", "info"], capture_output=True, text=True, check=False)
        if info.returncode != 0:
            checks.append(Check("docker", "fail", "Docker daemon not reachable; start Docker"))
        else:
            node = probe_local()["node_types"][0]["docker"] or {}
            memory = node.get("memory_gib") or 0
            status: Status = "ok" if memory >= MIN_DOCKER_MEMORY_GIB else "warn"
            detail = f"{node.get('cpus')} CPUs, {memory} GiB, {node.get('os')}"
            if status == "warn":
                detail += f" (give Docker at least {MIN_DOCKER_MEMORY_GIB} GiB)"
            checks.append(Check("docker", status, detail))
            for engine in load_engines(Path(config.engines_dir)).values():
                image = engine.definition.image
                present = subprocess.run(
                    ["docker", "image", "inspect", image], capture_output=True, check=False
                ).returncode == 0  # fmt: skip
                checks.append(
                    Check(
                        f"engine {engine.name}",
                        "ok" if present else "warn",
                        image
                        if present
                        else f"{image} not built: velocia engines build {engine.name}",
                    )
                )
    results = Path(config.results_dir)
    probe_dir = results if results.exists() else results.resolve().parent
    free_gib = shutil.disk_usage(probe_dir).free / 1024**3
    checks.append(
        Check(
            "disk",
            "ok" if free_gib >= MIN_FREE_GIB else "warn",
            f"{free_gib:.0f} GiB free for {results}",
        )
    )
    try:
        with duckdb.connect() as con:
            con.execute("SET autoinstall_known_extensions = false")
            con.execute("LOAD tpch")
        checks.append(Check("duckdb tpch", "ok", f"DuckDB {duckdb.__version__}"))
    except duckdb.Error:
        checks.append(Check("duckdb tpch", "warn", "not installed yet; the first datagen "
                            "downloads it from DuckDB"))  # fmt: skip
    kubectl = shutil.which("kubectl")
    checks.append(
        Check("kubernetes", "skip", "k8s runner not available yet" if kubectl else "no kubectl")
    )
    return checks


@dataclass(frozen=True)
class EngineCheckResult:
    ok: bool
    experiment_id: str | None
    lines: list[str]


def engine_check(name: str, spark: str | None, config: VelociaConfig) -> EngineCheckResult:
    """Smoke test: TPC-H-derived q1, q3, q6 at SF0.01 against vanilla, correctness required.

    Runs in ``<results_dir>/_checks`` so the main catalog only holds real experiments.
    """
    try:
        engine = get_engine(name, Path(config.engines_dir))
    except (ConfigError, KeyError) as exc:
        return EngineCheckResult(False, None, [str(exc)])
    if spark is not None and not any(
        spark == v or spark.startswith(v + ".") or v.startswith(spark + ".")
        for v in engine.definition.spark_versions
    ):
        return EngineCheckResult(False, None, [
            f"{name} supports Apache Spark {', '.join(engine.definition.spark_versions)}, "
            f"not {spark}"
        ])  # fmt: skip
    check_dir = Path(config.results_dir) / "_checks"
    check_config = replace_results_dir(config, check_dir)
    try:
        generate(Suite.tpch, 0.01, DataFormat.parquet_zstd, check_dir / "data")
    except DatagenError as exc:
        return EngineCheckResult(False, None, [f"test data: {exc}"])
    (check_dir / "datasets").mkdir(parents=True, exist_ok=True)
    manifest = check_dir / "data" / "tpch-sf0.01-parquet-zstd" / "_dataset.json"
    shutil.copyfile(manifest, check_dir / "datasets" / "tpch-sf0.01-parquet-zstd.json")

    engines = [REFERENCE] if name == REFERENCE else [REFERENCE, name]
    experiment = {
        "name": f"engine-check-{name}",
        "workload": {
            "kind": "sql",
            "suite": "tpch",
            "scale_factor": 0.01,
            "units": CHECK_UNITS,
        },
        "engines": engines,
        "reference_engine": REFERENCE,
        "cluster": {
            "runner": "local",
            "executors": 1,
            "executor_cores": 2,
            "executor_memory": "2g",
        },
        "measures": ["wall_time"],
        "repetitions": 1,
        "modes": ["cold"],
        "timeout_per_unit": "5m",
    }
    path = check_dir / f"engine-check-{name}.yaml"
    path.write_text(yaml.safe_dump(experiment, sort_keys=False), "utf-8")
    lines: list[str] = []
    try:
        prepared = prepare(path, check_config)
        # One repetition is the point of a smoke test; don't warn about it.
        prepared.warnings[:] = [w for w in prepared.warnings if "repetitions=" not in w]
        experiment_id = Orchestrator(prepared, check_config, check_dir, echo=lines.append).run()
    except RunError as exc:
        return EngineCheckResult(False, None, [*lines, str(exc)])
    with Catalog(check_dir) as catalog:
        report = build_report(catalog, experiment_id)
        runs = catalog.query(
            "SELECT run_id, status FROM run WHERE experiment_id = ?", [experiment_id]
        )
    problems = [f"{r['run_id']}: application {r['status']}" for r in runs
                if r["status"] != "finished"]  # fmt: skip
    for r in report["results"]:
        if r["engine"] != name or r["unit_id"] == TOTAL_UNIT:
            continue
        if r["correctness"] != "pass":
            problems.append(f"{r['unit_id']}: correctness {r['correctness']}")
        if r["n"] == 0:
            problems.append(f"{r['unit_id']}: no successful timed run")
    lines += [f"problem: {p}" for p in problems]
    return EngineCheckResult(not problems, experiment_id, lines)


def replace_results_dir(config: VelociaConfig, results_dir: Path) -> VelociaConfig:
    return config.model_copy(update={"results_dir": str(results_dir)})
