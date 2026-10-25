"""``velocia doctor`` and ``velocia engines check`` (docs/04-interfaces.md §1, plan T22)."""

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import duckdb
import yaml

from velocia.config import ConfigError, VelociaConfig
from velocia.config.models import Cluster, DataFormat, Suite
from velocia.datagen.duckdb_tpc import DatagenError, generate
from velocia.datagen.tera import generate_tera
from velocia.engines import cluster_checks
from velocia.engines.registry import get_engine, load_engines
from velocia.hardware.probe import probe_local
from velocia.orchestrator.run import Orchestrator, RunError, prepare
from velocia.report.build import TOTAL_UNIT, build_report
from velocia.store.catalog import Catalog

Status = Literal["ok", "warn", "fail", "skip"]
MIN_FREE_GIB = 5
MIN_DOCKER_MEMORY_GIB = 4
CHECK_UNITS = ["q1", "q3", "q6"]
CHECK_TERA_ROWS = 100_000
REFERENCE = "spark-vanilla"
LOCAL_REFERENCE = "local"  # where vanilla runs when the engine is checked on another cluster


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
                present = (
                    subprocess.run(
                        ["docker", "image", "inspect", image], capture_output=True, check=False
                    ).returncode
                    == 0
                )
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
        checks.append(
            Check(
                "duckdb tpch",
                "warn",
                "not installed yet; the first datagen downloads it from DuckDB",
            )
        )
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


def engine_check(
    name: str,
    spark: str | None,
    config: VelociaConfig,
    cluster: tuple[str, Cluster] | None = None,
) -> EngineCheckResult:
    """Smoke test against vanilla, correctness required: TPC-H-derived q1, q3, q6 at SF0.01
    and a 100,000-row TeraSort.

    Runs in ``<results_dir>/_checks`` so the main catalog only holds real experiments. With
    ``cluster`` (name, definition) the engine runs there and vanilla runs locally as the
    reference; a pass is recorded for the experiment gate (``engines.cluster_checks``).
    """
    try:
        engine = get_engine(name, Path(config.engines_dir))
    except (ConfigError, KeyError) as exc:
        return EngineCheckResult(False, None, [str(exc)])
    if spark is not None and not any(
        spark == v or spark.startswith(v + ".") or v.startswith(spark + ".")
        for v in engine.definition.spark_versions
    ):
        return EngineCheckResult(
            False,
            None,
            [
                f"{name} supports Apache Spark {', '.join(engine.definition.spark_versions)}, "
                f"not {spark}"
            ],
        )
    check_dir = Path(config.results_dir) / "_checks"
    check_config = replace_results_dir(config, check_dir)
    (check_dir / "datasets").mkdir(parents=True, exist_ok=True)
    try:
        for manifest, _ in (
            generate(Suite.tpch, 0.01, DataFormat.parquet_zstd, check_dir / "data"),
            generate_tera(
                CHECK_TERA_ROWS,
                DataFormat.parquet_zstd,
                check_dir / "data",
                check_dir / "drivers",
                cores=2,
            ),
        ):
            (check_dir / "datasets" / f"{manifest.dataset_id}.json").write_text(
                manifest.model_dump_json(indent=2) + "\n", "utf-8"
            )
    except DatagenError as exc:
        return EngineCheckResult(False, None, [f"test data: {exc}"])

    engines = [REFERENCE] if name == REFERENCE else [REFERENCE, name]
    lines: list[str] = []
    ok = True
    experiment_ids = []
    for label, workload in (
        ("sql", {"kind": "sql", "suite": "tpch", "scale_factor": 0.01, "units": CHECK_UNITS}),
        ("terasort", {"kind": "job", "suite": "tera", "rows": CHECK_TERA_ROWS}),
    ):
        passed, experiment_id, part = _smoke(
            name,
            f"engine-check-{name}-{label}",
            engines,
            workload,
            check_dir,
            check_config,
            cluster,
        )
        ok = ok and passed
        lines += part
        if experiment_id:
            experiment_ids.append(experiment_id)
    if ok and cluster is not None:
        cluster_checks.record_pass(
            Path(config.results_dir), engine, cluster[0], cluster[1], ", ".join(experiment_ids)
        )
    return EngineCheckResult(ok, ", ".join(experiment_ids) or None, lines)


def _smoke(
    name: str,
    experiment_name: str,
    engines: list[str],
    workload: dict[str, object],
    check_dir: Path,
    check_config: VelociaConfig,
    cluster: tuple[str, Cluster] | None = None,
) -> tuple[bool, str | None, list[str]]:
    local = {"runner": "local", "executors": 1, "executor_cores": 2, "executor_memory": "2g"}
    placement: dict[str, Any] = {"cluster": local, "engines": engines}
    measures = ["wall_time"]
    if cluster is not None:
        cluster_name, definition = cluster
        target = cluster_name if cluster_name != LOCAL_REFERENCE else f"{cluster_name}-target"
        refs = [{"name": e, "cluster": LOCAL_REFERENCE if e != name else target} for e in engines]
        placement = {
            "clusters": {LOCAL_REFERENCE: local, target: definition.model_dump(mode="json")},
            "engines": refs,
            "comparison": {"allow_cross_hardware": True},
        }
        measures = ["wall_time", "node_hours"]  # cross-hardware needs a normalized measure
    experiment = {
        "name": experiment_name,
        "workload": workload,
        **placement,
        "reference_engine": REFERENCE,
        "measures": measures,
        "repetitions": 1,
        "modes": ["cold"],
        "timeout_per_unit": "5m",
    }
    path = check_dir / f"{experiment_name}.yaml"
    path.write_text(yaml.safe_dump(experiment, sort_keys=False), "utf-8")
    lines: list[str] = []
    try:
        prepared = prepare(path, check_config, cluster_gate=False)
        # One repetition is the point of a smoke test; don't warn about it.
        prepared.warnings[:] = [w for w in prepared.warnings if "repetitions=" not in w]
        experiment_id = Orchestrator(prepared, check_config, check_dir, echo=lines.append).run()
    except RunError as exc:
        return False, None, [*lines, str(exc)]
    with Catalog(check_dir) as catalog:
        report = build_report(catalog, experiment_id)
        runs = catalog.query(
            "SELECT run_id, status FROM run WHERE experiment_id = ?", [experiment_id]
        )
    problems = [
        f"{r['run_id']}: application {r['status']}" for r in runs if r["status"] != "finished"
    ]
    for r in report["results"]:
        if r["engine"] != name or r["unit_id"] == TOTAL_UNIT:
            continue
        if r["correctness"] != "pass":
            problems.append(f"{r['unit_id']}: correctness {r['correctness']}")
        if r["n"] == 0:
            problems.append(f"{r['unit_id']}: no successful timed run")
    lines += [f"problem: {p}" for p in problems]
    return not problems, experiment_id, lines


def replace_results_dir(config: VelociaConfig, results_dir: Path) -> VelociaConfig:
    return config.model_copy(update={"results_dir": str(results_dir)})
