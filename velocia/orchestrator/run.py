"""``velocia run``: execute an experiment (docs/06-design.md §3).

Cells run one at a time. For each: quiet check, timed applications (cold: one per repetition;
warm: one application with a discarded warm-up), then one untimed correctness pass (ADR-5).
Every state change goes to the catalog before the next step, so ``--resume`` continues an
interrupted experiment with the first unfinished cell. Measures, statistics and correctness
against the reference engine are computed at the end.
"""

import hashlib
import json
import subprocess
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from velocia import __version__
from velocia.collect.collector import collect
from velocia.collect.redaction import Redactor
from velocia.config import ConfigError, load_model
from velocia.config.models import (
    Cluster,
    Experiment,
    ExternalCluster,
    HardwareProfile,
    K8sCluster,
    LocalCluster,
    Mode,
    Suite,
    VelociaConfig,
)
from velocia.datagen.duckdb_tpc import dataset_id as make_dataset_id
from velocia.datagen.manifest import DatasetManifest, results_label
from velocia.datagen.tera import SPARK_SUITES, spark_dataset_id, synth_dataset_id
from velocia.engines import cluster_checks
from velocia.engines.docker import inspect_image
from velocia.engines.registry import Engine, load_engines
from velocia.experiment.expander import Cell, expand, expand_configs, spark_value, stable_hash
from velocia.families.spark.eventlog.reader import EventLogError
from velocia.families.spark.eventlog.summary import UnitSummary
from velocia.families.spark.eventlog.summary import summarize as summarize_log
from velocia.families.spark.package import DriverPackage, build_driver
from velocia.hardware.probe import hardware_manifest_hash, probe_local
from velocia.measures.builtins import ClusterInputs, Measure, UnitInputs
from velocia.measures.formula import parse, unknown_hw_inputs
from velocia.measures.plugins import PluginError
from velocia.measures.registry import load_measures
from velocia.runners.base import AppResult, AppSpec
from velocia.runners.external import ExternalRunner, declared_hardware
from velocia.runners.local import LocalRunner, container_memory_mb, keep_awake
from velocia.stats.summary import ratio_ci, summarize
from velocia.store.catalog import Catalog, as_json
from velocia.workloads import Unit, UnknownUnitError, get_workload, select_units
from velocia.workloads.synth import render_sql, synth_spec

TOTAL_UNIT = "_total"  # sum over all units of one repetition; only when every unit finished
APP_OVERHEAD_S = 600.0


class RunError(RuntimeError):
    pass


@dataclass
class Prepared:
    experiment: Experiment
    yaml_text: str
    spec_hash: str
    engines: dict[str, Engine]
    units: list[Unit]
    dataset: DatasetManifest  # the experiment's dataset (synth: the first sweep point's)
    measures: list[Measure]
    profiles: dict[str, HardwareProfile | None]  # by cluster name
    warnings: list[str]
    datasets: dict[str, DatasetManifest] = field(default_factory=dict)  # synth: by config_hash

    def dataset_for(self, cell: Cell) -> DatasetManifest:
        return self.datasets.get(cell.config_hash, self.dataset)


def _git_sha() -> str | None:
    here = Path(__file__).parent
    out = subprocess.run(
        ["git", "-C", str(here), "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    )
    if out.returncode != 0:
        return None
    dirty = subprocess.run(
        ["git", "-C", str(here), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    return out.stdout.strip() + ("-dirty" if dirty else "")


def prepare(path: Path, config: VelociaConfig, cluster_gate: bool = True) -> Prepared:
    """Validate everything that can be checked before the first application starts.

    ``cluster_gate``: refuse external clusters whose smoke test hasn't passed for the engine
    (off only for the smoke test itself).
    """
    warnings: list[str] = []
    try:
        experiment = load_model(path, Experiment)
    except ConfigError as exc:
        raise RunError(str(exc)) from exc
    known = load_engines(Path(config.engines_dir))
    unknown = [r.name for r in experiment.engine_refs if r.name not in known]
    if unknown:
        raise RunError(f"unknown engine(s): {', '.join(unknown)}")
    workload = get_workload(experiment.workload.suite)
    if workload is None:
        raise RunError(f"suite {experiment.workload.suite.value!r} has no workload yet")
    try:
        units = select_units(workload, experiment.workload.units)
    except UnknownUnitError as exc:
        raise RunError(str(exc)) from exc
    try:
        available = load_measures(Path(config.measures_dir))
    except ConfigError as exc:
        raise RunError(str(exc)) from exc
    missing = [m for m in experiment.measures if m not in available]
    if missing:
        raise RunError(
            f"unknown measure(s): {', '.join(missing)} (available: {', '.join(available)})"
        )

    clusters = experiment.named_clusters
    for name, cluster in clusters.items():
        if isinstance(cluster, K8sCluster):
            raise RunError(f"cluster {name!r}: the k8s runner is not available yet")
        if isinstance(cluster, ExternalCluster):
            if cluster.deploy_mode != "client":
                raise RunError(f"cluster {name!r}: the external runner supports client mode only")
            if cluster.hardware_profile is None:
                warnings.append(
                    f"cluster {name!r}: no hardware_profile; Velocia can't probe external nodes, "
                    "so the manifest only records the executor shape"
                )
    profiles: dict[str, HardwareProfile | None] = {}
    for name, cluster in clusters.items():
        profiles[name] = None
        if cluster.hardware_profile:
            profile_path = Path(config.hardware_dir) / f"{cluster.hardware_profile}.yaml"
            try:
                profiles[name] = load_model(profile_path, HardwareProfile)
            except ConfigError as exc:
                raise RunError(str(exc)) from exc

    if cluster_gate:
        for ref in experiment.engine_refs:
            cluster_name = ref.cluster or next(iter(clusters))
            cluster = clusters[cluster_name]
            if isinstance(cluster, ExternalCluster) and not cluster_checks.passed(
                Path(config.results_dir), known[ref.name], cluster
            ):
                raise RunError(
                    f"{ref.name} has no passing smoke test on cluster {cluster_name!r} with this "
                    f"engine definition and cluster configuration; run: velocia engines check "
                    f"{ref.name} --cluster {cluster_name} --experiment {path}"
                )

    check_hardware(experiment, profiles, [available[m] for m in experiment.measures])
    warnings += memory_warnings(experiment, known, clusters)

    for measure in [available[m] for m in experiment.measures]:
        if measure.formula is None:
            continue
        unknown_hw = unknown_hw_inputs(parse(measure.formula), list(profiles.values()))
        if unknown_hw:
            warnings.append(
                f"measure {measure.name}: {', '.join(unknown_hw)} is in no hardware profile of "
                "this experiment; its values will be null"
            )

    wl = experiment.workload
    results_dir = Path(config.results_dir)
    datasets: dict[str, DatasetManifest] = {}
    if wl.suite is Suite.synth and not wl.dataset_id:
        for point in expand_configs(experiment):
            ds_id = synth_dataset_id(synth_spec(wl, point.sweep), wl.format)
            datasets[point.config_hash] = _load_dataset(
                results_dir,
                ds_id,
                "velocia run generates synth datasets; or: velocia datagen "
                "--suite synth --spec <spec>",
            )
        dataset = next(iter(datasets.values()))
    else:
        if wl.dataset_id:
            ds_id = wl.dataset_id
        elif wl.scale_factor is not None:
            ds_id = make_dataset_id(wl.suite, wl.scale_factor, wl.format)
        elif wl.rows is not None and wl.suite in SPARK_SUITES:
            ds_id = spark_dataset_id(wl.suite, wl.rows, wl.format)
        else:
            raise RunError("workload needs scale_factor, rows or dataset_id")
        sf = f" --sf {wl.scale_factor:g}" if wl.scale_factor is not None else ""
        sf += f" --rows {wl.rows}" if wl.rows is not None else ""
        dataset = _load_dataset(
            results_dir,
            ds_id,
            f"velocia datagen --suite {wl.suite.value}{sf} --format {wl.format.value}",
        )

    return Prepared(
        experiment=experiment,
        yaml_text=path.read_text("utf-8"),
        spec_hash=stable_hash({"experiment": experiment.model_dump(mode="json")}),
        engines={r.name: known[r.name] for r in experiment.engine_refs},
        units=units,
        dataset=dataset,
        datasets=datasets,
        measures=[available[m] for m in experiment.measures],
        profiles=profiles,
        warnings=[*experiment.warnings(), *warnings],
    )


MEMORY_HEADROOM = 0.85  # of Docker's memory; the rest is for the VM and other containers


def _load_dataset(results_dir: Path, ds_id: str, how: str) -> DatasetManifest:
    manifest_path = results_dir / "datasets" / f"{ds_id}.json"
    if not manifest_path.is_file():
        raise RunError(f"dataset {ds_id} not found; generate it first: {how}")
    dataset = DatasetManifest.model_validate_json(manifest_path.read_text("utf-8"))
    if not Path(dataset.location).is_dir():
        raise RunError(f"dataset {ds_id}: data directory {dataset.location} is missing")
    return dataset


def memory_warnings(
    experiment: Experiment,
    engines: dict[str, Engine],
    clusters: dict[str, Cluster],
    docker_memory_gib: float | None = None,
) -> list[str]:
    """Cells whose container likely needs more memory than Docker can give it."""
    if docker_memory_gib is None:
        docker = probe_local()["node_types"][0].get("docker") or {}
        docker_memory_gib = docker.get("memory_gib")
    if not docker_memory_gib:
        return []
    out = []
    for ref in experiment.engine_refs:
        cluster = clusters[ref.cluster or next(iter(clusters))]
        if not isinstance(cluster, LocalCluster):
            continue
        conf = {**engines[ref.name].definition.spark_conf}
        conf.update(
            {k: spark_value(v) for k, v in experiment.spark_conf.items() if not isinstance(v, list)}
        )
        need_gib = container_memory_mb(cluster, conf) / 1024
        if need_gib > docker_memory_gib * MEMORY_HEADROOM:
            out.append(
                f"{ref.name} needs about {need_gib:.1f} GiB per container (heap, off-heap, "
                f"overhead) but Docker has {docker_memory_gib:.0f} GiB; expect out-of-memory "
                "kills. Give Docker more memory or lower executor_memory"
            )
    return out


NORMALIZED_MEASURES = frozenset({"cost_usd", "node_hours", "accelerator_hours", "energy_kwh"})


def check_hardware(
    experiment: Experiment,
    profiles: dict[str, HardwareProfile | None],
    measures: list[Measure],
    probe: Callable[[HardwareProfile | None], dict[str, Any]] = probe_local,
) -> None:
    """Refuse a cross-hardware comparison unless the experiment asks for it (03 §5).

    With ``comparison.allow_cross_hardware``, at least one normalized measure (cost, node-hours,
    accelerator-hours, energy or a custom formula) must sit next to time.
    """
    used = {ref.cluster or next(iter(profiles)) for ref in experiment.engine_refs}
    hashes = {name: hardware_manifest_hash(probe(profiles[name])) for name in sorted(used)}
    if len(set(hashes.values())) <= 1:
        return
    if not experiment.comparison.allow_cross_hardware:
        raise RunError(
            "engines run on different hardware (clusters "
            f"{', '.join(sorted(hashes))} have different hardware manifests); results are only "
            "compared within one hardware manifest. To compare anyway, set "
            "comparison.allow_cross_hardware: true and add a normalized measure such as cost_usd"
        )
    if not any(
        m.name in NORMALIZED_MEASURES or m.formula or m.source.startswith("plugin:")
        for m in measures
    ):
        raise RunError(
            "a cross-hardware comparison needs at least one normalized measure next to time "
            "(cost_usd, node_hours, accelerator_hours, energy_kwh or a custom formula)"
        )


class Orchestrator:
    def __init__(
        self,
        prepared: Prepared,
        config: VelociaConfig,
        results_dir: Path,
        echo: Callable[[str], None] = print,
    ) -> None:
        self.p = prepared
        self.config = config
        self.results_dir = results_dir
        self.echo = echo
        self.redactor = Redactor(config.redaction.key_patterns)
        # (external cluster, dataset) pairs verified on the cluster's storage
        self._staged: set[tuple[str, str]] = set()

    # --- run ------------------------------------------------------------------------------

    def run(self, resume: bool = False) -> str:
        awake = keep_awake()
        try:
            return self._run(resume)
        finally:
            if awake is not None:
                awake.terminate()

    def _run(self, resume: bool) -> str:
        exp = self.p.experiment
        with Catalog(self.results_dir) as catalog:
            existing = catalog.latest_unfinished(exp.name, self.p.spec_hash) if resume else None
            if resume and existing is None:
                raise RunError(f"nothing to resume: no unfinished experiment {exp.name!r}")
            if existing is not None:
                experiment_id = str(existing["experiment_id"])
                self.echo(f"Resuming {experiment_id}")
            else:
                stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
                experiment_id = f"{exp.name}-{stamp}"
                seed = int(hashlib.sha256(experiment_id.encode()).hexdigest()[:8], 16)
                created_at = datetime.now(UTC).replace(tzinfo=None)  # stored as UTC
                catalog.insert(
                    "experiment",
                    {
                        "experiment_id": experiment_id,
                        "name": exp.name,
                        "yaml": self._redacted_yaml(),
                        "spec_hash": self.p.spec_hash,
                        "created_at": created_at,
                        "velocia_version": __version__,
                        "status": "running",
                        "bootstrap_seed": seed,
                        "dataset_id": self.p.dataset.dataset_id,
                    },
                )
            self._record_dataset(catalog)

            cells = expand(exp)
            done = catalog.done_cells(experiment_id)
            driver = build_driver(self.results_dir / "drivers")
            hardware = probe_local()  # the local runner's node; profiles added per cluster
            for warning in self.p.warnings:
                self.echo(f"warning: {warning}")
            for cell in cells:
                if cell.cell_key in done:
                    self.echo(f"[{cell.index}/{len(cells)}] {cell.label}: done, skipped")
                    continue
                self._run_cell(catalog, experiment_id, cell, len(cells), driver, hardware)

            self.echo("Computing measures, statistics and correctness")
            compute_results(catalog, experiment_id, self.p, cells, self.config)
            catalog.write_results_parquet(
                experiment_id, self.results_dir / "results" / f"{experiment_id}.parquet"
            )
            catalog.update("experiment", {"experiment_id": experiment_id}, {"status": "done"})
        return experiment_id

    def _redacted_yaml(self) -> str:
        """The experiment as stored and reported: secrets in spark_conf etc. redacted."""
        data = yaml.safe_load(self.p.yaml_text)
        return yaml.safe_dump(self.redactor.redact(data), sort_keys=False)

    def _record_dataset(self, catalog: Catalog) -> None:
        unique = {d.dataset_id: d for d in [self.p.dataset, *self.p.datasets.values()]}
        for d in unique.values():
            self._record_one_dataset(catalog, d)

    def _record_one_dataset(self, catalog: Catalog, d: DatasetManifest) -> None:
        catalog.insert(
            "dataset",
            {
                "dataset_id": d.dataset_id,
                "suite": d.suite.value,
                "generator_params": as_json(d.generator_params),
                "seed": d.seed,
                "scale": d.scale,
                "format": d.data_format.value,
                "compression": d.compression,
                "row_group_size": d.row_group_size,
                "files": d.files,
                "bytes": d.bytes,
                "checksum": d.checksum,
                "manifest_json": d.model_dump_json(),
            },
        )

    def _run_cell(
        self,
        catalog: Catalog,
        experiment_id: str,
        cell: Cell,
        total: int,
        driver: DriverPackage,
        hardware_base: dict[str, Any],
    ) -> None:
        exp = self.p.experiment
        runner: LocalRunner | ExternalRunner
        if isinstance(cell.cluster, ExternalCluster):
            runner = ExternalRunner(cell.cluster)
        else:
            assert isinstance(cell.cluster, LocalCluster)
            runner = LocalRunner(cell.cluster, self.config.quiet_check.max_cpu_pct)
        problems = runner.check()
        engine = self.p.engines[cell.engine]
        if not problems and not runner.image_present(engine.definition.image):
            problems.append(
                f"image {engine.definition.image} not found; build it: "
                f"velocia engines build {engine.name}"
            )
        if problems:
            raise RunError("; ".join(problems))

        profile = self.p.profiles[cell.cluster_name]
        if isinstance(runner, ExternalRunner):
            assert isinstance(cell.cluster, ExternalCluster)
            self._stage(runner, cell, driver, engine.definition.image)
            hardware = declared_hardware(cell.cluster_name, cell.cluster, profile)
        else:
            hardware = {
                **hardware_base,
                "profile": profile.model_dump(mode="json") if profile else None,
            }
            if profile is not None and profile.accelerators_per_node:
                hardware = probe_local(profile)
        hw_hash = hardware_manifest_hash(hardware)
        quiet = runner.quiet_check()
        if not quiet.ok:
            self.echo(f"warning: quiet check flagged: {'; '.join(quiet.reasons)}")

        catalog.forget_cell(experiment_id, cell.cell_key)
        catalog.insert(
            "cell",
            {
                "experiment_id": experiment_id,
                "cell_key": cell.cell_key,
                "idx": cell.index,
                "engine": cell.engine,
                "cluster": cell.cluster_name,
                "config_hash": cell.config_hash,
                "mode": cell.mode.value,
                "varying": as_json(cell.config.varying),
                "hardware_manifest_hash": hw_hash,
                "status": "running",
                "quiet_check": as_json(quiet.reasons),
            },
        )

        base = f"{experiment_id}-c{cell.index:03d}"
        reps = list(range(1, exp.repetitions + 1))
        apps: list[tuple[str, str, list[int], bool]] = []  # run_id, task, reps, warmup
        if cell.mode is Mode.cold:
            apps += [(f"{base}-r{rep}", "time", [rep], False) for rep in reps]
        else:
            apps.append((f"{base}-warm", "time", reps, True))
        apps.append((f"{base}-check", "check", [], False))

        conf = {**engine.definition.spark_conf, **cell.config.spark_conf}
        image = inspect_image(engine.definition.image)
        dataset = self.p.dataset_for(cell)
        timeout_s = exp.timeout.total_seconds()
        for run_id, task, app_reps, warmup in apps:
            passes = (len(app_reps) + (1 if warmup else 0)) if task == "time" else 1
            app = AppSpec(
                run_id=run_id,
                image=engine.definition.image,
                spark_conf=conf,
                driver=driver,
                task=task,  # type: ignore[arg-type]
                mode=cell.mode.value,
                reps=app_reps,
                warmup=warmup,
                units=self._units(cell),
                dataset_location=Path(dataset.location),
                tables=[t.name for t in dataset.tables],
                dataset_id=dataset.dataset_id,
                timeout_per_unit_s=timeout_s,
                app_timeout_s=APP_OVERHEAD_S + passes * len(self.p.units) * timeout_s,
                float_round_decimals=self.config.correctness.float_round_decimals,
            )
            catalog.insert(
                "run",
                {
                    "run_id": run_id,
                    "experiment_id": experiment_id,
                    "cell_key": cell.cell_key,
                    "engine": cell.engine,
                    "cluster": cell.cluster_name,
                    "config_hash": cell.config_hash,
                    "mode": cell.mode.value,
                    "rep": ",".join(map(str, app_reps)) or "check",
                    "task": task,
                    "app_id": None,
                    "status": "running",
                    "manifest_json": None,
                },
            )
            try:
                result = runner.run(app)
            finally:
                runner.teardown()
            run_dir = self.results_dir / "runs" / run_id
            output = collect(result, run_dir, self.redactor)
            manifest = self._manifest(
                experiment_id,
                cell,
                app,
                result,
                output,
                engine,
                image,
                driver,
                hardware,
                hw_hash,
                quiet.reasons,
            )
            (run_dir / "manifest.json").write_text(as_json(manifest) + "\n", "utf-8")
            app_id = (output.get("spark") or {}).get("app_id")
            catalog.update(
                "run",
                {"run_id": run_id},
                {"status": result.status, "app_id": app_id, "manifest_json": as_json(manifest)},
            )
            self._record_outputs(catalog, cell, app, result, output, run_dir)
            self._progress(cell, total, app, result, output)
        catalog.update(
            "cell",
            {"experiment_id": experiment_id, "cell_key": cell.cell_key},
            {"status": "done"},
        )

    def _units(self, cell: Cell) -> list[dict[str, str | None]]:
        """The units as the driver gets them; synth SQL gets the cell's parameters."""
        wl = self.p.experiment.workload
        spec = synth_spec(wl, cell.config.sweep) if wl.suite is Suite.synth else None
        return [
            {
                "id": u.id,
                "sql": render_sql(u.sql, spec) if spec is not None and u.sql else u.sql,
                "job": u.job,
            }
            for u in self.p.units
        ]

    def _stage(self, runner: ExternalRunner, cell: Cell, driver: DriverPackage, image: str) -> None:
        """Put the cell's dataset on the cluster's storage and verify it, once per run."""
        cluster_name = cell.cluster_name
        dataset = self.p.dataset_for(cell)
        if (cluster_name, dataset.dataset_id) in self._staged:
            return
        self.echo(f"Staging dataset {dataset.dataset_id} on cluster {cluster_name!r}")
        try:
            problems, tail = runner.stage(dataset, driver, image)
        finally:
            runner.teardown()
        if problems:
            log = self.redactor.redact_text(tail)
            raise RunError(
                f"dataset {dataset.dataset_id} on cluster {cluster_name!r}: "
                + "; ".join(problems)
                + (f"\n{log}" if log else "")
            )
        self._staged.add((cluster_name, dataset.dataset_id))

    def _manifest(
        self,
        experiment_id: str,
        cell: Cell,
        app: AppSpec,
        result: AppResult,
        output: dict[str, Any],
        engine: Engine,
        image: Any,
        driver: DriverPackage,
        hardware: dict[str, Any],
        hw_hash: str,
        quiet_reasons: list[str],
    ) -> dict[str, Any]:
        cold_label = None
        if cell.mode is Mode.cold and app.task == "time":
            cold_label = "cold" if result.cache_dropped else "cold (cache not dropped)"
        return {
            "format": "velocia-run-manifest",
            "version": 1,
            "run_id": app.run_id,
            "experiment_id": experiment_id,
            "cell_key": cell.cell_key,
            "config_hash": cell.config_hash,
            "task": app.task,
            "mode": cell.mode.value,
            "mode_label": cold_label or cell.mode.value,
            "reps": app.reps,
            "warmup": app.warmup,
            "status": result.status,
            "exit_code": result.exit_code,
            "started_at": datetime.fromtimestamp(result.started_at, UTC).isoformat(),
            "ended_at": datetime.fromtimestamp(result.ended_at, UTC).isoformat(),
            "family": engine.definition.family,
            "engine": {
                "name": engine.name,
                "image": engine.definition.image,
                "image_id": image.image_id if image else None,
                "repo_digests": list(image.repo_digests) if image else [],
                "architecture": image.architecture if image else None,
            },
            "spark": output.get("spark"),
            "spark_conf": self.redactor.redact(app.spark_conf),
            "cluster": {
                "name": cell.cluster_name,
                **self.redactor.redact(cell.cluster.model_dump(mode="json")),
            },
            "hardware": hardware,
            "hardware_manifest_hash": hw_hash,
            "dataset_id": self.p.dataset_for(cell).dataset_id,
            "dataset_checksum": self.p.dataset_for(cell).checksum,
            "units": [u.id for u in self.p.units],
            "driver_sha256": driver.sha256,
            "velocia_version": __version__,
            "velocia_git_sha": _git_sha(),
            "quiet_check": {"ok": not quiet_reasons, "reasons": quiet_reasons},
            "host_slept_s": result.host_slept_s,
            "notes": result.notes,
            "measures": [m.describe() for m in self.p.measures],
            "label": results_label(self.p.dataset_for(cell).label),
        }

    def _record_outputs(
        self,
        catalog: Catalog,
        cell: Cell,
        app: AppSpec,
        result: AppResult,
        output: dict[str, Any],
        run_dir: Path,
    ) -> None:
        reported = output.get("units", [])
        if app.task == "time":
            summaries = self._summaries(run_dir)
            rows = [
                {
                    "run_id": app.run_id,
                    "unit_id": u["unit_id"],
                    "rep": u["rep"],
                    "wall_ms": u["wall_ms"],
                    "status": u["status"],
                    "discarded": u["discarded"],
                    "error": u.get("error"),
                    "summary": as_json(summaries[(u["unit_id"], u["rep"])].as_dict())
                    if (u["unit_id"], u["rep"]) in summaries
                    else None,
                }
                for u in reported
            ]
            seen = {(r["unit_id"], r["rep"]) for r in rows}
            # Units the application never reached (it crashed or timed out) count as failed.
            status = "timeout" if result.status == "timeout" else "failed"
            for rep in ([0] if app.warmup else []) + app.reps:
                for unit in app.units:
                    if (unit["id"], rep) not in seen:
                        rows.append(
                            {
                                "run_id": app.run_id,
                                "unit_id": unit["id"],
                                "rep": rep,
                                "wall_ms": None,
                                "status": status,
                                "discarded": rep == 0,
                                "error": f"application {result.status}",
                                "summary": None,
                            }
                        )
            catalog.insert_many("unit_run", rows)
        else:
            by_id = {u["unit_id"]: u for u in reported}
            catalog.insert_many(
                "unit_check",
                [
                    {
                        "run_id": app.run_id,
                        "cell_key": cell.cell_key,
                        "unit_id": unit["id"],
                        "rows": by_id.get(unit["id"], {}).get("rows"),
                        "checksum": by_id.get(unit["id"], {}).get("checksum"),
                        "status": by_id.get(unit["id"], {}).get("status", "failed"),
                        "error": by_id.get(unit["id"], {}).get(
                            "error", f"application {result.status}"
                        ),
                        "check_values": (
                            as_json(by_id[unit["id"]]["values"])
                            if by_id.get(unit["id"], {}).get("values") is not None
                            else None
                        ),
                        "tolerance": by_id.get(unit["id"], {}).get("tolerance"),
                    }
                    for unit in app.units
                ],
            )

    def _summaries(self, run_dir: Path) -> dict[tuple[str, int], UnitSummary]:
        """Per-unit event-log summaries of a run; empty when its log is missing or unreadable."""
        log_dir = run_dir / "eventlog"
        if not log_dir.is_dir():
            return {}
        try:
            return summarize_log(log_dir).units
        except EventLogError as exc:
            self.echo(f"warning: {run_dir.name}: event log unreadable: {exc}")
            return {}

    def _progress(
        self, cell: Cell, total: int, app: AppSpec, result: AppResult, output: dict[str, Any]
    ) -> None:
        units = [u for u in output.get("units", []) if not u.get("discarded")]
        ok = sum(1 for u in units if u.get("status") == "ok")
        what = (
            "correctness pass"
            if app.task == "check"
            else (f"rep {app.reps[0]}" if len(app.reps) == 1 else f"reps 1-{app.reps[-1]} (warm)")
        )
        secs = result.ended_at - result.started_at
        line = f"[{cell.index}/{total}] {cell.label} {what}: {ok}/{len(units)} ok, {secs:.0f}s"
        if result.status != "finished":
            line += f" — application {result.status}"
        if result.host_slept_s:
            line += f" — WARNING: host slept {result.host_slept_s:.0f}s, timings unreliable"
        for note in result.notes:
            line += f" — {note}"
        self.echo(line)


# --- results ------------------------------------------------------------------------------


def _correctness(
    checks: dict[str, dict[str, dict[str, Any]]], cell_key: str, ref_key: str | None, unit: str
) -> str:
    mine = checks.get(cell_key, {}).get(unit)
    ref = checks.get(ref_key, {}).get(unit) if ref_key else None
    if mine is not None and mine["status"] in ("failed", "invalid"):
        return "fail"  # the query errors, or the job's validator rejects its output
    if mine is None or ref is None or mine["status"] != "ok" or ref["status"] != "ok":
        return "not_checked"  # missing, timed out, or no usable reference
    same = mine["rows"] == ref["rows"] and mine["checksum"] == ref["checksum"]
    if same and (mine.get("check_values") or ref.get("check_values")):
        same = within_tolerance(
            json.loads(mine.get("check_values") or "[]"),
            json.loads(ref.get("check_values") or "[]"),
            max(mine.get("tolerance") or 0.0, ref.get("tolerance") or 0.0),
        )
    return "pass" if same else "fail"


def within_tolerance(mine: list[float], ref: list[float], tolerance: float) -> bool:
    """Iterative floating-point results (PageRank, KMeans): every value within a relative
    ``tolerance`` of the reference's (absolute near zero); the exact parts are the checksum."""
    if len(mine) != len(ref):
        return False
    return all(
        abs(a - b) <= tolerance * max(abs(a), abs(b), 1.0) for a, b in zip(mine, ref, strict=True)
    )


def compute_results(
    catalog: Catalog,
    experiment_id: str,
    prepared: Prepared,
    cells: list[Cell],
    config: VelociaConfig,
) -> None:
    exp = prepared.experiment
    seed = catalog.experiment(experiment_id)["bootstrap_seed"]  # type: ignore[index]
    unit_ids = [u.id for u in prepared.units]
    cell_rows = {
        r["cell_key"]: r
        for r in catalog.query("SELECT * FROM cell WHERE experiment_id = ?", [experiment_id])
    }

    # Measure values per run, unit and repetition (warm-up excluded).
    unit_runs = catalog.query(
        "SELECT u.*, r.cell_key FROM unit_run u JOIN run r USING (run_id) "
        "WHERE r.experiment_id = ? AND NOT u.discarded",
        [experiment_id],
    )
    cluster_inputs = {
        c.cell_key: ClusterInputs.of(
            c.cluster, prepared.profiles[c.cluster_name], config.pricing.default_node_hour_usd
        )
        for c in cells
    }
    values: dict[tuple[str, str, str], list[float]] = defaultdict(list)  # cell, unit, measure
    timeouts: dict[tuple[str, str], int] = defaultdict(int)
    mv_rows = []
    per_rep: dict[tuple[str, str, int, str], list[float | None]] = defaultdict(list)
    for ur in unit_runs:
        if ur["status"] == "timeout":
            timeouts[(ur["cell_key"], ur["unit_id"])] += 1
            timeouts[(ur["cell_key"], TOTAL_UNIT)] += 1
        wall_s = ur["wall_ms"] / 1000 if ur["status"] == "ok" else None
        telemetry = json.loads(ur["summary"]) if ur.get("summary") else None
        inputs = UnitInputs(
            wall_s=wall_s,
            telemetry=UnitSummary(**telemetry).telemetry() if telemetry else {},
        )
        for m in prepared.measures:
            try:
                value, reason = m.compute(inputs, cluster_inputs[ur["cell_key"]])
            except PluginError as exc:
                raise RunError(str(exc)) from exc
            mv_rows.append(
                {
                    "run_id": ur["run_id"],
                    "unit_id": ur["unit_id"],
                    "rep": ur["rep"],
                    "measure": m.name,
                    "value": value,
                    "null_reason": reason,
                }
            )
            per_rep[(ur["cell_key"], ur["run_id"], ur["rep"], m.name)].append(value)
            if value is not None:
                values[(ur["cell_key"], ur["unit_id"], m.name)].append(value)
    for (cell_key, _run, _rep, measure), rep_values in per_rep.items():
        if len(rep_values) == len(unit_ids) and all(v is not None for v in rep_values):
            values[(cell_key, TOTAL_UNIT, measure)].append(
                sum(v for v in rep_values if v is not None)
            )
    catalog.execute(
        "DELETE FROM measure_value "
        "WHERE run_id IN (SELECT run_id FROM run WHERE experiment_id = ?)",
        [experiment_id],
    )
    catalog.insert_many("measure_value", mv_rows)

    checks: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in catalog.query(
        "SELECT c.* FROM unit_check c JOIN run r USING (run_id) WHERE r.experiment_id = ?",
        [experiment_id],
    ):
        checks[row["cell_key"]][row["unit_id"]] = row

    reference = {
        (c.config_hash, c.mode): c.cell_key for c in cells if c.engine == exp.reference_engine
    }
    results = []
    for cell in cells:
        ref_key = reference.get((cell.config_hash, cell.mode))
        is_ref = ref_key == cell.cell_key
        per_unit = {u: _correctness(checks, cell.cell_key, ref_key, u) for u in unit_ids}
        statuses = set(per_unit.values())
        per_unit[TOTAL_UNIT] = (
            "fail" if "fail" in statuses else "not_checked" if "not_checked" in statuses else "pass"
        )
        for unit in [*unit_ids, TOTAL_UNIT]:
            for m in prepared.measures:
                vals = values.get((cell.cell_key, unit, m.name), [])
                s = summarize(vals, config.statistics.cv_flag_threshold)
                r = None
                if not is_ref and ref_key is not None:
                    r = ratio_ci(
                        vals,
                        values.get((ref_key, unit, m.name), []),
                        m.direction,
                        config.statistics.bootstrap_resamples,
                        seed + hash_index(cell.cell_key, unit, m.name),
                    )
                results.append(
                    {
                        "experiment_id": experiment_id,
                        "cell_key": cell.cell_key,
                        "engine": cell.engine,
                        "cluster": cell.cluster_name,
                        "hardware_manifest_hash": cell_rows[cell.cell_key][
                            "hardware_manifest_hash"
                        ],
                        "config_hash": cell.config_hash,
                        "mode": cell.mode.value,
                        "unit_id": unit,
                        "measure": m.name,
                        "measure_unit": m.unit,
                        "direction": m.direction.value,
                        "n": s.n,
                        "median": s.median,
                        "p95": s.p95,
                        "cv": s.cv,
                        "cv_flag": s.cv_flag,
                        "ratio": r.ratio if r else None,
                        "ci_low": r.ci_low if r else None,
                        "ci_high": r.ci_high if r else None,
                        "correctness": per_unit[unit],
                        "timeouts": timeouts[(cell.cell_key, unit)],
                    }
                )
    catalog.replace_results(experiment_id, results)


def hash_index(*parts: str) -> int:
    """A stable per-series offset so each bootstrap has its own reproducible stream."""
    return int(hashlib.sha256("\x00".join(parts).encode()).hexdigest()[:6], 16)
