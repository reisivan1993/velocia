"""``velocia run``: execute an experiment (docs/06-design.md §3).

Cells run one at a time. For each: quiet check, timed applications (cold: one per repetition;
warm: one application with a discarded warm-up), then one untimed correctness pass (ADR-5).
Every state change goes to the catalog before the next step, so ``--resume`` continues an
interrupted experiment with the first unfinished cell. Measures, statistics and correctness
against the reference engine are computed at the end.
"""

import hashlib
import subprocess
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
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
    HardwareProfile,
    LocalCluster,
    Mode,
    VelociaConfig,
)
from velocia.datagen.duckdb_tpc import dataset_id as make_dataset_id
from velocia.datagen.manifest import DatasetManifest
from velocia.engines.docker import inspect_image
from velocia.engines.registry import Engine, load_engines
from velocia.experiment.expander import DEFAULT_CLUSTER, Cell, expand, stable_hash
from velocia.families.spark.package import DriverPackage, build_driver
from velocia.hardware.probe import hardware_manifest_hash, probe_local
from velocia.measures.builtins import BUILTINS, ClusterInputs, Measure, UnitInputs
from velocia.runners.base import AppResult, AppSpec
from velocia.runners.local import LocalRunner, keep_awake
from velocia.stats.summary import ratio_ci, summarize
from velocia.store.catalog import Catalog, as_json
from velocia.workloads import Unit, UnknownUnitError, get_workload, select_units

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
    dataset: DatasetManifest
    measures: list[Measure]
    profiles: dict[str, HardwareProfile | None]  # by cluster name


def _git_sha() -> str | None:
    here = Path(__file__).parent
    out = subprocess.run(
        ["git", "-C", str(here), "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    )
    if out.returncode != 0:
        return None
    dirty = subprocess.run(
        ["git", "-C", str(here), "status", "--porcelain"], capture_output=True, text=True,
        check=False,
    ).stdout.strip()  # fmt: skip
    return out.stdout.strip() + ("-dirty" if dirty else "")


def prepare(path: Path, config: VelociaConfig) -> Prepared:
    """Validate everything that can be checked before the first application starts."""
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
    missing = [m for m in experiment.measures if m not in BUILTINS]
    if missing:
        raise RunError(
            f"unknown measure(s): {', '.join(missing)} (custom measures are not supported yet)"
        )

    clusters: dict[str, Cluster] = experiment.clusters or {}
    if experiment.cluster is not None:
        clusters = {DEFAULT_CLUSTER: experiment.cluster}
    for name, cluster in clusters.items():
        if not isinstance(cluster, LocalCluster):
            raise RunError(f"cluster {name!r}: only the local runner is available so far")
    profiles: dict[str, HardwareProfile | None] = {}
    for name, cluster in clusters.items():
        profiles[name] = None
        if cluster.hardware_profile:
            profile_path = Path(config.hardware_dir) / f"{cluster.hardware_profile}.yaml"
            try:
                profiles[name] = load_model(profile_path, HardwareProfile)
            except ConfigError as exc:
                raise RunError(str(exc)) from exc

    wl = experiment.workload
    if wl.dataset_id:
        ds_id = wl.dataset_id
    elif wl.scale_factor is not None:
        ds_id = make_dataset_id(wl.suite, wl.scale_factor, wl.format)
    else:
        raise RunError("workload needs scale_factor or dataset_id")
    manifest_path = Path(config.results_dir) / "datasets" / f"{ds_id}.json"
    if not manifest_path.is_file():
        sf = f" --sf {wl.scale_factor:g}" if wl.scale_factor is not None else ""
        raise RunError(
            f"dataset {ds_id} not found; generate it first: "
            f"velocia datagen --suite {wl.suite.value}{sf} --format {wl.format.value}"
        )
    dataset = DatasetManifest.model_validate_json(manifest_path.read_text("utf-8"))
    if not Path(dataset.location).is_dir():
        raise RunError(f"dataset {ds_id}: data directory {dataset.location} is missing")

    return Prepared(
        experiment=experiment,
        yaml_text=path.read_text("utf-8"),
        spec_hash=stable_hash({"experiment": experiment.model_dump(mode="json")}),
        engines={r.name: known[r.name] for r in experiment.engine_refs},
        units=units,
        dataset=dataset,
        measures=[BUILTINS[m] for m in experiment.measures],
        profiles=profiles,
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
                catalog.insert("experiment", {
                    "experiment_id": experiment_id, "name": exp.name,
                    "yaml": self._redacted_yaml(),
                    "spec_hash": self.p.spec_hash, "created_at": created_at,
                    "velocia_version": __version__, "status": "running",
                    "bootstrap_seed": seed, "dataset_id": self.p.dataset.dataset_id,
                })  # fmt: skip
            self._record_dataset(catalog)

            cells = expand(exp)
            done = catalog.done_cells(experiment_id)
            driver = build_driver(self.results_dir / "drivers")
            hardware = probe_local()  # the local runner's node; profiles added per cluster
            for warning in exp.warnings():
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
        d = self.p.dataset
        catalog.insert("dataset", {
            "dataset_id": d.dataset_id, "suite": d.suite.value,
            "generator_params": as_json(d.generator_params), "seed": d.seed, "scale": d.scale,
            "format": d.data_format.value, "compression": d.compression,
            "row_group_size": d.row_group_size, "files": d.files, "bytes": d.bytes,
            "checksum": d.checksum, "manifest_json": d.model_dump_json(),
        })  # fmt: skip

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
        catalog.insert("cell", {
            "experiment_id": experiment_id, "cell_key": cell.cell_key, "idx": cell.index,
            "engine": cell.engine, "cluster": cell.cluster_name,
            "config_hash": cell.config_hash, "mode": cell.mode.value,
            "varying": as_json(cell.config.varying), "hardware_manifest_hash": hw_hash,
            "status": "running", "quiet_check": as_json(quiet.reasons),
        })  # fmt: skip

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
        timeout_s = exp.timeout.total_seconds()
        for run_id, task, app_reps, warmup in apps:
            passes = (len(app_reps) + (1 if warmup else 0)) if task == "time" else 1
            app = AppSpec(
                run_id=run_id, image=engine.definition.image, spark_conf=conf, driver=driver,
                task=task,  # type: ignore[arg-type]
                mode=cell.mode.value, reps=app_reps, warmup=warmup,
                units=[{"id": u.id, "sql": u.sql or ""} for u in self.p.units],
                dataset_location=Path(self.p.dataset.location),
                tables=[t.name for t in self.p.dataset.tables],
                timeout_per_unit_s=timeout_s,
                app_timeout_s=APP_OVERHEAD_S + passes * len(self.p.units) * timeout_s,
                float_round_decimals=self.config.correctness.float_round_decimals,
            )  # fmt: skip
            catalog.insert("run", {
                "run_id": run_id, "experiment_id": experiment_id, "cell_key": cell.cell_key,
                "engine": cell.engine, "cluster": cell.cluster_name,
                "config_hash": cell.config_hash, "mode": cell.mode.value,
                "rep": ",".join(map(str, app_reps)) or "check", "task": task,
                "app_id": None, "status": "running", "manifest_json": None,
            })  # fmt: skip
            try:
                result = runner.run(app)
            finally:
                runner.teardown()
            run_dir = self.results_dir / "runs" / run_id
            output = collect(result, run_dir, self.redactor)
            manifest = self._manifest(
                experiment_id, cell, app, result, output, engine, image, driver, hardware, hw_hash,
                quiet.reasons,
            )  # fmt: skip
            (run_dir / "manifest.json").write_text(as_json(manifest) + "\n", "utf-8")
            app_id = (output.get("spark") or {}).get("app_id")
            catalog.update(
                "run", {"run_id": run_id},
                {"status": result.status, "app_id": app_id, "manifest_json": as_json(manifest)},
            )  # fmt: skip
            self._record_outputs(catalog, cell, app, result, output)
            self._progress(cell, total, app, result, output)
        catalog.update(
            "cell", {"experiment_id": experiment_id, "cell_key": cell.cell_key},
            {"status": "done"},
        )  # fmt: skip

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
            "dataset_id": self.p.dataset.dataset_id,
            "dataset_checksum": self.p.dataset.checksum,
            "units": [u.id for u in self.p.units],
            "driver_sha256": driver.sha256,
            "velocia_version": __version__,
            "velocia_git_sha": _git_sha(),
            "quiet_check": {"ok": not quiet_reasons, "reasons": quiet_reasons},
            "host_slept_s": result.host_slept_s,
            "label": f"{self.p.dataset.label}; not an official TPC result",
        }

    def _record_outputs(
        self, catalog: Catalog, cell: Cell, app: AppSpec, result: AppResult, output: dict[str, Any]
    ) -> None:
        reported = output.get("units", [])
        if app.task == "time":
            rows = [
                {
                    "run_id": app.run_id, "unit_id": u["unit_id"], "rep": u["rep"],
                    "wall_ms": u["wall_ms"], "status": u["status"], "discarded": u["discarded"],
                    "error": u.get("error"),
                }
                for u in reported
            ]  # fmt: skip
            seen = {(r["unit_id"], r["rep"]) for r in rows}
            # Units the application never reached (it crashed or timed out) count as failed.
            status = "timeout" if result.status == "timeout" else "failed"
            for rep in ([0] if app.warmup else []) + app.reps:
                for unit in app.units:
                    if (unit["id"], rep) not in seen:
                        rows.append({
                            "run_id": app.run_id, "unit_id": unit["id"], "rep": rep,
                            "wall_ms": None, "status": status, "discarded": rep == 0,
                            "error": f"application {result.status}",
                        })  # fmt: skip
            catalog.insert_many("unit_run", rows)
        else:
            by_id = {u["unit_id"]: u for u in reported}
            catalog.insert_many("unit_check", [
                {
                    "run_id": app.run_id, "cell_key": cell.cell_key, "unit_id": unit["id"],
                    "rows": by_id.get(unit["id"], {}).get("rows"),
                    "checksum": by_id.get(unit["id"], {}).get("checksum"),
                    "status": by_id.get(unit["id"], {}).get("status", "failed"),
                    "error": by_id.get(unit["id"], {}).get("error", f"application {result.status}"),
                }
                for unit in app.units
            ])  # fmt: skip

    def _progress(
        self, cell: Cell, total: int, app: AppSpec, result: AppResult, output: dict[str, Any]
    ) -> None:
        units = [u for u in output.get("units", []) if not u.get("discarded")]
        ok = sum(1 for u in units if u.get("status") == "ok")
        what = "correctness pass" if app.task == "check" else (
            f"rep {app.reps[0]}" if len(app.reps) == 1 else f"reps 1-{app.reps[-1]} (warm)"
        )  # fmt: skip
        secs = result.ended_at - result.started_at
        line = f"[{cell.index}/{total}] {cell.label} {what}: {ok}/{len(units)} ok, {secs:.0f}s"
        if result.status != "finished":
            line += f" — application {result.status}"
        if result.host_slept_s:
            line += f" — WARNING: host slept {result.host_slept_s:.0f}s, timings unreliable"
        self.echo(line)


# --- results ------------------------------------------------------------------------------


def _correctness(
    checks: dict[str, dict[str, dict[str, Any]]], cell_key: str, ref_key: str | None, unit: str
) -> str:
    mine = checks.get(cell_key, {}).get(unit)
    ref = checks.get(ref_key, {}).get(unit) if ref_key else None
    if mine is None or ref is None or mine["status"] != "ok" or ref["status"] != "ok":
        return "fail" if mine is not None and mine["status"] != "ok" else "not_checked"
    same = mine["rows"] == ref["rows"] and mine["checksum"] == ref["checksum"]
    return "pass" if same else "fail"


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
        inputs = UnitInputs(wall_s=wall_s)
        for m in prepared.measures:
            value, reason = m.compute(inputs, cluster_inputs[ur["cell_key"]])
            mv_rows.append({
                "run_id": ur["run_id"], "unit_id": ur["unit_id"], "rep": ur["rep"],
                "measure": m.name, "value": value, "null_reason": reason,
            })  # fmt: skip
            per_rep[(ur["cell_key"], ur["run_id"], ur["rep"], m.name)].append(value)
            if value is not None:
                values[(ur["cell_key"], ur["unit_id"], m.name)].append(value)
    for (cell_key, _run, _rep, measure), rep_values in per_rep.items():
        if len(rep_values) == len(unit_ids) and all(v is not None for v in rep_values):
            values[(cell_key, TOTAL_UNIT, measure)].append(
                sum(v for v in rep_values if v is not None)
            )
    catalog.con.execute(
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
                        vals, values.get((ref_key, unit, m.name), []), m.direction,
                        config.statistics.bootstrap_resamples,
                        seed + hash_index(cell.cell_key, unit, m.name),
                    )  # fmt: skip
                results.append({
                    "experiment_id": experiment_id, "cell_key": cell.cell_key,
                    "engine": cell.engine, "cluster": cell.cluster_name,
                    "hardware_manifest_hash": cell_rows[cell.cell_key]["hardware_manifest_hash"],
                    "config_hash": cell.config_hash, "mode": cell.mode.value, "unit_id": unit,
                    "measure": m.name, "measure_unit": m.unit, "direction": m.direction.value,
                    "n": s.n, "median": s.median, "p95": s.p95, "cv": s.cv,
                    "cv_flag": s.cv_flag, "ratio": r.ratio if r else None,
                    "ci_low": r.ci_low if r else None, "ci_high": r.ci_high if r else None,
                    "correctness": per_unit[unit], "timeouts": timeouts[(cell.cell_key, unit)],
                })  # fmt: skip
    catalog.replace_results(experiment_id, results)


def hash_index(*parts: str) -> int:
    """A stable per-series offset so each bootstrap has its own reproducible stream."""
    return int(hashlib.sha256("\x00".join(parts).encode()).hexdigest()[:6], 16)
