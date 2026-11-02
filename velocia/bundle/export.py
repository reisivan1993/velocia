"""``velocia bundle <experiment_id>``: one zip with everything an analysis tool needs.

Layout (docs/04-interfaces.md §7)::

    bundle.json, experiment.yaml, datasets/<id>.json, hardware/<profile>.yaml,
    measures/<measure>.yaml, runs/<run_id>/{manifest,timings,checks}.json,
    runs/<run_id>/eventlog/..., results/results.parquet, pairing.parquet

Everything in the results directory is already redacted; the bundle passes it through the
redactor once more on the way out. Event logs are streamed line by line (CLAUDE.md).
``strip_sql`` blanks SQL text, plan strings and call sites in event logs (D-10), keeping plan
node names, metrics and Velocia's tags, so a bundle can leave the team.
"""

import json
import tempfile
import zipfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import yaml

from velocia import __version__
from velocia.bundle.model import BUNDLE_FILE, PAIRING_COLUMNS, BundleManifest, Producer
from velocia.collect.redaction import Redactor
from velocia.store.catalog import Catalog

TAG_PREFIX = "velocia:"
RUN_FILES = ("manifest.json", "timings.json", "checks.json")


class BundleError(RuntimeError):
    pass


def pairing_rows(catalog: Catalog, experiment_id: str, reference: str) -> list[dict[str, Any]]:
    """One row per engine per (unit, config, mode): the repetition at the median wall time.

    With an even count the lower of the two middle repetitions is chosen, so the row always
    points at a real execution a consumer can find in the event log by its tag.
    """
    rows = catalog.query(
        "SELECT u.run_id, u.unit_id, u.rep, u.wall_ms, r.engine, r.cluster, r.config_hash, "
        "r.mode, c.hardware_manifest_hash FROM unit_run u JOIN run r USING (run_id) "
        "JOIN cell c ON c.experiment_id = r.experiment_id AND c.cell_key = r.cell_key "
        "WHERE r.experiment_id = ? AND r.task = 'time' AND u.status = 'ok' "
        "AND NOT u.discarded AND u.wall_ms IS NOT NULL",
        [experiment_id],
    )
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(row["unit_id"], row["config_hash"], row["mode"], row["engine"])].append(row)
    out = []
    for (unit_id, config_hash, mode, engine), reps in sorted(groups.items()):
        reps.sort(key=lambda r: (r["wall_ms"], r["rep"]))
        median = reps[(len(reps) - 1) // 2]
        out.append(
            {
                "unit_id": unit_id,
                "config_hash": config_hash,
                "mode": mode,
                "engine": engine,
                "cluster": median["cluster"],
                "hardware_manifest_hash": median["hardware_manifest_hash"],
                "run_id": median["run_id"],
                "rep": median["rep"],
                "wall_ms": median["wall_ms"],
                "is_reference": engine == reference,
            }
        )
    return out


def _write_pairing(rows: list[dict[str, Any]], dest: Path) -> None:
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE pairing (unit_id VARCHAR, config_hash VARCHAR, mode VARCHAR, "
            "engine VARCHAR, cluster VARCHAR, hardware_manifest_hash VARCHAR, run_id VARCHAR, "
            "rep INTEGER, wall_ms DOUBLE, is_reference BOOLEAN)"
        )
        if rows:
            con.executemany(
                f"INSERT INTO pairing VALUES ({', '.join('?' * len(PAIRING_COLUMNS))})",
                [[r[c] for c in PAIRING_COLUMNS] for r in rows],
            )
        path = str(dest).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM pairing ORDER BY unit_id, config_hash, mode, engine) "
            f"TO '{path}' (FORMAT parquet)"
        )
    finally:
        con.close()


def _strip_plan(plan: dict[str, Any]) -> None:
    stack = [plan]
    while stack:
        node = stack.pop()
        node["simpleString"] = ""
        node["metadata"] = {}
        stack.extend(node.get("children") or [])


def strip_sql(event: dict[str, Any]) -> dict[str, Any]:
    """Blank SQL text, plan strings (literals, paths) and call sites; keep structure and tags."""
    kind = str(event.get("Event", ""))
    if kind.endswith(("SQLExecutionStart", "SQLAdaptiveExecutionUpdate")):
        for key in ("details", "physicalPlanDescription"):
            if key in event:
                event[key] = ""
        if not str(event.get("description", "")).startswith(TAG_PREFIX):
            event["description"] = ""
        if isinstance(event.get("sparkPlanInfo"), dict):
            _strip_plan(event["sparkPlanInfo"])
    props = event.get("Properties")
    if isinstance(props, dict):
        for key in list(props):
            sensitive = "callSite" in key or key == "spark.job.description"
            if sensitive and not str(props[key]).startswith(TAG_PREFIX):
                props[key] = ""
    stages = list(event.get("Stage Infos") or [])
    if isinstance(event.get("Stage Info"), dict):
        stages.append(event["Stage Info"])
    for stage in stages:
        if "Details" in stage:
            stage["Details"] = ""
    return event


def _add_event_log(
    zf: zipfile.ZipFile, src: Path, name: str, redactor: Redactor, strip: bool
) -> None:
    with (
        src.open(encoding="utf-8", errors="replace") as fin,
        zf.open(name, "w", force_zip64=True) as fout,
    ):
        for line in fin:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                fout.write(redactor.redact_text(line).encode("utf-8"))
                continue
            event = redactor.redact(event)
            if strip:
                event = strip_sql(event)
            fout.write((json.dumps(event, separators=(",", ":")) + "\n").encode("utf-8"))


def _add_json(zf: zipfile.ZipFile, name: str, data: Any, redactor: Redactor) -> None:
    zf.writestr(name, json.dumps(redactor.redact(data), indent=2, sort_keys=True) + "\n")


def _add_yaml(zf: zipfile.ZipFile, name: str, data: Any, redactor: Redactor) -> None:
    zf.writestr(name, yaml.safe_dump(redactor.redact(data), sort_keys=False))


def export_bundle(
    results_dir: Path,
    experiment_id: str,
    out: Path | None = None,
    strip: bool = False,
    redactor: Redactor | None = None,
) -> Path:
    redactor = redactor or Redactor()
    out = out or results_dir / "bundles" / f"{experiment_id}.bundle.zip"
    with Catalog(results_dir) as catalog:
        experiment = catalog.experiment(experiment_id)
        if experiment is None:
            raise BundleError(f"unknown experiment {experiment_id!r}")
        if experiment.get("status") != "done":
            raise BundleError(
                f"experiment {experiment_id} is {experiment.get('status')}; finish it first "
                "(velocia run --resume)"
            )
        spec = yaml.safe_load(experiment["yaml"])
        reference = spec["reference_engine"]
        runs = catalog.query(
            "SELECT run_id FROM run WHERE experiment_id = ? ORDER BY run_id", [experiment_id]
        )
        cells = catalog.query(
            "SELECT engine, hardware_manifest_hash FROM cell WHERE experiment_id = ?",
            [experiment_id],
        )
        pairing = pairing_rows(catalog, experiment_id, reference)
        results_file = results_dir / "results" / f"{experiment_id}.parquet"
        if not results_file.is_file():
            catalog.write_results_parquet(experiment_id, results_file)

    engines = sorted({c["engine"] for c in cells})
    hashes = sorted({c["hardware_manifest_hash"] for c in cells})
    label = ""
    profiles: dict[str, Any] = {}
    measures: dict[str, Any] = {}
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = out.with_name(out.name + ".partial")  # renamed when complete
    with (
        tempfile.TemporaryDirectory() as tmp,
        zipfile.ZipFile(tmp_out, "w", zipfile.ZIP_DEFLATED) as zf,
    ):
        _add_yaml(zf, "experiment.yaml", spec, redactor)
        dataset = results_dir / "datasets" / f"{experiment['dataset_id']}.json"
        if dataset.is_file():
            _add_json(zf, f"datasets/{dataset.name}", json.loads(dataset.read_text()), redactor)
        for run in runs:
            run_dir = results_dir / "runs" / run["run_id"]
            for name in RUN_FILES:
                path = run_dir / name
                if not path.is_file():
                    continue
                data = json.loads(path.read_text("utf-8"))
                _add_json(zf, f"runs/{run['run_id']}/{name}", data, redactor)
                if name == "manifest.json":
                    label = data.get("label") or label
                    profile = (data.get("hardware") or {}).get("profile")
                    if profile:
                        profiles[profile["name"]] = profile
                    for m in data.get("measures") or []:
                        measures[m["name"]] = m
            log_dir = run_dir / "eventlog"
            if log_dir.is_dir():
                for log in sorted(p for p in log_dir.rglob("*") if p.is_file()):
                    rel = log.relative_to(run_dir).as_posix()
                    _add_event_log(zf, log, f"runs/{run['run_id']}/{rel}", redactor, strip)
        for name, profile in sorted(profiles.items()):
            _add_yaml(zf, f"hardware/{name}.yaml", profile, redactor)
        for name, measure in sorted(measures.items()):
            _add_yaml(zf, f"measures/{name}.yaml", measure, redactor)
        zf.write(results_file, "results/results.parquet")
        pairing_file = Path(tmp) / "pairing.parquet"
        _write_pairing(pairing, pairing_file)
        zf.write(pairing_file, "pairing.parquet")
        manifest = BundleManifest(
            created_at=datetime.now(UTC),
            producer=Producer(version=__version__),
            experiment_id=experiment_id,
            reference_engine=reference,
            engines=engines,
            hardware_manifest_hashes=hashes,
            cross_hardware=len(hashes) > 1,
            sql_stripped=strip,
            label=label,
        )
        zf.writestr(BUNDLE_FILE, manifest.model_dump_json(indent=2) + "\n")
    tmp_out.replace(out)
    return out
