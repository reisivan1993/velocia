"""``velocia bundle --validate FILE``: does a bundle match format v1 (docs/04-interfaces.md §7)?"""

import json
import tempfile
import zipfile
from pathlib import Path

import duckdb
from pydantic import ValidationError

from velocia.bundle.model import BUNDLE_FILE, PAIRING_COLUMNS, BundleManifest

REQUIRED = ("experiment.yaml", "results/results.parquet", "pairing.parquet")


def validate_bundle(path: Path) -> tuple[list[str], list[str]]:
    """``(problems, notes)``; the bundle is valid when there are no problems."""
    try:
        zf = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as exc:
        return [f"not a zip file: {exc}"], []
    with zf:
        names = set(zf.namelist())
        if BUNDLE_FILE not in names:
            return [f"{BUNDLE_FILE} missing"], []
        try:
            manifest = BundleManifest.model_validate_json(zf.read(BUNDLE_FILE))
        except ValidationError as exc:
            return [f"{BUNDLE_FILE}: {e['loc']}: {e['msg']}" for e in exc.errors()], []
        problems = [f"{name} missing" for name in REQUIRED if name not in names]
        runs = {n.split("/")[1] for n in names if n.startswith("runs/") and n.count("/") >= 2}
        for run_id in sorted(runs):
            member = f"runs/{run_id}/manifest.json"
            if member not in names:
                problems.append(f"{member} missing")
                continue
            data = json.loads(zf.read(member))
            if data.get("experiment_id") != manifest.experiment_id:
                problems.append(f"{member}: experiment_id {data.get('experiment_id')!r}")
        if "pairing.parquet" not in names:
            return problems, []
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "pairing.parquet"
            local.write_bytes(zf.read("pairing.parquet"))
            con = duckdb.connect()
            try:
                rel = con.read_parquet(str(local))
                columns = rel.columns
                rows = rel.fetchall()
            finally:
                con.close()
    if columns != PAIRING_COLUMNS:
        return [*problems, f"pairing.parquet columns {columns}, expected {PAIRING_COLUMNS}"], []
    pairing = [dict(zip(columns, row, strict=True)) for row in rows]
    notes = []
    seen = set()
    groups: dict[tuple[str, str, str], set[str]] = {}
    for row in pairing:
        key = (row["unit_id"], row["config_hash"], row["mode"], row["engine"])
        if key in seen:
            problems.append(f"pairing: more than one row for {key}")
        seen.add(key)
        if row["run_id"] not in runs:
            problems.append(f"pairing: run {row['run_id']} is not in the bundle")
        if row["engine"] not in manifest.engines:
            problems.append(f"pairing: engine {row['engine']} not in {BUNDLE_FILE}")
        if row["is_reference"] != (row["engine"] == manifest.reference_engine):
            problems.append(f"pairing: is_reference wrong for {key}")
        groups.setdefault(key[:3], set()).add(row["engine"])
    for group, engines in sorted(groups.items()):
        if manifest.reference_engine not in engines:
            notes.append(f"pairing: no reference row for {group} (the reference didn't finish)")
    return problems, notes
