"""``catalog.duckdb``: datasets, experiments, runs, unit runs, checks, measure values, results.

Every state change is written before the next step starts, so an interrupted experiment can
resume from here (docs/06-design.md §3).

DuckDB lets one process hold the file at a time, so every operation opens the database, does
its work and closes it again, waiting briefly when another process (a running experiment, a
report) holds it. That keeps ``velocia report`` and ``velocia compare`` usable during a run.
"""

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

LOCK_WAIT_S = 60.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS dataset (
    dataset_id VARCHAR PRIMARY KEY, suite VARCHAR, generator_params VARCHAR, seed BIGINT,
    scale DOUBLE, format VARCHAR, compression VARCHAR, row_group_size BIGINT, files BIGINT,
    bytes BIGINT, checksum VARCHAR, manifest_json VARCHAR);
CREATE TABLE IF NOT EXISTS experiment (
    experiment_id VARCHAR PRIMARY KEY, name VARCHAR, yaml VARCHAR, spec_hash VARCHAR,
    created_at TIMESTAMP, velocia_version VARCHAR, status VARCHAR, bootstrap_seed BIGINT,
    dataset_id VARCHAR);
CREATE TABLE IF NOT EXISTS cell (
    experiment_id VARCHAR, cell_key VARCHAR, idx INTEGER, engine VARCHAR, cluster VARCHAR,
    config_hash VARCHAR, mode VARCHAR, varying VARCHAR, hardware_manifest_hash VARCHAR,
    status VARCHAR, quiet_check VARCHAR, PRIMARY KEY (experiment_id, cell_key));
CREATE TABLE IF NOT EXISTS run (
    run_id VARCHAR PRIMARY KEY, experiment_id VARCHAR, cell_key VARCHAR, engine VARCHAR,
    cluster VARCHAR, config_hash VARCHAR, mode VARCHAR, rep VARCHAR, task VARCHAR,
    app_id VARCHAR, status VARCHAR, manifest_json VARCHAR);
CREATE TABLE IF NOT EXISTS unit_run (
    run_id VARCHAR, unit_id VARCHAR, rep INTEGER, wall_ms DOUBLE, status VARCHAR,
    discarded BOOLEAN, error VARCHAR, summary VARCHAR);
-- Catalogs created before event-log summaries existed (2026-10-21).
ALTER TABLE unit_run ADD COLUMN IF NOT EXISTS summary VARCHAR;
CREATE TABLE IF NOT EXISTS unit_check (
    run_id VARCHAR, cell_key VARCHAR, unit_id VARCHAR, rows BIGINT, checksum VARCHAR,
    status VARCHAR, error VARCHAR);
CREATE TABLE IF NOT EXISTS measure_value (
    run_id VARCHAR, unit_id VARCHAR, rep INTEGER, measure VARCHAR, value DOUBLE,
    null_reason VARCHAR);
CREATE TABLE IF NOT EXISTS result (
    experiment_id VARCHAR, cell_key VARCHAR, engine VARCHAR, cluster VARCHAR,
    hardware_manifest_hash VARCHAR, config_hash VARCHAR, mode VARCHAR, unit_id VARCHAR,
    measure VARCHAR, measure_unit VARCHAR, direction VARCHAR, n INTEGER, median DOUBLE,
    p95 DOUBLE, cv DOUBLE, cv_flag BOOLEAN, ratio DOUBLE, ci_low DOUBLE, ci_high DOUBLE,
    correctness VARCHAR, timeouts INTEGER);
"""

RESULT_COLUMNS = [
    "experiment_id",
    "cell_key",
    "engine",
    "cluster",
    "hardware_manifest_hash",
    "config_hash",
    "mode",
    "unit_id",
    "measure",
    "measure_unit",
    "direction",
    "n",
    "median",
    "p95",
    "cv",
    "cv_flag",
    "ratio",
    "ci_low",
    "ci_high",
    "correctness",
    "timeouts",
]


class Catalog:
    def __init__(self, results_dir: Path) -> None:
        results_dir.mkdir(parents=True, exist_ok=True)
        self.path = results_dir / "catalog.duckdb"
        with self._connection() as con:
            con.execute(SCHEMA)

    @contextmanager
    def _connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        deadline = time.monotonic() + LOCK_WAIT_S
        while True:
            try:
                con = duckdb.connect(self.path)
                break
            except duckdb.IOException as exc:
                if "lock" not in str(exc).lower() or time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        try:
            yield con
        finally:
            con.close()

    def close(self) -> None:
        """Nothing stays open between operations; kept for ``with`` blocks."""

    def __enter__(self) -> "Catalog":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        with self._connection() as con:
            con.execute(sql, params or [])

    # --- writes --------------------------------------------------------------------------

    def insert(self, table: str, row: dict[str, Any]) -> None:
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        self.execute(
            f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({marks})", list(row.values())
        )

    def insert_many(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        cols = list(rows[0])
        marks = ", ".join("?" for _ in cols)
        with self._connection() as con:
            con.executemany(
                f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({marks})",
                [[r[c] for c in cols] for r in rows],
            )

    def update(self, table: str, key: dict[str, Any], values: dict[str, Any]) -> None:
        sets = ", ".join(f"{k} = ?" for k in values)
        where = " AND ".join(f"{k} = ?" for k in key)
        self.execute(f"UPDATE {table} SET {sets} WHERE {where}", [*values.values(), *key.values()])

    def forget_cell(self, experiment_id: str, cell_key: str) -> None:
        """Drop everything an unfinished cell wrote, before it runs again on resume."""
        with self._connection() as con:
            runs = [
                r[0]
                for r in con.execute(
                    "SELECT run_id FROM run WHERE experiment_id = ? AND cell_key = ?",
                    [experiment_id, cell_key],
                ).fetchall()
            ]
            for table in ("unit_run", "unit_check", "measure_value"):
                for run_id in runs:
                    con.execute(f"DELETE FROM {table} WHERE run_id = ?", [run_id])
            con.execute(
                "DELETE FROM run WHERE experiment_id = ? AND cell_key = ?",
                [experiment_id, cell_key],
            )

    def replace_results(self, experiment_id: str, rows: list[dict[str, Any]]) -> None:
        self.execute("DELETE FROM result WHERE experiment_id = ?", [experiment_id])
        self.insert_many("result", rows)

    # --- reads ---------------------------------------------------------------------------

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        with self._connection() as con:
            cur = con.execute(sql, params or [])
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]

    def experiment(self, experiment_id: str) -> dict[str, Any] | None:
        rows = self.query("SELECT * FROM experiment WHERE experiment_id = ?", [experiment_id])
        return rows[0] if rows else None

    def latest_unfinished(self, name: str, spec_hash: str) -> dict[str, Any] | None:
        rows = self.query(
            "SELECT * FROM experiment WHERE name = ? AND spec_hash = ? AND status != 'done' "
            "ORDER BY created_at DESC LIMIT 1",
            [name, spec_hash],
        )
        return rows[0] if rows else None

    def done_cells(self, experiment_id: str) -> set[str]:
        rows = self.query(
            "SELECT cell_key FROM cell WHERE experiment_id = ? AND status = 'done'",
            [experiment_id],
        )
        return {r["cell_key"] for r in rows}

    def write_results_parquet(self, experiment_id: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        # COPY can't take prepared parameters, so both literals are quoted here.
        path = str(dest).replace("'", "''")
        exp = experiment_id.replace("'", "''")
        self.execute(
            f"COPY (SELECT {', '.join(RESULT_COLUMNS)} FROM result WHERE experiment_id = '{exp}' "
            f"ORDER BY cell_key, unit_id, measure) TO '{path}' (FORMAT parquet)"
        )


def as_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)
