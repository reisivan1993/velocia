import json
import math
import shutil
import subprocess
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.config.models import DataFormat, Suite, WorkloadKind
from velocia.datagen.duckdb_tpc import generate
from velocia.engines.registry import get_engine
from velocia.workloads import UnknownUnitError, get_workload, select_units
from velocia.workloads.sql import QUERIES_DIR, TPCH
from velocia.workloads.sql.generate import tpch_queries

VALID = Path(__file__).parent / "fixtures" / "config" / "valid"


def test_tpch_has_22_sql_units_in_natural_order() -> None:
    units = TPCH.units()
    assert [u.id for u in units] == [f"q{i}" for i in range(1, 23)]
    assert all(u.kind is WorkloadKind.sql and u.sql for u in units)
    assert all(not u.sql.startswith("--") and not u.sql.endswith(";") for u in units if u.sql)
    assert get_workload(Suite.tpch) is TPCH
    assert TPCH.label == "derived from TPC-H"


def test_committed_queries_match_the_generator() -> None:
    expected = tpch_queries()
    committed = {p.name: p.read_text() for p in (QUERIES_DIR / "tpch").glob("*.sql")}
    assert committed == expected, "run: uv run python -m velocia.workloads.sql.generate"


def test_select_units() -> None:
    assert [u.id for u in select_units(TPCH, ["q6", "q1"])] == ["q1", "q6"]
    assert len(select_units(TPCH, "all")) == 22
    with pytest.raises(UnknownUnitError, match="unknown unit"):
        select_units(TPCH, ["q1", "q23"])


def test_dry_run_rejects_unknown_units(tmp_path: Path) -> None:
    experiment = tmp_path / "exp.yaml"
    experiment.write_text(
        (VALID / "experiment-minimal-local.yaml")
        .read_text()
        .replace("scale_factor: 0.1}", "scale_factor: 0.1, units: [q1, q99]}")
    )
    result = CliRunner().invoke(app, ["run", str(experiment), "--dry-run"])
    assert result.exit_code == 1
    assert "unknown unit(s) for suite tpch: q99" in result.output


# --- every query on Apache Spark matches DuckDB ---------------------------------------------

SPARK_SCRIPT = """
import json, sys
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("velocia-tpch-check").getOrCreate()
for table in Path("/data").iterdir():
    if table.is_dir():
        spark.read.parquet(str(table)).createOrReplaceTempView(table.name)
out = {}
for q in json.loads(Path("/work/queries.json").read_text()):
    rows = spark.sql(q["sql"]).collect()
    out[q["id"]] = [[str(v) if v is not None else None for v in r] for r in rows]
Path("/work/spark.json").write_text(json.dumps(out))
"""


def _same(spark_value: str | None, duck_value: Any) -> bool:
    if spark_value is None or duck_value is None:
        return spark_value is None and duck_value is None
    if isinstance(duck_value, int | float | Decimal):
        return math.isclose(float(spark_value), float(duck_value), rel_tol=1e-9, abs_tol=1e-6)
    if isinstance(duck_value, date):
        return spark_value == duck_value.isoformat()
    return spark_value == str(duck_value)


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_all_22_queries_on_spark_match_duckdb(tmp_path: Path) -> None:
    manifest, _ = generate(Suite.tpch, 0.1, DataFormat.parquet_zstd, tmp_path / "data")
    data = Path(manifest.location)
    units = TPCH.units()

    with duckdb.connect() as con:
        for table in manifest.tables:
            path = data / table.files[0].path  # under tmp_path; no quotes in it
            con.execute(f"CREATE VIEW {table.name} AS SELECT * FROM read_parquet('{path}')")
        expected = {u.id: con.execute(u.sql or "").fetchall() for u in units}

    work = tmp_path / "work"
    work.mkdir()
    (work / "run.py").write_text(SPARK_SCRIPT)
    (work / "queries.json").write_text(json.dumps([{"id": u.id, "sql": u.sql} for u in units]))
    image = get_engine("spark-vanilla").definition.image
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{data}:/data:ro",
            "-v",
            f"{work}:/work",
            image,
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[4]",
            "/work/run.py",
        ],
        check=True,
        capture_output=True,
        timeout=900,
    )
    actual = json.loads((work / "spark.json").read_text())

    for unit in units:
        spark_rows, duck_rows = actual[unit.id], expected[unit.id]
        assert duck_rows, f"{unit.id} returns no rows at SF0.1; the check would be empty"
        assert len(spark_rows) == len(duck_rows), unit.id
        for i, (s_row, d_row) in enumerate(zip(spark_rows, duck_rows, strict=True)):
            assert all(_same(s, d) for s, d in zip(s_row, d_row, strict=True)), (
                f"{unit.id} row {i}: spark {s_row} != duckdb {d_row}"
            )
