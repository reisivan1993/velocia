"""Bundle export and validation without Docker: a catalog and run directories built by hand."""

import json
import zipfile
from pathlib import Path

import duckdb
import yaml
from conftest import PlantedSecrets

from velocia.bundle.export import export_bundle, strip_sql
from velocia.bundle.validate import validate_bundle
from velocia.store.catalog import Catalog

EXP = "exp-1"
WALLS = {"spark-vanilla": [100.0, 120.0, 110.0], "spark-comet": [50.0, 40.0, 45.0, 60.0]}
SQL_START = {
    "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
    "executionId": 0,
    "description": "velocia:r:q1:1",
    "details": "select * from lineitem where l_comment = 'customer-literal'",
    "physicalPlanDescription": "== Physical Plan == Filter (l_comment = customer-literal)",
    "sparkPlanInfo": {
        "nodeName": "Filter",
        "simpleString": "Filter (l_comment = customer-literal)",
        "metadata": {"Location": "file:/data/customer/lineitem"},
        "children": [{"nodeName": "Scan parquet", "simpleString": "s3://x", "children": []}],
        "metrics": [],
    },
}


def _results(root: Path, planted: PlantedSecrets) -> Path:
    results = root / "velocia-results"
    spec = {"name": "e", "reference_engine": "spark-vanilla", "spark_conf": planted.spark_conf}
    with Catalog(results) as cat:
        cat.insert(
            "experiment",
            {
                "experiment_id": EXP,
                "yaml": yaml.safe_dump(spec),
                "status": "done",
                "dataset_id": "tpch-sf1",
                "bootstrap_seed": 1,
            },
        )
        for engine, walls in WALLS.items():
            cat.insert(
                "cell",
                {
                    "experiment_id": EXP,
                    "cell_key": engine,
                    "engine": engine,
                    "mode": "cold",
                    "config_hash": "cfg",
                    "hardware_manifest_hash": "sha256:hw",
                },
            )
            for rep, wall in enumerate(walls, 1):
                run_id = f"{EXP}-{engine}-r{rep}"
                cat.insert(
                    "run",
                    {
                        "run_id": run_id,
                        "experiment_id": EXP,
                        "cell_key": engine,
                        "engine": engine,
                        "cluster": "default",
                        "config_hash": "cfg",
                        "mode": "cold",
                        "task": "time",
                    },
                )
                cat.insert_many(
                    "unit_run",
                    [
                        {
                            "run_id": run_id,
                            "unit_id": "q1",
                            "rep": rep,
                            "wall_ms": wall,
                            "status": "ok",
                            "discarded": False,
                        }
                    ],
                )
                run_dir = results / "runs" / run_id
                (run_dir / "eventlog").mkdir(parents=True)
                manifest = {
                    "experiment_id": EXP,
                    "label": "Data derived from TPC-H; not an official TPC result.",
                    "spark_conf": planted.spark_conf,
                    "hardware": {"profile": {"name": "laptop", "price_per_node_hour_usd": 0.1}},
                    "measures": [{"name": "wall_time", "unit": "s", "direction": "lower"}],
                }
                (run_dir / "manifest.json").write_text(json.dumps(manifest))
                (run_dir / "timings.json").write_text(json.dumps({"units": []}))
                (run_dir / "eventlog" / "app-1").write_text(
                    planted.event_log_line + "\n" + json.dumps(SQL_START) + "\n"
                )
        cat.write_results_parquet(EXP, results / "results" / f"{EXP}.parquet")
    (results / "datasets").mkdir()
    (results / "datasets" / "tpch-sf1.json").write_text(json.dumps({"dataset_id": "tpch-sf1"}))
    return results


def test_bundle_export_and_validate(tmp_path: Path, planted_secrets: PlantedSecrets) -> None:
    results = _results(tmp_path, planted_secrets)
    path = export_bundle(results, EXP)
    assert path == results / "bundles" / f"{EXP}.bundle.zip"
    assert validate_bundle(path) == ([], [])

    with zipfile.ZipFile(path) as zf:
        names = set(zf.namelist())
        manifest = json.loads(zf.read("bundle.json"))
        zf.extractall(tmp_path / "unzipped")
    assert {
        "experiment.yaml",
        "datasets/tpch-sf1.json",
        "hardware/laptop.yaml",
        "measures/wall_time.yaml",
        "results/results.parquet",
        "pairing.parquet",
        f"runs/{EXP}-spark-comet-r1/eventlog/app-1",
    } <= names
    assert manifest["format"] == "benchmark-bundle" and manifest["version"] == 1
    assert manifest["engines"] == ["spark-comet", "spark-vanilla"]
    assert not manifest["cross_hardware"] and not manifest["sql_stripped"]
    planted_secrets.assert_absent(tmp_path / "unzipped")

    pairing = duckdb.sql(
        f"SELECT engine, rep, wall_ms, is_reference FROM "
        f"'{tmp_path / 'unzipped' / 'pairing.parquet'}' ORDER BY engine"
    ).fetchall()
    # Median repetition per engine; with 4 values the lower middle one (45 ms, rep 3).
    assert pairing == [("spark-comet", 3, 45.0, False), ("spark-vanilla", 3, 110.0, True)]

    # Without --strip-sql the SQL text stays (D-10: tools need it).
    log = tmp_path / "unzipped" / "runs" / f"{EXP}-spark-comet-r1" / "eventlog" / "app-1"
    assert "customer-literal" in log.read_text()


def test_strip_sql(tmp_path: Path, planted_secrets: PlantedSecrets) -> None:
    results = _results(tmp_path, planted_secrets)
    path = export_bundle(results, EXP, tmp_path / "shared.zip", strip=True)
    with zipfile.ZipFile(path) as zf:
        text = "".join(zf.read(n).decode() for n in zf.namelist() if "/eventlog/" in n)
        assert json.loads(zf.read("bundle.json"))["sql_stripped"]
    assert "customer-literal" not in text and "/data/customer" not in text
    event = strip_sql(json.loads(json.dumps(SQL_START)))
    assert event["description"] == "velocia:r:q1:1"  # the tag stays
    assert event["sparkPlanInfo"]["nodeName"] == "Filter"  # plan markers still work
    assert event["sparkPlanInfo"]["children"][0]["simpleString"] == ""


def test_validate_rejects_broken_bundles(tmp_path: Path, planted_secrets: PlantedSecrets) -> None:
    results = _results(tmp_path, planted_secrets)
    good = export_bundle(results, EXP)
    broken = tmp_path / "broken.zip"
    with zipfile.ZipFile(good) as src, zipfile.ZipFile(broken, "w") as dst:
        for item in src.infolist():
            if item.filename == "pairing.parquet":
                continue
            data = src.read(item)
            if item.filename == "bundle.json":
                data = json.dumps({**json.loads(data), "version": 2}).encode()
            dst.writestr(item, data)
    problems, _ = validate_bundle(broken)
    assert problems and "version" in problems[0]
    assert validate_bundle(tmp_path / "missing.zip")[0][0].startswith("not a zip file")
