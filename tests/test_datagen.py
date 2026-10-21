import json
from pathlib import Path

import jsonschema
import pytest
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.config.models import DataFormat, Suite
from velocia.datagen.duckdb_tpc import DatagenError, generate
from velocia.datagen.manifest import MANIFEST_FILE, DatasetManifest

ROOT = Path(__file__).parent.parent
TPCH_TABLES = {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}


@pytest.fixture(scope="module")
def sf01(tmp_path_factory: pytest.TempPathFactory) -> DatasetManifest:
    manifest, created = generate(
        Suite.tpch, 0.1, DataFormat.parquet_zstd, tmp_path_factory.mktemp("a")
    )
    assert created
    return manifest


def test_sf01_generated_twice_gives_identical_checksum(
    sf01: DatasetManifest, tmp_path: Path
) -> None:
    again, _ = generate(Suite.tpch, 0.1, DataFormat.parquet_zstd, tmp_path)
    assert again.checksum == sf01.checksum
    assert [t.checksum for t in again.tables] == [t.checksum for t in sf01.tables]


def test_manifest_validates_against_published_schema(sf01: DatasetManifest) -> None:
    schema = json.loads((ROOT / "schemas" / "dataset-manifest.schema.json").read_text())
    on_disk = json.loads((Path(sf01.location) / MANIFEST_FILE).read_text())
    jsonschema.validate(on_disk, schema)
    assert on_disk["format"] == "velocia-dataset"
    assert on_disk["label"] == "derived from TPC-H"


def test_layout_and_row_counts(sf01: DatasetManifest) -> None:
    location = Path(sf01.location)
    assert sf01.dataset_id == "tpch-sf0.1-parquet-zstd"
    assert {t.name for t in sf01.tables} == TPCH_TABLES
    rows = {t.name: t.rows for t in sf01.tables}
    assert rows["lineitem"] == 600_572
    assert rows["nation"] == 25
    assert sf01.files == 8
    assert sf01.bytes == sum(
        (location / f.path).stat().st_size for t in sf01.tables for f in t.files
    )
    assert sorted(p.name for p in location.iterdir()) == sorted([*TPCH_TABLES, MANIFEST_FILE])


def test_checksum_is_independent_of_compression(tmp_path: Path) -> None:
    zstd, _ = generate(Suite.tpch, 0.01, DataFormat.parquet_zstd, tmp_path)
    snappy, _ = generate(Suite.tpch, 0.01, DataFormat.parquet_snappy, tmp_path)
    assert zstd.dataset_id != snappy.dataset_id
    assert snappy.compression == "snappy"
    assert zstd.checksum == snappy.checksum


def test_existing_dataset_is_reused(tmp_path: Path) -> None:
    first, created = generate(Suite.tpch, 0.01, DataFormat.parquet_zstd, tmp_path)
    second, created_again = generate(Suite.tpch, 0.01, DataFormat.parquet_zstd, tmp_path)
    assert created and not created_again
    assert second == first
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".")]


def test_incomplete_directory_is_refused(tmp_path: Path) -> None:
    (tmp_path / "tpch-sf0.01-parquet-zstd").mkdir()
    with pytest.raises(DatagenError, match=r"has no _dataset\.json"):
        generate(Suite.tpch, 0.01, DataFormat.parquet_zstd, tmp_path)


def test_cli_datagen_writes_catalog_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["datagen", "--suite", "tpch", "--sf", "0.01"])
    assert result.exit_code == 0, result.output
    assert "Generated dataset tpch-sf0.01-parquet-zstd" in result.output
    copy = tmp_path / "velocia-results" / "datasets" / "tpch-sf0.01-parquet-zstd.json"
    data_dir = tmp_path / "velocia-results" / "data" / "tpch-sf0.01-parquet-zstd"
    assert copy.read_text() == (data_dir / MANIFEST_FILE).read_text()


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--suite", "tpch"], "--sf is required"),
        (["--suite", "tpch", "--sf", "1", "--rows", "5"], "--rows does not apply"),
        (["--suite", "tpch", "--sf", "1", "--iceberg"], "--iceberg is not supported"),
        (["--suite", "tpch", "--sf", "-1"], "scale factor must be > 0"),
    ],
)
def test_cli_datagen_rejects_bad_options(
    args: list[str], message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["datagen", *args])
    assert result.exit_code == 1
    assert message in result.output
