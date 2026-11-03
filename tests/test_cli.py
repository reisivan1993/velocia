from pathlib import Path

from typer.testing import CliRunner

from velocia import __version__
from velocia.cli.app import app

runner = CliRunner()

V01_COMMANDS = ["datagen", "run", "report", "doctor", "engines", "measures", "hardware"]


def test_help_lists_all_v01_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in V01_COMMANDS:
        assert command in result.output


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_workloads_list_and_show() -> None:
    listed = runner.invoke(app, ["workloads", "list"])
    assert listed.exit_code == 0
    for suite in ("tpch", "tpcds", "tera", "text", "etl", "graph", "kmeans", "synth"):
        assert suite in listed.output
    shown = runner.invoke(app, ["workloads", "show", "tpch"])
    assert shown.exit_code == 0 and "q22" in shown.output
    assert runner.invoke(app, ["workloads", "show", "nope"]).exit_code == 1


def test_hardware_probe_of_a_declared_cluster(tmp_path: Path) -> None:
    exp = tmp_path / "exp.yaml"
    exp.write_text(
        "name: h\nworkload: {kind: sql, suite: tpch, scale_factor: 1}\n"
        "engines: [spark-vanilla]\nreference_engine: spark-vanilla\n"
        "cluster: {runner: external, master: 'spark://h:7077', executors: 4, "
        "executor_cores: 8, executor_memory: 32g, data_uri: 's3a://b/', "
        "eventlog_uri: 's3a://b/e'}\n"
    )
    result = runner.invoke(
        app, ["hardware", "probe", "--cluster", "default", "--experiment", str(exp)]
    )
    assert result.exit_code == 0, result.output
    assert '"source": "declared"' in result.output
    assert "hardware_manifest_hash: sha256:" in result.output
