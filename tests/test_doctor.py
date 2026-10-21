import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.config import VelociaConfig
from velocia.doctor import doctor, engine_check


def test_doctor_reports_missing_docker_clearly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(shutil, "which", lambda name: None)
    checks = {c.name: c for c in doctor(VelociaConfig(results_dir=str(tmp_path)))}
    assert checks["docker"].status == "fail"
    assert "docker CLI not found" in checks["docker"].detail
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["doctor"])
    assert result.exit_code == 1 and "FAIL  docker" in result.output


def test_engine_check_rejects_unknown_engine_and_spark_version(tmp_path: Path) -> None:
    config = VelociaConfig(results_dir=str(tmp_path))
    assert not engine_check("nope", None, config).ok
    wrong = engine_check("spark-comet", "4.0", config)
    assert not wrong.ok and "not 4.0" in wrong.lines[0]


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_engine_check_passes_comet_and_fails_a_planted_broken_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "engines").mkdir()
    # Vanilla image with a plugin class that doesn't exist: every application fails.
    (tmp_path / "engines" / "spark-broken.yaml").write_text(
        "name: spark-broken\nimage: velocia/engine-spark-vanilla:3.5.9\n"
        "spark_versions: ['3.5']\nspark_conf: {spark.plugins: com.example.Missing}\n"
    )
    cli = CliRunner()
    ok = cli.invoke(app, ["engines", "check", "spark-comet", "--spark", "3.5"])
    assert ok.exit_code == 0, ok.output
    assert "spark-comet: ok" in ok.output
    broken = cli.invoke(app, ["engines", "check", "spark-broken"])
    assert broken.exit_code == 1
    assert "application failed" in broken.output
    assert "engine check failed for spark-broken" in broken.output
