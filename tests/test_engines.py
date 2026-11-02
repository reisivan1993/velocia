import shutil
from pathlib import Path

import pytest

from velocia.config import ConfigError
from velocia.engines.docker import build_image, run_in_image
from velocia.engines.registry import BUILTIN_DIR, get_engine, load_engines


def test_builtin_vanilla_engine() -> None:
    engine = get_engine("spark-vanilla")
    assert engine.builtin
    assert engine.definition.family == "spark"
    assert engine.definition.spark_versions == ["3.5"]
    assert engine.build_context == BUILTIN_DIR / "spark-vanilla"


def test_user_definition_overrides_builtin(tmp_path: Path) -> None:
    (tmp_path / "spark-vanilla.yaml").write_text(
        "name: spark-vanilla\nimage: registry.example/spark:pinned\nspark_versions: ['3.5']\n"
    )
    engine = load_engines(tmp_path)["spark-vanilla"]
    assert not engine.builtin
    assert engine.definition.image == "registry.example/spark:pinned"


def test_name_must_match_file(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text("name: b\nimage: x:1\nspark_versions: ['3.5']\n")
    with pytest.raises(ConfigError, match="must match the file name"):
        load_engines(tmp_path)


def test_unknown_engine() -> None:
    with pytest.raises(KeyError, match="unknown engine 'nope'"):
        get_engine("nope")


def test_builtin_comet_engine_enables_plugin() -> None:
    conf = get_engine("spark-comet").definition.spark_conf
    assert conf["spark.plugins"] == "org.apache.spark.CometPlugin"
    assert conf["spark.memory.offHeap.enabled"] == "true"


@pytest.mark.docker
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
@pytest.mark.parametrize("name", ["spark-vanilla", "spark-comet"])
def test_builtin_image_builds_and_reports_spark_35(name: str) -> None:
    engine = get_engine(name)
    assert engine.build_context is not None
    info = build_image(engine.build_context, engine.definition.image)
    assert info.image_id.startswith("sha256:")
    output = run_in_image(
        engine.definition.image, "/opt/spark/bin/spark-submit", "--version", timeout=120
    )
    assert "version 3.5." in output


def test_engine_platforms_are_checked_where_containers_run() -> None:
    from velocia.config.models import Experiment
    from velocia.engines.registry import load_engines
    from velocia.orchestrator.run import RunError, check_platforms

    known = load_engines(None)
    assert known["spark-gluten"].definition.platforms == ["linux/amd64"]
    experiment = Experiment.model_validate(
        {
            "name": "g",
            "workload": {"kind": "sql", "suite": "tpch", "scale_factor": 1},
            "engines": ["spark-vanilla", "spark-gluten"],
            "reference_engine": "spark-vanilla",
            "cluster": {
                "runner": "local",
                "executors": 1,
                "executor_cores": 1,
                "executor_memory": "1g",
            },
        }
    )
    clusters = experiment.named_clusters
    check_platforms(experiment, known, clusters, lambda: "linux/amd64")
    check_platforms(experiment, known, clusters, lambda: None)  # unknown: let it run
    with pytest.raises(RunError, match="spark-gluten runs on linux/amd64 only"):
        check_platforms(experiment, known, clusters, lambda: "linux/arm64")
