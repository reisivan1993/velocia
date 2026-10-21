import re
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import BaseModel

from velocia.config import (
    ConfigError,
    EngineDef,
    Experiment,
    HardwareProfile,
    MeasureDef,
    VelociaConfig,
    load_model,
)
from velocia.config.models import parse_duration
from velocia.config.schema_export import SCHEMAS, render

FIXTURES = Path(__file__).parent / "fixtures" / "config"
ROOT = Path(__file__).parent.parent

MODELS: dict[str, type[BaseModel]] = {
    "experiment": Experiment,
    "engine": EngineDef,
    "measure": MeasureDef,
    "hardware": HardwareProfile,
    "config": VelociaConfig,
}


def _model_for(path: Path) -> type[BaseModel]:
    return MODELS[path.name.split("-", 1)[0]]


@pytest.mark.parametrize("path", sorted((FIXTURES / "valid").glob("*.yaml")), ids=lambda p: p.stem)
def test_valid_fixtures_load_and_round_trip(path: Path) -> None:
    model = _model_for(path)
    loaded = load_model(path, model)
    assert model.model_validate(loaded.model_dump()) == loaded


@pytest.mark.parametrize(
    "path", sorted((FIXTURES / "invalid").glob("*.yaml")), ids=lambda p: p.stem
)
def test_invalid_fixtures_report_exact_field_path(path: Path) -> None:
    header = path.read_text().splitlines()[0]
    match = re.fullmatch(r"# expect: (\S+)", header)
    assert match, f"{path.name} needs a '# expect: <field path>' header"
    with pytest.raises(ConfigError) as info:
        load_model(path, _model_for(path))
    paths = [loc for loc, _ in info.value.problems]
    assert match.group(1) in paths, paths
    assert str(path) in str(info.value)


def test_unreadable_yaml_is_a_config_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: [unclosed")
    with pytest.raises(ConfigError):
        load_model(bad, Experiment)


def test_low_repetitions_warns() -> None:
    exp = load_model(FIXTURES / "valid" / "experiment-minimal-local.yaml", Experiment)
    assert exp.repetitions == 3
    assert any("repetitions=3" in w for w in exp.warnings())


def test_defaults() -> None:
    exp = load_model(FIXTURES / "valid" / "experiment-minimal-local.yaml", Experiment)
    assert exp.measures == ["wall_time", "task_cpu_time"]
    assert exp.timeout == timedelta(minutes=30)
    assert [r.name for r in exp.engine_refs] == ["spark-vanilla"]
    assert VelociaConfig().statistics.bootstrap_resamples == 10_000


def test_engine_refs_normalize_names_and_pins() -> None:
    exp = load_model(FIXTURES / "valid" / "experiment-terasort-cpu-vs-apu.yaml", Experiment)
    assert [(r.name, r.cluster) for r in exp.engine_refs] == [
        ("spark-vanilla", "cpu"),
        ("spark-apu", "apu"),
    ]
    assert exp.comparison.allow_cross_hardware


@pytest.mark.parametrize(
    ("text", "expected"),
    [("90s", timedelta(seconds=90)), ("30m", timedelta(minutes=30)), ("2h", timedelta(hours=2))],
)
def test_parse_duration(text: str, expected: timedelta) -> None:
    assert parse_duration(text) == expected


@pytest.mark.parametrize("text", ["0m", "30", "1d", "-5s"])
def test_parse_duration_rejects(text: str) -> None:
    with pytest.raises(ValueError):
        parse_duration(text)


@pytest.mark.parametrize("name", sorted(SCHEMAS))
def test_published_schemas_are_up_to_date(name: str) -> None:
    published = (ROOT / "schemas" / f"{name}.schema.json").read_text()
    assert published == render(SCHEMAS[name], name), (
        "run: uv run python -m velocia.config.schema_export"
    )
