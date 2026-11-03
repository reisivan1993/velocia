import json
import os
import random
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.config import Experiment, load_model
from velocia.experiment.estimate import estimate, format_duration
from velocia.experiment.expander import Cell, expand, spark_value

ROOT = Path(__file__).parent.parent
VALID = ROOT / "tests" / "fixtures" / "config" / "valid"
GOLDEN = ROOT / "tests" / "fixtures" / "expansion"
UPDATE_GOLDEN = os.environ.get("VELOCIA_UPDATE_GOLDEN") == "1"


def _load(name: str) -> Experiment:
    return load_model(VALID / f"{name}.yaml", Experiment)


def _summary(cells: list[Cell]) -> list[dict[str, Any]]:
    return [
        {
            "index": c.index,
            "engine": c.engine,
            "cluster": c.cluster_name,
            "mode": c.mode.value,
            "spark_conf": c.config.spark_conf,
            "varying": c.config.varying,
            "config_hash": c.config_hash,
            "cell_key": c.cell_key,
        }
        for c in cells
    ]


# --- golden expansions ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "experiment-minimal-local",
        "experiment-tpch-engines-sf10",
        "experiment-terasort-cpu-vs-apu",
    ],
)
def test_golden_expansion(name: str) -> None:
    golden = GOLDEN / f"{name}.json"
    actual = _summary(expand(_load(name)))
    if UPDATE_GOLDEN:
        golden.write_text(json.dumps(actual, indent=2) + "\n")
    assert golden.exists(), f"missing {golden}; run with VELOCIA_UPDATE_GOLDEN=1 and review it"
    assert actual == json.loads(golden.read_text())


def test_single_cluster_cells() -> None:
    cells = expand(_load("experiment-minimal-local"))
    assert [(c.engine, c.cluster_name, c.mode.value) for c in cells] == [
        ("spark-vanilla", "default", "cold"),
        ("spark-vanilla", "default", "warm"),
    ]
    assert cells[0].config_hash == cells[1].config_hash
    assert cells[0].cell_key != cells[1].cell_key


def test_spark_conf_lists_expand_with_engines_innermost() -> None:
    cells = expand(_load("experiment-tpch-engines-sf10"))
    assert len(cells) == 2 * 2 * 3  # configs x modes x engines
    assert [c.engine for c in cells[:3]] == ["spark-vanilla", "spark-gluten", "spark-comet"]
    assert len({c.cell_key for c in cells}) == 12
    assert len({c.config_hash for c in cells}) == 2
    # Engines of the same config and mode share the config hash, so they pair.
    assert len({c.config_hash for c in cells[:3]}) == 1
    assert cells[0].config.varying == {"spark.sql.adaptive.enabled": "true"}
    assert cells[6].config.varying == {"spark.sql.adaptive.enabled": "false"}


def test_engines_on_different_clusters_pair_on_config_hash() -> None:
    cells = expand(_load("experiment-terasort-cpu-vs-apu"))
    assert [(c.engine, c.cluster_name) for c in cells] == [
        ("spark-vanilla", "cpu"),
        ("spark-apu", "apu"),
    ]
    assert cells[0].config_hash == cells[1].config_hash
    assert cells[0].config.varying == {}


def test_sweep_expands_for_synth() -> None:
    experiment = Experiment.model_validate(
        {
            "name": "skew",
            "workload": {
                "kind": "sql",
                "suite": "synth",
                "rows": 1000,
                "sweep": {"zipf_s": [0.5, 1.0, 1.5]},
            },
            "engines": ["spark-vanilla"],
            "reference_engine": "spark-vanilla",
            "cluster": {
                "runner": "local",
                "executors": 1,
                "executor_cores": 1,
                "executor_memory": "1g",
            },
            "spark_conf": {"spark.sql.shuffle.partitions": [8, 64]},
            "modes": ["cold"],
        }
    )
    cells = expand(experiment)
    assert len(cells) == 6
    assert cells[0].config.varying == {"spark.sql.shuffle.partitions": "8", "zipf_s": "0.5"}
    assert cells[0].config.sweep == {"zipf_s": "0.5"}
    assert len({c.config_hash for c in cells}) == 6


# --- stability --------------------------------------------------------------------------------


def _shuffled(value: Any, rng: random.Random) -> Any:
    if isinstance(value, dict):
        items = list(value.items())
        rng.shuffle(items)
        return {k: _shuffled(v, rng) for k, v in items}
    if isinstance(value, list):
        return [_shuffled(v, rng) for v in value]
    return value


@pytest.mark.parametrize("seed", range(5))
def test_keys_ignore_dict_ordering(seed: int) -> None:
    raw = yaml.safe_load((VALID / "experiment-terasort-cpu-vs-apu.yaml").read_text())
    raw["spark_conf"] = {"spark.a": "1", "spark.b": [True, False], "spark.c": 3}
    reference = expand(Experiment.model_validate(raw))
    # Shuffle every mapping, but keep list order: list order is meaningful (engines, values).
    shuffled = expand(Experiment.model_validate(_shuffled(raw, random.Random(seed))))
    assert _summary(shuffled) == _summary(reference)


def test_yaml_spelling_of_a_value_does_not_matter() -> None:
    base = _load("experiment-minimal-local").model_dump()
    as_bool = expand(Experiment.model_validate({**base, "spark_conf": {"spark.x": True}}))
    as_str = expand(Experiment.model_validate({**base, "spark_conf": {"spark.x": "true"}}))
    assert [c.cell_key for c in as_bool] == [c.cell_key for c in as_str]
    assert spark_value(False) == "false"
    assert spark_value(200) == "200"


def test_units_do_not_change_config_hash() -> None:
    base = _load("experiment-minimal-local").model_dump()
    some = {**base, "workload": {**base["workload"], "units": ["q1", "q6"]}}
    assert (
        expand(Experiment.model_validate(some))[0].config_hash
        == expand(Experiment.model_validate(base))[0].config_hash
    )


def test_cluster_settings_change_cell_key_not_config_hash() -> None:
    base = _load("experiment-minimal-local").model_dump()
    bigger = {**base, "cluster": {**base["cluster"], "executors": 2}}
    a, b = expand(Experiment.model_validate(base))[0], expand(Experiment.model_validate(bigger))[0]
    assert a.config_hash == b.config_hash
    assert a.cell_key != b.cell_key


def test_keys_are_stable_across_python_processes() -> None:
    script = (
        "from pathlib import Path; from velocia.config import Experiment, load_model; "
        "from velocia.experiment.expander import expand; "
        f"print([c.cell_key for c in expand(load_model(Path({str(VALID)!r}) / "
        "'experiment-tpch-engines-sf10.yaml', Experiment))])"
    )
    outputs = {
        subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ, "PYTHONHASHSEED": str(seed)},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for seed in (1, 2, 3)
    }
    assert len(outputs) == 1


# --- estimates and dry run --------------------------------------------------------------------


def test_estimate_counts_runs_and_bounds_duration() -> None:
    experiment = _load("experiment-tpch-engines-sf10")
    est = estimate(experiment, expand(experiment))
    assert (est.cells, est.runs, est.correctness_passes, est.units) == (12, 60, 12, 22)
    assert est.duration is not None and est.worst_case is not None
    assert timedelta(minutes=10) < est.duration < est.worst_case


def test_estimate_without_known_units() -> None:
    experiment = _load("experiment-terasort-cpu-vs-apu")
    assert estimate(experiment, expand(experiment)).units == 1
    raw = experiment.model_dump()
    raw["workload"] = {**raw["workload"], "units": "all"}
    assert estimate(Experiment.model_validate(raw), expand(experiment)).units == 1  # terasort
    raw["workload"] = {**raw["workload"], "kind": "sql", "suite": "custom", "rows": None}
    raw["workload"]["sql_dir"] = "queries"  # user SQL: unknown until read
    raw["workload"]["scale_factor"] = 1
    est = estimate(Experiment.model_validate(raw), expand(experiment))
    assert est.units is None and est.duration is None and est.worst_case is None


def test_format_duration() -> None:
    assert format_duration(timedelta(seconds=20)) == "< 1m"
    assert format_duration(timedelta(minutes=7)) == "7m"
    assert format_duration(timedelta(hours=26, minutes=5)) == "26h 05m"


def test_cli_dry_run() -> None:
    result = CliRunner().invoke(
        app, ["run", str(VALID / "experiment-minimal-local.yaml"), "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "2 cells x 3 repetitions = 6 runs (22 units each)" in result.output
    assert "repetitions=3 is below the recommended 5" in result.output
    assert "Estimated duration: ~" in result.output
    for cell in expand(_load("experiment-minimal-local")):
        assert cell.cell_key in result.output


def test_cli_dry_run_rejects_unknown_engines(tmp_path: Path) -> None:
    text = (VALID / "experiment-tpch-engines-sf10.yaml").read_text()
    path = tmp_path / "exp.yaml"
    path.write_text(text.replace("spark-gluten", "spark-nonexistent"))
    result = CliRunner().invoke(app, ["run", str(path), "--dry-run"])
    assert result.exit_code == 1
    assert "unknown engine(s): spark-nonexistent" in result.output
