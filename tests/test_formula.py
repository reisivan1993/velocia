from pathlib import Path

import pytest
from typer.testing import CliRunner

from velocia.cli.app import app
from velocia.config import ConfigError
from velocia.config.models import Direction, HardwareProfile, LocalCluster
from velocia.measures.builtins import ClusterInputs, UnitInputs
from velocia.measures.formula import FormulaError, parse, unknown_hw_inputs
from velocia.measures.registry import load_measure_file, load_measures

FIXTURES = Path(__file__).parent / "fixtures" / "config" / "valid"
APU = HardwareProfile(
    name="apu-node",
    price_per_node_hour_usd=6.1,
    accelerators_per_node=2,
    custom={"x": 3.0, "cu_per_node_hour": 1.5},
)


def _cluster(profile: HardwareProfile | None = APU) -> ClusterInputs:
    local = LocalCluster(runner="local", executors=2, executor_cores=4, executor_memory="8g")
    return ClusterInputs.of(local, profile, 0.4)


def test_acceptance_formula_evaluates() -> None:
    """T16: unit.wall_s / 3600 * cluster.nodes * hw.x evaluates."""
    formula = parse("unit.wall_s / 3600 * cluster.nodes * hw.x")
    assert formula.inputs == {"unit.wall_s", "cluster.nodes", "hw.x"}
    values = {"unit.wall_s": 7200.0, "cluster.nodes": 1.0, "hw.x": 3.0}
    assert formula.evaluate(values) == (6.0, None)


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("__import__('os').system('true')", "only min, max and abs"),
        ("unit.wall_s.__class__", "is not allowed"),
        ("unit.__dict__", "unknown input"),
        ("hw._secret", "unknown input"),
        ("unit.nope * 2", "unknown input 'unit.nope'"),
        ("os.path", "unknown input namespace 'os'"),
        ("unit.wall_s if 1 else 2", "IfExp is not allowed"),
        ("[1][0]", "Subscript is not allowed"),
        ("unit.wall_s + 'a'", "only numeric constants"),
        ("unit.wall_s < 1", "Compare is not allowed"),
        ("lambda: 1", "Lambda is not allowed"),
        ("max(unit.wall_s, key=abs)", "positional arguments only"),
        ("3 * 4", "uses no inputs"),
        ("unit.wall_s +", "syntax error"),
        ("x" * 1001, "longer than"),
    ],
)
def test_rejected_formulas(text: str, message: str) -> None:
    with pytest.raises(FormulaError, match=message):
        parse(text)


def test_missing_inputs_and_math_errors_give_null_with_reason() -> None:
    f = parse("unit.task_cpu_s / unit.wall_s")
    assert f.evaluate({"unit.wall_s": 2.0}) == (None, "input unit.task_cpu_s not available")
    assert f.evaluate({"unit.wall_s": 0.0, "unit.task_cpu_s": 1.0}) == (None, "division by zero")
    assert parse("unit.wall_s ** 1000").evaluate({"unit.wall_s": 2.0})[0] is None
    assert parse("max(unit.wall_s, 5) - abs(-1)").evaluate({"unit.wall_s": 2.0}) == (4.0, None)


def test_formula_measure_from_file_uses_profile_and_telemetry(tmp_path: Path) -> None:
    measure = load_measure_file(FIXTURES / "measure-vendor-cu.yaml", match_file_name=False)
    assert measure.direction is Direction.LOWER and measure.unit == "CU"
    assert measure.definition_sha256 and measure.definition_sha256.startswith("sha256:")
    # 1 h on 1 local node at 1.5 CU per node-hour.
    assert measure.compute(UnitInputs(wall_s=3600), _cluster()) == (1.5, None)
    value, reason = measure.compute(UnitInputs(wall_s=3600), _cluster(None))
    assert value is None and reason == "input hw.cu_per_node_hour not available"
    assert measure.compute(UnitInputs(wall_s=None), _cluster())[1] == "unit did not finish"

    (tmp_path / "cpu_share.yaml").write_text(
        "name: cpu_share\nunit: ratio\ndirection: higher\n"
        "formula: unit.task_cpu_s / (unit.wall_s * cluster.executors * cluster.executor_cores)\n"
    )
    share = load_measures(tmp_path)["cpu_share"]
    assert share.compute(UnitInputs(4.0, {"task_cpu_s": 16.0}), _cluster()) == (0.5, None)


def test_registry_rejects_bad_files(tmp_path: Path) -> None:
    (tmp_path / "wall_time.yaml").write_text(
        "name: wall_time\nunit: s\ndirection: lower\nformula: unit.wall_s\n"
    )
    with pytest.raises(ConfigError, match="is a built-in measure"):
        load_measures(tmp_path)
    (tmp_path / "wall_time.yaml").unlink()
    (tmp_path / "bad.yaml").write_text(
        "name: bad\nunit: s\ndirection: lower\nformula: __import__('os')\n"
    )
    with pytest.raises(ConfigError, match=r"bad\.yaml: formula: only min, max and abs"):
        load_measures(tmp_path)


def test_unknown_hw_inputs() -> None:
    f = parse("unit.wall_s * hw.cu_per_node_hour * hw.price_per_node_hour_usd * hw.typo")
    assert unknown_hw_inputs(f, [APU]) == ["hw.typo"]
    assert unknown_hw_inputs(f, [None]) == sorted(i for i in f.inputs if i.startswith("hw."))


def test_cli_measures_list_and_check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "measures").mkdir()
    (tmp_path / "measures" / "vendor_cu.yaml").write_text(
        (FIXTURES / "measure-vendor-cu.yaml").read_text()
    )
    cli = CliRunner()
    listed = cli.invoke(app, ["measures", "list"])
    assert listed.exit_code == 0
    assert "wall_time" in listed.output and "vendor_cu" in listed.output
    checked = cli.invoke(app, ["measures", "check", "measures/vendor_cu.yaml"])
    assert checked.exit_code == 0, checked.output
    assert "inputs: cluster.nodes, hw.cu_per_node_hour, unit.wall_s" in checked.output
    assert "custom coefficient" in checked.output
    (tmp_path / "evil.yaml").write_text(
        "name: evil\nunit: s\ndirection: lower\nformula: unit.wall_s.__class__\n"
    )
    rejected = cli.invoke(app, ["measures", "check", "evil.yaml"])
    assert rejected.exit_code == 1 and "is not allowed" in rejected.output
