"""Built-in measures plus formula measures from the user's measures directory."""

import hashlib
from pathlib import Path

from velocia.config import ConfigError, MeasureDef, load_model
from velocia.measures.builtins import BUILTINS, ClusterInputs, Measure, UnitInputs, Value
from velocia.measures.formula import Formula, FormulaError, hw_inputs, parse


def _inputs(u: UnitInputs, c: ClusterInputs) -> dict[str, float | None]:
    values: dict[str, float | None] = {"unit.wall_s": u.wall_s}
    values.update({f"unit.{k}": float(v) for k, v in u.telemetry.items()})
    values.update({
        "cluster.nodes": float(c.nodes),
        "cluster.executors": float(c.executors),
        "cluster.executor_cores": float(c.executor_cores),
        "cluster.executor_memory_gb": c.executor_memory_gb,
    })  # fmt: skip
    values.update(hw_inputs(c.profile))
    return values


def formula_measure(definition: MeasureDef, formula: Formula, source: str) -> Measure:
    def compute(u: UnitInputs, c: ClusterInputs) -> Value:
        if u.wall_s is None:
            return None, "unit did not finish"
        return formula.evaluate(_inputs(u, c))

    text = definition.model_dump_json()
    return Measure(
        name=definition.name,
        unit=definition.unit,
        direction=definition.direction,
        compute=compute,
        source=source,
        formula=definition.formula,
        definition_sha256="sha256:" + hashlib.sha256(text.encode()).hexdigest(),
    )


def load_measure_file(path: Path, match_file_name: bool = True) -> Measure:
    definition = load_model(path, MeasureDef)
    if match_file_name and definition.name != path.stem:
        raise ConfigError(path, [("name", f"must match the file name {path.stem!r}")])
    if definition.name in BUILTINS:
        raise ConfigError(path, [("name", f"{definition.name!r} is a built-in measure")])
    try:
        formula = parse(definition.formula)
    except FormulaError as exc:
        raise ConfigError(path, [("formula", str(exc))]) from exc
    return formula_measure(definition, formula, str(path))


def load_measures(measures_dir: Path | None) -> dict[str, Measure]:
    measures = dict(BUILTINS)
    if measures_dir is not None and measures_dir.is_dir():
        for path in sorted(measures_dir.glob("*.yaml")):
            measure = load_measure_file(path)
            measures[measure.name] = measure
    return measures
