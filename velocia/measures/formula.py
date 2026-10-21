"""Formula measures: safe arithmetic over named inputs (docs/03-spec.md §6, ADR-11).

A formula is parsed with :mod:`ast` and checked against an allow-list; it is never passed to
``eval``. Allowed: numbers, ``+ - * / // % **``, unary ``-``/``+``, parentheses, the functions
``min``, ``max`` and ``abs``, and inputs written ``namespace.field``:

- ``unit.*``: one unit repetition (``UNIT_INPUTS``),
- ``cluster.*``: the cluster it ran on (``CLUSTER_INPUTS``),
- ``hw.*``: the cluster's hardware profile, including its ``custom`` coefficients.

Unknown ``unit``/``cluster`` fields are rejected before a run. A ``hw`` field may be a custom
coefficient that only some profiles define; where it is missing the value is ``None`` with a
reason.
"""

import ast
import hashlib
import math
import operator
from collections.abc import Callable
from dataclasses import dataclass

from velocia.config.models import HardwareProfile, MeasureDef

UNIT_INPUTS = frozenset(
    {
        "wall_s",
        "task_cpu_s",
        "task_run_s",
        "gc_s",
        "input_rows",
        "input_bytes",
        "output_rows",
        "output_bytes",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "shuffle_bytes",
        "spill_bytes",
        "stages",
        "tasks",
    }
)
CLUSTER_INPUTS = frozenset({"nodes", "executors", "executor_cores", "executor_memory_gb"})
HW_STANDARD = frozenset({"price_per_node_hour_usd", "watts_per_node", "accelerators_per_node"})
NAMESPACES = {"unit": UNIT_INPUTS, "cluster": CLUSTER_INPUTS, "hw": None}  # None: open set

_BINOPS: dict[type[ast.operator], Callable[[float, float], float]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY: dict[type[ast.unaryop], Callable[[float], float]] = {
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}
_FUNCS: dict[str, Callable[..., float]] = {"min": min, "max": max, "abs": abs}
MAX_LENGTH = 1000


class FormulaError(ValueError):
    pass


class MissingInput(Exception):
    def __init__(self, name: str) -> None:
        self.name = name


@dataclass(frozen=True)
class Formula:
    text: str
    tree: ast.Expression
    inputs: frozenset[str]  # "namespace.field" names used

    @property
    def sha256(self) -> str:
        return "sha256:" + hashlib.sha256(self.text.encode()).hexdigest()

    def evaluate(self, values: dict[str, float | None]) -> tuple[float | None, str | None]:
        try:
            result = _eval(self.tree.body, values)
        except MissingInput as missing:
            return None, f"input {missing.name} not available"
        except ZeroDivisionError:
            return None, "division by zero"
        except (OverflowError, ValueError) as exc:
            return None, f"arithmetic error: {exc}"
        if not math.isfinite(result):
            return None, "result is not a finite number"
        return float(result), None


def _input_name(node: ast.AST) -> str | None:
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and isinstance(node.ctx, ast.Load)
    ):
        return f"{node.value.id}.{node.attr}"
    return None


def parse(text: str) -> Formula:
    if len(text) > MAX_LENGTH:
        raise FormulaError(f"formula longer than {MAX_LENGTH} characters")
    try:
        tree = ast.parse(text, mode="eval")
    except SyntaxError as exc:
        raise FormulaError(f"syntax error: {exc.msg}") from None
    inputs: set[str] = set()
    _check(tree.body, inputs)
    if not inputs:
        raise FormulaError("formula uses no inputs")
    return Formula(text, tree, frozenset(inputs))


def _check(node: ast.AST, inputs: set[str]) -> None:
    name = _input_name(node)
    if name is not None:
        namespace, field = name.split(".", 1)
        if namespace not in NAMESPACES:
            raise FormulaError(f"unknown input namespace {namespace!r} (use unit, cluster, hw)")
        known = NAMESPACES[namespace]
        if field.startswith("_") or (known is not None and field not in known):
            raise FormulaError(f"unknown input {name!r}")
        inputs.add(name)
        return
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool) or not isinstance(node.value, int | float):
            raise FormulaError(f"only numeric constants are allowed, not {node.value!r}")
    elif isinstance(node, ast.BinOp):
        if type(node.op) not in _BINOPS:
            raise FormulaError(f"operator {type(node.op).__name__} is not allowed")
        _check(node.left, inputs)
        _check(node.right, inputs)
    elif isinstance(node, ast.UnaryOp):
        if type(node.op) not in _UNARY:
            raise FormulaError(f"operator {type(node.op).__name__} is not allowed")
        _check(node.operand, inputs)
    elif isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name) or node.func.id not in _FUNCS:
            raise FormulaError("only min, max and abs can be called")
        if node.keywords or not node.args:
            raise FormulaError(f"{node.func.id}() takes positional arguments only")
        for arg in node.args:
            _check(arg, inputs)
    else:
        raise FormulaError(f"{type(node).__name__} is not allowed in a formula")


def _eval(node: ast.AST, values: dict[str, float | None]) -> float:
    name = _input_name(node)
    if name is not None:
        value = values.get(name)
        if value is None:
            raise MissingInput(name)
        return value
    if isinstance(node, ast.Constant):
        assert isinstance(node.value, int | float)  # parse() allows numbers only
        return float(node.value)
    if isinstance(node, ast.BinOp):
        left, right = _eval(node.left, values), _eval(node.right, values)
        if isinstance(node.op, ast.Pow) and abs(right) > 100:
            raise ValueError("exponent too large")
        return _BINOPS[type(node.op)](left, right)
    if isinstance(node, ast.UnaryOp):
        return _UNARY[type(node.op)](_eval(node.operand, values))
    if isinstance(node, ast.Call):
        assert isinstance(node.func, ast.Name)
        return _FUNCS[node.func.id](*[_eval(a, values) for a in node.args])
    raise AssertionError(f"unchecked node {type(node).__name__}")  # parse() rejects it


def hw_inputs(profile: HardwareProfile | None) -> dict[str, float | None]:
    if profile is None:
        return {}
    values: dict[str, float | None] = {
        "hw.price_per_node_hour_usd": profile.price_per_node_hour_usd,
        "hw.watts_per_node": profile.watts_per_node,
        "hw.accelerators_per_node": float(profile.accelerators_per_node),
    }
    values.update({f"hw.{k}": v for k, v in profile.custom.items()})
    return values


def check_definition(definition: MeasureDef) -> Formula:
    return parse(definition.formula)


def describe_inputs() -> dict[str, list[str]]:
    return {
        "unit": sorted(UNIT_INPUTS),
        "cluster": sorted(CLUSTER_INPUTS),
        "hw": [*sorted(HW_STANDARD), "<custom coefficient from the hardware profile>"],
    }


def unknown_hw_inputs(formula: Formula, profiles: list[HardwareProfile | None]) -> list[str]:
    """``hw.*`` inputs that none of the given profiles defines (likely a typo)."""
    defined = set().union(*(hw_inputs(p) for p in profiles)) if profiles else set()
    return sorted(name for name in formula.inputs if name.startswith("hw.") and name not in defined)
