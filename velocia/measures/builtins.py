"""Built-in measures (docs/03-spec.md §6).

Each is a pure function of one unit repetition's inputs and the cluster. A missing input gives
``None`` with a reason, never a guess.
"""

from collections.abc import Callable
from dataclasses import dataclass, field

from velocia.config.models import Cluster, Direction, HardwareProfile


@dataclass(frozen=True)
class UnitInputs:
    wall_s: float | None
    # Filled by the event-log reader: task_cpu_s, task_run_s, gc_s, shuffle bytes, ...
    telemetry: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ClusterInputs:
    nodes: int
    executors: int
    executor_cores: int
    profile: HardwareProfile | None
    default_node_hour_usd: float

    @classmethod
    def of(
        cls, cluster: Cluster, profile: HardwareProfile | None, default_price: float
    ) -> "ClusterInputs":
        # local: one machine; k8s and external: one executor per node (v0.1 assumption).
        nodes = 1 if cluster.runner == "local" else cluster.executors
        return cls(nodes, cluster.executors, cluster.executor_cores, profile, default_price)


Value = tuple[float | None, str | None]  # (value, reason when None)


@dataclass(frozen=True)
class Measure:
    name: str
    unit: str
    direction: Direction
    compute: Callable[[UnitInputs, ClusterInputs], Value]


def _wall(u: UnitInputs, c: ClusterInputs) -> Value:
    return (u.wall_s, None) if u.wall_s is not None else (None, "unit did not finish")


def _telemetry(key: str, scale: float = 1.0) -> Callable[[UnitInputs, ClusterInputs], Value]:
    def compute(u: UnitInputs, c: ClusterInputs) -> Value:
        if key not in u.telemetry:
            return None, f"event-log input {key} not available"
        return u.telemetry[key] * scale, None

    return compute


def _throughput(key: str) -> Callable[[UnitInputs, ClusterInputs], Value]:
    def compute(u: UnitInputs, c: ClusterInputs) -> Value:
        if u.wall_s is None:
            return None, "unit did not finish"
        if key not in u.telemetry:
            return None, f"event-log input {key} not available"
        if u.wall_s <= 0:
            return None, "wall time is zero"
        return u.telemetry[key] / u.wall_s, None

    return compute


def _node_hours(u: UnitInputs, c: ClusterInputs) -> Value:
    if u.wall_s is None:
        return None, "unit did not finish"
    return u.wall_s / 3600 * c.nodes, None


def _accelerator_hours(u: UnitInputs, c: ClusterInputs) -> Value:
    hours, reason = _node_hours(u, c)
    if hours is None:
        return None, reason
    if c.profile is None or not c.profile.accelerators_per_node:
        return None, "cluster has no accelerators in its hardware profile"
    return hours * c.profile.accelerators_per_node, None


def _cost(u: UnitInputs, c: ClusterInputs) -> Value:
    hours, reason = _node_hours(u, c)
    if hours is None:
        return None, reason
    price = c.profile.price_per_node_hour_usd if c.profile is not None else None
    return hours * (price if price is not None else c.default_node_hour_usd), None


def _energy(u: UnitInputs, c: ClusterInputs) -> Value:
    hours, reason = _node_hours(u, c)
    if hours is None:
        return None, reason
    if c.profile is None or c.profile.watts_per_node is None:
        return None, "hardware profile has no watts_per_node"
    return hours * c.profile.watts_per_node / 1000, None


LOWER, HIGHER = Direction.LOWER, Direction.HIGHER

BUILTINS: dict[str, Measure] = {
    m.name: m
    for m in [
        Measure("wall_time", "s", LOWER, _wall),
        Measure("task_cpu_time", "s", LOWER, _telemetry("task_cpu_s")),
        Measure("task_run_time", "s", LOWER, _telemetry("task_run_s")),
        Measure("gc_time", "s", LOWER, _telemetry("gc_s")),
        Measure("shuffle_bytes", "bytes", LOWER, _telemetry("shuffle_bytes")),
        Measure("spill_bytes", "bytes", LOWER, _telemetry("spill_bytes")),
        Measure("throughput_rows", "rows/s", HIGHER, _throughput("input_rows")),
        Measure("throughput_bytes", "bytes/s", HIGHER, _throughput("input_bytes")),
        Measure("node_hours", "node·h", LOWER, _node_hours),
        Measure("accelerator_hours", "device·h", LOWER, _accelerator_hours),
        Measure("cost_usd", "USD", LOWER, _cost),
        Measure("energy_kwh", "kWh (estimate)", LOWER, _energy),
    ]
}
