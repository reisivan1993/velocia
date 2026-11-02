"""Per-unit summaries from one application's event log (docs/03-spec.md §7, design §5.4).

Units are found by their job description tag (``velocia:<run_id>:<unit_id>:<rep>``): a job's
tag comes from its start event, its stages inherit it, and every task end is added to the unit
of its stage. Memory grows with the number of stages and live SQL executions, never with
the size of the log.

Plan markers come from each tagged root SQL execution's final plan (after adaptive updates):
row/columnar transitions (``ColumnarToRow``, ``RowToColumnar`` and engine variants such as
``CometColumnarToRow``) and accelerated operators (``Comet*``, ``Gluten*``, ``Velox*``, ``Gpu*``,
``*Transformer``). A transition inside an accelerated plan is a fallback to row-based Spark.

Fallback share: a stage is accelerated when any of its RDDs comes from an accelerated operator
(RDD name or scope such as ``CometExchange``); task time in other stages ran on vanilla Spark.
For an RDD job on an accelerated engine that share is ~100%.

In opaque mode (third-party jobs without tags) the whole application is one unit, timed from
application start to end.
"""

import contextlib
import json
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from velocia.collect.redaction import Redactor
from velocia.families.spark.eventlog.reader import iter_events

TAG = re.compile(r"^velocia:([^:]+):([^:]+):(-?\d+)$")
TRANSITION = re.compile(r"ColumnarToRow|RowToColumnar")
ACCELERATED = re.compile(r"^(Comet|Gluten|Velox|Gpu)|Transformer$")
PLAN_WRAPPERS = re.compile(r"^(AdaptiveSparkPlan|InputAdapter|WholeStageCodegen.*|.*QueryStage)$")

UnitKey = tuple[str, int]  # (unit_id, rep)


def parse_tag(description: str | None) -> tuple[str, str, int] | None:
    match = TAG.match(description or "")
    return (match.group(1), match.group(2), int(match.group(3))) if match else None


@dataclass
class UnitSummary:
    unit_id: str
    rep: int
    jobs: int = 0
    stages: int = 0
    tasks: int = 0
    failed_tasks: int = 0
    task_run_ms: int = 0
    task_cpu_ns: int = 0
    gc_ms: int = 0
    input_bytes: int = 0
    input_rows: int = 0
    output_bytes: int = 0
    output_rows: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spill_bytes: int = 0
    disk_spill_bytes: int = 0
    sql_executions: int = 0
    sql_exec_ms: int = 0
    plan_operators: int = 0
    plan_transitions: int = 0
    plan_accelerated: int = 0
    fallback_task_ms: int = 0  # task run time in stages without accelerated operators
    wall_ms: int | None = None  # opaque mode only: application start to end

    def telemetry(self) -> dict[str, float]:
        """Inputs for measures (docs/03-spec.md §6, ``unit.*``)."""
        return {
            "task_cpu_s": self.task_cpu_ns / 1e9,
            "task_run_s": self.task_run_ms / 1e3,
            "gc_s": self.gc_ms / 1e3,
            "input_rows": self.input_rows,
            "input_bytes": self.input_bytes,
            "output_rows": self.output_rows,
            "output_bytes": self.output_bytes,
            "shuffle_read_bytes": self.shuffle_read_bytes,
            "shuffle_write_bytes": self.shuffle_write_bytes,
            "shuffle_bytes": self.shuffle_read_bytes + self.shuffle_write_bytes,
            "spill_bytes": self.disk_spill_bytes,
            "stages": self.stages,
            "tasks": self.tasks,
            "plan_transitions": self.plan_transitions,
            "plan_accelerated_share": (
                self.plan_accelerated / self.plan_operators if self.plan_operators else 0.0
            ),
            "fallback_share": (
                self.fallback_task_ms / self.task_run_ms if self.task_run_ms else 0.0
            ),
        }

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AppSummary:
    app_id: str | None = None
    spark_version: str | None = None
    started_ms: int | None = None
    ended_ms: int | None = None
    complete: bool = False  # saw SparkListenerApplicationEnd
    units: dict[UnitKey, UnitSummary] = field(default_factory=dict)
    untagged_tasks: int = 0


def _count_plan(plan: dict[str, Any]) -> tuple[int, int, int]:
    """(operators, transitions, accelerated) in a sparkPlanInfo tree, iteratively."""
    operators = transitions = accelerated = 0
    stack = [plan]
    while stack:
        node = stack.pop()
        name = node.get("nodeName", "")
        stack.extend(node.get("children") or [])
        if PLAN_WRAPPERS.match(name):
            continue
        operators += 1
        if TRANSITION.search(name):
            transitions += 1
        elif ACCELERATED.search(name):
            accelerated += 1
    return operators, transitions, accelerated


def _stage_accelerated(info: dict[str, Any]) -> bool:
    for rdd in info.get("RDD Info") or []:
        scope = rdd.get("Scope")
        names = [rdd.get("Name") or ""]
        if scope:
            with contextlib.suppress(ValueError, AttributeError):  # malformed scope: skip it
                names.append(json.loads(scope).get("name") or "")
        if any(ACCELERATED.search(n) for n in names):
            return True
    return False


def _add_task(unit: UnitSummary, event: dict[str, Any], accelerated: bool = False) -> None:
    unit.tasks += 1
    reason = (event.get("Task End Reason") or {}).get("Reason")
    if reason not in (None, "Success"):
        unit.failed_tasks += 1
    m = event.get("Task Metrics")
    if not m:
        return
    unit.task_run_ms += m.get("Executor Run Time", 0)
    if not accelerated:
        unit.fallback_task_ms += m.get("Executor Run Time", 0)
    unit.task_cpu_ns += m.get("Executor CPU Time", 0)
    unit.gc_ms += m.get("JVM GC Time", 0)
    unit.memory_spill_bytes += m.get("Memory Bytes Spilled", 0)
    unit.disk_spill_bytes += m.get("Disk Bytes Spilled", 0)
    inp = m.get("Input Metrics") or {}
    unit.input_bytes += inp.get("Bytes Read", 0)
    unit.input_rows += inp.get("Records Read", 0)
    out = m.get("Output Metrics") or {}
    unit.output_bytes += out.get("Bytes Written", 0)
    unit.output_rows += out.get("Records Written", 0)
    sr = m.get("Shuffle Read Metrics") or {}
    unit.shuffle_read_bytes += sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0)
    sw = m.get("Shuffle Write Metrics") or {}
    unit.shuffle_write_bytes += sw.get("Shuffle Bytes Written", 0)


def summarize_events(
    events: Iterable[dict[str, Any]], opaque_unit: str | None = None
) -> AppSummary:
    app = AppSummary()
    job_unit: dict[int, UnitKey] = {}
    stage_unit: dict[int, UnitKey] = {}
    accelerated_stages: set[int] = set()
    executions: dict[int, dict[str, Any]] = {}  # live root SQL executions

    def unit_for(key: UnitKey) -> UnitSummary:
        if key not in app.units:
            app.units[key] = UnitSummary(unit_id=key[0], rep=key[1])
        return app.units[key]

    opaque_key: UnitKey | None = (opaque_unit, 1) if opaque_unit else None
    for event in events:
        kind = event.get("Event", "")
        if kind == "SparkListenerTaskEnd":
            key = opaque_key or stage_unit.get(event.get("Stage ID", -1))
            if key is None:
                app.untagged_tasks += 1
            else:
                stage = event.get("Stage ID", -1)
                _add_task(unit_for(key), event, stage in accelerated_stages)
        elif kind == "SparkListenerStageSubmitted":
            info = event.get("Stage Info") or {}
            if _stage_accelerated(info):
                accelerated_stages.add(info.get("Stage ID", -1))
        elif kind == "SparkListenerJobStart":
            tag = parse_tag((event.get("Properties") or {}).get("spark.job.description"))
            key = opaque_key or ((tag[1], tag[2]) if tag else None)
            if key is None:
                continue
            job_unit[event["Job ID"]] = key
            unit_for(key).jobs += 1
            for stage_id in event.get("Stage IDs", []):
                stage_unit[stage_id] = key
        elif kind == "SparkListenerStageCompleted":
            info = event.get("Stage Info") or {}
            key = stage_unit.get(info.get("Stage ID", -1))
            if key is not None and "Completion Time" in info:
                unit_for(key).stages += 1
        elif kind == "SparkListenerJobEnd":
            job_unit.pop(event.get("Job ID", -1), None)
        elif kind.endswith("SQLExecutionStart"):
            tag = parse_tag(event.get("description"))
            root = event.get("rootExecutionId", event["executionId"])
            key = opaque_key or ((tag[1], tag[2]) if tag else None)
            if key is not None and root == event["executionId"]:
                executions[event["executionId"]] = {
                    "key": key,
                    "start": event.get("time"),
                    "plan": event.get("sparkPlanInfo"),
                }
        elif kind.endswith("SQLAdaptiveExecutionUpdate"):
            live = executions.get(event.get("executionId", -1))
            if live is not None and event.get("sparkPlanInfo"):
                live["plan"] = event["sparkPlanInfo"]
        elif kind.endswith("SQLExecutionEnd"):
            live = executions.pop(event.get("executionId", -1), None)
            if live is None:
                continue
            unit = unit_for(live["key"])
            unit.sql_executions += 1
            if live["start"] is not None and event.get("time") is not None:
                unit.sql_exec_ms += event["time"] - live["start"]
            if live["plan"]:
                ops, transitions, accelerated = _count_plan(live["plan"])
                unit.plan_operators += ops
                unit.plan_transitions += transitions
                unit.plan_accelerated += accelerated
        elif kind == "SparkListenerApplicationStart":
            app.app_id = event.get("App ID")
            app.started_ms = event.get("Timestamp")
        elif kind == "SparkListenerApplicationEnd":
            app.ended_ms = event.get("Timestamp")
            app.complete = True
        elif kind == "SparkListenerLogStart":
            app.spark_version = event.get("Spark Version")
    if opaque_key is not None and app.started_ms is not None and app.ended_ms is not None:
        unit_for(opaque_key).wall_ms = app.ended_ms - app.started_ms
    return app


def summarize(
    path: Path, redactor: Redactor | None = None, opaque_unit: str | None = None
) -> AppSummary:
    return summarize_events(iter_events(path, redactor), opaque_unit)
