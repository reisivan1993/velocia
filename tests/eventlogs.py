"""Build small synthetic Apache Spark event logs with known contents for tests.

The event shapes follow Apache Spark 3.5's JsonProtocol, trimmed to the fields Velocia reads.
"""

import json
from typing import Any


def plan(*names: str, children: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """A linear plan: names[0] is the root, each following name the child of the previous."""
    node: dict[str, Any] = {"nodeName": names[-1], "children": children or []}
    for name in reversed(names[:-1]):
        node = {"nodeName": name, "children": [node]}
    return node


def task_end(
    stage_id: int,
    cpu_ns: int = 0,
    run_ms: int = 0,
    rows: int = 0,
    nbytes: int = 0,
    shuffle_read: int = 0,
    shuffle_write: int = 0,
    spill: int = 0,
    gc_ms: int = 0,
    reason: str = "Success",
) -> dict[str, Any]:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Task End Reason": {"Reason": reason},
        "Task Metrics": {
            "Executor Run Time": run_ms,
            "Executor CPU Time": cpu_ns,
            "JVM GC Time": gc_ms,
            "Memory Bytes Spilled": spill * 2,
            "Disk Bytes Spilled": spill,
            "Input Metrics": {"Bytes Read": nbytes, "Records Read": rows},
            "Output Metrics": {"Bytes Written": 0, "Records Written": 0},
            "Shuffle Read Metrics": {"Remote Bytes Read": shuffle_read, "Local Bytes Read": 0},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": shuffle_write},
        },
    }


def job_start(job_id: int, stage_ids: list[int], tag: str | None) -> dict[str, Any]:
    props = {"spark.job.description": tag} if tag else {}
    return {
        "Event": "SparkListenerJobStart",
        "Job ID": job_id,
        "Stage IDs": stage_ids,
        "Properties": props,
    }


def stage_completed(stage_id: int) -> dict[str, Any]:
    return {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {"Stage ID": stage_id, "Completion Time": 1},
    }


SQL = "org.apache.spark.sql.execution.ui."


def sql_start(
    execution_id: int, tag: str, time: int, plan_info: dict[str, Any], root: int | None = None
) -> dict[str, Any]:
    return {
        "Event": SQL + "SparkListenerSQLExecutionStart",
        "executionId": execution_id,
        "rootExecutionId": execution_id if root is None else root,
        "description": tag,
        "time": time,
        "sparkPlanInfo": plan_info,
    }


def sql_update(execution_id: int, plan_info: dict[str, Any]) -> dict[str, Any]:
    return {
        "Event": SQL + "SparkListenerSQLAdaptiveExecutionUpdate",
        "executionId": execution_id,
        "sparkPlanInfo": plan_info,
    }


def sql_end(execution_id: int, time: int) -> dict[str, Any]:
    return {
        "Event": SQL + "SparkListenerSQLExecutionEnd",
        "executionId": execution_id,
        "time": time,
    }


def two_unit_app() -> list[dict[str, Any]]:
    """q1 (2 jobs, 3 tasks, accelerated plan after AQE) and q2 (1 job), plus setup and noise."""
    t1, t2 = "velocia:run1:q1:1", "velocia:run1:q2:1"
    return [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.9"},
        {"Event": "SparkListenerApplicationStart", "App ID": "local-1", "Timestamp": 1000},
        job_start(0, [0], "velocia:run1:_setup:0"),
        task_end(0, cpu_ns=1_000_000),
        stage_completed(0),
        sql_start(1, t1, 2000, plan("AdaptiveSparkPlan", "Project", "Scan parquet")),
        job_start(1, [1], t1),
        task_end(1, cpu_ns=2_000_000_000, run_ms=2500, rows=100, nbytes=1000, gc_ms=10),
        task_end(1, cpu_ns=1_000_000_000, run_ms=1500, rows=50, nbytes=500, shuffle_write=64),
        stage_completed(1),
        # AQE replaces the plan: Comet operators with one transition back to rows.
        sql_update(
            1, plan("AdaptiveSparkPlan", "CometColumnarToRow", "CometProject", "CometScan parquet")
        ),
        sql_start(2, t1, 2100, plan("Project", "Scan"), root=1),  # nested: ignored
        sql_end(2, 2200),
        job_start(2, [2, 3], t1),  # stage 3 is skipped: never completes
        task_end(2, cpu_ns=500_000_000, run_ms=600, shuffle_read=64, spill=7, reason="Killed"),
        stage_completed(2),
        sql_end(1, 5000),
        job_start(3, [4], t2),
        task_end(4, cpu_ns=3_000_000_000, run_ms=3000, rows=10, nbytes=99),
        stage_completed(4),
        job_start(4, [5], None),  # untagged
        task_end(5, cpu_ns=1),
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 9000},
    ]


def lines(events: list[dict[str, Any]]) -> str:
    return "".join(json.dumps(e) + "\n" for e in events)
