import json
from pathlib import Path
from typing import Any

import pytest
import zstandard
from conftest import SECRET_MARKER, PlantedSecrets
from eventlogs import lines, two_unit_app

from velocia.collect.redaction import Redactor
from velocia.families.spark.eventlog.reader import EventLogError, iter_events, log_files
from velocia.families.spark.eventlog.summary import parse_tag, summarize


def _write(path: Path, text: str, zstd: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = text.encode()
    path.write_bytes(zstandard.ZstdCompressor().compress(data) if zstd else data)
    return path


def test_two_unit_summary(tmp_path: Path) -> None:
    app = summarize(_write(tmp_path / "local-1", lines(two_unit_app())))
    assert (app.app_id, app.spark_version, app.complete) == ("local-1", "3.5.9", True)
    assert app.untagged_tasks == 1
    assert set(app.units) == {("_setup", 0), ("q1", 1), ("q2", 1)}

    q1 = app.units[("q1", 1)]
    assert (q1.jobs, q1.stages, q1.tasks, q1.failed_tasks) == (2, 2, 3, 1)
    assert q1.task_cpu_ns == 3_500_000_000 and q1.task_run_ms == 4600 and q1.gc_ms == 10
    assert (q1.input_rows, q1.input_bytes) == (150, 1500)
    assert (q1.shuffle_read_bytes, q1.shuffle_write_bytes) == (64, 64)
    assert (q1.disk_spill_bytes, q1.memory_spill_bytes) == (7, 14)
    # Root execution only, final (adaptive) plan: 3 operators, 1 transition, 2 accelerated.
    assert (q1.sql_executions, q1.sql_exec_ms) == (1, 3000)
    assert (q1.plan_operators, q1.plan_transitions, q1.plan_accelerated) == (3, 1, 2)
    t = q1.telemetry()
    assert t["task_cpu_s"] == 3.5 and t["shuffle_bytes"] == 128 and t["spill_bytes"] == 7
    assert t["plan_accelerated_share"] == pytest.approx(2 / 3)

    q2 = app.units[("q2", 1)]
    assert (q2.jobs, q2.tasks, q2.task_cpu_ns, q2.input_rows) == (1, 1, 3_000_000_000, 10)


def test_zstd_rolling_and_directory_layouts(tmp_path: Path) -> None:
    events = two_unit_app()
    reference = summarize(_write(tmp_path / "plain" / "local-1", lines(events))).units
    zst = _write(tmp_path / "zstd" / "local-1.zstd", lines(events), zstd=True)
    assert summarize(zst).units == reference
    # Rolling: events_<n>_<app>, read by number (10 after 2), parts may be compressed.
    rolling = tmp_path / "eventlog_v2_local-1"
    _write(rolling / "events_10_local-1.zstd", lines(events[12:]), zstd=True)
    _write(rolling / "events_2_local-1", lines(events[:12]))
    _write(rolling / "appstatus_local-1", "")
    assert [p.name for p in log_files(rolling)] == ["events_2_local-1", "events_10_local-1.zstd"]
    assert summarize(rolling).units == reference
    # A run's eventlog/ directory holding the rolling directory.
    assert summarize(tmp_path / "zstd").units == reference


def test_truncated_last_line_is_tolerated_but_corruption_is_not(tmp_path: Path) -> None:
    text = lines(two_unit_app())
    unfinished = _write(tmp_path / "a.inprogress", text[:-20])
    app = summarize(unfinished)
    assert not app.complete and ("q2", 1) in app.units
    corrupt = _write(tmp_path / "b", "{not json}\n" + text)
    with pytest.raises(EventLogError, match="not JSON"):
        list(iter_events(corrupt))


def test_unsupported_codec_and_missing_log(tmp_path: Path) -> None:
    with pytest.raises(EventLogError, match="codec not supported"):
        list(iter_events(_write(tmp_path / "local-1.lz4", "x")))
    with pytest.raises(EventLogError, match="no event log"):
        list(iter_events(tmp_path / "missing"))


def test_opaque_application_is_one_unit(tmp_path: Path) -> None:
    app = summarize(_write(tmp_path / "log", lines(two_unit_app())), opaque_unit="job")
    assert list(app.units) == [("job", 1)]
    unit = app.units[("job", 1)]
    assert unit.tasks == 6 and unit.jobs == 5 and unit.wall_ms == 8000


def test_redaction_on_read(tmp_path: Path, planted_secrets: PlantedSecrets) -> None:
    log = _write(tmp_path / "log", planted_secrets.event_log_line + "\n")
    events = list(iter_events(log, Redactor()))
    planted_secrets.assert_absent(json.dumps(events))
    assert SECRET_MARKER in json.dumps(list(iter_events(log)))


def test_parse_tag() -> None:
    assert parse_tag("velocia:exp-c001-r1:q12:3") == ("exp-c001-r1", "q12", 3)
    assert parse_tag("velocia:exp:q1:-1") == ("exp", "q1", -1)
    assert parse_tag("some user description") is None
    assert parse_tag(None) is None


# --- golden logs from real Apache Spark 3.5.9 runs (tests/golden/make_eventlogs.py) ---------

GOLDEN = Path(__file__).parent / "fixtures" / "eventlogs"


@pytest.mark.parametrize("engine", ["vanilla", "comet"])
def test_golden_summaries(engine: str) -> None:
    app = summarize(GOLDEN / f"{engine}.zstd")
    actual = {f"{k[0]}:{k[1]}": u.as_dict() for k, u in app.units.items()}
    expected = json.loads((GOLDEN / f"{engine}.summary.json").read_text())
    assert actual == expected
    assert app.complete and app.spark_version == "3.5.9" and app.untagged_tasks == 0
    assert set(actual) == {"_setup:0", "q1:1", "q6:1"}
    for unit in ("q1:1", "q6:1"):
        assert actual[unit]["jobs"] > 0 and actual[unit]["tasks"] > 0
        assert actual[unit]["input_rows"] == expected[unit]["input_rows"] > 0


def test_golden_comet_log_shows_acceleration_and_transitions() -> None:
    vanilla = summarize(GOLDEN / "vanilla.zstd").units[("q1", 1)]
    comet = summarize(GOLDEN / "comet.zstd").units[("q1", 1)]
    assert vanilla.plan_accelerated == 0
    assert comet.plan_accelerated > comet.plan_operators / 2
    assert comet.plan_transitions >= 1  # back to rows for the result
    # Same data read by both engines.
    assert comet.input_rows == vanilla.input_rows


def _stage_submitted(stage_id: int, *rdds: tuple[str, str | None]) -> dict[str, Any]:
    return {
        "Event": "SparkListenerStageSubmitted",
        "Stage Info": {
            "Stage ID": stage_id,
            "RDD Info": [
                {"Name": name, "Scope": json.dumps({"id": "1", "name": scope}) if scope else None}
                for name, scope in rdds
            ],
        },
    }


def test_fallback_share_of_rdd_and_sql_units() -> None:
    from eventlogs import job_start, stage_completed, task_end

    from velocia.families.spark.eventlog.summary import summarize_events

    rdd, sql = "velocia:r:rdd_job:1", "velocia:r:sql_unit:1"
    events = [
        # An RDD job on an accelerated engine: no accelerated operator anywhere.
        job_start(0, [0], rdd),
        _stage_submitted(0, ("MapPartitionsRDD", "map"), ("ParallelCollectionRDD", "parallelize")),
        task_end(0, run_ms=900),
        stage_completed(0),
        # A SQL unit: one Comet stage, one stage that fell back.
        job_start(1, [1, 2], sql),
        _stage_submitted(1, ("CometShuffledBatchRDD", "CometExchange")),
        task_end(1, run_ms=750),
        stage_completed(1),
        _stage_submitted(2, ("MapPartitionsRDD", "WholeStageCodegen (1)")),
        task_end(2, run_ms=250),
        stage_completed(2),
    ]
    units = summarize_events(events).units
    assert units[("rdd_job", 1)].telemetry()["fallback_share"] == 1.0
    assert units[("sql_unit", 1)].telemetry()["fallback_share"] == 0.25
