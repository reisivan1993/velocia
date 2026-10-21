"""Move a finished application's raw outputs into the results directory, redacted.

Raw outputs (driver JSON, Spark event logs, the driver log) are written by Spark into a
temporary directory outside the results directory. Everything passes through the redactor on
the way in; event logs are streamed line by line, never loaded whole (CLAUDE.md).
"""

import json
import shutil
from pathlib import Path
from typing import Any

from velocia.collect.redaction import Redactor
from velocia.runners.base import AppResult


def _redact_json_file(src: Path, dest: Path, redactor: Redactor) -> None:
    data = json.loads(src.read_text("utf-8"))
    dest.write_text(json.dumps(redactor.redact(data), indent=2) + "\n", "utf-8")


def redact_event_log(src: Path, dest: Path, redactor: Redactor) -> None:
    """Event-log redaction (docs/06-design.md §5.4): every line, one at a time."""
    with (
        src.open(encoding="utf-8", errors="replace") as fin,
        dest.open("w", encoding="utf-8") as fout,
    ):
        for line in fin:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                fout.write(redactor.redact_text(line))
                continue
            fout.write(json.dumps(redactor.redact(event), separators=(",", ":")) + "\n")


def collect(result: AppResult, run_dir: Path, redactor: Redactor) -> dict[str, Any]:
    """Copy redacted outputs into ``run_dir``; returns the parsed driver output, if any."""
    run_dir.mkdir(parents=True, exist_ok=True)
    driver_output: dict[str, Any] = {}
    try:
        for name in ("timings.json", "checks.json"):
            src = result.work_dir / "out" / name
            if src.is_file():
                _redact_json_file(src, run_dir / name, redactor)
                driver_output = json.loads((run_dir / name).read_text("utf-8"))
        logs = sorted(p for p in (result.work_dir / "eventlog").rglob("*") if p.is_file())
        if logs:
            (run_dir / "eventlog").mkdir(exist_ok=True)
            for log in logs:
                rel = log.relative_to(result.work_dir / "eventlog")
                dest = run_dir / "eventlog" / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                redact_event_log(log, dest, redactor)
        (run_dir / "driver.log").write_text(redactor.redact_text(result.log_tail) + "\n", "utf-8")
    finally:
        shutil.rmtree(result.work_dir, ignore_errors=True)
    return driver_output
