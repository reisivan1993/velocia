"""Stream events from finished Apache Spark event logs (ADR-7, docs/06-design.md §5.4).

Reads one line at a time and never holds a whole log in memory (CLAUDE.md). Supported:

- a single file, plain or zstd (``.zst``/``.zstd``; Spark's ``spark.eventLog.compress`` default),
- a rolling event-log directory (``eventlog_v2_<app>/events_<n>_<app>[.zstd]``), read in order,
- a directory holding one or more of the above, read in name order.

Other codecs (lz4, snappy) are rejected with a clear error. ``.inprogress`` logs are read like
finished ones; the caller decides whether a missing ``SparkListenerApplicationEnd`` matters.
"""

import io
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import IO, Any

import zstandard

from velocia.collect.redaction import Redactor

ZSTD_SUFFIXES = (".zst", ".zstd")
UNSUPPORTED_SUFFIXES = (".lz4", ".snappy", ".lzf")
_ROLLING_PART = re.compile(r"^events_(\d+)_")


class EventLogError(RuntimeError):
    pass


def _base_name(path: Path) -> str:
    return path.name.removesuffix(".inprogress")


def log_files(path: Path) -> list[Path]:
    """The files of one log location, in reading order."""
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise EventLogError(f"no event log at {path}")
    parts = [p for p in path.iterdir() if _ROLLING_PART.match(p.name)]
    if parts:  # a rolling event-log directory
        return sorted(parts, key=lambda p: int(_ROLLING_PART.match(p.name).group(1)))  # type: ignore[union-attr]
    files: list[Path] = []
    for child in sorted(path.iterdir()):
        if child.name.startswith(".") or child.name.startswith("appstatus_"):
            continue
        files.extend(log_files(child))
    return files


def _open_text(path: Path) -> IO[str]:
    name = _base_name(path)
    if name.endswith(UNSUPPORTED_SUFFIXES):
        raise EventLogError(
            f"{path}: compression codec not supported; use zstd (spark.io.compression.codec=zstd) "
            "or uncompressed event logs"
        )
    if name.endswith(ZSTD_SUFFIXES):
        raw = path.open("rb")
        stream = zstandard.ZstdDecompressor().stream_reader(raw, closefd=True)
        return io.TextIOWrapper(io.BufferedReader(stream), encoding="utf-8", errors="replace")
    return path.open(encoding="utf-8", errors="replace")


def iter_events(path: Path, redactor: Redactor | None = None) -> Iterator[dict[str, Any]]:
    """Every event of the log at ``path``; redacted on read when a redactor is given."""
    for file in log_files(path):
        with _open_text(file) as lines:
            for number, line in enumerate(lines, 1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    if number > 1 and not line.endswith("\n"):
                        break  # truncated last line of an unfinished log
                    raise EventLogError(f"{file}:{number}: not JSON: {exc}") from exc
                yield redactor.redact(event) if redactor is not None else event
