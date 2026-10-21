"""Regenerate the golden event logs: ``uv run python tests/golden/make_eventlogs.py`` (Docker).

Runs a tiny TPC-H-derived experiment (SF0.01, q1 and q6, one cold repetition) on vanilla Apache
Spark and on Comet through Velocia itself, then stores each timed run's redacted event log as
zstd in tests/fixtures/eventlogs/ with its summary as the expected golden output. The logs come
only from data this repo generates (CLAUDE.md).
"""

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import zstandard

from velocia.families.spark.eventlog.reader import log_files
from velocia.families.spark.eventlog.summary import summarize

OUT = Path(__file__).parent.parent / "fixtures" / "eventlogs"
EXPERIMENT = """
name: golden
workload: {kind: sql, suite: tpch, scale_factor: 0.01, units: [q1, q6]}
engines: [spark-vanilla, spark-comet]
reference_engine: spark-vanilla
cluster: {runner: local, executors: 1, executor_cores: 2, executor_memory: 2g}
measures: [wall_time]
repetitions: 1
modes: [cold]
"""


def main() -> None:
    work = Path(tempfile.mkdtemp(prefix="velocia-golden-"))
    try:
        velocia = [sys.executable, "-m", "velocia.cli.app"]
        subprocess.run([*velocia, "datagen", "--suite", "tpch", "--sf", "0.01"], cwd=work,
                       check=True)  # fmt: skip
        (work / "exp.yaml").write_text(EXPERIMENT)
        subprocess.run([*velocia, "run", "exp.yaml"], cwd=work, check=True)
        OUT.mkdir(parents=True, exist_ok=True)
        for engine, cell in (("vanilla", "c001"), ("comet", "c002")):
            run = next((work / "velocia-results" / "runs").glob(f"*-{cell}-r1"))
            (log,) = log_files(run / "eventlog")
            dest = OUT / f"{engine}.zstd"
            dest.write_bytes(zstandard.ZstdCompressor(level=19).compress(log.read_bytes()))
            summary = {f"{k[0]}:{k[1]}": u.as_dict() for k, u in summarize(dest).units.items()}
            (OUT / f"{engine}.summary.json").write_text(
                json.dumps(summary, indent=2, sort_keys=True) + "\n"
            )
            print(f"{dest.name}: {dest.stat().st_size:,} bytes, {len(summary)} units")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
