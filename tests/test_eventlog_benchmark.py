"""T12 acceptance: a 1 GB event log reads at >= 1 GB/min on one core with < 300 MB RSS.

Slow (it writes a 1 GB file), so it is marked ``slow``; run with ``pytest -m slow``. The size
can be lowered with VELOCIA_BENCH_MB for a quick check.
"""

import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

import pytest

from velocia.families.spark.eventlog.reader import iter_events

GOLDEN = Path(__file__).parent / "fixtures" / "eventlogs" / "comet.zstd"
SIZE_MB = int(os.environ.get("VELOCIA_BENCH_MB", "1024"))

SCRIPT = """
import sys, time
from pathlib import Path
from velocia.families.spark.eventlog.summary import summarize
start = time.process_time()
app = summarize(Path(sys.argv[1]))
print(time.process_time() - start, sum(u.tasks for u in app.units.values()))
"""


@pytest.mark.slow
def test_reader_throughput_and_memory(tmp_path: Path) -> None:
    events = list(iter_events(GOLDEN))
    head = [e for e in events if e["Event"] != "SparkListenerTaskEnd"]
    tasks = [json.dumps(e) + "\n" for e in events if e["Event"] == "SparkListenerTaskEnd"]
    log = tmp_path / "big"
    with log.open("w") as f:
        for e in head[:-1]:
            f.write(json.dumps(e) + "\n")
        written = 0
        while written < SIZE_MB * 1024 * 1024:
            for line in tasks:
                f.write(line)
                written += len(line)
        f.write(json.dumps(head[-1]) + "\n")
    size = log.stat().st_size

    before = time.time()
    out = subprocess.run([sys.executable, "-c", SCRIPT, str(log)], capture_output=True, text=True,
                         check=True)  # fmt: skip
    cpu_s, tasks_seen = out.stdout.split()
    rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    rss_mb = rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024  # bytes vs KiB
    gb_per_min = size / 1024**3 / (float(cpu_s) / 60)
    wall_s = time.time() - before
    print(
        f"{size / 1e6:.0f} MB in {float(cpu_s):.1f} CPU s ({wall_s:.1f} s wall): "
        f"{gb_per_min:.2f} GB/min/core, peak RSS {rss_mb:.0f} MB, {tasks_seen} tasks"
    )
    assert int(tasks_seen) > 0
    assert gb_per_min >= 1.0
    assert rss_mb < 300
