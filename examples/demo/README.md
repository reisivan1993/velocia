# Local demo: vanilla Apache Spark vs Apache DataFusion Comet

Runs the 22 TPC-H-derived queries at scale factor 1 on vanilla Apache Spark 3.5.9 and on
Apache Spark 3.5.9 with Comet 1.1.0, each in its own Docker container on this machine, and
reports the speedup per query and in total as a ratio of medians with a 95% confidence
interval, after checking every query's result against vanilla Spark.

Results are derived from TPC-H and are not official TPC results.

## Needs

- Docker running (Docker Desktop on macOS: 8 GB of memory is enough).
- [uv](https://docs.astral.sh/uv/).
- About 2 GB of disk for images and 250 MB for the data.

## Steps

From the repository root:

```bash
uv sync                                      # install Velocia and its dependencies
uv run velocia engines build spark-vanilla   # official Apache Spark 3.5.9 image, pinned by digest
uv run velocia engines build spark-comet     # same base + the Comet jar from Maven Central
uv run velocia datagen --suite tpch --sf 1   # ~10 s; one-time download of DuckDB's tpch extension
uv run velocia run examples/demo/tpch-vanilla-vs-comet.yaml --dry-run   # what will run, rough time
uv run velocia run examples/demo/tpch-vanilla-vs-comet.yaml             # ~4-8 min
```

The run prints one line per Spark application and ends with a summary. Two runs on an
Apple M4 Pro (Docker Desktop: 14 CPUs, 8 GB) on 2026-10-21 gave:

```
spark-comet vs spark-vanilla (cold (cache not dropped)): speedup 2.19x [95% CI 2.14-2.22]; total wall_time median 8.861 s vs 19.4 s; correctness pass
spark-comet vs spark-vanilla (cold (cache not dropped)): speedup 2.21x [95% CI 2.10-2.30]; total wall_time median 8.799 s vs 19.5 s; correctness pass
```

Your numbers will differ with your hardware. On macOS, `velocia run` keeps the Mac from idle
sleep while it runs; if the host sleeps anyway (lid closed), the run is flagged as unreliable.

Then open the HTML report it names (`velocia-results/reports/<experiment_id>/report.html`):
a headline tile per engine, a chart of every query's speedup with its confidence interval, and
the tables behind them. It is one self-contained file; it works offline and can be emailed.

```bash
open velocia-results/reports/<experiment_id>/report.html   # macOS
uv run velocia report <experiment_id>                 # Markdown in the terminal
uv run velocia report <experiment_id> --format json   # everything, machine-readable
```

## What you get

Everything lands in `./velocia-results/` (ignored by git):

| Path | What |
| --- | --- |
| `reports/<experiment_id>/report.html`, `report.md`, `report.json` | The report |
| `results/<experiment_id>.parquet` | One row per engine x query x measure: median, p95, CV, ratio, CI, correctness |
| `runs/<run_id>/manifest.json` | Per Spark application: image digests, Spark conf, hardware, driver hash, dataset checksum |
| `runs/<run_id>/timings.json`, `checks.json` | Per-query timings and correctness checksums |
| `runs/<run_id>/eventlog/` | Spark event logs, secrets redacted |
| `catalog.duckdb` | Everything above, queryable with DuckDB |

## Reading the numbers

- **Speedup** = median time of vanilla / median time of Comet over 5 repetitions; > 1 means
  Comet is faster. The bracket is a 95% bootstrap confidence interval: if it is wide or
  crosses 1, the difference is not clear.
- **cold (cache not dropped)**: each repetition starts a fresh Spark application, but macOS
  keeps the data files in its page cache, so reads are warm. On Linux the cache is dropped.
- **Correctness**: an untimed pass computes a row count and checksum of every query result;
  Comet's must equal vanilla's, or the query shows as FAILED instead of a speedup.
- **Flags** list noisy queries (CV > 10%) and a busy machine (quiet check). Close other heavy
  applications for steadier numbers.

## Change it

Edit `tpch-vanilla-vs-comet.yaml`: `repetitions`, `modes: [cold, warm]`, `units: [q1, q6]`,
`spark_conf` (a list of values expands into one cell per value), `executor_cores`,
`executor_memory`. Larger scale factors need more Docker memory.

`velocia run ... --resume` continues an interrupted experiment from the first unfinished cell.

- **task_cpu_time** comes from Spark's event logs. It counts Spark task threads only; Comet
  does much of its work on native threads that Spark doesn't see, so Comet's CPU time is
  understated and the report says so.
- **Plan markers** show how many SQL plan operators ran on Comet and how often the plan
  switches between columnar and row format (a fallback indicator).

## Not in this demo yet

Kubernetes and external clusters, TeraSort and other job workloads, TPC-DS queries.
Custom measures work: put a formula in `measures/<name>.yaml` (see `velocia measures check`).
