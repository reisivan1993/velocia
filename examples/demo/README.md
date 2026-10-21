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

The run prints one line per Spark application and ends with a summary such as:

```
spark-comet vs spark-vanilla (cold (cache not dropped)): speedup 1.40x [95% CI 1.37-1.42]; ...; correctness pass
```

Then:

```bash
uv run velocia report <experiment_id>                 # Markdown, per query
uv run velocia report <experiment_id> --format json   # everything, machine-readable
```

## What you get

Everything lands in `./velocia-results/` (ignored by git):

| Path | What |
| --- | --- |
| `reports/<experiment_id>/report.md`, `report.json` | The report |
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

## Not in this demo yet

Event-log measures (`task_cpu_time` etc.), the HTML report, Kubernetes and external clusters.
