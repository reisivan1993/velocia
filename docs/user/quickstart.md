# Quickstart

Compare vanilla Apache Spark 3.5.9 with Apache DataFusion Comet on TPC-H-derived data on your
own machine in about 15 minutes, most of it building images.

## Needs

- Docker (Docker Desktop on macOS: 8 GB of memory is enough for scale factor 1).
- Python 3.11+ with [uv](https://docs.astral.sh/uv/) (or pip).

## Steps

```bash
uv sync                                     # from a clone; or: pip install .
uv run velocia doctor                       # checks Docker, disk, images
uv run velocia engines build spark-vanilla  # official Apache Spark image, pinned by digest
uv run velocia engines build spark-comet    # same base + the Comet jar
uv run velocia datagen --suite tpch --sf 1  # TPC-H-derived data, ~250 MB of Parquet
uv run velocia run examples/demo/tpch-vanilla-vs-comet.yaml --dry-run
uv run velocia run examples/demo/tpch-vanilla-vs-comet.yaml
```

The run prints one line per Spark application, then a summary such as

```
spark-comet vs spark-vanilla (cold (cache not dropped)): speedup 2.19x [95% CI 2.14-2.22]; total wall_time median 8.861 s vs 19.4 s; correctness pass
```

and the path of the report. Open `velocia-results/reports/<experiment_id>/report.html` in a
browser: a headline per engine, a chart of every query's ratio with its confidence interval, and
the tables behind it.

## Next

- Read [Methodology](methodology.md) to know what the numbers mean.
- Write your own experiment: [Experiments](experiments.md).
- Add your engine or accelerator: [Engines](engines.md).
