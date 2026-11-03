# Velocia for Apache Spark

Reproducible, statistically honest benchmarks of Apache Spark engines.

Velocia runs the same workload on vanilla Apache Spark and on accelerated engines (Apache
DataFusion Comet, Apache Gluten with Velox, or a vendor's hardware accelerator) and tells you
how much faster each one is, how sure that is, and whether it still gets the right answer.

- **Every speedup comes with a 95% confidence interval**: a ratio of medians over repeated
  runs, never a bare "3.2x".
- **Every result is checked against the reference engine.** A wrong answer is shown as
  FAILED, never as a speedup.
- **Every run writes a manifest**: image digests, Spark configuration, hardware, dataset
  checksum, so a result can be traced and reproduced.
- **Any unit of measure**: time, CPU, shuffle, cost, node-hours, accelerator-hours, or your
  own formula (for example a vendor's "compute unit").

Velocia is a standalone command-line tool. It needs Python 3.11+ and Docker (or an existing
Spark cluster), starts no services of its own, and sends nothing anywhere.

## Try it

```bash
git clone https://github.com/reisivan1993/velocia && cd velocia
uv sync
uv run velocia engines build spark-vanilla
uv run velocia engines build spark-comet
uv run velocia datagen --suite tpch --sf 1
uv run velocia run examples/demo/tpch-vanilla-vs-comet.yaml
```

About 15 minutes on a laptop, most of it building images. On an Apple M4 Pro this prints
something like:

```
spark-comet vs spark-vanilla (cold (cache not dropped)): speedup 2.13x [95% CI 2.04-2.26];
total wall_time median 9.751 s vs 20.7 s; correctness pass
```

and writes an HTML report you can open offline. `uv run velocia ui` browses all results.
See [`examples/demo/`](examples/demo/README.md).

## What it can run

| | |
| --- | --- |
| SQL workloads | 22 TPC-H-derived and 99 TPC-DS-derived queries; your own SQL files |
| Job workloads | TeraSort, Sort, WordCount, PageRank, KMeans (MLlib), a five-step ETL pipeline; your own Python or jar jobs |
| Edge cases | Seeded skewed joins, hot keys, nulls, small files, wide rows, nested types, decimals, strings, windows, UDF fallback, swept as curves with CIs |
| Engines | `spark-vanilla`, `spark-comet`, `spark-gluten` (x86 only), or any image you define |
| Where | Docker on this machine, Kubernetes (Spark Operator), an EKS cluster it creates on AWS with a hard budget cap, or a Spark cluster Velocia doesn't manage (for example a vendor's accelerator servers), with S3, HDFS or shared storage |
| Outputs | HTML, Markdown and JSON reports; Parquet results; a benchmark bundle any analysis tool can read; a local read-only viewer |

Results on TPC data are labeled "derived from TPC-H / TPC-DS". They are not official TPC
results.

## Documentation

- [User guide](docs/user/index.md): quickstart, methodology, experiments, measures, engines,
  external clusters, results, troubleshooting, CLI and file-format reference
  (`uv run --group docs mkdocs serve`)
- [Specification](docs/03-spec.md), [interfaces](docs/04-interfaces.md),
  [design](docs/06-design.md) and the [changelog](docs/CHANGELOG.md)
- [Plan](docs/plan/00-overview.md) and [decisions](docs/plan/decisions.md)
- [Origin](docs/00-origin.md): how the project was specified

## Status

Pre-release (0.1.0.dev0). Milestones v0.1 (local harness) and v0.2 (jobs, edge cases,
external clusters, bundles, viewer) are implemented, and the Kubernetes runner works (Spark
Operator). The AWS runner (v0.3) is built and tested without a real AWS account; its first real
run is next. Not published to PyPI yet: install from a clone.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Apache Spark, Apache
DataFusion Comet and Apache Gluten are trademarks of the Apache Software Foundation; Velocia is
not affiliated with or endorsed by it.
