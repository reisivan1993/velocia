# 02 — Architecture

## Components

```
 velocia CLI
   ├── datagen ──────────► datasets/ (Parquet, manifest; TPC, tera, text, graph, synth)
   ├── workload plugins: sql (TPC-H/DS, custom) | job (TeraSort, PageRank, …) | synth sweeps
   ├── experiment expander (YAML → cells)
   ├── runner plugin: local (Docker) | k8s (Spark Operator) | external (existing cluster, v0.2) | cloud (v0.3)
   │      └── engine family plugin: spark (others after v1.0)
   │             └── engine plugin: image + conf (vanilla, gluten, comet, vendor accelerators)
   │                    └── driver program (shipped at submit time):
   │                          runs each unit (query or job step), tags it, times it,
   │                          writes timings.json; validators run in an untimed pass
   ├── hardware probes + hardware profiles → manifest
   ├── collector: event logs + timings + manifest → results/runs/<run_id>/
   ├── telemetry reader per family (spark: streaming event-log reader, finished logs only)
   ├── measures: built-in + custom (YAML formula or Python plugin; pure, offline)
   ├── statistics: medians, p95, CV, bootstrap CIs per measure, correctness
   ├── report: CLI · Markdown · JSON · static HTML
   ├── bundle export (v0.2): documented format for analysis tools
   └── velocia ui (v0.2): localhost viewer over the results directory
```

## Deployment

| Topology | Contents |
| --- | --- |
| Laptop | `pip install velocia`; `local` runner on Docker; results in `./velocia-results/` |
| Kubernetes | Same CLI from a workstation or CI; `k8s` runner submits `SparkApplication`s via the Spark Operator on kind, k3s or a managed cluster |
| Existing / vendor cluster (v0.2) | Same CLI; `external` runner calls `spark-submit` against the cluster's master URL; data and event logs go through shared storage |
| Cloud cluster (v0.3) | One managed provider via a runner plugin; hard budget cap; teardown always runs |

## Data flow

1. `velocia datagen` writes a dataset and its manifest.
2. `velocia run exp.yaml` expands cells and runs them one at a time: quiet-cluster check → submit → driver program executes units (SQL into Spark's `noop` sink; jobs into their declared sink) → correctness pass (checksums or job validators) → collect event log, timings and manifest.
3. The summary reader parses each run's event log for per-unit breakdowns.
4. Measures are computed per unit and repetition; statistics per cell, unit and measure; results land in the catalog and Parquet.
5. Reports are generated; optionally a bundle is exported.

## Tech stack

| Area | Choice |
| --- | --- |
| Language | Python 3.11+ (CLI, orchestration, statistics, reports); PySpark driver program inside engine images; Scala `velocia-jobs` jar for RDD job workloads (built per Spark/Scala version) |
| CLI | Typer |
| Data generation | DuckDB `tpch` / `tpcds` extensions (SF1–SF10); official kits for larger SF (user-installed); seeded Spark generators for tera, text, graph, KMeans and synth data |
| Measure formulas | Own evaluator over Python's `ast` with an allow-list; no `eval` |
| Storage | Parquet via pyarrow; DuckDB catalog file |
| Statistics | NumPy / SciPy; bootstrap implemented in-house for transparency |
| Event-log reading | orjson + zstandard, streaming |
| Reports | Jinja2 → self-contained HTML with inlined charting library; Markdown; JSON |
| Viewer (v0.2) | FastAPI + small React app, localhost only, reads the results directory |
| Kubernetes | Spark Operator `SparkApplication`; kind in CI |
| Packaging | PyPI; engine Dockerfiles in repo |

## Decisions

| # | Decision | Rationale | Rejected alternative |
| --- | --- | --- | --- |
| ADR-1 | Standalone project, own repo | Benchmarking must be usable and trustworthy without any analysis product | Shared monorepo with an analyzer |
| ADR-2 | Results stored as files: Parquet + DuckDB catalog | Zero infrastructure; portable; archivable | Postgres or a server |
| ADR-3 | Units run by a Velocia driver program shipped at submit time (`--py-files` zip, `--jars`), hashed into the manifest; engine images hold only the engine | Works with any engine plugin and with clusters whose images Velocia can't change (`external`); full control over tagging and timing | Driver baked into engine images (breaks `external`); Spark Connect (engine plugin compatibility uncertain); spark-sql CLI (weak timing control) |
| ADR-4 | Timed runs use the `noop` sink | Measures execution without collecting results to the driver | `collect()` (driver becomes the bottleneck) |
| ADR-5 | Correctness in a separate untimed pass per cell | Checksum aggregation would change the timed plan | Checksum inside timed runs |
| ADR-6 | Integration only through a versioned bundle export | Any analysis tool can consume it; no coupling | Pushing to a specific tool's API |
| ADR-7 | Minimal event-log reader of its own | Only finished logs of its own runs; avoids depending on another project | Depending on an analyzer's parser |
| ADR-8 | Apache-2.0 | Matches Spark, Gluten, Comet; patent grant | MIT; AGPL |
| ADR-9 | "Unit of work" (query or job step) replaces "query" everywhere | One pipeline for SQL and general jobs | Separate job harness |
| ADR-10 | Engine family layer above engines; only `spark` until v1.0 | Lets other engines plug in later without reshaping results or bundles | Spark-only interfaces; building several families now |
| ADR-11 | Measures are pure offline functions declared in YAML or plugins | Supports vendor-defined units while keeping the independence invariant and reproducibility | Calling vendor tools or APIs to compute units |
| ADR-12 | RDD jobs in Scala, DataFrame jobs in PySpark | Avoids measuring Python worker overhead instead of the engine | All jobs in PySpark |
| ADR-13 | Cross-hardware comparison only with explicit `allow_cross_hardware` and a normalized measure | The user override CLAUDE.md allows, made visible and hard to misuse | Forbidding it (blocks accelerator evaluation); allowing it silently |
