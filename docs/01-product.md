# 01 — Product

## Problem

Teams choosing an Apache Spark engine (vanilla, Gluten/Velox, Comet, a software or hardware vendor accelerator), a config, a file format or a cluster shape, for SQL queries and general Spark jobs alike, rely on benchmarks that are hard to trust:

- One run per query, no variance, no confidence intervals.
- No check that the faster engine returned the same answer.
- Hardware, versions and configs not recorded, so results cannot be reproduced.
- Each comparison is a one-off script.
- Speedups are quoted in one unit (usually wall time) when the decision depends on another (cost, node-hours, a vendor's own compute unit).
- Benchmarks use uniform data and miss the skew and edge cases that break real jobs.

## Users

| Persona | Needs |
| --- | --- |
| Platform / performance architect | Fair engine and config comparisons before migrating |
| Data engineer tuning jobs | Measure the effect of a config or format change on a known workload |
| Engine or accelerator developer | Repeatable regression and comparison runs; edge-case sweeps; their own unit of measure next to standard ones |
| Analysis-tool builders | Paired runs (reference vs accelerated) to calibrate projection models, via the bundle export |

## Positioning

| Existing option | What it does well | Gap Velocia fills |
| --- | --- | --- |
| databricks/spark-sql-perf | Generates TPC-DS data and runs queries | No statistics, correctness checks, engine matrix or reproducible manifests |
| NVIDIA NDS | TPC-DS-derived runs for Spark RAPIDS | GPU-focused; same methodological gaps |
| HiBench-style suites | General jobs (TeraSort, WordCount, PageRank, KMeans) | No CIs, no correctness against a reference engine, no skew or edge-case sweeps |
| Engine projects' own benchmark scripts | Show their engine's speedups | Written by the vendor of the engine being measured; not a neutral harness |
| Ad-hoc notebooks | Fast to start | Not reproducible, not comparable over time |

**Differentiators, in priority order:**
1. **Methodology you can defend:** repetitions, cold and warm modes, medians with bootstrap CIs, variance flags.
2. **Correctness on every result:** checksums against a reference engine.
3. **Reproducibility:** one YAML reproduces an experiment; every run has a full manifest.
4. **Engine-neutral and extensible:** vanilla Spark, Gluten/Velox and Comet out of the box; any Spark engine or accelerator through a plugin; other engine families later through the same interfaces.
5. **Any unit of measure:** time, CPU, cost, node-hours, energy, or a custom unit defined in YAML, all with the same statistics.
6. **SQL, jobs and edge cases:** TPC-derived SQL, classic Spark jobs, and seeded skew and stress sweeps.
7. **Standalone and portable:** `pip install`, no services, results directory you can archive or share; open bundle format.

## Goals (v1.0)

- From `pip install` to a first engine comparison with CIs on TPC-H SF1 in under 30 minutes on a laptop with Docker.
- Any experiment reproducible from its YAML and dataset manifest.
- No result shown without its correctness status.

## Non-goals

| Non-goal | Why |
| --- | --- |
| Official TPC results | TPC fair-use rules |
| Concurrency and throughput benchmarks | v1 measures one unit of work at a time |
| Streaming workloads | v1 covers batch SQL and batch jobs |
| Non-Spark engine families before v1.0 | Interfaces are designed for them now; the first one ships after v1.0 |
| Deep per-operator analysis of why an engine is faster | Belongs to an analysis tool; Velocia exports bundles for that |
| Running on production clusters with real workloads | Controlled experiments only |
| A hosted service | CLI and local files |

## Product principles

1. **Honest numbers.** CIs, variance flags and correctness status everywhere.
2. **Reproducible by default.** If it isn't in the manifest, it didn't happen.
3. **Zero infrastructure.** No server, no database service.
4. **Neutral.** No engine is favored; engine definitions are plugins.
5. **Open outputs.** Results and bundles in documented, versioned formats.

## Milestones

| Release | Scope | Acceptance (summary; details in 03) |
| --- | --- | --- |
| **v0.1 — harness** | Datagen, experiment YAML, `local` and `k8s` runners, vanilla + one accelerated engine, TPC-H-derived SQL + TeraSort job, statistics, checksums and job validators, measures (built-in + custom YAML), event-log summaries, CLI + Markdown/JSON + static HTML report | `pip install` + Docker is enough; one YAML reproduces medians within CIs; checksums enforced; a custom measure reported with CIs |
| **v0.2 — depth** | Second accelerated engine, full job suite, `synth` skew and edge-case sweeps, hardware profiles, `external` runner for existing and vendor clusters, cross-hardware experiments, local viewer (`velocia ui`), benchmark bundle export, experiment comparison over time | Bundle validates against its schema; sweeps produce curves with CIs; cross-hardware runs are labeled; viewer works offline |
| **v0.3 — scale** | Larger scale factors via official kits, cloud-cluster runner (one provider), cost reporting | SF100 run on a short-lived cluster with a hard budget cap |
| **v1.0** | Stable CLI, schemas and formats; complete docs; CI compatibility matrix | No breaking change for one minor release cycle |
| **v1.x** | First non-Spark engine family, running the portable SQL workloads | Same statistics, correctness and bundle format as the `spark` family |

## Risks

| Risk | Mitigation |
| --- | --- |
| Gluten/Comet incompatible with the chosen Spark version | Pick Spark from their published compatibility matrices; time-box engine images |
| Noisy results on laptops and shared clouds | Quiet-cluster check, CV flags, more repetitions, manifest-scoped comparisons |
| Cross-hardware comparisons read as apples-to-oranges | Explicit opt-in, labeled reports, normalized measures (cost, node-hours, custom units) required next to time |
| Vendor Spark builds differ from Apache Spark API behavior | `engines check --cluster` smoke test before any experiment; correctness checks on every result |
| Little control over vendor-hosted clusters (caches, noise, logs) | Label cold mode and declared hardware; flag skipped quiet checks; time-only results when logs can't be fetched |
| Custom measures used to cherry-pick | Every report shows the built-in time and cost measures too; measure definitions are hashed into the manifest |
| "Just another benchmark script" perception | Lead with methodology and correctness; publish example reports |
