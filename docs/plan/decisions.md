# Implementation decisions log

Decisions made while executing the plan. Spec-level decisions (D-1 … D-13) are in `00-overview.md`.

## 2026-10-21 — T05: accelerated engine for v0.1 is Apache DataFusion Comet (resolves D-7)

- **Chosen:** Comet 1.1.0 (released 2026-10-03) on Apache Spark 3.5.9, Java 17, Scala 2.12. Spark 3.5.9 + Java 17 is a supported combination in Comet's published compatibility table.
- **Image:** official `spark:3.5.9-scala2.12-java17-python3-ubuntu` pinned by digest, plus `comet-spark-spark3.5_2.12-1.1.0.jar` from Maven Central, verified by SHA-256 (SHA-1 matched Maven's published checksum). The jar bundles native libraries for linux/amd64 and linux/aarch64.
- **Required Spark conf:** `spark.plugins=org.apache.spark.CometPlugin`, `spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager`, `spark.memory.offHeap.enabled=true`, `spark.memory.offHeap.size` (2g default, override per experiment), `spark.comet.explain.fallback.enabled=true`. The jar sits in `/opt/spark/jars`, which covers the driver and executor classpath requirement.
- **Smoke result** (TPC-H-derived SF0.1, `local[2]`, Docker on arm64): Q1, Q3, Q6 give identical row counts and identical order-independent checksums (`sum(xxhash64(...))`, numeric columns rounded to 4 decimals) on vanilla and Comet; Comet operators present in every executed plan (10, 19 and 7 occurrences). Timings were not measured rigorously and are not a result.
- **Time used:** well inside the 2-day time-box; Gluten was not needed for v0.1 and moves to T27 (v0.2).
- **Found on the way:** engine images ship Python 3.10, so the driver program targets Python 3.10 (design §5.1).

## 2026-10-21 — T08: TPC-H-derived queries; DuckDB `tpcds` at SF10 fits a laptop (spike)

- **Query source:** the 22 TPC-H-derived queries are generated from DuckDB's `tpch_queries()` (default substitution parameters, dates precomputed) by `python -m velocia.workloads.sql.generate`, and committed as `velocia/workloads/sql/tpch/q<N>.sql`. Only change: trailing semicolon removed. Nothing from the TPC kits is vendored; a test keeps the files equal to the generator output.
- **Checked:** all 22 queries give the same rows on Apache Spark 3.5.9 (vanilla image, `local[4]`) and on DuckDB over the same SF0.1 Parquet files.
- **Spike — `velocia datagen --suite tpcds --sf 10`** (DuckDB 1.5.6, macOS arm64, 14 cores, 48 GB): 2 min, 24 tables, 2.5 GB Parquet zstd. Peak RSS 11.3 GB with DuckDB's default memory limit (80% of RAM); 6.5 GB with `memory_limit = '4GB'` (dsdgen allocates outside DuckDB's buffer manager). Same checksum in both runs.
- **Decision:** no Spark-side TPC-DS generator for v0.1. Laptop SF10 needs about 7 GB of free memory; document that in the quickstart (T26). The cluster-side generator stays the fallback for SF100 and up.

## 2026-10-21 — T09-T19: decisions taken on the way to a local end-to-end demo

The user asked to keep going until a local demo runs, so the execution path was built in one stretch: driver (T09), local runner (T10), manifest and built-in probe (T11), correctness pass (T14, in the driver), orchestrator (T15), built-in measures (T16, without formula measures), statistics (T17), store and Markdown/JSON reports (T19).

- **Local runner = one container per application, Spark `local[N]`** with N = executors x executor_cores, `--cpus N`; the driver JVM gets all executor memory (`spark.driver.memory`). A standalone cluster inside Docker adds little for one machine.
- **Raw outputs never touch the results directory:** Spark writes event logs and driver JSON to a temporary directory; the collector streams them through redaction into `runs/<run_id>/`. Checked on a real Apache Spark 3.5 event log: deny-list matches only occur inside property maps, so whole events can be redacted without damaging the log structure. The experiment YAML is stored redacted too (an end-to-end planted-secret test caught it raw in the catalog).
- **Every job is tagged:** table registration (Parquet footer reads) runs as unit `_setup`; the correctness pass uses rep `-1`.
- **Per-unit timeout in the driver** cancels the unit's job group from a watchdog thread; the runner has an app-level timeout as well. Units an application never reached are recorded as failed or timeout, never dropped.
- **Correctness** is compared per (config_hash, mode) against the reference engine's cell; the reference's own units are `pass` when its check ran.
- **`_total` unit** (sum over units per repetition) gives the headline speedup as a ratio of medians with a CI.
- **Bootstrap seed** derived from the experiment ID, plus a stable per-series offset; recorded in the catalog and the report.
- **Quiet check (local):** other Velocia containers running, and host 1-minute load average as % of CPUs vs `quiet_check.max_cpu_pct`. It flags, never blocks. On macOS the load average includes Docker Desktop and is often above 10%; the threshold may need a macOS-specific default.
- **Cold mode on macOS** is labeled "cold (cache not dropped)"; the page cache is dropped only on Linux hosts (privileged container).
- **Docker Desktop memory:** the VM on the dev machine has 8 GB; demo cells use 1 x 4 cores x 3g.
- **Not done yet:** event-log measures (T12/T13: `task_cpu_time` and friends are `null` with a reason), formula and plugin measures, HTML report (T21), `k8s`/`external` runners, the full-size bootstrap coverage test (a reduced one runs on every PR).
- **Host sleep (found by the demo rerun):** the dev Mac went to idle sleep 2 s into a run and kept dozing for 5 hours; vanilla runs crept forward during brief wakes and their timings became noisy (CI 2.63-4.08 instead of 2.14-2.22), with nothing flagging it. Now `velocia run` holds a `caffeinate -i` assertion on macOS, and every application records `host_slept_s` (wall-clock minus monotonic time; the monotonic clock stops during sleep) in its manifest; the run line and report flag it.

## 2026-10-21 — T12/T13: event-log reader and per-unit summaries

- **Reader:** plain and zstd (`.zst`, `.zstd`), rolling directories ordered by part number, a truncated last line tolerated (unfinished log), other codecs rejected with a message. Benchmark: 1 GB in 3.5 CPU s (~17 GB/min/core), 15 MB peak RSS (target: 1 GB/min, < 300 MB).
- **Attribution:** job tag from `spark.job.description` at job start; stages inherit it; task ends go to their stage's unit. Stages count when they complete (skipped stages don't). Only root SQL executions count for SQL time and plan markers; the final plan after adaptive updates is used.
- **Plan markers:** operators exclude wrappers (`AdaptiveSparkPlan`, `InputAdapter`, `WholeStageCodegen*`, `*QueryStage`); transitions match `ColumnarToRow|RowToColumnar` (engine variants included); accelerated = `Comet*`, `Gluten*`, `Velox*`, `Gpu*`, `*Transformer`. Vanilla shows transitions too (vectorized Parquet reads), so transitions alone are not a fallback signal; the accelerated share is the main marker. The "task-time share of stages without accelerated operators" for job units is not done yet.
- **CPU time caveat (found on real logs):** Comet q1 at SF1 reported 0.29 s task CPU vs vanilla 4.30 s, because Spark's `Executor CPU Time` counts the task thread only and DataFusion runs work on native threads. Reports flag this whenever an engine has accelerated operators and `task_cpu_time` is selected. Input rows match across engines, so throughput measures are sound.
- **Golden logs:** `tests/golden/make_eventlogs.py` regenerates `tests/fixtures/eventlogs/{vanilla,comet}.zstd` (SF0.01, q1 and q6) through Velocia itself; checked for personal data (only container identities) and un-ignored in `.gitignore`.

## 2026-10-21 — T21: HTML report

- **SVG drawn by Velocia, no chart library:** one self-contained file, works offline, nothing third-party to vendor or keep patched; a 99-unit x 3-engine report is well under 10 MB (test).
- **Chart form:** per candidate cell and measure, a dot-and-whisker chart on a log x-axis (ratio and 95% CI per unit, reference line at 1x), hover tooltips via SVG `<title>`, the table behind it in a collapsible section. A failed unit shows a red cross and "FAILED", never a ratio. Colors from the dataviz reference palette (single series: slot 1; status colors with icon + label), light and dark tokens.
- **Flags:** high-variation flags are grouped per engine/mode/unit, listing the measures (cost is proportional to wall time, so they always moved together).

## 2026-10-21 — T22/T23: doctor, engine check, planted faults

- **Engine check** runs in `<results_dir>/_checks` (own catalog), q1/q3/q6 at SF0.01 vs `spark-vanilla`. A broken engine (missing plugin class) fails it.
- **Planted faults** use a test-only image (`tests/fixtures/engines/spark-faulty`): vanilla plus a Python `.pth` startup hook (Ubuntu's own `sitecustomize.py` shadows a custom one) that wraps `SparkSession.sql` per `spark.velocia.test.fault`: `wrong` (results cut to one row), `noisy` (random 0-1.5 s sleep), `slow` (a Spark job far beyond the timeout). All three are reported as specified; cross-hardware is a unit test; unsorted job output waits for TeraSort (T18).
- **Found on the way:** the correctness pass had no timeout, so a hanging query would have blocked a run forever; it now uses the same job-group watchdog (`guarded()` in the driver). A check that times out is `not_checked`, not `fail`.
- **Cross-hardware rule enforced** before any application starts, comparing `hardware_manifest_hash` per used cluster (hardware profile accelerators count; prices don't).
