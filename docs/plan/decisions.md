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
