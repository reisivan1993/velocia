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
