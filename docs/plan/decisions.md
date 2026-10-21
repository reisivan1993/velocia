# Implementation decisions log

Decisions made while executing the plan. Spec-level decisions (D-1 … D-13) are in `00-overview.md`.

## 2026-10-21 — T05: accelerated engine for v0.1 is Apache DataFusion Comet (resolves D-7)

- **Chosen:** Comet 1.1.0 (released 2026-10-03) on Apache Spark 3.5.9, Java 17, Scala 2.12. Spark 3.5.9 + Java 17 is a supported combination in Comet's published compatibility table.
- **Image:** official `spark:3.5.9-scala2.12-java17-python3-ubuntu` pinned by digest, plus `comet-spark-spark3.5_2.12-1.1.0.jar` from Maven Central, verified by SHA-256 (SHA-1 matched Maven's published checksum). The jar bundles native libraries for linux/amd64 and linux/aarch64.
- **Required Spark conf:** `spark.plugins=org.apache.spark.CometPlugin`, `spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager`, `spark.memory.offHeap.enabled=true`, `spark.memory.offHeap.size` (2g default, override per experiment), `spark.comet.explain.fallback.enabled=true`. The jar sits in `/opt/spark/jars`, which covers the driver and executor classpath requirement.
- **Smoke result** (TPC-H-derived SF0.1, `local[2]`, Docker on arm64): Q1, Q3, Q6 give identical row counts and identical order-independent checksums (`sum(xxhash64(...))`, numeric columns rounded to 4 decimals) on vanilla and Comet; Comet operators present in every executed plan (10, 19 and 7 occurrences). Timings were not measured rigorously and are not a result.
- **Time used:** well inside the 2-day time-box; Gluten was not needed for v0.1 and moves to T27 (v0.2).
- **Found on the way:** engine images ship Python 3.10, so the driver program targets Python 3.10 (design §5.1).
