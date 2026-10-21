# Velocia for Apache Spark

Velocia is a dynamometer for Apache Spark engines. It runs the same workload on several engines
(vanilla Apache Spark, Apache DataFusion Comet, a vendor's hardware accelerator, ...) and
configurations, checks that every engine returns the same results, and reports each difference
as a ratio of medians with a 95% confidence interval.

What it gives you:

- **Honest numbers.** Every speedup is a ratio of medians over repeated runs with a bootstrap
  confidence interval. A result whose correctness check fails is shown as FAILED, never as a
  speedup.
- **Reproducibility.** Every Spark application writes a manifest (image digests, Spark
  configuration, hardware, dataset checksum, driver hash), and `velocia compare` checks that a
  rerun reproduces a result.
- **Any unit of measure.** Wall time, CPU time, cost, node-hours, accelerator-hours, or your own
  unit as a formula in YAML.
- **No services, no telemetry.** Velocia starts only short-lived Spark containers for each run
  and removes them afterwards. Everything lands in a local results directory.

Workloads in this version: 22 queries derived from TPC-H and a DataFrame TeraSort. Results on
TPC-derived data are labeled as such and are never official TPC results.

Start with the [quickstart](quickstart.md).
