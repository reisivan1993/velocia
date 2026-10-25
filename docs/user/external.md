# External clusters

The `external` runner benchmarks on an Apache Spark cluster that Velocia doesn't create or
manage, for example a vendor's accelerator servers that expose the regular Spark API. Velocia
runs `spark-submit` against the cluster's master URL with its driver program; the vendor's
Spark build and images stay as they are.

## A CPU vs accelerator experiment

```yaml
name: tpch-cpu-vs-apu
workload: {kind: sql, suite: tpch, scale_factor: 10}
clusters:
  cpu: {runner: local, executors: 1, executor_cores: 8, executor_memory: 8g,
        hardware_profile: cpu-node}
  apu:
    runner: external
    master: "spark://apu-host:7077"
    executors: 2
    executor_cores: 8
    executor_memory: 32g
    hardware_profile: apu-node
    data_uri: "s3a://bench/velocia/"
    eventlog_uri: "s3a://bench/velocia/eventlog/"
    rest_url: "http://apu-host:8080"
engines:
  - {name: spark-vanilla, cluster: cpu}
  - {name: spark-apu, cluster: apu}
reference_engine: spark-vanilla
comparison: {allow_cross_hardware: true}
measures: [wall_time, node_hours, accelerator_hours, cost_usd]
repetitions: 5
modes: [cold]
```

`spark-apu` is an engine file (see [Engines](engines.md)) with the vendor's image and Spark
configuration. `hardware/apu-node.yaml` declares what Velocia can't probe on someone else's
nodes:

```yaml
name: apu-node
price_per_node_hour_usd: 4.10
accelerators_per_node: 1
accelerator_model: "Speedata APU"
```

Two clusters mean two hardware manifests, so the experiment needs
`comparison.allow_cross_hardware: true` and a normalized measure such as cost or node-hours
next to time. The report carries a cross-hardware banner.

## Cluster fields

| Field | Meaning |
| --- | --- |
| `master` | Spark master URL, e.g. `spark://host:7077` (standalone; tested). `yarn` and `k8s://...` go through the same `spark-submit` but aren't tested yet |
| `deploy_mode` | `client` (the only mode so far): the driver runs on this machine |
| `data_uri` | Storage the executors can read: `s3a://`, `hdfs://`, or `file://` on a shared mount |
| `eventlog_uri` | Where Spark writes event logs (`spark.eventLog.dir`); Velocia copies them back |
| `rest_url` | The master's web UI, for the quiet check (standalone or YARN) |
| `submit_args` | Extra `spark-submit` arguments, e.g. `["--packages", "org.apache.hadoop:hadoop-aws:3.3.4"]` |
| `spark_home` | Use the Spark install at this path on this machine instead of the engine image |
| `docker_network` | Network of the submitting container (default `host`) |
| `docker_args` | Extra `docker run` arguments for the submitting container, e.g. mounts |

Credentials (S3 keys, Kerberos) come from the environment or Spark configuration; secrets in
Spark configuration are redacted from everything Velocia writes.

## Smoke test first

Before the first experiment on a cluster, check the engine there:

```bash
velocia engines check spark-apu --cluster apu --experiment tpch-cpu-vs-apu.yaml
```

It runs TPC-H-derived q1, q3, q6 at scale factor 0.01 and a 100,000-row TeraSort on the
cluster, with `spark-vanilla` locally as the reference, and fails unless every result
matches. `velocia run` refuses the cluster until this has passed for the same engine
definition and cluster configuration; change either (image, Spark conf, master, executors,
storage) and the check runs again. An API gap in a vendor's Spark shows up here as a clear
failure, not as a broken benchmark.

## What happens in a run

1. **Data.** Before the first cell on the cluster the dataset is copied to
   `<data_uri>/<dataset_id>/` and verified there: every file's size and each table's row
   count, read back by Spark. A marker file records the verified copy, so later runs skip the
   copy. A mismatch stops the run.
2. **Runs.** Each repetition is a fresh application (cold mode). The driver runs here, so
   executors must be able to connect back to it: run Velocia on a machine in the cluster's
   network (an edge node), with `docker_network: host` or `spark_home`. If no executor
   registers within 5 minutes the application fails with a message saying so.
3. **Event logs.** Spark writes them to `eventlog_uri`; when the application ends the driver
   copies its log back and Velocia reads it like a local one. If the log can't be fetched, the
   run keeps its driver timings, event-log measures are null and the run carries a note.
4. **Scratch.** Job output (TeraSort) goes to `<data_uri>/_scratch/<run_id>` and is deleted
   after the run.

## Labels you will see

| Label | Why |
| --- | --- |
| `cold (cache not dropped)` | Velocia can't drop the OS page cache on nodes it doesn't control |
| `quiet check skipped` | No `rest_url`, or it wasn't reachable, so other load on the cluster is unknown |
| hardware `source: declared` | Nodes and accelerators come from the hardware profile, not a probe |
