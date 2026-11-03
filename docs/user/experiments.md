# Experiments

An experiment is a YAML file. The full schema is `schemas/experiment.schema.json`; every field is
checked before anything runs, and errors name the field (for example `engines[1].cluster`).

```yaml
name: tpch-aqe                     # results are stored under <name>-<timestamp>
workload:
  kind: sql                        # sql or job
  suite: tpch                      # tpch (22 queries), tpcds (99), tera, text, etl, ...
  units: all                       # or a list: [q1, q6]
  scale_factor: 1                  # tpch; generate first: velocia datagen --suite tpch --sf 1
  format: parquet-zstd             # or parquet-snappy
engines: [spark-vanilla, spark-comet]
reference_engine: spark-vanilla    # ratios are against this engine
cluster: {runner: local, executors: 1, executor_cores: 4, executor_memory: 3g}
spark_conf:
  spark.sql.adaptive.enabled: [true, false]   # a list expands into one cell per value
measures: [wall_time, task_cpu_time, cost_usd]
repetitions: 5                     # 5 or more recommended
modes: [cold, warm]
timeout_per_unit: 30m
```

TeraSort:

```yaml
workload: {kind: job, suite: tera, units: [terasort], rows: 10000000}
```

with data from `velocia datagen --suite tera --rows 10000000` (a seeded Spark job; the same row
count always gives the same data).

Sort and WordCount on generated text (lines of ten words from a skewed vocabulary):

```yaml
workload: {kind: job, suite: text, units: [sort, wordcount], rows: 10000000}
```

with data from `velocia datagen --suite text --rows 10000000` (`rows` counts lines). `sort`
sorts the lines and writes them; its validator checks the order and that the output has the
same rows as the input. `wordcount` writes `(word, count)` pairs; the counts must add up to
the number of words in the input, and the result must match the reference engine.

An ETL pipeline in five steps, each its own timed unit:

```yaml
workload: {kind: job, suite: etl, rows: 10000000}
```

with data from `velocia datagen --suite etl --rows 10000000` (generated events with 5% nulls
and a dimension table). `read` lands the raw events, `clean` drops incomplete rows and
duplicates and normalizes text, `join` adds the dimension, `aggregate` totals per group, and
`write` publishes the result partitioned by group. Each step reads the previous step's output;
every step's output must match the reference engine's.

PageRank and KMeans:

```yaml
workload: {kind: job, suite: graph, rows: 10000000}     # rows = edges; unit pagerank
workload: {kind: job, suite: kmeans, rows: 10000000}    # rows = points; unit kmeans-train
```

with `velocia datagen --suite graph --rows N` (a seeded power-law graph, rows / 10 vertices)
and `velocia datagen --suite kmeans --rows N` (points in 10 dimensions around 8 centers).
`pagerank` runs 10 iterations with the DataFrame API; `kmeans-train` trains MLlib's KMeans
(k = 8, seed 42). Engines add floating-point numbers in different orders, so these two are
checked within a relative tolerance of 1e-6 of the reference engine (PageRank: total rank and
the first 50 vertices' ranks; KMeans: cost and centers), while the exact parts must match (the
vertex set; KMeans' iteration count).

Your own Python job:

```yaml
workload:
  kind: job
  suite: custom
  dataset_id: tpch-sf1-parquet-zstd        # any generated dataset; its tables are passed in
  units: [count, priorities]               # or `all`: the whole job is one unit, "app"
  job: {artifact: jobs/my_job.py, entrypoint: "my_job:run", validator: "my_job:validate"}
```

`artifact` is a `.py` file or a `.zip` of modules, relative to the experiment file. Velocia
calls `run(spark, tables, scratch)` once per repetition, where `tables` maps table names to
paths and `scratch` is a directory removed after the run. Inside, `with velocia.unit("count"):`
tags and times one unit (`import velocia` works inside a run; outside one, the helper does
nothing). A job that doesn't use the helper is one opaque unit named `app`, timed around the
call. The optional validator `validate(spark, tables, scratch)` returns, per unit (or once for
the whole job), `{"rows": ..., "checksum": ..., "problem": None}`, compared with the
reference engine's; without one, correctness is "not checked".

A compiled job (Scala or Java) works too:

```yaml
  job: {artifact: jobs/my-job.jar, entrypoint: com.example.MyJob,
        args: ["${table:lineitem}", "--out", "/tmp/out"]}
```

Velocia submits it with `--class`; `${table:<name>}` in `args` becomes that table's path. A
jar job that sets its job description to `velocia:<run_id>:<unit>:<rep>` (both values are given
to it as the Spark settings `spark.velocia.run_id` and `spark.velocia.rep`) is timed per unit,
from its first job's start to its last job's end in the event log; otherwise the whole
application is one unit `app`. Jar jobs run in cold mode only (each repetition is one
application) and are not checked for correctness.

Jobs use the DataFrame API, so engines that replace Spark's operators (Comet, Gluten, hardware
accelerators) are measured on them.

## Edge cases: synth sweeps

The `synth` suite generates seeded data and sweeps one property of it, so you see how each
engine copes as the data gets harder:

```yaml
workload:
  kind: sql
  suite: synth
  units: [join]
  synth: {rows: 5000000, keys: 100000, key_dist: zipf, files: 8}
  sweep: {zipf_s: [0, 0.5, 1.0, 1.5]}
spark_conf:
  spark.sql.adaptive.skewJoin.enabled: [true, false]
```

Every sweep point of a data parameter is its own dataset; `velocia run` generates the missing
ones first. Units: `join` (fact to dim, forced sort-merge), `join_auto` (Spark picks the
strategy), `aggregate`, `exploding_join` (many-to-many, fan-out = rows / keys), `sort`,
`scan_filter`, `wide_projection`, `nulls`, `decimals` (`decimal(38, 10)`), `strings`
(multi-byte UTF-8, LIKE, regexp), `nested` (explode an array), `window` and `udf` (a Python
UDF in the middle of an aggregation).

Synth fields, any of which can be swept: `seed`, `rows`, `keys`, `key_dist` (`uniform`,
`zipf`, `hotkey`), `zipf_s`, `hot_fraction`, `payload_columns`, `null_fraction`,
`string_length`, `files`, `nest_fanout`, and two query parameters that change the SQL but not
the data (sweeping them reuses one dataset): `selectivity` and `window_frame`. Spark settings
are swept with a list in `spark_conf`. `examples/synth/` has one experiment per edge case in
the spec: skewed join, hot key, skewed aggregate, exploding join, spill, shuffle partitions,
small files, wide rows, nested types, nulls, decimals, strings, selectivity, join strategy,
window functions and UDF fallback.

`velocia datagen --suite synth --spec synth.yaml` generates one dataset from a spec file with
the same fields.

## Clusters

`runner: local` runs each Spark application in its own Docker container on this machine, in
Spark's local mode with `executors x executor_cores` cores and all executor memory for the driver.
`runner: external` submits to an existing cluster, for example a vendor's accelerator servers:
see [External clusters](external.md). Kubernetes (`k8s`) is planned; its fields are already in
the schema.

Several clusters, with engines pinned to them:

```yaml
clusters:
  cpu: {runner: local, executors: 1, executor_cores: 4, executor_memory: 3g, hardware_profile: cpu-box}
  apu: {runner: local, executors: 1, executor_cores: 4, executor_memory: 3g, hardware_profile: apu-box}
engines:
  - {name: spark-vanilla, cluster: cpu}
  - {name: spark-apu, cluster: apu}
comparison: {allow_cross_hardware: true}   # needed when the hardware differs
```

## Commands

- `velocia run exp.yaml --dry-run`: cells, run count and a rough duration.
- `velocia run exp.yaml`: run it; `--resume` continues an interrupted experiment from the first
  unfinished cell.
- `velocia report <experiment_id> [--format md|json|html] [--measures a,b]`
- `velocia compare <experiment_a> <experiment_b>`: does b reproduce a?

## velocia.yaml

Optional, in the working directory:

```yaml
results_dir: ./velocia-results
engines_dir: ./engines            # your engine definitions
measures_dir: ./measures          # your formula measures
hardware_dir: ./hardware          # hardware profiles
pricing: {default_node_hour_usd: 0.40}
quiet_check: {max_cpu_pct: 10, max_other_spark_apps: 0}
correctness: {float_round_decimals: 4}
statistics: {bootstrap_resamples: 10000, cv_flag_threshold: 0.10}
redaction: {key_patterns: ["*vault*"]}   # extra keys to treat as secrets
```
