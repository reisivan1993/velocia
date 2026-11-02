# Experiments

An experiment is a YAML file. The full schema is `schemas/experiment.schema.json`; every field is
checked before anything runs, and errors name the field (for example `engines[1].cluster`).

```yaml
name: tpch-aqe                     # results are stored under <name>-<timestamp>
workload:
  kind: sql                        # sql or job
  suite: tpch                      # tpch (22 queries), tera (TeraSort) or text
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

Every sweep point is its own dataset; `velocia run` generates the missing ones first. Units:
`join` (fact to dim, sort-merge), `aggregate` (group by key), `exploding_join` (many-to-many
self-join, fan-out = rows / keys) and `sort` (full sort). Synth fields: `seed`, `rows`, `keys`,
`key_dist` (`uniform`, `zipf`, `hotkey`), `zipf_s`, `hot_fraction`, `payload_columns`,
`null_fraction`, `string_length`, `files`; any of them can be swept. Spark settings are swept
with a list in `spark_conf`. `examples/synth/` has ready experiments: skewed join, hot key,
skewed aggregate, exploding join, spill and shuffle partitions.

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
