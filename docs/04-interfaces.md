# 04 — Interfaces

All public interfaces are versioned and change only with a spec update and a changelog entry (`docs/CHANGELOG.md`).

## 1. CLI

| Command | Purpose |
| --- | --- |
| `velocia datagen --suite tpch\|tpcds\|tera\|text\|graph\|kmeans\|synth [suite options] --format F [--iceberg] [--out DIR]` | Generate a dataset and manifest |
| `velocia run <experiment.yaml> [--dry-run] [--results-dir DIR] [--resume]` | Expand and execute; `--resume` continues an interrupted experiment |
| `velocia report <experiment_id> [--format md\|json\|html] [--measures M,...]` | Results with CIs and correctness |
| `velocia compare <exp_a> <exp_b> [--force]` | Compare two experiments on the same workload |
| `velocia bundle <experiment_id> [--out FILE] [--strip-sql]` | Export a benchmark bundle; `--strip-sql` removes SQL text and plan literals from event logs for sharing outside the team |
| `velocia engines list\|build <name>\|check <name> (--spark <version> \| --cluster <name>)` | Engine images and compatibility; `--cluster` runs the smoke test on a named (for example external) cluster |
| `velocia workloads list\|show <suite>` | Available workloads and their units |
| `velocia measures list\|check <file>` | Built-in and custom measures; `check` validates a formula against the input catalog |
| `velocia hardware probe [--cluster NAME]` | Run the hardware probes and print what the manifest will record |
| `velocia ui [--results-dir DIR] [--port]` | Localhost viewer |
| `velocia doctor` | Check Docker or Kubernetes access, Spark Operator, disk space |

## 2. Configuration — `velocia.yaml` (optional)

```yaml
results_dir: ./velocia-results
engines_dir: ./engines              # custom engine definitions
measures_dir: ./measures            # custom measure definitions
hardware_dir: ./hardware            # hardware profiles
pricing: {default_node_hour_usd: 0.40}
quiet_check: {max_cpu_pct: 10, max_other_spark_apps: 0}
correctness: {float_round_decimals: 4}
statistics: {bootstrap_resamples: 10000, cv_flag_threshold: 0.10}
redaction: {key_patterns: ["*mycorp.vault*"]}   # extra globs on top of the built-in deny-list
```

Redaction always applies the built-in deny-list (keys matching `*password*`, `*passwd*`, `*secret*`, `*token*`, `*key*`, `*credential*`, case-insensitive); `key_patterns` only adds to it.

## 3. Experiment schema

| Field | Type | Notes |
| --- | --- | --- |
| `name` | string | Unique per results directory |
| `workload` | object | `kind` (sql, job), `suite` (tpch, tpcds, tera, text, graph, kmeans, etl, synth, custom), `units` (all or list), suite parameters (`scale_factor`, `rows`, `sweep`, …), `format`, `dataset_id?`, `sql_dir?`, `job?` (custom job: artifact, entrypoint, validator) |
| `engines` | list | Engine names, or `{name, cluster}` to pin an engine to a named cluster |
| `reference_engine` | string | Must be in `engines` |
| `cluster` / `clusters` | object / map | One cluster, or named clusters. Each: `runner`, `executors`, `executor_cores`, `executor_memory`, `driver_memory`, `hardware_profile?`, runner-specific fields (`k8s`: `node_selector`, `tolerations`, `resources`; `external`: `master`, `deploy_mode`, `data_uri`, `eventlog_uri`, `submit_args`, `rest_url?`) |
| `spark_conf` | map | Value or list of values; lists expand into cells |
| `measures` | list | Measure names; default `[wall_time, task_cpu_time]` |
| `comparison` | object | `allow_cross_hardware` (bool, default false) |
| `repetitions` | int | ≥ 5 recommended; < 5 allowed with a warning |
| `modes` | list | `cold`, `warm` |
| `timeout_per_unit` | duration | Default 30m |
| `price_table` | map | Optional override |

## 4. Definition files

**Engine** (`engines/<name>.yaml`):

```yaml
name: spark-comet
family: spark
image: ghcr.io/<org>/velocia-engine-comet@sha256:...
spark_versions: ["3.5"]
spark_conf:
  spark.plugins: org.apache.spark.CometPlugin
  spark.comet.enabled: "true"
notes: "Built from engines/comet/Dockerfile"
```

**Measure** (`measures/<name>.yaml`): `name`, `unit`, `direction` (`lower` | `higher`), `description`, `formula`. Formula grammar: numbers, named inputs (`unit.*`, `cluster.*`, `hw.*`), `+ - * /`, parentheses, `min`, `max`, `abs`. Anything else is rejected.

**Hardware profile** (`hardware/<name>.yaml`):

```yaml
name: apu-node
price_per_node_hour_usd: 6.10
watts_per_node: 900
accelerators_per_node: 2         # e.g. two APU cards per server
accelerator_model: "<declared model>"   # used when no probe plugin can detect it
custom:                 # free-form coefficients for custom measures, exposed as hw.<key>
  cu_per_node_hour: 1.0
```

## 5. Plugin interfaces (Python entry points)

| Group | Interface |
| --- | --- |
| `velocia.runners` | `Runner.check()`, `prepare(cluster)`, `submit(cell) → handle`, `wait(handle)`, `collect(handle, dest)`, `teardown()` |
| `velocia.families` | `Family.driver_program`, `submission(cell) → spec`, `tag(run_id, unit_id, rep) → str`, `read_telemetry(dir) → [UnitSummary]`, `checksum(result) → Checksum` |
| `velocia.engines` | `Engine.family`, `image`, `config`, `supports(version) → bool` |
| `velocia.workloads` | `Workload.kind`, `units(dataset) → [Unit]`, `datagen(spec)`, `validator(unit) → Validator` |
| `velocia.measures` | `Measure.name`, `unit`, `direction`, `inputs`, `compute(inputs) → float \| None` (pure, offline) |
| `velocia.hardware` | `Probe.detect(node) → dict` |

## 6. Output formats

**`results/<experiment_id>.parquet`** (long format, one row per measure): `experiment_id, cell_key, engine, cluster, hardware_manifest_hash, config_hash, mode, unit_id, measure, measure_unit, direction, n, median, p95, cv, cv_flag, ratio, ci_low, ci_high, correctness (pass|fail|not_checked), timeouts`.

**`datasets/<dataset_id>.json`** (dataset manifest; a copy sits next to the data as `_dataset.json`): `{format: "velocia-dataset", version: 1, dataset_id, suite, label, generator, generator_version, generator_params, seed, scale, data_format, compression, row_group_size, location, tables: [{name, rows, checksum, files: [{path, bytes}]}], files, bytes, checksum, created_at, velocia_version}`. `dataset_id` is `<suite>-sf<scale>-<data_format>`. Checksums are over the data content (rows rendered with column names), so the same data has the same checksum in any format or compression. Schema: `schemas/dataset-manifest.schema.json`.

**Report JSON:** `{format: "velocia-report", version: 1, experiment, dataset, hardware_manifests, cross_hardware, measures, results: [...], flags: [...]}`.

## 7. Benchmark bundle format v1

A zip file `<experiment_id>.bundle.zip`:

```
bundle.json
experiment.yaml
datasets/<dataset_id>.json
hardware/<profile>.yaml
measures/<measure>.yaml
runs/<run_id>/manifest.json
runs/<run_id>/timings.json
runs/<run_id>/eventlog/...        # redacted Spark event logs
results/results.parquet
pairing.parquet
```

`bundle.json`:
```json
{
  "format": "benchmark-bundle",
  "version": 1,
  "created_at": "2026-12-01T10:00:00Z",
  "producer": {"name": "velocia", "version": "0.2.0"},
  "experiment_id": "exp-...",
  "reference_engine": "spark-vanilla",
  "engines": ["spark-vanilla", "spark-gluten"],
  "hardware_manifest_hashes": ["sha256:..."],
  "cross_hardware": false,
  "unit_tag_format": "velocia:<run_id>:<unit_id>:<rep>"
}
```

`pairing.parquet`: `unit_id, config_hash, mode, engine, cluster, hardware_manifest_hash, run_id, rep, wall_ms, is_reference`. One row per engine per (unit, config, mode), pointing at the median repetition, so a consumer can find the matching reference and accelerated executions inside the event logs by their job-description tags.

The bundle format is documented in the repo with a JSON Schema and a validator (`velocia bundle --validate FILE`).
