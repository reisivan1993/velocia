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
| `velocia engines list\|build <name>\|check <name> (--spark <version> \| --cluster <name> --experiment <file>)` | Engine images and compatibility; `--cluster` runs the smoke test on a named (for example external) cluster defined in the experiment file, with `spark-vanilla` as the local reference; `velocia run` refuses an external cluster until this passed for the same engine definition and cluster configuration |
| `velocia workloads list\|show <suite>` | Available workloads and their units |
| `velocia measures list\|check <file>` | Built-in and custom measures; `check` validates a formula against the input catalog |
| `velocia hardware probe [--cluster NAME]` | Run the hardware probes and print what the manifest will record |
| `velocia ui [--port N] [--host LOOPBACK]` | Read-only viewer on localhost (default port 8765): experiments, reports, compare, history; refuses non-loopback addresses; the results directory comes from `velocia.yaml` |
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
| `cluster` / `clusters` | object / map | One cluster, or named clusters. Each: `runner`, `executors`, `executor_cores`, `executor_memory`, `driver_memory`, `hardware_profile?`, runner-specific fields (`k8s`: `node_selector`, `tolerations`, `resources`; `external`: `master`, `deploy_mode` (`client` only so far), `data_uri`, `eventlog_uri`, `submit_args`, `rest_url?`, `spark_home?` (submit with a Spark install on this machine instead of the engine image in Docker), `docker_network` (default `host`; on another network the driver's container name is its `spark.driver.host`), `docker_args` (extra `docker run` arguments)) |
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
| `velocia.measures` | An object with `name`, `unit`, `direction` (`lower`\|`higher`) and `compute(inputs) → float \| None`; `inputs` is a dict of the formula input names (`unit.*`, `cluster.*`, `hw.*`). Pure and offline: `compute` runs with sockets blocked, and a plugin that uses the network or raises fails the run. Manifests record `source: plugin:<module>:<attr>` and the SHA-256 of the plugin module's source. A name that clashes with a built-in or file measure is rejected |
| `velocia.hardware` | `Probe.detect(node) → dict` |

## 6. Output formats

**`results/<experiment_id>.parquet`** (long format, one row per measure): `experiment_id, cell_key, engine, cluster, hardware_manifest_hash, config_hash, mode, unit_id, measure, measure_unit, direction, n, median, p95, cv, cv_flag, ratio, ci_low, ci_high, correctness (pass|fail|not_checked), timeouts`.

**`cell_key` and `config_hash`** (16 hex characters each): the first 16 hex digits of the SHA-256 of canonical JSON (sorted keys, version `v: 1`). `config_hash` covers the workload (without `units`) and one resolved configuration (`spark_conf` and `sweep` values as the strings Apache Spark receives, so `true` and `"true"` are equal); it is the same for every engine and cluster, which is what pairs cells. `cell_key` covers engine, cluster name and settings, `config_hash` and mode.

**`datasets/<dataset_id>.json`** (dataset manifest; a copy sits next to the data as `_dataset.json`): `{format: "velocia-dataset", version: 1, dataset_id, suite, label, generator, generator_version, generator_params, seed, scale, data_format, compression, row_group_size, location, tables: [{name, rows, checksum, files: [{path, bytes}]}], files, bytes, checksum, created_at, velocia_version}`. `dataset_id` is `<suite>-sf<scale>-<data_format>`. Checksums are over the data content (rows rendered with column names), so the same data has the same checksum in any format or compression. Schema: `schemas/dataset-manifest.schema.json`.

**`runs/<run_id>/timings.json`** (driver output, version 1): `{format: "velocia-timings", version, run_id, mode, started_at, finished_at, spark: {version, master, app_id, app_name, default_parallelism, executors, executor_cores, plugins, python}, units: [{unit_id, rep, tag, discarded, status (ok|failed|timeout), wall_ms, started_at, error}]}`. Rep 0 is the discarded warm-up of warm mode. Rewritten after every unit.

**`runs/<run_id>/checks.json`** (correctness pass, version 1): `{format: "velocia-checks", version, run_id, float_round_decimals, spark, units: [{unit_id, tag, rows, checksum, status, error}]}`.

**`runs/<run_id>/manifest.json`** (version 1): `{format: "velocia-run-manifest", version, run_id, experiment_id, cell_key, config_hash, task (time|check), mode, mode_label ("cold (cache not dropped)" where the page cache was not dropped), reps, warmup, status, exit_code, started_at, ended_at, family, engine: {name, image, image_id, repo_digests, architecture}, spark, spark_conf (redacted), cluster (redacted), hardware, hardware_manifest_hash, dataset_id, dataset_checksum, units, driver_sha256, velocia_version, velocia_git_sha, quiet_check, host_slept_s, measures: [{name, unit, direction, source, formula, sha256}], label}`.

**Unit tags:** `velocia:<run_id>:<unit_id>:<rep>`; rep 0 is the warm-up, `-1` the correctness pass, and unit `_setup` tags table registration before the first unit, so every Spark job carries a tag.

**`_total` unit:** results include a `_total` row per cell and measure: the sum over all units of one repetition, only for repetitions where every unit finished. Its correctness is `fail` if any unit failed, `not_checked` if any was not checked, else `pass`. It is the headline comparison.

**Report JSON:** `{format: "velocia-report", version: 1, experiment, dataset, label, hardware_manifests, cross_hardware, cells, measures, results: [...], measure_definitions, plan_markers: {cell_key: {operators, accelerated, transitions}}, flags: [...]}`. `reports/<experiment_id>/report.json` and `report.md` are written at the end of every run. The experiment spec in the catalog and reports is stored redacted.

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
runs/<run_id>/checks.json         # correctness pass runs
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
  "unit_tag_format": "velocia:<run_id>:<unit_id>:<rep>",
  "sql_stripped": false,
  "label": "Data derived from TPC-H; not an official TPC result."
}
```

`pairing.parquet`: `unit_id, config_hash, mode, engine, cluster, hardware_manifest_hash, run_id, rep, wall_ms, is_reference`. One row per engine per (unit, config, mode), pointing at the median repetition (successful, non-discarded timed repetitions; with an even count the lower middle one, so the row is always a real execution), so a consumer can find the matching reference and accelerated executions inside the event logs by their job-description tags.

The bundle format is documented in the repo with a JSON Schema (`schemas/bundle.schema.json`, for `bundle.json`) and a validator (`velocia bundle --validate FILE`), which checks `bundle.json`, the required files, run manifests, pairing columns, one row per engine per (unit, config, mode), and that every pairing row points at a run in the bundle. A bundle is exported only for a finished experiment. With `--strip-sql`, event logs keep plan node names, metrics and Velocia tags, but SQL text, plan strings (literals, paths), plan metadata, stage details and call sites are blanked.
