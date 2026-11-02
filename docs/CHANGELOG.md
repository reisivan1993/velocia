# Spec changelog

## 2026-11-03 — external runner on S3 (T42)

- Nightly test: the external runner against a standalone cluster with S3 storage (`s3a`, SeaweedFS's S3 gateway; MinIO no longer publishes container images), `hadoop-aws` fetched with `--packages`. It found two problems, both fixed: s3a's `copyFromLocalFile` fails on single files ("Cannot get relative path"), so dataset upload and event-log download now stream through Hadoop's `FileUtil.copy`; and the Spark image's user has no writable home, so the external runner sets `spark.jars.ivy=/tmp/ivy2` when submitting from Docker (overridable).

## 2026-11-02 — viewer (T46, T47)

- `velocia ui [--port N] [--host LOOPBACK]`: experiments (with each candidate engine's total ratio and CI), each experiment's report rendered fresh from the catalog (and its JSON), compare of two experiments, and an experiment name's history (total ratio with CI over time). Binds to loopback only; GET only; the catalog is opened read-only and reports are redacted again before display. `--results-dir` (in 04 §1) is dropped: the results directory comes from `velocia.yaml` like every other command.
- Built on the Python standard library's HTTP server with Jinja templates, not FastAPI + React as planned: no extra runtime dependencies, no JavaScript build toolchain, and nothing to ship in the wheel but templates. The pages reuse the HTML report's charts.
- The catalog can be opened read-only.

## 2026-11-02 — Gluten engine (T27)

- Built-in engine `spark-gluten`: Apache Spark 3.5.9 with Apache Gluten 1.7.0 (Velox), from the official binary release (checksum-pinned). Its native libraries are linux/amd64 only.
- Engine definitions gain `platforms` (Docker platforms the image runs on natively; empty = any). `velocia run` and `engines check` refuse an engine whose platforms don't include the local Docker's where Velocia starts the containers (local runner, external runner in Docker), instead of letting native code crash under emulation.
- CI job `gluten` (amd64): engine check and all 22 TPC-H-derived queries at SF1 with correctness against vanilla.

## 2026-11-02 — bring-your-own Python jobs (T32, part)

- `workload: {kind: job, suite: custom, dataset_id, units, job: {artifact, entrypoint, validator?}}` runs a Python job: `artifact` is a `.py` or `.zip` (relative to the experiment file, shipped with `--py-files`), `entrypoint` and `validator` are `module:function` (or `function` of the artifact's module), called as `fn(spark, tables, scratch)`. Units are the experiment's `units`, recorded by the job with `with velocia.unit("<id>"):`; with `units: all` the job is one opaque unit `app` timed around the call. A validator returns `{unit_id: {rows, checksum, problem?, values?, tolerance?}}` or one such dict for every unit; without it units are `not_checked`.
- `run.json` gains `custom: {path, entrypoint, validator}`. Not yet: jar artifacts and opaque third-party applications timed from the event log (03 §1.2; the Scala helper follows with them).

## 2026-11-02 — fallback share (T37)

- Event-log summaries gain `fallback_task_ms`: task run time in stages none of whose RDDs comes from an accelerated operator (RDD name or scope matching `Comet*`, `Gluten*`, `Velox*`, `Gpu*`, `*Transformer`). Formula input `unit.fallback_share` (fallback / task run time). Report JSON `plan_markers` gains `fallback_share` per cell (null for summaries stored before this change); the plan-marker tables show it.

## 2026-11-02 — PageRank and KMeans; tolerance checks (T29, T30)

- `velocia datagen --suite graph --rows N` (edges; rows / 10 vertices; targets Zipf 1.0; table `graph(src, dst)`) and `--suite kmeans --rows N` (10 dimensions around 8 centers; table `kmeans(f0..f9)`) instead of 03 §2's `--vertices/--edges` and `--points/--dims` (one `rows` field, as for the other job suites).
- Workloads `graph` (unit `pagerank`: 10 iterations, DataFrame API) and `kmeans` (unit `kmeans-train`: MLlib KMeans, k = 8, seed 42, called through the JVM API so no numpy is needed in the engine image).
- `checks.json` units may carry `values` (numbers) and `tolerance` (relative); correctness then also requires every value within the tolerance of the reference's, while rows and checksum stay exact. The catalog's `unit_check` gains `check_values` and `tolerance`. Tolerance is 1e-6 for both jobs.

## 2026-11-02 — ETL pipeline (T31)

- `velocia datagen --suite etl --rows N` (synth data with 5% nulls, `keys` = rows / 100, at least 1,000; id `etl-rows<N>-<format>`); workload `etl` with units `read`, `clean`, `join`, `aggregate`, `write` (driver jobs `etl_<unit>`). Each step reads the previous step's scratch output and writes its own (03 §1.2: `write` sink); each output's rows and checksum are compared with the reference engine's.
- CI's Docker integration runs as two parallel groups (pipeline, workloads).

## 2026-11-02 — synth workloads, part 2 (T35)

- Synth `fact` gains `d` (`decimal(38,10)`), `u` (multi-byte UTF-8 words) and `nest` (`struct<a: bigint, arr: array<bigint>>`); spec fields `nest_fanout` (data) and the query parameters `selectivity` and `window_frame`, which are left out of the dataset id. Dataset ids include a generator version (now 2), so data from an older generator is never reused.
- Units `scan_filter`, `wide_projection`, `nulls`, `decimals`, `strings`, `nested`, `join_auto`, `window`, `udf`. Unit SQL may contain `${field}` (and `${selectivity_ppm}`), filled from the cell's synth spec. The driver registers the Python UDF `velocia_mix`. Pandas UDFs are not covered: the official Apache Spark image has no pyarrow.
- `examples/synth/` covers every edge case in 03 §1.3.

## 2026-11-02 — sweep reporting (T36)

- HTML and Markdown reports gain a section per swept setting (any setting that varies between cells): for the first measure, each unit and the total, every candidate engine's ratio with its 95% CI at each value, as a line with a CI band (HTML) and a table (both). A point that failed correctness shows as ✕ / FAILED, never as a ratio. The JSON report is unchanged; the section is derived from its `cells` and `results`.

## 2026-11-02 — synth workloads, part 1 (T34)

- Synth suite units are operators over `fact` and `dim`: `join`, `aggregate`, `exploding_join`, `sort` (SQL). The spec's named workloads (03 §1.3) are experiments pairing a unit with a swept parameter; `examples/synth/` has skewed join (± AQE skew handling), hot key, skewed aggregate, exploding join, spill (`spark.memory.fraction`) and shuffle partitions.
- Each sweep point of a synth experiment is its own dataset (the spec at that point); `velocia run` generates missing ones before the first cell. Run manifests carry their cell's dataset; the experiment's `dataset_id` is the first point's.

## 2026-11-02 — synth generator (T33)

- `velocia datagen --suite synth --spec synth.yaml` with the synth spec (`schemas/synth.schema.json`): `seed`, `rows`, `keys` (cardinality), `key_dist` (`uniform`, `zipf`, `hotkey`), `zipf_s`, `hot_fraction`, `payload_columns`, `null_fraction`, `string_length` (mean), `files`. Two tables: `fact` (`id`, `key`, `p0`.. longs, string `s`) and `dim` (`key`, `name`, `grp`). Dataset id `synth-<12 hex of the spec hash>-<format>`; manifest `generator_params` is the whole spec. Nesting depth, column types beyond long/string and multi-byte strings (03 §2) follow with the workloads that need them (T35).
- Experiments: `workload.synth` (a synth spec, synth suite only); `workload.sweep` keys must be synth spec fields (Spark settings are swept with lists in `spark_conf`).
- `run.json`'s `teragen` parameters gain `synth`; every generator writes `<output>/<table>`.

## 2026-11-02 — Sort and WordCount (T28)

- `velocia datagen --suite text --rows N` (03 §2 said `--bytes N`; `rows` counts lines and reuses the existing `workload.rows` field, so experiment schemas and config hashes don't change). Dataset ids `text-rows<N>-<format>`, table `text` with one `line` column: ten words per line from a 50,000-word vocabulary skewed toward a few common words, seed 42.
- Workload `text` with units `sort` and `wordcount`, written with the DataFrame API like TeraSort (D-11), not Scala RDD as 03 §1.2 lists, so accelerated engines are measured. `sort` is validated like TeraSort (order within and across files, rows and checksum equal to the input); `wordcount` by its counts adding up to the input's word count, each word once, and rows and checksum equal to the reference engine.
- `run.json`'s `teragen` parameters gain `generator` (`tera` or `text`).

## 2026-11-02 — measure plugins (T39)

- Python measure plugins via the `velocia.measures` entry point group, as specified in 04 §5 (interface made precise: `compute` takes a dict of formula inputs). Network access or an exception in a plugin fails the run. Plugin measures count as normalized measures for cross-hardware experiments, like formula measures. `accelerator_hours` and `energy_kwh` were already built in; accelerators are recorded as `declared` (profile) everywhere until a probe plugin detects them.

## 2026-11-02 — benchmark bundle (T44)

- `velocia bundle <experiment_id> [--out FILE] [--strip-sql]` and `velocia bundle --validate FILE` implemented (04 §7). `bundle.json` gains `sql_stripped` and `label`; bundles also carry `runs/<run_id>/checks.json`. The pairing row is the median successful repetition, lower middle for even counts. JSON Schema `schemas/bundle.schema.json`. Default output `<results_dir>/bundles/<experiment_id>.bundle.zip`.

## 2026-10-25 — cluster smoke-test gate (T43)

- `velocia engines check <engine> --cluster <name> --experiment <file>`: the smoke test (q1, q3, q6 at SF0.01 and a 100,000-row TeraSort, correctness required) with the engine on that cluster and `spark-vanilla` locally as the reference. A pass is recorded in `<results_dir>/_checks/cluster-checks.json` under a hash of the engine definition and the cluster configuration; `velocia run` refuses an external cluster without a matching pass, so a changed image, conf or cluster needs a new check.

## 2026-10-25 — external runner (T40, part of T41)

- `external` clusters run: `spark-submit --deploy-mode client` to `master`, from the engine image in Docker or from `spark_home`. New optional cluster fields `spark_home`, `docker_network`, `docker_args` (04 §3). `deploy_mode: cluster` is refused for now.
- Before the first cell on an external cluster the dataset is copied to `<data_uri>/<dataset_id>/` unless a `_velocia_staged.json` marker with the same dataset checksum is there, then verified in place (every file's size and each table's row count read back by Spark); a mismatch stops the run. Copies use the cluster's own Hadoop filesystems, so no storage client is shipped.
- `run.json` gains task `stage` (writes `stage.json`), `eventlog_fetch` (after `stop()` the driver copies its event log from `eventlog_uri` to the local work dir), `cleanup` (scratch URIs deleted after the run; job output goes to `<data_uri>/_scratch/<run_id>`), `executor_wait_s` (fail clearly when no executor registers); the driver reports into `fetch.json`. A log that can't be fetched leaves driver timings, null event-log measures and a manifest note.
- Manifests of external runs record hardware as declared (`source: declared`, executor shape, the profile's name and accelerators); the profile name is part of the hardware manifest hash for declared nodes. Quiet check uses `rest_url` (standalone `/json/` or YARN `/ws/v1/cluster/apps`), otherwise "quiet check skipped". Cold mode is always "cold (cache not dropped)".

## 2026-10-21 — compare (from v0.2) and concurrent catalog access

- `velocia compare <exp_a> <exp_b> [--force]` implemented early because the v0.1 reproducibility criterion (D-3) needs it: per cell, unit and measure, is the second run's median inside a seeded bootstrap 95% CI of the first run's median; does any ratio CI move from better to worse. Exit 1 when the criterion (>= 90% inside, no flips) fails; refuses different hardware manifests unless `--force`.
- The catalog is opened per operation (with a short wait on a held lock), so `velocia report` and `compare` work while an experiment runs.

## 2026-10-21 — TeraSort (T18, DataFrame fallback D-11)

- `velocia datagen --suite tera --rows N` (TeraGen as a seeded Spark job, Parquet with a 10-byte binary `key` and a 90-byte binary `value`); workload `tera` with unit `terasort`; its validator (sortedness within and across files, rows and checksum equal to the input) runs in the correctness pass and an invalid output is a correctness `fail` (checks status `invalid`). `run.json` gains task `teragen`, `scratch_dir` and job units (`{"id", "job"}`). `velocia engines check` adds a 100,000-row TeraSort.
- Results label: "not an official TPC result" only for TPC-derived data; other data reads "Data: <label>."

## 2026-10-21 — planted faults and cross-hardware rule (T23)

- `velocia run` refuses engines whose clusters have different hardware manifests unless `comparison.allow_cross_hardware: true`; with it, at least one normalized measure (cost, node-hours, accelerator-hours, energy or a custom formula) is required (03 §5, now enforced).
- The correctness pass has the per-unit timeout too; a unit whose check timed out is `not_checked`, a unit whose query errors is `fail`.

## 2026-10-21 — doctor and engine check (T22)

- `velocia doctor` checks Docker (CPUs, memory), engine images, free disk, DuckDB's tpch extension; exits 1 on a failing check. `velocia engines check <engine> [--spark V]` runs TPC-H-derived q1, q3, q6 at SF0.01 against `spark-vanilla` with correctness required, in `<results_dir>/_checks/`. The small TeraSort part of the smoke test follows T18; `--cluster` follows the external runner.

## 2026-10-21 — HTML report (T21)

- `reports/<experiment_id>/report.html` written with every run; `velocia report --format html` writes it. Charts are SVG generated by Velocia instead of an inlined chart library (03 §10 said "inlined chart library"): same self-contained, offline file, nothing third-party to vendor.

## 2026-10-21 — formula measures (T16)

- Formula measures from `measures_dir` work in runs; `velocia measures list|check` implemented. Formula grammar (03 §6) made precise: numbers, `+ - * / // % **` (exponent at most 100), unary `-`/`+`, `min`/`max`/`abs`, inputs `unit.*`, `cluster.*` (`nodes`, `executors`, `executor_cores`, `executor_memory_gb`), `hw.*` (profile fields and `custom` coefficients). A measure file's name must match its `name` when loaded from `measures_dir`; a custom measure can't reuse a built-in name. Manifests list every measure with its definition hash (`measures`, `host_slept_s` added to 04 §6); the report adds `measure_definitions`.

## 2026-10-21 — event-log measures (T12/T13)

- Event-log measures (`task_cpu_time`, `task_run_time`, `gc_time`, `shuffle_bytes`, `spill_bytes`) and `throughput_rows`/`throughput_bytes` now have values. Report JSON gains `plan_markers` (04 §6); the catalog's `unit_run` gains a `summary` column with the per-unit event-log summary (03 §7).

## 2026-10-21 — run outputs (T09-T19, local demo path)

- Defined `timings.json`, `checks.json`, `manifest.json`, the unit tag uses (warm-up rep 0, correctness `-1`, `_setup`), the `_total` unit and the report JSON fields (04 §6). `velocia run` (local runner) and `velocia report --format md|json` work; `--format html` follows with T21.

## 2026-10-21 — cell keys and config hashes (T07)

- Defined `cell_key` and `config_hash` (04 §6). `velocia run --dry-run` prints cells, run count (cells × repetitions, plus one correctness pass per cell) and a rough duration with a worst case at the per-unit timeout; it fails on engines not in the registry.

## 2026-10-21 — dataset manifest (T06)

- Dataset manifest format v1 defined (04 §6) with `schemas/dataset-manifest.schema.json`. Data lives in `<results_dir>/data/<dataset_id>/<table>/` unless `--out` is given.

## 2026-10-21 — redaction patterns (T03)

- `velocia.yaml` gains `redaction.key_patterns` (04 §2): user globs added to the built-in secret deny-list. Optional, default empty.

## 2026-10-21 — plan decisions approved (D-1 … D-13)

- ADR-3: driver program and `velocia-jobs` jar shipped at submit time; engine images hold only the engine (D-1).
- CLAUDE.md: ephemeral per-run Spark containers/pods are not services (D-2).
- v0.1 reproducibility acceptance made statistically realistic (D-3).
- Prebuilt engine images on GHCR by digest (D-4).
- Cold mode labeled "cache not dropped" where the cache can't be dropped (D-5).
- Spark 3.5 for all engines in v0.1; 4.x as a separate row later (D-6).
- Correctness tolerance mode for float-noise-only differences (D-9).
- `velocia bundle --strip-sql` (D-10).

## 2026-10-21 — draft v2

Scope widened from Spark SQL to general Apache Spark jobs, extensible engines and custom units of measure.

- **Concepts** (03 §0): engine family, unit of work, measure. "Query" becomes "unit" in schemas and formats (`query_id` → `unit_id`, `timeout_per_query` → `timeout_per_unit`, `query_run` → `unit_run`, `query_tag_format` → `unit_tag_format`).
- **Workloads** (03 §1): job workloads (TeraSort, Sort, WordCount, PageRank, KMeans, ETL, bring-your-own job) with validators; `synth` skew and edge-case sweeps replace the operator-probe list.
- **Data** (03 §2): `tera`, `text`, `graph`, `kmeans`, `synth` generators, seeded.
- **Engines** (03 §4, 04 §5): `velocia.families` plugin group; `family` field on engine definitions; hardware probe plugins (`velocia.hardware`).
- **Measures** (03 §6, 04 §4–5): built-in measures, including `accelerator_hours`; custom measures as YAML formulas or `velocia.measures` plugins; hardware profiles.
- **Hardware accelerators** (03 §3–4): worked example is vanilla Spark on CPU nodes vs Spark on APU nodes; accelerators declared in hardware profiles when no probe detects them.
- **`external` runner** (03 §4, 04 §1, §3): submit to an existing Spark cluster Velocia doesn't manage (for example a vendor's APU servers); data and event logs via shared storage; declared hardware; labeled cold mode; `engines check --cluster` smoke test.
- **Methodology** (03 §5): ratios with CIs for every measure, oriented so > 1 means better; job validators; cross-hardware comparison only with `comparison.allow_cross_hardware`.
- **Experiment schema** (04 §3): `workload.kind`, `clusters` (named), engines pinned to clusters, `measures`, `comparison`.
- **Results and bundle** (04 §6–7): results in long format with one row per measure; bundle adds hardware profiles, measure definitions, `hardware_manifest_hashes`, `cross_hardware`.
- **Milestones** (01): TeraSort and custom measures in v0.1; full job suite, synth sweeps and cross-hardware runs in v0.2; first non-Spark family in v1.x.

No implementation exists yet, so no migration is needed.
