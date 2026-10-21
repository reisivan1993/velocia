# Spec changelog

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
