# CLI reference

Generated from the CLI by `python -m velocia.docs_reference`; do not edit.

## `velocia bundle`

Export a benchmark bundle (or validate one with --validate).

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `experiment_id` |  | Experiment to export. |
| `--out` |  | Output zip file. |
| `--strip-sql` |  | Remove SQL text and plan literals from event logs. |
| `--validate` |  | Validate a bundle file instead of exporting. |

## `velocia compare`

Check that a rerun reproduces an experiment (medians inside CIs, no ratio flips).

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `experiment_a` |  | First experiment ID. |
| `experiment_b` |  | Second experiment ID (e.g. a rerun). |
| `--force` |  | Compare even across different hardware manifests. |

## `velocia datagen`

Generate a dataset and its manifest.

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `--suite` |  | Dataset suite. One of: tpch, tpcds, tera, text, etl, graph, kmeans, synth. |
| `--sf` |  | Scale factor (tpch, tpcds). |
| `--rows` |  | Rows: records, lines (text), edges (graph), points (kmeans). |
| `--format` | parquet-zstd | File format. One of: parquet-snappy, parquet-zstd. |
| `--spec` |  | Generator spec (synth). |
| `--iceberg` |  | Write as an Iceberg table. |
| `--out` |  | Output directory. |

## `velocia doctor`

Check Docker or Kubernetes access, Spark Operator and disk space.

## `velocia engines build`

Build an engine image.

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `name` |  | Engine name. |

## `velocia engines check`

Run the engine smoke test.

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `name` |  | Engine name. |
| `--spark` |  | Spark version to check. |
| `--cluster` |  | Named cluster to smoke-test on. |
| `--experiment` |  | Experiment file that defines --cluster. |

## `velocia engines list`

List engine definitions.

## `velocia hardware probe`

Print what the manifest will record about the hardware, and its hash.

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `--cluster` |  | Named cluster. |
| `--experiment` |  | Experiment file that defines --cluster. |

## `velocia measures check`

Validate a measure formula against the input catalog.

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `file` |  | Measure YAML. |

## `velocia measures list`

List built-in and custom measures.

## `velocia report`

Show results with confidence intervals and correctness.

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `experiment_id` |  | Experiment ID. |
| `--format` | md | Report format. One of: md, json, html. |
| `--measures` |  | Comma-separated measures to show. |

## `velocia run`

Expand an experiment into cells and execute them.

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `experiment` |  | Experiment YAML. |
| `--dry-run` |  | Print expanded cells and estimates. |
| `--results-dir` |  | Results directory. |
| `--resume` |  | Continue an interrupted experiment. |

## `velocia ui`

Browse results in a read-only viewer on this machine (localhost only).

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `--port` | 8765 | Port on localhost. |
| `--host` | 127.0.0.1 | Loopback address to bind. |

## `velocia workloads list`

List the built-in and plugin workloads.

## `velocia workloads show`

Show a workload's units.

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `suite` |  | Suite name. |
