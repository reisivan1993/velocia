# File formats

Every field of every file Velocia reads or writes, generated from the published JSON
Schemas in `schemas/` by `python -m velocia.docs_reference`; do not edit.

## velocia.yaml

Optional settings in the working directory. Schema: `schemas/velocia-config.schema.json`.

Optional ``velocia.yaml``.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `results_dir` | string | './velocia-results' | Where datasets, runs, reports, bundles and the catalog go. |
| `engines_dir` | string | './engines' | Your engine definitions; a file named like a built-in engine replaces it. |
| `measures_dir` | string | './measures' | Your formula measures (`<name>.yaml`). |
| `hardware_dir` | string | './hardware' | Hardware profiles (`<name>.yaml`) referenced by clusters. |
| `pricing` | [Pricing](#pricing) | {'default_node_hour_usd': 0.4} | Prices used by `cost_usd` when a hardware profile has none. |
| `quiet_check` | [QuietCheck](#quietcheck) | {'max_cpu_pct': 10.0, 'max_other_spark_apps': 0} | What counts as a quiet machine before each run. |
| `correctness` | [Correctness](#correctness) | {'float_round_decimals': 4} | How results are compared with the reference engine. |
| `statistics` | [Statistics](#statistics) | {'bootstrap_resamples': 10000, 'cv_flag_threshold': 0.1} | Bootstrap and variation settings. |
| `redaction` | [Redaction](#redaction) | {'key_patterns': []} | Extra secrets to redact. |

### Correctness

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `float_round_decimals` | integer | 4 | Floating and decimal columns are rounded to this many decimals before checksums. |

### Pricing

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `default_node_hour_usd` | number | 0.4 | USD per node-hour when the hardware profile sets no price. |

### QuietCheck

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `max_cpu_pct` | number | 10 | Host CPU busy above this share (%) flags the run. |
| `max_other_spark_apps` | integer | 0 | Other Spark applications allowed on the cluster. |

### Redaction

Extra key patterns redacted on top of the built-in deny-list (shell-style globs).

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `key_patterns` | list of string | [] | Shell-style globs of setting names to redact, on top of the built-in list. |

### Statistics

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `bootstrap_resamples` | integer | 10000 | Resamples for the 95% confidence intervals. |
| `cv_flag_threshold` | number | 0.1 | Cells whose coefficient of variation exceeds this are flagged. |

## Experiment files

What to run: `velocia run <file>`. Schema: `schemas/experiment.schema.json`.

An experiment file (docs/04-interfaces.md §3).

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Experiment name; results are `<name>-<timestamp>`, and history groups by it. |
| `workload` | [Workload](#workload) | required | What to run. |
| `engines` | list of string or [EngineRef](#engineref) | required | Engines to compare: names, or `{name, cluster}` with several clusters. |
| `reference_engine` | string | required | The engine every ratio and correctness check is relative to. |
| `cluster` | [LocalCluster](#localcluster) or [K8sCluster](#k8scluster) or [ExternalCluster](#externalcluster) (optional) | None | One cluster for every engine (or use `clusters`). |
| `clusters` | object (optional) | None | Named clusters; each engine names its cluster. |
| `spark_conf` | map of string or integer or number or boolean or list of string or integer or number or boolean | {} | Spark settings for every engine; a list sweeps the setting. |
| `measures` | list of string | ['wall_time', 'task_cpu_time'] | Measures to compute; the first one is the headline. |
| `comparison` | [Comparison](#comparison) | {'allow_cross_hardware': False} | Rules for comparing across hardware. |
| `repetitions` | integer | 5 | Timed repetitions per cell (5 or more recommended). |
| `modes` | list of [Mode](#mode) | ['cold', 'warm'] | `cold` (fresh application per repetition) and/or `warm` (one application, one discarded warm-up). |
| `timeout_per_unit` | string | '30m' | Per-unit time limit, e.g. `30m`; timed-out units are reported. |
| `price_table` | object (optional) | None | USD per node-hour by hardware profile name; overrides the profiles' prices for `cost_usd`. |

### Comparison

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `allow_cross_hardware` | boolean | False | Allow engines on different hardware; needs a normalized measure (cost, node-hours, ...). |

### CustomJob

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `artifact` | string | required | A `.py`, `.zip` or `.jar`, relative to the experiment file. |
| `entrypoint` | string | required | `module:function` (Python) or the main class (jar). |
| `validator` | string (optional) | None | Python only: `module:function` returning rows and checksum per unit. |
| `args` | list of string | [] | Jar only: program arguments; `${table:<name>}` becomes the table's path. |

### DataFormat

### EngineRef

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Engine name. |
| `cluster` | string (optional) | None | Cluster (from `clusters`) the engine runs on. |

### ExternalCluster

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `executors` | integer | required | Executors (local: multiplied by `executor_cores` for local[N]). |
| `executor_cores` | integer | required | Cores per executor. |
| `executor_memory` | string | required | Memory per executor, e.g. `4g`. |
| `driver_memory` | string | '2g' | Driver memory, e.g. `2g`. |
| `hardware_profile` | string (optional) | None | Hardware profile name (file in `hardware_dir`) describing the nodes. |
| `runner` | `external` | required | `local` (Docker), `k8s` or `external` (a cluster Velocia doesn't manage). |
| `master` | string | required | Spark master URL, e.g. `spark://host:7077`. |
| `deploy_mode` | `client` \| `cluster` | 'client' | Only `client` so far: the driver runs on this machine. |
| `data_uri` | string | required | Storage the executors read (`s3a://`, `hdfs://`, `file://`); datasets are copied there. |
| `eventlog_uri` | string | required | Where Spark writes event logs; Velocia copies them back. |
| `submit_args` | list of string | [] | Extra spark-submit arguments, e.g. `--packages` or `--conf` for storage. |
| `rest_url` | string (optional) | None | The master's web UI, for the quiet check. |
| `spark_home` | string (optional) | None | Submit with the Spark install here instead of the engine image in Docker. |
| `docker_network` | string | 'host' | Docker network of the submitting container. |
| `docker_args` | list of string | [] | Extra `docker run` arguments for the submitting container. |

### K8sCluster

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `executors` | integer | required | Executors (local: multiplied by `executor_cores` for local[N]). |
| `executor_cores` | integer | required | Cores per executor. |
| `executor_memory` | string | required | Memory per executor, e.g. `4g`. |
| `driver_memory` | string | '2g' | Driver memory, e.g. `2g`. |
| `hardware_profile` | string (optional) | None | Hardware profile name (file in `hardware_dir`) describing the nodes. |
| `runner` | `k8s` | required | `local` (Docker), `k8s` or `external` (a cluster Velocia doesn't manage). |
| `namespace` | string | 'default' | Kubernetes namespace. |
| `node_selector` | map of string | {} | Node labels the pods must match. |
| `tolerations` | list of map of string | [] | Pod tolerations. |
| `resources` | map of integer | {} | Extended resources per executor, e.g. accelerator devices. |

### KeyDistribution

### LocalCluster

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `executors` | integer | required | Executors (local: multiplied by `executor_cores` for local[N]). |
| `executor_cores` | integer | required | Cores per executor. |
| `executor_memory` | string | required | Memory per executor, e.g. `4g`. |
| `driver_memory` | string | '2g' | Driver memory, e.g. `2g`. |
| `hardware_profile` | string (optional) | None | Hardware profile name (file in `hardware_dir`) describing the nodes. |
| `runner` | `local` | required | `local` (Docker), `k8s` or `external` (a cluster Velocia doesn't manage). |

### Mode

### Suite

### SynthSpec

Seeded edge-case data (docs/03-spec.md §1.3, §2): ``synth.yaml`` or ``workload.synth``. Two tables: ``fact`` (``rows`` rows: ``key``, ``payload_columns`` longs ``p0``.., a string ``s``) and ``dim`` (one row per key). Every field can be swept (``workload.sweep``).

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `seed` | integer | 42 | Generator seed: the same spec and seed give the same data. |
| `rows` | integer | 1000000 | Rows in `fact`. |
| `keys` | integer | 100000 | key cardinality; dim has one row per key |
| `key_dist` | [KeyDistribution](#keydistribution) | 'uniform' | Key distribution: uniform, zipf or hotkey. |
| `zipf_s` | number | 1.0 | Zipf exponent (key_dist: zipf) |
| `hot_fraction` | number | 0.1 | share of rows on key 0 (key_dist: hotkey) |
| `payload_columns` | integer | 4 | Number of long columns `p0`.. (wide rows). |
| `null_fraction` | number | 0.0 | per payload column |
| `string_length` | integer | 16 | mean length of column s |
| `files` | integer | 8 | files per table |
| `nest_fanout` | integer | 4 | elements in nest.arr |
| `selectivity` | number | 0.1 | share of rows scan_filter keeps |
| `window_frame` | integer | 10 | rows preceding in window |

### Workload

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `kind` | [WorkloadKind](#workloadkind) | required | `sql` (queries) or `job` (job units). |
| `suite` | [Suite](#suite) | required | Which workload: tpch, tpcds, tera, text, etl, graph, kmeans, synth or custom. |
| `units` | `all` or list of string | 'all' | `all`, or the units to run. |
| `scale_factor` | number (optional) | None | TPC-H/TPC-DS-derived scale factor. |
| `rows` | integer (optional) | None | Size of a generated job dataset (records, lines, edges or points). |
| `format` | [DataFormat](#dataformat) | 'parquet-zstd' | Data file format. |
| `dataset_id` | string (optional) | None | Use this generated dataset instead of one derived from the fields above. |
| `sql_dir` | string (optional) | None | Custom SQL: a directory of `<unit>.sql` files, relative to the experiment file. |
| `job` | [CustomJob](#customjob) (optional) | None | Custom job (Python or jar). |
| `sweep` | map of list of string or integer or number or boolean (optional) | None | Synth parameters to sweep: each value is its own cell (and dataset). |
| `synth` | [SynthSpec](#synthspec) (optional) | None | Synth generator settings (synth suite only). |

### WorkloadKind

## Engine definitions

`engines/<name>.yaml` in `engines_dir`. Schema: `schemas/engine.schema.json`.

``engines/<name>.yaml``.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Engine name used in experiments. |
| `family` | string | 'spark' | Engine family (only `spark`). |
| `image` | string | required | Container image, ideally pinned by digest. |
| `spark_versions` | list of string | required | Apache Spark versions the engine supports. |
| `spark_conf` | map of string | {} | Spark settings the engine needs (plugins, memory, ...). |
| `platforms` | list of string | [] | Docker platforms the image runs on natively; empty means any. |
| `notes` | string (optional) | None | Free text shown in manifests. |

## Measure definitions

`measures/<name>.yaml` in `measures_dir`. Schema: `schemas/measure.schema.json`.

``measures/<name>.yaml``. The formula is parsed and checked by the measures module.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Measure name (must match the file name). |
| `unit` | string | required | Unit shown in reports. |
| `direction` | [Direction](#direction) | required | `lower` or `higher` is better. |
| `description` | string | '' | Free text shown in reports. |
| `formula` | string | required | Arithmetic over `unit.*`, `cluster.*` and `hw.*` inputs. |

### Direction

## Hardware profiles

`hardware/<name>.yaml` in `hardware_dir`. Schema: `schemas/hardware-profile.schema.json`.

``hardware/<name>.yaml``: per-node facts that hardware probes cannot observe.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Profile name (the file name in `hardware_dir`). |
| `price_per_node_hour_usd` | number (optional) | None | Price of one node for one hour (for `cost_usd`). |
| `watts_per_node` | number (optional) | None | Power draw per node (for `energy_kwh`, an estimate). |
| `accelerators_per_node` | integer | 0 | Accelerator devices per node (for `accelerator_hours`). |
| `accelerator_model` | string (optional) | None | Accelerator model, recorded as declared. |
| `custom` | object | {} | Coefficients for formula measures, available as `hw.<key>`. |

## Synth specs

`velocia datagen --suite synth --spec <file>` and `workload.synth`. Schema: `schemas/synth.schema.json`.

Seeded edge-case data (docs/03-spec.md §1.3, §2): ``synth.yaml`` or ``workload.synth``. Two tables: ``fact`` (``rows`` rows: ``key``, ``payload_columns`` longs ``p0``.., a string ``s``) and ``dim`` (one row per key). Every field can be swept (``workload.sweep``).

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `seed` | integer | 42 | Generator seed: the same spec and seed give the same data. |
| `rows` | integer | 1000000 | Rows in `fact`. |
| `keys` | integer | 100000 | key cardinality; dim has one row per key |
| `key_dist` | [KeyDistribution](#keydistribution) | 'uniform' | Key distribution: uniform, zipf or hotkey. |
| `zipf_s` | number | 1.0 | Zipf exponent (key_dist: zipf) |
| `hot_fraction` | number | 0.1 | share of rows on key 0 (key_dist: hotkey) |
| `payload_columns` | integer | 4 | Number of long columns `p0`.. (wide rows). |
| `null_fraction` | number | 0.0 | per payload column |
| `string_length` | integer | 16 | mean length of column s |
| `files` | integer | 8 | files per table |
| `nest_fanout` | integer | 4 | elements in nest.arr |
| `selectivity` | number | 0.1 | share of rows scan_filter keeps |
| `window_frame` | integer | 10 | rows preceding in window |

### KeyDistribution

## Dataset manifests

Written by `velocia datagen`; read-only. Schema: `schemas/dataset-manifest.schema.json`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `format` | `velocia-dataset` | 'velocia-dataset' | Always `velocia-dataset`. |
| `version` | `1` | 1 | Format version. |
| `dataset_id` | string | required | Dataset name, e.g. `tpch-sf1-parquet-zstd`. |
| `suite` | [Suite](#suite) | required | Suite that generated it. |
| `label` | string | required | How results on this data are labeled (never an official TPC result). |
| `generator` | string | required | Generator used. |
| `generator_version` | string | required | Generator version. |
| `generator_params` | map of number or integer or string | required | Every generator parameter. |
| `seed` | integer (optional) | required | Generator seed, when it has one. |
| `scale` | number | required | Scale factor or row count. |
| `data_format` | [DataFormat](#dataformat) | required | File format. |
| `compression` | string | required | Parquet compression codec. |
| `row_group_size` | integer | required | Rows per Parquet row group (target). |
| `location` | string | required | Directory of the data. |
| `tables` | list of [DatasetTable](#datasettable) | required | Tables with rows, content checksum and files. |
| `files` | integer | required | Number of data files. |
| `bytes` | integer | required | Total size of the data files. |
| `checksum` | string | required | Content checksum: the same rows give the same checksum in any format. |
| `created_at` | string | required | When the dataset was generated (UTC). |
| `velocia_version` | string | required | Velocia version that wrote this. |

### DataFormat

### DatasetFile

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `path` | string | required | Path relative to the dataset location. |
| `bytes` | integer | required | File size. |

### DatasetTable

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Table name. |
| `rows` | integer | required | Row count. |
| `checksum` | string | required | Content checksum of the table's rows. |
| `files` | list of [DatasetFile](#datasetfile) | required | The table's data files. |

### Suite

## Benchmark bundles (`bundle.json`)

Written by `velocia bundle`; read-only. Schema: `schemas/bundle.schema.json`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `format` | `benchmark-bundle` | 'benchmark-bundle' | Always `benchmark-bundle`. |
| `version` | `1` | 1 | Format version. |
| `created_at` | string | required | When the bundle was written (UTC). |
| `producer` | [Producer](#producer) | required | The tool that wrote the bundle. |
| `experiment_id` | string | required | The exported experiment. |
| `reference_engine` | string | required | Reference engine of the experiment. |
| `engines` | list of string | required | Engines in the experiment. |
| `hardware_manifest_hashes` | list of string | required | Hardware manifests the cells ran on. |
| `cross_hardware` | boolean | required | True when cells ran on more than one hardware manifest. |
| `unit_tag_format` | `velocia:<run_id>:<unit_id>:<rep>` | 'velocia:<run_id>:<unit_id>:<rep>' | Job description tag that finds a unit's executions in the event logs. |
| `sql_stripped` | boolean | False | True when SQL text and plan literals were removed from event logs. |
| `label` | string | required | Results label (never an official TPC result). |

### Producer

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | `velocia` | 'velocia' | Always `velocia`. |
| `version` | string | required | Velocia version. |
