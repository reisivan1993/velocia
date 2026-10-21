# Results and reports

Everything goes into the results directory (`./velocia-results` by default):

| Path | Contents |
| --- | --- |
| `reports/<experiment_id>/report.html` | self-contained report: open it offline, attach it |
| `reports/<experiment_id>/report.md`, `report.json` | the same as Markdown and JSON |
| `results/<experiment_id>.parquet` | one row per cell x unit x measure: `median`, `p95`, `cv`, `ratio`, `ci_low`, `ci_high`, `correctness`, `timeouts` |
| `runs/<run_id>/manifest.json` | per Spark application: image digests, Spark version and configuration, hardware and its hash, dataset checksum, driver hash, measure definitions |
| `runs/<run_id>/timings.json` | per unit and repetition: status and wall time |
| `runs/<run_id>/checks.json` | per unit: row count and checksum, or the validator's verdict |
| `runs/<run_id>/eventlog/` | Spark event logs |
| `datasets/<dataset_id>.json` | dataset manifests (generator, parameters, checksum) |
| `catalog.duckdb` | all of the above, queryable with DuckDB |

Formats are versioned (`format` and `version` fields) and documented in
`docs/04-interfaces.md`.

## Secrets

Before anything is written, values under keys that look like secrets (`*password*`, `*secret*`,
`*token*`, `*key*`, `*credential*`, plus your own `redaction.key_patterns`), `KEY=value`
assignments in command lines and passwords in URLs are replaced with `*********(redacted)`. That
applies to manifests, event logs, reports and the stored experiment.

## Reading the catalog

```sql
-- duckdb velocia-results/catalog.duckdb
SELECT engine, unit_id, median, ratio, ci_low, ci_high, correctness
FROM result WHERE measure = 'wall_time' AND unit_id = '_total';
```
