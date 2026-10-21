# 05 — Quality, security and open-source release

## 1. Non-functional requirements

| Area | Requirement |
| --- | --- |
| Footprint | No services except the optional localhost viewer |
| Overhead | Orchestration overhead per unit < 1 s outside Spark execution |
| Reader | ≥ 1 GB uncompressed event log per minute per core; < 300 MB memory |
| Resumability | Interrupted experiments resume without re-running completed cells |
| Reports | HTML report < 10 MB for a 99-query, 3-engine experiment |

## 2. Compatibility

| Component | Supported |
| --- | --- |
| Spark | 3.5 for all engines from v0.1; vanilla 4.x added as a separate compatibility row once a shipped accelerated engine supports it |
| Python | 3.11+ |
| Scala | The Scala version of each supported Spark build (2.12/2.13 for 3.5, 2.13 for 4.x) |
| Kubernetes | Upstream-supported minors; CI on kind with the Spark Operator |
| Engines | Gluten and Comet versions per their compatibility matrices, pinned by image digest |

## 3. Testing strategy

| Layer | What | When |
| --- | --- | --- |
| Unit | Expander, statistics (bootstrap vs known distributions), checksum rounding, reader, redaction, measure formula evaluator (including rejected inputs), synth generator determinism | Every PR |
| Golden logs | Small generated event logs with expected summaries | Every PR |
| Integration | TPC-H SF0.1 and a small TeraSort on Docker with vanilla Spark, full pipeline to HTML report with one custom measure | Every PR |
| Engine smoke | Each shipped engine runs 3 queries and passes correctness | Nightly |
| Kubernetes e2e | kind + Spark Operator, one small experiment | Nightly |
| External runner e2e | A Spark standalone cluster started by the test harness (not by Velocia) used as an `external` cluster, with event logs on a MinIO bucket | Nightly |
| Planted faults | An engine definition that returns wrong results; a job whose output is unsorted; a noisy cell; a timeout; a cross-hardware experiment without the override | Every PR |
| Bundle | Export validates against the JSON Schema | Every PR |

## 4. Security and privacy

- Redact secrets in Spark configs and environment before writing anything.
- No telemetry.
- Custom measures: formulas are parsed with an allow-list, never executed as code; Python measure plugins are installed by the user like any other package and must not use the network.
- Cloud runner (v0.3): credentials only from the environment; hard budget cap; teardown in a `finally` path and a separate `velocia teardown` command.
- Supply chain: pinned dependencies, Dependabot, SBOM per release, engine images pinned by digest.

## 5. Open-source release

### License
Apache-2.0 with a `NOTICE` file.

### Name and trademark
- The Apache Spark trademark guidelines forbid "Spark" in third-party product names, or names derived from it, except "X for Apache Spark" or "X powered by Apache Spark". Domains containing "spark" need written permission from the Spark PMC. Identifiers such as `spark-foo` are common practice. Sources: [Apache Spark trademark guidelines](https://spark.apache.org/trademarks.html), [Powered By page](https://spark.apache.org/powered-by.html).
- Product: "Velocia for Apache Spark". First and prominent mentions say "Apache Spark".
- Checked on 2026-10-21: `velocia` is free on PyPI and npm; the GitHub login `Velocia` is taken (use `velocia-dev` or similar).
- Open risk: Velocia Inc. (velocia.io), a Toronto-based mobility-rewards SaaS company, uses the same name in software. Different market, same broad trademark category. Get a trademark opinion before launch; renaming is a search-and-replace.

### Artifacts
| Artifact | Where |
| --- | --- |
| Python package | PyPI (trusted publishing from GitHub Actions) |
| Engine Dockerfiles | In repo; publish images only with correct upstream `NOTICE` files |
| Docs | GitHub Pages (MkDocs Material): quickstart, methodology, engines, bundle format |

### TPC
Do not vendor TPC kits. Generate data at runtime. Label results "derived from TPC-H / TPC-DS"; never present them as official or comparable TPC results.

### Community
`README` with a 30-minute quickstart and a sample report, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, issue and PR templates, DCO sign-off. SemVer, 0.x until v1.0; Conventional Commits with automated changelogs.

### Launch checklist
- [ ] Employment agreement checked for IP assignment clauses; built on personal time and hardware; written confirmation if the clause is broad.
- [ ] No data, logs or code from any employer or customer anywhere in git history.
- [ ] Trademark opinion on "Velocia" obtained; no "spark" in the domain.
- [ ] Quickstart verified on a clean machine by someone else.
- [ ] Published example report comparing vanilla Spark with Gluten and Comet, with methodology notes.
- [ ] Announce: GitHub release, LinkedIn, r/dataengineering, Hacker News (Show HN).
