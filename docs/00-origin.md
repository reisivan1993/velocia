> This was the repository's README while it was being specified and planned (October 2026).
> It is kept as the project's origin; the current overview is the [README](../README.md).

# Velocia for Apache Spark — Product Specification

As of 2026-10-21. Author: Rei. Status: draft v1, ready for implementation planning.

> **Name.** "Velocia for Apache Spark" (ASF trademark rules require this form, see `docs/05-quality-and-release.md`). Checked on 2026-10-21: free on PyPI and npm; the GitHub login `Velocia` is taken, so use an org such as `velocia-dev`. **Open risk:** Velocia Inc. (velocia.io), a Toronto mobility-rewards SaaS company, uses the same name. Get a trademark opinion before launch; if needed, rename with one search-and-replace.

**Velocia is a dynamometer for Apache Spark engines:** reproducible, statistically honest benchmarks of SQL queries and general Spark jobs (TeraSort, PageRank, ETL pipelines, skew and edge-case sweeps) across engines (vanilla Spark, Gluten/Velox, DataFusion Comet, vendor accelerators), configs, data formats and clusters. Every result has a correctness check, and results can be reported in any unit of measure: time, cost, node-hours or a custom unit defined in YAML. Spark comes first; the interfaces are built so other engine families can be added later.

It is a **standalone project**: its own repo, its own releases, no dependency on any other tool. It installs with `pip`, needs only Docker or a Kubernetes cluster to run Spark, and stores everything in a local results directory. Its only connection to the outside world is a documented **benchmark bundle** export that any analysis tool can consume.

## Try it

A local comparison of vanilla Apache Spark and Apache DataFusion Comet on TPC-H-derived data
runs in about 15 minutes: see [`examples/demo/README.md`](../examples/demo/README.md). User
documentation (quickstart, methodology, experiments, measures, engines, results) is in
[`docs/user/`](user/index.md); build it with `uv run --group docs mkdocs serve`.

## Files

| File | Contents |
| --- | --- |
| `CLAUDE.md` | Invariants for the coding agent (stays at the repo root) |
| `docs/01-product.md` | Problem, users, positioning, goals, non-goals, principles, milestones |
| `docs/02-architecture.md` | Components, deployment, data flow, tech stack, decisions |
| `docs/03-spec.md` | Workloads, data, experiments, runners, engines, methodology, event-log summaries, storage, bundle export, outputs, acceptance |
| `docs/04-interfaces.md` | CLI, config files, experiment schema, plugin interfaces, output and bundle formats |
| `docs/05-quality-and-release.md` | Non-functional requirements, testing, security, compatibility, open-source release |
| `docs/06-design.md` | Design: architecture diagrams, run lifecycle, module layout, key mechanisms |
| `docs/plan/` | Implementation plan: overview with decisions to approve, one file per milestone |
| `docs/CHANGELOG.md` | Spec changes |

## Prompt for Claude Code

```
Read every file in docs/ and CLAUDE.md before doing anything else.

1. Review first. List contradictions, gaps and risky assumptions in the spec,
   each with a proposed resolution. Stop and wait for my answers.
2. Then write an implementation plan in docs/plan/:
   - docs/plan/00-overview.md: milestone order matching docs/01-product.md,
     critical path, and an effort table.
   - One file per milestone: tasks of at most one working day each, with
     dependencies, the acceptance test that proves each task done, and the
     spec section it implements.
   - Estimates in MY hours (spec, review, testing, debugging), assuming you
     write most of the code and I have 10 effective hours a week.
   - Risks and time-boxes for the uncertain tasks (engine images first).
3. Do not write product code until I approve the plan.
```
