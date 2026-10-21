"""Build the report document from the catalog and render it.

Rules from CLAUDE.md that every rendering follows: a speedup is always a ratio of medians with
its 95% CI, never a bare number; a unit that failed correctness against the reference is shown
as FAILED instead of a ratio; TPC-derived data is labeled as such, never as an official result.
"""

import json
from pathlib import Path
from typing import Any

import yaml

from velocia.datagen.manifest import results_label
from velocia.store.catalog import Catalog
from velocia.workloads.sql import natural_key

REPORT_FORMAT = "velocia-report"
TOTAL_UNIT = "_total"


class ReportError(RuntimeError):
    pass


def build_report(catalog: Catalog, experiment_id: str) -> dict[str, Any]:
    exp = catalog.experiment(experiment_id)
    if exp is None:
        raise ReportError(f"unknown experiment {experiment_id!r}")
    spec = yaml.safe_load(exp["yaml"])
    dataset = catalog.query("SELECT * FROM dataset WHERE dataset_id = ?", [exp["dataset_id"]])
    dataset_manifest = json.loads(dataset[0]["manifest_json"]) if dataset else None
    cells = catalog.query(
        "SELECT * FROM cell WHERE experiment_id = ? ORDER BY idx", [experiment_id]
    )
    results = sorted(
        catalog.query("SELECT * FROM result WHERE experiment_id = ?", [experiment_id]),
        key=lambda r: (
            r["cell_key"],
            r["measure"],
            r["unit_id"] != TOTAL_UNIT,
            natural_key(r["unit_id"]),
        ),
    )
    runs = catalog.query(
        "SELECT run_id, cell_key, task, status, manifest_json FROM run WHERE experiment_id = ? "
        "ORDER BY run_id",
        [experiment_id],
    )
    manifests = [json.loads(r["manifest_json"]) for r in runs if r["manifest_json"]]
    cells_by_key = {c["cell_key"]: c for c in cells}
    hardware: dict[str, Any] = {}
    mode_labels: dict[str, set[str]] = {}
    for m in manifests:
        hardware.setdefault(m["hardware_manifest_hash"], m["hardware"])
        if m["task"] == "time":
            mode_labels.setdefault(m["cell_key"], set()).add(m["mode_label"])

    flags = []
    first_measure = spec_measures(spec)[0]
    noisy: dict[tuple[str, str, str], list[str]] = {}
    for r in results:
        if r["cv_flag"]:
            unit = "total" if r["unit_id"] == TOTAL_UNIT else r["unit_id"]
            noisy.setdefault((r["engine"], r["mode"], unit), []).append(
                f"{r['measure']} {r['cv']:.0%}"
            )
    for (engine, mode, unit), measures in noisy.items():
        flags.append(
            f"high variation (CV > threshold): {engine} {mode} {unit}: {', '.join(measures)}"
        )
    for r in results:
        if r["unit_id"] == TOTAL_UNIT or r["measure"] != first_measure:
            continue
        if r["timeouts"]:
            flags.append(f"{r['timeouts']} timeout(s): {r['engine']} {r['mode']} {r['unit_id']}")
        if r["correctness"] == "fail":
            flags.append(f"correctness FAILED: {r['engine']} {r['mode']} {r['unit_id']}")
    for c in cells:
        reasons = json.loads(c["quiet_check"] or "[]")
        if reasons:
            flags.append(f"quiet check flagged for {c['engine']} {c['mode']}: {'; '.join(reasons)}")
    for m in manifests:
        if m.get("host_slept_s"):
            flags.append(
                f"host slept {m['host_slept_s']:.0f}s during {m['run_id']}: its timings are "
                "unreliable; rerun the experiment"
            )
    markers = _plan_markers(catalog, experiment_id)
    accelerated = sorted(
        {
            cells_by_key[k]["engine"]
            for k, m in markers.items()
            if m["accelerated"] and k in cells_by_key
        }
    )
    if accelerated and "task_cpu_time" in spec_measures(spec):
        flags.append(
            "task_cpu_time counts Spark task threads only; native work that "
            f"{', '.join(accelerated)} runs on other threads is not included, so its CPU "
            "ratios overstate the advantage"
        )
    if spec.get("repetitions", 5) < 5:
        flags.append("fewer than 5 repetitions: confidence intervals are wide")

    return {
        "format": REPORT_FORMAT,
        "version": 1,
        "experiment": {
            "id": experiment_id,
            "name": exp["name"],
            "status": exp["status"],
            "created_at": str(exp["created_at"]),
            "velocia_version": exp["velocia_version"],
            "reference_engine": spec["reference_engine"],
            "repetitions": spec.get("repetitions", 5),
            "bootstrap_seed": exp["bootstrap_seed"],
            "spec": spec,
        },
        "dataset": dataset_manifest,
        "label": (results_label(dataset_manifest["label"]) if dataset_manifest else None),
        "hardware_manifests": hardware,
        "cross_hardware": len(hardware) > 1,
        "cells": [
            {
                "cell_key": c["cell_key"],
                "engine": c["engine"],
                "cluster": c["cluster"],
                "mode": c["mode"],
                "mode_label": ", ".join(sorted(mode_labels.get(c["cell_key"], {c["mode"]}))),
                "config": json.loads(c["varying"] or "{}"),
                "config_hash": c["config_hash"],
                "status": c["status"],
            }
            for c in cells
        ],
        "measures": spec_measures(spec),
        "results": results,
        "measure_definitions": next((m["measures"] for m in manifests if m.get("measures")), []),
        "plan_markers": markers,
        "flags": flags,
    }


def _plan_markers(catalog: Catalog, experiment_id: str) -> dict[str, dict[str, Any]]:
    """Per cell, from its first timed repetition: operators, accelerated ones, transitions."""
    rows = catalog.query(
        "SELECT r.cell_key, u.unit_id, u.rep, u.summary FROM unit_run u JOIN run r USING (run_id) "
        "WHERE r.experiment_id = ? AND NOT u.discarded AND u.summary IS NOT NULL "
        "AND u.unit_id != '_setup' ORDER BY u.rep",
        [experiment_id],
    )
    out: dict[str, dict[str, Any]] = {}
    first_rep: dict[str, int] = {}
    for row in rows:
        rep = first_rep.setdefault(row["cell_key"], row["rep"])
        if row["rep"] != rep:
            continue
        summary = json.loads(row["summary"])
        m = out.setdefault(row["cell_key"], {"operators": 0, "accelerated": 0, "transitions": 0})
        m["operators"] += summary["plan_operators"]
        m["accelerated"] += summary["plan_accelerated"]
        m["transitions"] += summary["plan_transitions"]
    return out


def spec_measures(spec: dict[str, Any]) -> list[str]:
    return list(spec.get("measures") or ["wall_time", "task_cpu_time"])


# --- rendering ----------------------------------------------------------------------------


def _num(value: float | None, unit: str = "") -> str:
    if value is None:
        return "n/a"
    if unit == "s":
        return f"{value:.3f} s" if value < 10 else f"{value:.1f} s"
    if abs(value) >= 1000 or (abs(value) < 0.01 and value != 0):
        return f"{value:.3g}{' ' + unit if unit else ''}"
    return f"{value:.3f}{' ' + unit if unit else ''}"


def ratio_text(r: dict[str, Any]) -> str:
    """The only way a ratio is printed: with its CI, or FAILED when correctness failed."""
    if r["correctness"] == "fail":
        return "FAILED (correctness)"
    if r["ratio"] is None:
        return "n/a"
    if r["ci_low"] is None or r["ci_high"] is None:
        return f"{r['ratio']:.2f}x (CI n/a)"
    return f"{r['ratio']:.2f}x [95% CI {r['ci_low']:.2f}-{r['ci_high']:.2f}]"


def _key(r: dict[str, Any]) -> tuple[str, str]:
    return r["config_hash"], r["mode"]


def headline_lines(report: dict[str, Any]) -> list[str]:
    ref = report["experiment"]["reference_engine"]
    measure = report["measures"][0]
    cells = {c["cell_key"]: c for c in report["cells"]}
    totals = [
        r for r in report["results"] if r["unit_id"] == TOTAL_UNIT and r["measure"] == measure
    ]
    ref_totals = {_key(r): r for r in totals if r["engine"] == ref}
    lines = []
    for r in totals:
        if r["engine"] == ref:
            continue
        cell = cells[r["cell_key"]]
        base = ref_totals.get(_key(r))
        config = " ".join(f"{k}={v}" for k, v in cell["config"].items())
        what = "speedup" if measure == "wall_time" else f"{measure} ratio"
        lines.append(
            f"{r['engine']} vs {ref} ({cell['mode_label']}{', ' + config if config else ''}): "
            f"{what} {ratio_text(r)}; total {measure} median "
            f"{_num(r['median'], r['measure_unit'])} vs "
            f"{_num(base['median'] if base else None, r['measure_unit'])}; "
            f"correctness {r['correctness']}"
        )
    return lines


def render_markdown(report: dict[str, Any]) -> str:
    exp = report["experiment"]
    ref = exp["reference_engine"]
    out = [
        f"# {exp['id']}",
        "",
        "Velocia for Apache Spark benchmark report.",
        "",
        f"**{report['label']}**" if report["label"] else "",
        "",
        f"- Reference engine: `{ref}`; repetitions: {exp['repetitions']}; "
        f"bootstrap seed {exp['bootstrap_seed']}",
        f"- Ratios are oriented so that > 1 means better than `{ref}` "
        "(for wall time: the speedup), as a ratio of medians with a 95% bootstrap CI.",
    ]
    if report["cross_hardware"]:
        out.append("- **Cross-hardware comparison**: cells ran on different hardware manifests.")
    out += ["", "## Summary", ""]
    out += [f"- {line}" for line in headline_lines(report)] or ["- (no candidate engines)"]

    cells = {c["cell_key"]: c for c in report["cells"]}
    results = report["results"]
    definitions = {d["name"]: d for d in report.get("measure_definitions") or []}
    for measure in report["measures"]:
        rows = [r for r in results if r["measure"] == measure]
        if not rows:
            continue
        unit = rows[0]["measure_unit"]
        out += ["", f"## {measure} ({unit}, {rows[0]['direction']} is better)", ""]
        definition = definitions.get(measure)
        if definition and definition.get("formula"):
            out += [
                f"Custom measure from `{definition['source']}`: `{definition['formula']}` "
                f"(definition {str(definition['sha256'])[:19]}).",
                "",
            ]
        if all(r["n"] == 0 for r in rows):
            out.append("No values: this measure's inputs are not available yet.")
            continue
        refs = {(_key(r), r["unit_id"]): r for r in rows if r["engine"] == ref}
        for cell_key, cell in cells.items():
            if cell["engine"] == ref:
                continue
            config = " ".join(f"{k}={v}" for k, v in cell["config"].items())
            out += [
                f"### {cell['engine']} vs {ref} — {cell['mode_label']}"
                + (f", {config}" if config else ""),
                "",
                f"| unit | {ref} median | {cell['engine']} median | ratio | CV | correctness |",
                "| --- | ---: | ---: | --- | ---: | --- |",
            ]
            for r in [r for r in rows if r["cell_key"] == cell_key]:
                base = refs.get((_key(r), r["unit_id"]))
                cv = "n/a" if r["cv"] is None else f"{r['cv']:.1%}" + (" ⚠" if r["cv_flag"] else "")
                unit_id = "**total**" if r["unit_id"] == TOTAL_UNIT else r["unit_id"]
                out.append(
                    f"| {unit_id} | {_num(base['median'] if base else None, unit)} | "
                    f"{_num(r['median'], unit)} | {ratio_text(r)} | {cv} | {r['correctness']} |"
                )
            out.append("")
    markers = report.get("plan_markers") or {}
    if markers:
        out += [
            "## Plan markers",
            "",
            "From each engine's first timed repetition, all units: operators in the final SQL "
            "plans, how many ran on an accelerator, and row/columnar transitions (fallbacks).",
            "",
            "| engine | mode | operators | accelerated | transitions |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
        for key, cell in cells.items():
            m = markers.get(key)
            if m:
                share = m["accelerated"] / m["operators"] if m["operators"] else 0
                out.append(
                    f"| {cell['engine']} | {cell['mode_label']} | {m['operators']} | "
                    f"{m['accelerated']} ({share:.0%}) | {m['transitions']} |"
                )
        out.append("")
    if report["flags"]:
        out += ["## Flags", ""] + [f"- {f}" for f in report["flags"]]
    out += ["", "## Hardware", ""]
    for digest, hw in report["hardware_manifests"].items():
        for node in hw["node_types"]:
            host, docker = node.get("host") or {}, node.get("docker") or {}
            out.append(
                f"- `{digest[:19]}`: {node['count']} x {host.get('cpu_model')} "
                f"({host.get('arch')}), Docker: {docker.get('cpus')} CPUs, "
                f"{docker.get('memory_gib')} GiB"
            )
    return "\n".join(out).replace("\n\n\n", "\n\n") + "\n"


def write_reports(report: dict[str, Any], results_dir: Path) -> Path:
    dest = results_dir / "reports" / str(report["experiment"]["id"])
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n", "utf-8")
    (dest / "report.md").write_text(render_markdown(report), "utf-8")
    from velocia.report.html import render_html  # html imports this module

    (dest / "report.html").write_text(render_html(report), "utf-8")
    return dest
