import re
from typing import Any

from velocia.report.html import render_html


def _report(units: int, engines: list[str], fail: set[str] | None = None) -> dict[str, Any]:
    ref = engines[0]
    measures = ["wall_time", "cost_usd"]
    cells = [
        {
            "cell_key": f"k{i}",
            "engine": e,
            "cluster": "default",
            "mode": "cold",
            "mode_label": "cold (cache not dropped)",
            "config": {},
            "config_hash": "h",
            "status": "done",
        }
        for i, e in enumerate(engines)
    ]
    results = []
    for i, engine in enumerate(engines):
        for u in [*[f"q{n}" for n in range(1, units + 1)], "_total"]:
            for m in measures:
                is_ref = engine == ref
                failed = not is_ref and u in (fail or set())
                results.append(
                    {
                        "cell_key": f"k{i}",
                        "engine": engine,
                        "mode": "cold",
                        "config_hash": "h",
                        "unit_id": u,
                        "measure": m,
                        "measure_unit": "s" if m == "wall_time" else "USD",
                        "direction": "lower",
                        "n": 5,
                        "median": 1.0 + i,
                        "p95": 1.2,
                        "cv": 0.02,
                        "cv_flag": False,
                        "ratio": None if is_ref else 1.5 + 0.01 * i,
                        "ci_low": None if is_ref else 1.4,
                        "ci_high": None if is_ref else 1.6,
                        "correctness": "fail"
                        if failed or (u == "_total" and fail and not is_ref)
                        else "pass",
                        "timeouts": 0,
                    }
                )
    return {
        "format": "velocia-report",
        "version": 1,
        "experiment": {
            "id": "e-1",
            "name": "exp",
            "reference_engine": ref,
            "repetitions": 5,
            "bootstrap_seed": 1,
            "spec": {"name": "exp"},
        },
        "dataset": None,
        "label": "Data derived from TPC-DS; not an official TPC result.",
        "hardware_manifests": {},
        "cross_hardware": False,
        "cells": cells,
        "measures": measures,
        "results": results,
        "measure_definitions": [],
        "plan_markers": {},
        "flags": [],
    }


def test_self_contained_and_small_for_99_units_and_3_engines() -> None:
    html = render_html(_report(99, ["spark-vanilla", "spark-comet", "spark-gluten"]))
    assert len(html.encode()) < 10 * 1024 * 1024
    # Nothing loads from the network: no external scripts, styles, images or fonts.
    assert not re.search(r"""(src|href)\s*=\s*["']?(https?:)?//""", html)
    assert "@import" not in html and "url(" not in html
    assert html.count("<svg") == 2 * 2  # two candidates x two measures
    assert "derived from TPC-DS; not an official TPC result" in html


def test_failed_units_show_failed_never_a_ratio() -> None:
    html = render_html(_report(3, ["v", "c"], fail={"q2"}))
    row = re.search(r"<g class=\"row\"><title>q2:[^<]*</title>.*?</g>", html)
    assert row and "FAILED (correctness)" in row.group(0) and "1.50x" not in row.group(0)
    assert '<div class="hero fail-cell">FAILED</div>' in html
    assert "✕ correctness FAILED" in html


def test_text_is_escaped() -> None:
    report = _report(1, ["v", "c"])
    for r in report["results"]:
        if r["unit_id"] == "q1":
            r["unit_id"] = "<script>alert(1)</script>"
    report["flags"] = ["<img src=x onerror=alert(1)>"]
    html = render_html(report)
    assert "<script>alert" not in html and "<img src=x" not in html
    assert "&lt;script&gt;" in html
