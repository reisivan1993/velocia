"""Self-contained HTML report (docs/03-spec.md §10, plan T21).

One file, no network: styles inline, charts drawn here as SVG (no chart library to vendor).
Each candidate engine gets a dot-and-whisker chart per measure: one row per unit, the oriented
ratio of medians as a dot, its 95% CI as a whisker, a reference line at 1x, log-scaled so that
2x better and 2x worse sit at the same distance. A unit that failed correctness has no dot:
it shows a cross and "FAILED", never a ratio (CLAUDE.md).
"""

import math
from html import escape
from importlib.resources import files
from typing import Any

from jinja2 import Environment, select_autoescape
from markupsafe import Markup

from velocia.report.build import (
    TOTAL_UNIT,
    hardware_line,
    ratio_text,
    render_markdown,
    sweeps,
)

ROW_H = 22
LABEL_W = 70
VALUE_W = 190
PLOT_W = 420
PAD = 28


def _x(value: float, lo: float, hi: float) -> float:
    return LABEL_W + (math.log(value) - math.log(lo)) / (math.log(hi) - math.log(lo)) * PLOT_W


def _ticks(lo: float, hi: float) -> list[float]:
    candidates = [0.1, 0.25, 0.5, 1, 2, 4, 8, 16, 32]
    return [t for t in candidates if lo <= t <= hi]


def ratio_chart(rows: list[dict[str, Any]], measure: str) -> Markup:
    """Dot-and-whisker SVG of oriented ratios with CIs; ``rows`` are result rows of one cell."""
    values = [
        v for r in rows for v in (r["ratio"], r["ci_low"], r["ci_high"]) if v is not None and v > 0
    ]
    lo = min([0.5, *values]) / 1.15
    hi = max([2.0, *values]) * 1.15
    height = PAD + ROW_H * len(rows) + PAD
    width = LABEL_W + PLOT_W + VALUE_W
    out = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="{escape(measure)} ratio per unit with 95% confidence intervals">'
    ]
    for t in _ticks(lo, hi):
        x = _x(t, lo, hi)
        cls = "ref" if t == 1 else "grid"
        out.append(
            f'<line class="{cls}" x1="{x:.1f}" x2="{x:.1f}" y1="{PAD - 6}" '
            f'y2="{height - PAD + 6}"/>'
        )
        out.append(f'<text class="tick" x="{x:.1f}" y="{height - 8}">{t:g}x</text>')
    out.append(
        f'<text class="axis-note" x="{_x(1, lo, hi) + 4:.1f}" y="{PAD - 10}">'
        "same as reference</text>"
    )
    for i, r in enumerate(rows):
        y = PAD + ROW_H * i + ROW_H / 2
        unit = "total" if r["unit_id"] == TOTAL_UNIT else r["unit_id"]
        text = ratio_text(r)
        weight = " total" if r["unit_id"] == TOTAL_UNIT else ""
        tip = f"{unit}: {text}; CV {_pct(r['cv'])}; n={r['n']}; correctness {r['correctness']}"
        out.append(f'<g class="row{weight}"><title>{escape(tip)}</title>')
        out.append(
            f'<rect class="hit" x="0" y="{y - ROW_H / 2:.1f}" width="{width}" height="{ROW_H}"/>'
        )
        out.append(f'<text class="unit" x="{LABEL_W - 10}" y="{y + 4:.1f}">{escape(unit)}</text>')
        if r["correctness"] == "fail":
            x = _x(1, lo, hi)
            out.append(f'<text class="fail" x="{x:.1f}" y="{y + 4:.1f}">✕</text>')
        elif r["ratio"] is not None and r["ratio"] > 0:
            if r["ci_low"] and r["ci_high"]:
                x1, x2 = _x(r["ci_low"], lo, hi), _x(r["ci_high"], lo, hi)
                out.append(
                    f'<line class="ci" x1="{x1:.1f}" x2="{x2:.1f}" y1="{y:.1f}" y2="{y:.1f}"/>'
                )
            out.append(
                f'<circle class="dot" cx="{_x(r["ratio"], lo, hi):.1f}" cy="{y:.1f}" r="4.5"/>'
            )
        cls = "value fail-text" if r["correctness"] == "fail" else "value"
        out.append(
            f'<text class="{cls}" x="{LABEL_W + PLOT_W + 12}" y="{y + 4:.1f}">'
            f"{escape(text)}</text></g>"
        )
    out.append("</svg>")
    return Markup("".join(out))


SWEEP_W, SWEEP_H = 640, 240
SWEEP_LEFT, SWEEP_RIGHT, SWEEP_TOP, SWEEP_BOTTOM = 56, 120, 20, 40
MAX_SERIES = 4  # more candidates than this get a table only; colors never cycle


def _y(value: float, lo: float, hi: float) -> float:
    plot_h = SWEEP_H - SWEEP_TOP - SWEEP_BOTTOM
    return (
        SWEEP_TOP + (1 - (math.log(value) - math.log(lo)) / (math.log(hi) - math.log(lo))) * plot_h
    )


def sweep_chart(sweep: dict[str, Any], ref: str) -> Markup:
    """Ratio vs the swept setting: one line per candidate engine with its 95% CI band."""
    candidates = [e for e in sweep["engines"] if e != ref][:MAX_SERIES]
    points = sweep["points"]
    values = [
        v
        for p in points
        for e in candidates
        if p["results"][e]
        for v in (p["results"][e]["ratio"], p["results"][e]["ci_low"], p["results"][e]["ci_high"])
        if v is not None and v > 0
    ]
    if not candidates or not values:
        return Markup("")
    lo = min([0.8, *values]) / 1.1
    hi = max([1.25, *values]) * 1.1
    plot_w = SWEEP_W - SWEEP_LEFT - SWEEP_RIGHT
    step = plot_w / max(len(points) - 1, 1)

    def x(i: int) -> float:
        return SWEEP_LEFT + (i * step if len(points) > 1 else plot_w / 2)

    axis, unit = escape(sweep["axis"]), escape(sweep["unit_label"])
    out = [
        f'<svg class="chart sweep" viewBox="0 0 {SWEEP_W} {SWEEP_H}" role="img" '
        f'aria-label="{escape(sweep["measure"])} ratio vs {axis} for {unit}, 95% CI bands">'
    ]
    for t in _ticks(lo, hi):
        y = _y(t, lo, hi)
        cls = "ref" if t == 1 else "grid"
        out.append(
            f'<line class="{cls}" x1="{SWEEP_LEFT}" x2="{SWEEP_W - SWEEP_RIGHT}" '
            f'y1="{y:.1f}" y2="{y:.1f}"/>'
        )
        out.append(f'<text class="ytick" x="{SWEEP_LEFT - 8}" y="{y + 4:.1f}">{t:g}x</text>')
    for i, p in enumerate(points):
        out.append(
            f'<text class="tick" x="{x(i):.1f}" y="{SWEEP_H - SWEEP_BOTTOM + 18}">'
            f"{escape(p['value'])}</text>"
        )
    out.append(
        f'<text class="tick" x="{SWEEP_LEFT + plot_w / 2:.1f}" y="{SWEEP_H - 4}">{axis}</text>'
    )
    for n, engine in enumerate(candidates, 1):
        series = [(i, p["results"][engine]) for i, p in enumerate(points)]
        ok = [
            (i, r)
            for i, r in series
            if r and r["correctness"] != "fail" and r["ratio"] and r["ratio"] > 0
        ]
        band = [(i, r) for i, r in ok if r["ci_low"] and r["ci_high"]]
        if len(band) > 1:
            upper = " ".join(f"{x(i):.1f},{_y(r['ci_high'], lo, hi):.1f}" for i, r in band)
            lower = " ".join(f"{x(i):.1f},{_y(r['ci_low'], lo, hi):.1f}" for i, r in reversed(band))
            out.append(f'<polygon class="band s{n}" points="{upper} {lower}"/>')
        if len(ok) > 1:
            line = " ".join(f"{x(i):.1f},{_y(r['ratio'], lo, hi):.1f}" for i, r in ok)
            out.append(f'<polyline class="line s{n}" points="{line}"/>')
        for i, r in series:
            if r is None:
                continue
            tip = escape(f"{engine}, {sweep['axis']}={points[i]['value']}: {ratio_text(r)}")
            if r["correctness"] == "fail":
                out.append(
                    f'<text class="fail" x="{x(i):.1f}" y="{_y(1, lo, hi) + 4:.1f}">'
                    f"<title>{tip}</title>✕</text>"
                )
            elif r["ratio"] and r["ratio"] > 0:
                out.append(
                    f'<circle class="dot s{n}" cx="{x(i):.1f}" cy="{_y(r["ratio"], lo, hi):.1f}" '
                    f'r="4.5"><title>{tip}</title></circle>'
                )
        if ok:
            i, r = ok[-1]
            out.append(
                f'<text class="series-label s{n}" x="{x(i) + 10:.1f}" '
                f'y="{_y(r["ratio"], lo, hi) + 4:.1f}">{escape(engine)}</text>'
            )
    out.append("</svg>")
    return Markup("".join(out))


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1%}"


def _num(value: float | None, unit: str) -> str:
    if value is None:
        return "n/a"
    if unit == "s":
        return f"{value:.3f} s" if value < 10 else f"{value:.1f} s"
    return f"{value:.4g} {unit}"


def render_html(report: dict[str, Any]) -> str:
    env = Environment(autoescape=select_autoescape(default=True, default_for_string=True))
    env.filters["num"] = _num
    env.filters["pct"] = _pct
    env.filters["hardware"] = hardware_line
    env.globals["ratio_text"] = ratio_text
    env.globals["sweep_chart"] = sweep_chart
    template = env.from_string(
        (files("velocia.report") / "templates" / "report.html.j2").read_text("utf-8")
    )

    ref = report["experiment"]["reference_engine"]
    results = report["results"]
    sections = []
    for cell in report["cells"]:
        if cell["engine"] == ref:
            continue
        measures = []
        for measure in report["measures"]:
            rows = [
                r for r in results if r["cell_key"] == cell["cell_key"] and r["measure"] == measure
            ]
            if not rows or all(r["n"] == 0 for r in rows):
                continue
            refs = {
                r["unit_id"]: r
                for r in results
                if r["engine"] == ref
                and r["measure"] == measure
                and r["config_hash"] == cell["config_hash"]
                and r["mode"] == cell["mode"]
            }
            measures.append(
                {
                    "name": measure,
                    "unit": rows[0]["measure_unit"],
                    "direction": rows[0]["direction"],
                    "chart": ratio_chart(rows, measure),
                    "rows": [(r, refs.get(r["unit_id"])) for r in rows],
                }
            )
        total = next(
            (
                r
                for r in results
                if r["cell_key"] == cell["cell_key"]
                and r["unit_id"] == TOTAL_UNIT
                and r["measure"] == report["measures"][0]
            ),
            None,
        )
        sections.append({"cell": cell, "total": total, "measures": measures})

    return template.render(
        report=report,
        ref=ref,
        sections=sections,
        total_unit=TOTAL_UNIT,
        sweeps=sweeps(report),
        markdown=render_markdown(report),
    )
