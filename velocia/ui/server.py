"""``velocia ui``: a read-only viewer over the results directory, on localhost only.

Pages: the experiments, each experiment's report (rendered fresh from the catalog), a
comparison of two experiments, and the history of one experiment name (its headline ratio with
CI over time). Standard library HTTP server, GET only, no outbound requests and no assets from
anywhere else (CLAUDE.md: the viewer is the only service Velocia starts, and it stays local).
"""

import ipaddress
import json
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

import duckdb
import yaml
from jinja2 import Environment, select_autoescape

from velocia.collect.redaction import Redactor
from velocia.report.build import TOTAL_UNIT, build_report, ratio_text
from velocia.report.compare import CompareError, compare
from velocia.report.html import render_html, sweep_chart
from velocia.store.catalog import Catalog

DEFAULT_PORT = 8765
LOCK_WAIT_S = 2.0  # an experiment is writing: say so instead of hanging


class UiError(RuntimeError):
    pass


def check_host(host: str) -> None:
    """The viewer serves only this machine."""
    if host == "localhost":
        return
    try:
        if ipaddress.ip_address(host).is_loopback:
            return
    except ValueError:
        pass
    raise UiError(f"velocia ui binds to localhost only, not {host!r}")


def _env() -> Environment:
    env = Environment(autoescape=select_autoescape(default=True, default_for_string=True))
    env.globals["ratio_text"] = ratio_text
    env.filters["q"] = lambda v: quote(str(v), safe="")
    return env


def _template(name: str) -> str:
    return (files("velocia.ui") / "templates" / name).read_text("utf-8")


class Viewer:
    """Builds the pages from the catalog; one read-only connection per request."""

    def __init__(self, results_dir: Path, redactor: Redactor | None = None) -> None:
        self.results_dir = results_dir
        self.env = _env()
        # Stored data is redacted already; the viewer redacts again before showing it.
        self.redactor = redactor or Redactor()

    def _catalog(self) -> Catalog:
        return Catalog(self.results_dir, read_only=True, lock_wait_s=LOCK_WAIT_S)

    def _page(self, title: str, body_template: str, **context: Any) -> str:
        body = self.env.from_string(_template(body_template)).render(**context)
        return self.env.from_string(_template("base.html.j2")).render(title=title, body=body)

    def headlines(self, catalog: Catalog, experiment_ids: list[str]) -> dict[str, list[str]]:
        """Per experiment: each candidate engine's total wall-time ratio, as the report says it."""
        if not experiment_ids:
            return {}
        marks = ", ".join("?" for _ in experiment_ids)
        rows = catalog.query(
            f"SELECT * FROM result WHERE experiment_id IN ({marks}) AND unit_id = ? "
            "AND measure = 'wall_time'",
            [*experiment_ids, TOTAL_UNIT],
        )
        candidates = [r for r in rows if r["ratio"] is not None or r["correctness"] == "fail"]
        out: dict[str, list[str]] = {}
        for r in sorted(candidates, key=lambda r: (r["engine"], r["mode"])):
            out.setdefault(r["experiment_id"], []).append(
                f"{r['engine']} ({r['mode']}): {ratio_text(r)}"
            )
        return out

    def index(self) -> str:
        with self._catalog() as catalog:
            experiments = catalog.query(
                "SELECT experiment_id, name, status, created_at FROM experiment "
                "ORDER BY created_at DESC"
            )
            headlines = self.headlines(catalog, [e["experiment_id"] for e in experiments])
        return self._page(
            "Experiments",
            "index.html.j2",
            experiments=experiments,
            headlines=headlines,
            results_dir=str(self.results_dir),
        )

    def report(self, experiment_id: str) -> str:
        with self._catalog() as catalog:
            if catalog.experiment(experiment_id) is None:
                raise KeyError(experiment_id)
            return render_html(self.redactor.redact(build_report(catalog, experiment_id)))

    def report_json(self, experiment_id: str) -> str:
        with self._catalog() as catalog:
            if catalog.experiment(experiment_id) is None:
                raise KeyError(experiment_id)
            report = self.redactor.redact(build_report(catalog, experiment_id))
            return json.dumps(report, indent=2, default=str)

    def compare(self, a: str, b: str, force: bool) -> str:
        with self._catalog() as catalog:
            try:
                result = compare(catalog, a, b, force)
                error = None
            except CompareError as exc:
                result, error = None, str(exc)
        return self._page(
            f"Compare {a} and {b}", "compare.html.j2", a=a, b=b, result=result, error=error
        )

    def history(self, name: str) -> str:
        """The headline ratio of every finished run of an experiment name, oldest first."""
        with self._catalog() as catalog:
            experiments = catalog.query(
                "SELECT experiment_id, created_at, yaml FROM experiment WHERE name = ? "
                "AND status = 'done' ORDER BY created_at",
                [name],
            )
            if not experiments:
                raise KeyError(name)
            ids = [e["experiment_id"] for e in experiments]
            marks = ", ".join("?" for _ in ids)
            totals = catalog.query(
                f"SELECT * FROM result WHERE experiment_id IN ({marks}) AND unit_id = ? "
                "AND measure = 'wall_time'",
                [*ids, TOTAL_UNIT],
            )
        reference = yaml.safe_load(experiments[-1]["yaml"]).get("reference_engine", "")
        series: dict[str, dict[str, Any]] = {}  # mode -> history chart input
        for r in totals:
            chart = series.setdefault(
                r["mode"],
                {
                    "axis": "experiment",
                    "measure": "wall_time",
                    "unit_label": f"total ({r['mode']})",
                    "engines": [],
                    "points": [
                        {
                            "value": str(e["created_at"])[:16],
                            "id": e["experiment_id"],
                            "results": {},
                        }
                        for e in experiments
                    ],
                },
            )
            if r["engine"] not in chart["engines"]:
                chart["engines"].append(r["engine"])
            point = next(p for p in chart["points"] if p["id"] == r["experiment_id"])
            point["results"][r["engine"]] = r
        for chart in series.values():
            for point in chart["points"]:
                for engine in chart["engines"]:
                    point["results"].setdefault(engine, None)
        charts = [(chart, sweep_chart(chart, reference)) for chart in series.values()]
        return self._page(
            f"History of {name}", "history.html.j2", name=name, ref=reference, charts=charts
        )


def make_handler(viewer: Viewer) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "velocia-ui"

        def log_message(self, format: str, *args: Any) -> None:
            pass  # quiet; the terminal shows the URL only

        def _send(self, status: HTTPStatus, body: str, content_type: str) -> None:
            data = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            url = urlparse(self.path)
            parts = [unquote(p) for p in url.path.strip("/").split("/") if p]
            query = parse_qs(url.query)
            routes: dict[tuple[str, int], Callable[[], tuple[str, str]]] = {
                ("", 0): lambda: (viewer.index(), "text/html"),
                ("e", 2): lambda: (viewer.report(parts[1]), "text/html"),
                ("e", 3): lambda: (
                    (viewer.report_json(parts[1]), "application/json")
                    if parts[2] == "report.json"
                    else self._missing()
                ),
                ("history", 2): lambda: (viewer.history(parts[1]), "text/html"),
                ("compare", 1): lambda: (
                    viewer.compare(
                        query.get("a", [""])[0],
                        query.get("b", [""])[0],
                        query.get("force", [""])[0] == "1",
                    ),
                    "text/html",
                ),
            }
            route = routes.get((parts[0] if parts else "", len(parts)))
            try:
                if route is None:
                    raise KeyError(url.path)
                body, content_type = route()
                self._send(HTTPStatus.OK, body, content_type)
            except KeyError:
                self._send(HTTPStatus.NOT_FOUND, "not found", "text/plain")
            except FileNotFoundError:
                self._send(
                    HTTPStatus.NOT_FOUND,
                    f"no results yet in {viewer.results_dir}: run an experiment first",
                    "text/plain",
                )
            except duckdb.IOException:
                self._send(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "the catalog is busy (an experiment is writing); reload in a moment",
                    "text/plain",
                )
            except Exception as exc:  # a page must fail visibly, not drop the connection
                self._send(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    f"could not build this page: {type(exc).__name__}: {exc}",
                    "text/plain",
                )

        def _missing(self) -> tuple[str, str]:
            raise KeyError(self.path)

        def _not_allowed(self) -> None:
            self._send(HTTPStatus.METHOD_NOT_ALLOWED, "read-only viewer", "text/plain")

        do_POST = do_PUT = do_DELETE = do_PATCH = _not_allowed

    return Handler


def make_server(
    results_dir: Path,
    host: str = "127.0.0.1",
    port: int = DEFAULT_PORT,
    redactor: Redactor | None = None,
) -> Any:
    check_host(host)
    return ThreadingHTTPServer((host, port), make_handler(Viewer(results_dir, redactor)))
