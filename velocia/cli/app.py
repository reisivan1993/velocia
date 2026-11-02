"""Command-line entry point.

v0.1 commands only (docs/04-interfaces.md §1). Each command is a stub until its plan task
lands; stubs exit with code 2 and name the task, so nothing pretends to work.
"""

import json
import os
from enum import StrEnum
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from velocia import __version__
from velocia.bundle.export import BundleError, export_bundle
from velocia.bundle.validate import validate_bundle
from velocia.collect.redaction import Redactor
from velocia.config import ConfigError, VelociaConfig, load_model, models
from velocia.datagen.duckdb_tpc import DatagenError, generate
from velocia.datagen.tera import generate_tera
from velocia.doctor import doctor as run_doctor
from velocia.doctor import engine_check
from velocia.engines.docker import DockerError, build_image, inspect_image
from velocia.engines.registry import get_engine, load_engines
from velocia.experiment.estimate import estimate, format_duration
from velocia.experiment.expander import expand
from velocia.measures.formula import HW_STANDARD, parse
from velocia.measures.registry import load_measure_file, load_measures
from velocia.orchestrator.run import Orchestrator, RunError, memory_warnings, prepare
from velocia.report.build import (
    ReportError,
    build_report,
    headline_lines,
    render_markdown,
    write_reports,
)
from velocia.report.compare import CompareError
from velocia.report.compare import compare as run_compare
from velocia.store.catalog import Catalog
from velocia.workloads import UnknownUnitError, get_workload, select_units

app = typer.Typer(
    name="velocia",
    help="Velocia for Apache Spark: reproducible benchmarks of Apache Spark engines.",
    no_args_is_help=True,
    add_completion=False,
)
engines_app = typer.Typer(help="Engine images and compatibility.", no_args_is_help=True)
measures_app = typer.Typer(help="Built-in and custom measures.", no_args_is_help=True)
hardware_app = typer.Typer(help="Hardware probes and profiles.", no_args_is_help=True)
app.add_typer(engines_app, name="engines")
app.add_typer(measures_app, name="measures")
app.add_typer(hardware_app, name="hardware")

NOT_IMPLEMENTED_EXIT = 2
CONFIG_FILE = Path("velocia.yaml")


class Suite(StrEnum):
    tpch = "tpch"
    tpcds = "tpcds"
    tera = "tera"


class DataFormat(StrEnum):
    parquet_snappy = "parquet-snappy"
    parquet_zstd = "parquet-zstd"


class ReportFormat(StrEnum):
    md = "md"
    json = "json"
    html = "html"


def _not_implemented(task: str) -> NoReturn:
    typer.echo(f"Not implemented yet (plan task {task}, docs/plan/01-v0.1-harness.md).", err=True)
    raise typer.Exit(NOT_IMPLEMENTED_EXIT)


def _fail(message: str) -> NoReturn:
    typer.echo(f"error: {message}", err=True)
    raise typer.Exit(1)


def _config() -> VelociaConfig:
    if not CONFIG_FILE.is_file():
        return VelociaConfig()
    try:
        return load_model(CONFIG_FILE, VelociaConfig)
    except ConfigError as exc:
        _fail(str(exc))


def _user_engines_dir() -> Path:
    return Path(_config().engines_dir)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"velocia {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_version_callback, is_eager=True, help="Show version."),
    ] = False,
) -> None:
    """Velocia for Apache Spark."""


@app.command()
def datagen(
    suite: Annotated[Suite, typer.Option(help="Dataset suite.")],
    sf: Annotated[float | None, typer.Option(help="Scale factor (tpch, tpcds).")] = None,
    rows: Annotated[int | None, typer.Option(help="Row count (tera).")] = None,
    data_format: Annotated[
        DataFormat, typer.Option("--format", help="File format.")
    ] = DataFormat.parquet_zstd,
    iceberg: Annotated[bool, typer.Option(help="Write as an Iceberg table.")] = False,
    out: Annotated[Path | None, typer.Option(help="Output directory.")] = None,
) -> None:
    """Generate a dataset and its manifest."""
    if iceberg:
        _fail("--iceberg is not supported yet")
    results_dir = Path(_config().results_dir)
    fmt = models.DataFormat(data_format.value)
    try:
        if suite is Suite.tera:
            if rows is None:
                _fail("--rows is required for --suite tera")
            if sf is not None:
                _fail("--sf does not apply to --suite tera")
            manifest, created = generate_tera(
                rows,
                fmt,
                out or results_dir / "data",
                results_dir / "drivers",
                cores=min(os.cpu_count() or 4, 8),
                progress=typer.echo,
            )
        else:
            if sf is None:
                _fail(f"--sf is required for --suite {suite.value}")
            if rows is not None:
                _fail(f"--rows does not apply to --suite {suite.value}")
            manifest, created = generate(
                models.Suite(suite.value),
                sf,
                fmt,
                out or results_dir / "data",
                progress=typer.echo,
            )
    except DatagenError as exc:
        _fail(str(exc))
    catalog_copy = results_dir / "datasets" / f"{manifest.dataset_id}.json"
    catalog_copy.parent.mkdir(parents=True, exist_ok=True)
    catalog_copy.write_text(manifest.model_dump_json(indent=2) + "\n", "utf-8")

    typer.echo(f"{'Generated' if created else 'Reusing existing'} dataset {manifest.dataset_id}")
    typer.echo(f"  {manifest.label}, {manifest.files} files, {manifest.bytes:,} bytes")
    typer.echo(f"  location: {manifest.location}")
    typer.echo(f"  checksum: {manifest.checksum}")


@app.command()
def run(
    experiment: Annotated[Path, typer.Argument(help="Experiment YAML.")],
    dry_run: Annotated[bool, typer.Option(help="Print expanded cells and estimates.")] = False,
    results_dir: Annotated[Path | None, typer.Option(help="Results directory.")] = None,
    resume: Annotated[bool, typer.Option(help="Continue an interrupted experiment.")] = False,
) -> None:
    """Expand an experiment into cells and execute them."""
    try:
        spec = load_model(experiment, models.Experiment)
    except ConfigError as exc:
        _fail(str(exc))
    known = load_engines(_user_engines_dir())
    unknown = [ref.name for ref in spec.engine_refs if ref.name not in known]
    if unknown:
        _fail(f"unknown engine(s): {', '.join(unknown)} (known: {', '.join(known) or 'none'})")
    workload = get_workload(spec.workload.suite)
    if workload is not None:
        try:
            select_units(workload, spec.workload.units)
        except UnknownUnitError as exc:
            _fail(str(exc))
    if dry_run:
        warnings = [*spec.warnings(), *memory_warnings(spec, known, spec.named_clusters)]
        for warning in warnings:
            typer.echo(f"warning: {warning}", err=True)
        _print_dry_run(spec)
        return
    config = _config()
    out_dir = results_dir or Path(config.results_dir)
    try:
        prepared = prepare(experiment, config)
        experiment_id = Orchestrator(prepared, config, out_dir, echo=typer.echo).run(resume)
    except RunError as exc:
        _fail(str(exc))
    with Catalog(out_dir) as catalog:
        doc = build_report(catalog, experiment_id)
    dest = write_reports(doc, out_dir)
    typer.echo("")
    typer.echo(f"Experiment {experiment_id} finished. {doc['label'] or ''}")
    for line in headline_lines(doc):
        typer.echo(f"  {line}")
    for flag in doc["flags"]:
        typer.echo(f"  flag: {flag}")
    typer.echo(f"Report: {dest / 'report.html'} (also report.md, report.json)")
    typer.echo(f"Results: {out_dir / 'results' / (experiment_id + '.parquet')}")


def _print_dry_run(spec: models.Experiment) -> None:
    cells = expand(spec)
    est = estimate(spec, cells)
    units = f"{est.units} units each" if est.units is not None else "unit count not known yet"
    typer.echo(
        f"Experiment {spec.name}: {est.cells} cells x {spec.repetitions} repetitions = "
        f"{est.runs} runs ({units}), plus {est.correctness_passes} correctness passes"
    )
    rows = [("#", "engine", "cluster", "mode", "config", "cell_key", "config_hash")]
    rows += [
        (
            str(c.index),
            c.engine,
            c.cluster_name,
            c.mode.value,
            " ".join(f"{k}={v}" for k, v in c.config.varying.items()) or "-",
            c.cell_key,
            c.config_hash,
        )
        for c in cells
    ]
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    for row in rows:
        typer.echo("  " + "  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)).rstrip())
    if est.duration is None:
        typer.echo("Estimated duration: unknown (no cost model for this workload yet)")
    else:
        assert est.worst_case is not None
        typer.echo(
            f"Estimated duration: ~{format_duration(est.duration)} (rough; worst case "
            f"{format_duration(est.worst_case)} if every unit hits the "
            f"{spec.timeout_per_unit} timeout)"
        )


@app.command()
def report(
    experiment_id: Annotated[str, typer.Argument(help="Experiment ID.")],
    report_format: Annotated[
        ReportFormat, typer.Option("--format", help="Report format.")
    ] = ReportFormat.md,
    measures: Annotated[str | None, typer.Option(help="Comma-separated measures to show.")] = None,
) -> None:
    """Show results with confidence intervals and correctness."""
    results_dir = Path(_config().results_dir)
    with Catalog(results_dir) as catalog:
        try:
            doc = build_report(catalog, experiment_id)
        except ReportError as exc:
            _fail(str(exc))
    if measures:
        wanted = [m.strip() for m in measures.split(",") if m.strip()]
        unknown = [m for m in wanted if m not in doc["measures"]]
        if unknown:
            _fail(f"measure(s) not in this experiment: {', '.join(unknown)}")
        doc["measures"] = wanted
        doc["results"] = [r for r in doc["results"] if r["measure"] in wanted]
    if report_format is ReportFormat.html:
        dest = write_reports(doc, results_dir) / "report.html"
        typer.echo(f"Wrote {dest}")
    elif report_format is ReportFormat.json:
        typer.echo(json.dumps(doc, indent=2, default=str))
    else:
        typer.echo(render_markdown(doc))


@app.command()
def compare(
    experiment_a: Annotated[str, typer.Argument(help="First experiment ID.")],
    experiment_b: Annotated[str, typer.Argument(help="Second experiment ID (e.g. a rerun).")],
    force: Annotated[
        bool, typer.Option(help="Compare even across different hardware manifests.")
    ] = False,
) -> None:
    """Check that a rerun reproduces an experiment (medians inside CIs, no ratio flips)."""
    with Catalog(Path(_config().results_dir)) as catalog:
        try:
            result = run_compare(catalog, experiment_a, experiment_b, force)
        except CompareError as exc:
            _fail(str(exc))
    typer.echo(
        f"{result.inside}/{result.checked} medians of {experiment_b} fall inside the 95% CI of "
        f"{experiment_a}'s ({result.share_inside:.0%}; criterion >= 90%)"
    )
    for line in result.outside[:20]:
        typer.echo(f"  outside: {line}")
    if len(result.outside) > 20:
        typer.echo(f"  ... and {len(result.outside) - 20} more")
    for line in result.flips:
        typer.echo(f"  FLIP: {line}")
    if result.unmatched_cells:
        typer.echo(f"  {result.unmatched_cells} cells exist in only one experiment")
    verdict = "reproduces" if result.reproduces else "does NOT reproduce"
    typer.echo(f"{experiment_b} {verdict} {experiment_a}")
    if not result.reproduces:
        raise typer.Exit(1)


@app.command()
def bundle(
    experiment_id: Annotated[
        str | None, typer.Argument(help="Experiment to export.", show_default=False)
    ] = None,
    out: Annotated[Path | None, typer.Option(help="Output zip file.")] = None,
    strip_sql: Annotated[
        bool, typer.Option(help="Remove SQL text and plan literals from event logs.")
    ] = False,
    validate: Annotated[
        Path | None, typer.Option(help="Validate a bundle file instead of exporting.")
    ] = None,
) -> None:
    """Export a benchmark bundle (or validate one with --validate)."""
    if validate is not None:
        problems, notes = validate_bundle(validate)
        for line in notes:
            typer.echo(f"  note: {line}")
        for line in problems:
            typer.echo(f"  problem: {line}")
        if problems:
            _fail(f"{validate}: not a valid benchmark bundle v1")
        typer.echo(f"{validate}: valid benchmark bundle v1")
        return
    if experiment_id is None:
        _fail("give an experiment ID to export, or --validate FILE")
    config = _config()
    try:
        path = export_bundle(
            Path(config.results_dir),
            experiment_id,
            out,
            strip_sql,
            Redactor(config.redaction.key_patterns),
        )
    except BundleError as exc:
        _fail(str(exc))
    typer.echo(f"Bundle: {path}")


@app.command()
def doctor() -> None:
    """Check Docker or Kubernetes access, Spark Operator and disk space."""
    checks = run_doctor(_config())
    marks = {"ok": "ok  ", "warn": "WARN", "fail": "FAIL", "skip": "skip"}
    for check in checks:
        typer.echo(f"{marks[check.status]}  {check.name:24} {check.detail}")
    if any(c.status == "fail" for c in checks):
        raise typer.Exit(1)


@engines_app.command("list")
def engines_list() -> None:
    """List engine definitions."""
    try:
        engines = load_engines(_user_engines_dir())
    except ConfigError as exc:
        _fail(str(exc))
    for engine in engines.values():
        d = engine.definition
        source = "built-in" if engine.builtin else str(engine.source)
        built = "built" if inspect_image(d.image) is not None else "not built"
        versions = ",".join(d.spark_versions)
        typer.echo(f"{d.name:<20} {d.family:<6} spark {versions:<8} {d.image}  [{built}; {source}]")


@engines_app.command("build")
def engines_build(name: Annotated[str, typer.Argument(help="Engine name.")]) -> None:
    """Build an engine image."""
    try:
        engine = get_engine(name, _user_engines_dir())
    except (ConfigError, KeyError) as exc:
        _fail(str(exc))
    context = engine.build_context
    if context is None:
        _fail(f"engine {name!r} has no Dockerfile next to {engine.source}")
    typer.echo(f"building {engine.definition.image} from {context} ...")
    try:
        info = build_image(context, engine.definition.image)
    except DockerError as exc:
        _fail(str(exc))
    typer.echo(f"built {engine.definition.image} id={info.image_id} arch={info.architecture}")


@engines_app.command("check")
def engines_check(
    name: Annotated[str, typer.Argument(help="Engine name.")],
    spark: Annotated[str | None, typer.Option(help="Spark version to check.")] = None,
    cluster: Annotated[str | None, typer.Option(help="Named cluster to smoke-test on.")] = None,
    experiment: Annotated[
        Path | None, typer.Option(help="Experiment file that defines --cluster.")
    ] = None,
) -> None:
    """Run the engine smoke test."""
    target = None
    if cluster is not None:
        if experiment is None:
            _fail("--cluster needs --experiment, the experiment file that defines the cluster")
        try:
            clusters = load_model(experiment, models.Experiment).named_clusters
        except ConfigError as exc:
            _fail(str(exc))
        if cluster not in clusters:
            _fail(f"{experiment} has no cluster {cluster!r} (has: {', '.join(clusters)})")
        target = (cluster, clusters[cluster])
    where = f" on cluster {cluster!r}" if cluster else ""
    typer.echo(
        f"Checking {name}{where} against spark-vanilla: TPC-H-derived q1, q3, q6 at SF0.01 "
        "and a 100,000-row TeraSort ..."
    )
    result = engine_check(name, spark, _config(), target)
    for line in result.lines:
        typer.echo(f"  {line}")
    if not result.ok:
        _fail(f"engine check failed for {name}")
    typer.echo(f"{name}: ok (experiments {result.experiment_id})")


@measures_app.command("list")
def measures_list() -> None:
    """List built-in and custom measures."""
    try:
        measures = load_measures(Path(_config().measures_dir))
    except ConfigError as exc:
        _fail(str(exc))
    for m in measures.values():
        what = m.formula if m.formula else "built-in"
        typer.echo(f"{m.name:20} {m.unit:16} {m.direction.value:7} {what}")


@measures_app.command("check")
def measures_check(file: Annotated[Path, typer.Argument(help="Measure YAML.")]) -> None:
    """Validate a measure formula against the input catalog."""
    try:
        measure = load_measure_file(file, match_file_name=False)
    except ConfigError as exc:
        _fail(str(exc))
    assert measure.formula is not None
    formula = parse(measure.formula)
    typer.echo(f"{measure.name}: ok ({measure.unit}, {measure.direction.value} is better)")
    typer.echo(f"  inputs: {', '.join(sorted(formula.inputs))}")
    custom_hw = [i for i in formula.inputs if i.startswith("hw.") and i[3:] not in HW_STANDARD]
    if custom_hw:
        typer.echo(
            f"  {', '.join(sorted(custom_hw))}: custom coefficient(s); every hardware profile "
            "used with this measure needs them under custom:, or values are null"
        )


@hardware_app.command("probe")
def hardware_probe(
    cluster: Annotated[str | None, typer.Option(help="Named cluster.")] = None,
) -> None:
    """Print what the manifest will record about the hardware."""
    _not_implemented("T11")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
