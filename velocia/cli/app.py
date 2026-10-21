"""Command-line entry point.

v0.1 commands only (docs/04-interfaces.md §1). Each command is a stub until its plan task
lands; stubs exit with code 2 and name the task, so nothing pretends to work.
"""

from enum import StrEnum
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from velocia import __version__
from velocia.config import ConfigError, VelociaConfig, load_model
from velocia.engines.docker import DockerError, build_image, inspect_image
from velocia.engines.registry import get_engine, load_engines

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
    _not_implemented("T06")


@app.command()
def run(
    experiment: Annotated[Path, typer.Argument(help="Experiment YAML.")],
    dry_run: Annotated[bool, typer.Option(help="Print expanded cells and estimates.")] = False,
    results_dir: Annotated[Path | None, typer.Option(help="Results directory.")] = None,
    resume: Annotated[bool, typer.Option(help="Continue an interrupted experiment.")] = False,
) -> None:
    """Expand an experiment into cells and execute them."""
    _not_implemented("T15")


@app.command()
def report(
    experiment_id: Annotated[str, typer.Argument(help="Experiment ID.")],
    report_format: Annotated[
        ReportFormat, typer.Option("--format", help="Report format.")
    ] = ReportFormat.md,
    measures: Annotated[str | None, typer.Option(help="Comma-separated measures to show.")] = None,
) -> None:
    """Show results with confidence intervals and correctness."""
    _not_implemented("T19")


@app.command()
def doctor() -> None:
    """Check Docker or Kubernetes access, Spark Operator and disk space."""
    _not_implemented("T22")


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
) -> None:
    """Run the engine smoke test."""
    _not_implemented("T22")


@measures_app.command("list")
def measures_list() -> None:
    """List built-in and custom measures."""
    _not_implemented("T16")


@measures_app.command("check")
def measures_check(file: Annotated[Path, typer.Argument(help="Measure YAML.")]) -> None:
    """Validate a measure formula against the input catalog."""
    _not_implemented("T16")


@hardware_app.command("probe")
def hardware_probe(
    cluster: Annotated[str | None, typer.Option(help="Named cluster.")] = None,
) -> None:
    """Print what the manifest will record about the hardware."""
    _not_implemented("T11")


def main() -> None:
    app()
