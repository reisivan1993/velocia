"""Configuration and definition-file models (docs/04-interfaces.md §2-4).

These models are a public interface: a change here needs a spec update and a changelog entry.
Every model forbids unknown fields so typos fail loudly instead of being ignored.
"""

import re
from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    StringConstraints,
    Tag,
    ValidationError,
    ValidatorFunctionWrapHandler,
    WrapValidator,
    model_validator,
)

Name = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_.-]*$", max_length=64)]
Identifier = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$", max_length=64)]
Duration = Annotated[str, StringConstraints(pattern=r"^[1-9][0-9]*(s|m|h)$")]
MemorySize = Annotated[str, StringConstraints(pattern=r"^[1-9][0-9]*[kmgt]$")]


class FieldError(ValueError):
    """A validation error that belongs to a specific sub-field of the model raising it."""

    def __init__(self, field: str, message: str) -> None:
        self.field = field
        self.message = message
        super().__init__(message)


# Tags of tagged unions; the loader strips them from error locations.
UNION_TAGS = frozenset(
    {"conf-scalar", "conf-list", "engine-name", "engine-ref", "local", "k8s", "external"}
)


def _single_error(message: str) -> WrapValidator:
    def validate(value: Any, handler: ValidatorFunctionWrapHandler) -> Any:
        try:
            return handler(value)
        except ValidationError:
            raise ValueError(message) from None

    return WrapValidator(validate)


def _is_list(value: Any) -> str:
    return "conf-list" if isinstance(value, list) else "conf-scalar"


ConfScalar = Annotated[
    str | int | float | bool, _single_error("must be a string, number or boolean")
]
ConfValue = Annotated[
    Annotated[ConfScalar, Tag("conf-scalar")]
    | Annotated[Annotated[list[ConfScalar], Field(min_length=1)], Tag("conf-list")],
    Discriminator(_is_list),
]

_DURATION_UNITS = {"s": "seconds", "m": "minutes", "h": "hours"}


def parse_duration(value: str) -> timedelta:
    """Parse a duration such as ``30m`` (seconds, minutes or hours)."""
    match = re.fullmatch(r"([1-9][0-9]*)(s|m|h)", value)
    if match is None:
        raise ValueError(f"invalid duration {value!r}, expected e.g. 90s, 30m, 2h")
    return timedelta(**{_DURATION_UNITS[match.group(2)]: int(match.group(1))})


def _describe(schema: dict[str, Any], model: type) -> None:
    """Fill in field descriptions from ``field_docs`` for the published JSON Schemas."""
    from velocia.config.field_docs import FIELD_DOCS

    for name, prop in (schema.get("properties") or {}).items():
        if "description" not in prop:
            text = FIELD_DOCS.get(f"{model.__name__}.{name}") or FIELD_DOCS.get(f"*.{name}")
            if text:
                prop["description"] = text


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, json_schema_extra=_describe)


# --- velocia.yaml ---------------------------------------------------------------------------


class Pricing(StrictModel):
    default_node_hour_usd: float = Field(0.40, ge=0)


class QuietCheck(StrictModel):
    max_cpu_pct: float = Field(10, ge=0, le=100)
    max_other_spark_apps: int = Field(0, ge=0)


class Correctness(StrictModel):
    float_round_decimals: int = Field(4, ge=0, le=18)


class Statistics(StrictModel):
    bootstrap_resamples: int = Field(10_000, ge=1_000)
    cv_flag_threshold: float = Field(0.10, gt=0, lt=1)


class Redaction(StrictModel):
    """Extra key patterns redacted on top of the built-in deny-list (shell-style globs)."""

    key_patterns: list[Annotated[str, StringConstraints(min_length=1)]] = []


class VelociaConfig(StrictModel):
    """Optional ``velocia.yaml``."""

    results_dir: str = "./velocia-results"
    engines_dir: str = "./engines"
    measures_dir: str = "./measures"
    hardware_dir: str = "./hardware"
    pricing: Pricing = Pricing()
    quiet_check: QuietCheck = QuietCheck()
    correctness: Correctness = Correctness()
    statistics: Statistics = Statistics()
    redaction: Redaction = Redaction()


# --- Experiment -----------------------------------------------------------------------------


class WorkloadKind(StrEnum):
    sql = "sql"
    job = "job"


class Suite(StrEnum):
    tpch = "tpch"
    tpcds = "tpcds"
    tera = "tera"
    text = "text"
    graph = "graph"
    kmeans = "kmeans"
    etl = "etl"
    synth = "synth"
    custom = "custom"


SUITE_KIND: dict[Suite, WorkloadKind] = {
    Suite.tpch: WorkloadKind.sql,
    Suite.tpcds: WorkloadKind.sql,
    Suite.synth: WorkloadKind.sql,
    Suite.tera: WorkloadKind.job,
    Suite.text: WorkloadKind.job,
    Suite.graph: WorkloadKind.job,
    Suite.kmeans: WorkloadKind.job,
    Suite.etl: WorkloadKind.job,
}


class DataFormat(StrEnum):
    parquet_snappy = "parquet-snappy"
    parquet_zstd = "parquet-zstd"


class CustomJob(StrictModel):
    artifact: str  # .py or .zip (Python), or .jar
    entrypoint: str  # module:function (Python) or the main class (jar)
    validator: str | None = None  # Python jobs only
    args: list[str] = []  # jar jobs: program arguments; ${table:<name>} is that table's path


class KeyDistribution(StrEnum):
    uniform = "uniform"
    zipf = "zipf"
    hotkey = "hotkey"


class SynthSpec(StrictModel):
    """Seeded edge-case data (docs/03-spec.md §1.3, §2): ``synth.yaml`` or ``workload.synth``.

    Two tables: ``fact`` (``rows`` rows: ``key``, ``payload_columns`` longs ``p0``.., a string
    ``s``) and ``dim`` (one row per key). Every field can be swept (``workload.sweep``).
    """

    seed: int = 42
    rows: int = Field(1_000_000, gt=0)
    keys: int = Field(100_000, gt=0, description="key cardinality; dim has one row per key")
    key_dist: KeyDistribution = KeyDistribution.uniform
    zipf_s: float = Field(1.0, ge=0, le=3, description="Zipf exponent (key_dist: zipf)")
    hot_fraction: float = Field(
        0.1, ge=0, le=1, description="share of rows on key 0 (key_dist: hotkey)"
    )
    payload_columns: int = Field(4, ge=1, le=2000)
    null_fraction: float = Field(0.0, ge=0, le=1, description="per payload column")
    string_length: int = Field(16, ge=0, le=10_000, description="mean length of column s")
    files: int = Field(8, ge=1, le=100_000, description="files per table")
    nest_fanout: int = Field(4, ge=0, le=1000, description="elements in nest.arr")
    # Query parameters: they shape the SQL, not the data (no new dataset when swept).
    selectivity: float = Field(0.1, ge=0, le=1, description="share of rows scan_filter keeps")
    window_frame: int = Field(10, ge=0, le=100_000, description="rows preceding in window")


SYNTH_FIELDS = frozenset(SynthSpec.model_fields)
SYNTH_QUERY_FIELDS = frozenset({"selectivity", "window_frame"})


class Workload(StrictModel):
    kind: WorkloadKind
    suite: Suite
    units: Literal["all"] | Annotated[list[str], Field(min_length=1)] = "all"
    scale_factor: float | None = Field(None, gt=0)
    rows: int | None = Field(None, gt=0)
    format: DataFormat = DataFormat.parquet_zstd
    dataset_id: str | None = None
    sql_dir: str | None = None
    job: CustomJob | None = None
    sweep: dict[str, Annotated[list[ConfScalar], Field(min_length=1)]] | None = None
    synth: SynthSpec | None = None

    @model_validator(mode="after")
    def _check_suite(self) -> Self:
        expected = SUITE_KIND.get(self.suite)
        if expected is not None and expected != self.kind:
            raise FieldError("suite", f"suite {self.suite.value!r} is a {expected.value} workload")
        if isinstance(self.units, list) and len(set(self.units)) != len(self.units):
            raise FieldError("units", "contains duplicates")
        if self.suite is Suite.custom:
            if self.kind is WorkloadKind.sql and self.sql_dir is None:
                raise FieldError("sql_dir", "required for a custom sql workload")
            if self.kind is WorkloadKind.job and self.job is None:
                raise FieldError("job", "required for a custom job workload")
        if self.sweep is not None and self.suite is not Suite.synth:
            raise FieldError("sweep", "only valid for the synth suite")
        if self.synth is not None and self.suite is not Suite.synth:
            raise FieldError("synth", "only valid for the synth suite")
        unknown = sorted(set(self.sweep or {}) - SYNTH_FIELDS)
        if unknown:
            raise FieldError(
                "sweep",
                f"unknown synth parameter(s) {', '.join(unknown)} (sweep Spark settings with "
                "a list in spark_conf)",
            )
        return self


class ClusterBase(StrictModel):
    executors: int = Field(ge=1)
    executor_cores: int = Field(ge=1)
    executor_memory: MemorySize
    driver_memory: MemorySize = "2g"
    hardware_profile: Name | None = None


class LocalCluster(ClusterBase):
    runner: Literal["local"]


class K8sCluster(ClusterBase):
    runner: Literal["k8s"]
    namespace: str = "default"
    node_selector: dict[str, str] = {}
    tolerations: list[dict[str, str]] = []
    resources: dict[str, int] = {}
    volume_claim: str = "velocia"  # PVC mounted at /velocia in every pod (data, runs, logs)
    service_account: str = "spark"  # the driver's account; it creates executor pods
    context: str | None = None  # kubectl context; default: the current one


class ExternalCluster(ClusterBase):
    runner: Literal["external"]
    master: str
    deploy_mode: Literal["client", "cluster"] = "client"
    data_uri: str
    eventlog_uri: str
    submit_args: list[str] = []
    rest_url: str | None = None
    # Where spark-submit runs: the engine image in Docker (default), or a Spark install here.
    spark_home: str | None = None
    docker_network: str = "host"  # executors must reach the driver (client mode)
    docker_args: list[str] = []  # extra `docker run` arguments, e.g. mounts or env files


Cluster = Annotated[LocalCluster | K8sCluster | ExternalCluster, Field(discriminator="runner")]


class EngineRef(StrictModel):
    name: Name
    cluster: Name | None = None


def _is_name(value: Any) -> str:
    return "engine-name" if isinstance(value, str) else "engine-ref"


EngineEntry = Annotated[
    Annotated[Name, Tag("engine-name")] | Annotated[EngineRef, Tag("engine-ref")],
    Discriminator(_is_name),
]


class Comparison(StrictModel):
    allow_cross_hardware: bool = False


class Mode(StrEnum):
    cold = "cold"
    warm = "warm"


RECOMMENDED_MIN_REPETITIONS = 5
DEFAULT_CLUSTER = "default"  # name of the cluster given with `cluster:` instead of `clusters:`


class Experiment(StrictModel):
    """An experiment file (docs/04-interfaces.md §3)."""

    name: Name
    workload: Workload
    engines: Annotated[list[EngineEntry], Field(min_length=1)]
    reference_engine: Name
    cluster: Cluster | None = None
    clusters: dict[Name, Cluster] | None = None
    spark_conf: dict[str, ConfValue] = {}
    measures: Annotated[list[Name], Field(min_length=1)] = ["wall_time", "task_cpu_time"]
    comparison: Comparison = Comparison()
    repetitions: int = Field(RECOMMENDED_MIN_REPETITIONS, ge=1)
    modes: Annotated[list[Mode], Field(min_length=1)] = [Mode.cold, Mode.warm]
    timeout_per_unit: Duration = "30m"
    price_table: dict[Name, float] | None = None

    @property
    def engine_refs(self) -> list[EngineRef]:
        return [EngineRef(name=e) if isinstance(e, str) else e for e in self.engines]

    @property
    def named_clusters(self) -> dict[str, "Cluster"]:
        """``clusters``, or the single ``cluster`` under the name ``default``."""
        if self.cluster is not None:
            return {DEFAULT_CLUSTER: self.cluster}
        return dict(self.clusters or {})

    @property
    def timeout(self) -> timedelta:
        return parse_duration(self.timeout_per_unit)

    @model_validator(mode="after")
    def _check_references(self) -> Self:
        names = [e.name for e in self.engine_refs]
        if len(set(names)) != len(names):
            raise FieldError("engines", "contains duplicates")
        if self.reference_engine not in names:
            raise FieldError("reference_engine", f"{self.reference_engine!r} is not in engines")
        if (self.cluster is None) == (self.clusters is None):
            raise FieldError("cluster", "set exactly one of cluster or clusters")
        for i, ref in enumerate(self.engine_refs):
            if self.clusters is None:
                if ref.cluster is not None:
                    raise FieldError(f"engines[{i}].cluster", "only allowed with clusters")
            elif ref.cluster is None and len(self.clusters) > 1:
                raise FieldError(
                    f"engines[{i}].cluster", "required when there are several clusters"
                )
            elif ref.cluster is not None and ref.cluster not in self.clusters:
                raise FieldError(f"engines[{i}].cluster", f"unknown cluster {ref.cluster!r}")
        if self.clusters is not None and not self.clusters:
            raise FieldError("clusters", "is empty")
        if len(set(self.modes)) != len(self.modes):
            raise FieldError("modes", "contains duplicates")
        if len(set(self.measures)) != len(self.measures):
            raise FieldError("measures", "contains duplicates")
        return self

    def warnings(self) -> list[str]:
        """Allowed but discouraged settings, shown to the user before a run."""
        out = []
        if self.repetitions < RECOMMENDED_MIN_REPETITIONS:
            out.append(
                f"repetitions={self.repetitions} is below the recommended "
                f"{RECOMMENDED_MIN_REPETITIONS}; confidence intervals will be wide"
            )
        return out


# --- Definition files -----------------------------------------------------------------------


class EngineDef(StrictModel):
    """``engines/<name>.yaml``."""

    name: Name
    family: Name = "spark"
    image: str = Field(min_length=1)
    spark_versions: Annotated[list[str], Field(min_length=1)]
    spark_conf: dict[str, str] = {}
    # Docker platforms the image runs on natively, e.g. [linux/amd64]; empty means any.
    platforms: list[str] = []
    notes: str | None = None


class Direction(StrEnum):
    # Upper-case members: a member named `lower` would shadow str.lower.
    LOWER = "lower"
    HIGHER = "higher"


class MeasureDef(StrictModel):
    """``measures/<name>.yaml``. The formula is parsed and checked by the measures module."""

    name: Name
    unit: str = Field(min_length=1)
    direction: Direction
    description: str = ""
    formula: str = Field(min_length=1)


class HardwareProfile(StrictModel):
    """``hardware/<name>.yaml``: per-node facts that hardware probes cannot observe."""

    name: Name
    price_per_node_hour_usd: float | None = Field(None, ge=0)
    watts_per_node: float | None = Field(None, ge=0)
    accelerators_per_node: int = Field(0, ge=0)
    accelerator_model: str | None = None
    custom: dict[Identifier, float] = {}
