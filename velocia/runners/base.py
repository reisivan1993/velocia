"""What a runner is given and what it returns."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol

from velocia.families.spark.package import DriverPackage

AppStatus = Literal["finished", "failed", "timeout"]


@dataclass(frozen=True)
class AppSpec:
    """One Spark application: a timed repetition (or warm series) or a correctness pass."""

    run_id: str
    image: str
    spark_conf: dict[str, str]
    driver: DriverPackage
    task: Literal["time", "check", "teragen", "stage"]
    mode: str
    reps: list[int]
    warmup: bool
    units: list[dict[str, str | None]]  # {"id", "sql"} or {"id", "job"}
    dataset_location: Path
    tables: list[str]
    timeout_per_unit_s: float
    app_timeout_s: float
    float_round_decimals: int = 4
    teragen: dict[str, int | str] | None = None  # rows, seed, partitions (task teragen)
    dataset_id: str = ""  # where the data sits on a cluster's own storage (external runner)
    stage: dict[str, object] | None = None  # files, tables, checksum (task stage)


@dataclass(frozen=True)
class AppResult:
    status: AppStatus
    exit_code: int | None
    work_dir: Path  # out/ (timings.json or checks.json) and eventlog/, raw, outside results
    log_tail: str
    started_at: float
    ended_at: float
    cache_dropped: bool
    host_slept_s: float = 0.0  # wall-clock time the host spent asleep during the application
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class QuietCheck:
    ok: bool
    reasons: list[str]


class Runner(Protocol):
    def check(self) -> list[str]:
        """Problems that prevent running at all; empty when ready."""
        ...

    def quiet_check(self) -> QuietCheck: ...

    def run(self, app: AppSpec) -> AppResult: ...

    def teardown(self) -> None: ...
