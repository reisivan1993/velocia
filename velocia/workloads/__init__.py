"""Workloads: what runs as units of work (docs/03-spec.md §1, docs/04-interfaces.md §5).

Built-in workloads are registered here; other packages add workloads for further suites through
the ``velocia.workloads`` entry-point group (name = suite, value = a ``Workload`` object). A
built-in workload can't be replaced by a plugin.
"""

from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Literal, Protocol

from velocia.config.models import Suite, WorkloadKind
from velocia.datagen.manifest import DatasetManifest


@dataclass(frozen=True)
class Unit:
    id: str  # appears in job tags (velocia:<run_id>:<unit_id>:<rep>) and results
    kind: WorkloadKind
    sql: str | None = None
    job: str | None = None  # name of a driver job (velocia/families/spark/driver/jobs.py)


class Workload(Protocol):
    @property
    def suite(self) -> Suite: ...

    @property
    def kind(self) -> WorkloadKind: ...

    @property
    def label(self) -> str:
        """For example "derived from TPC-H"; shown wherever results are."""
        ...

    def units(self, dataset: DatasetManifest | None = None) -> list[Unit]: ...


class UnknownUnitError(ValueError):
    pass


def _builtin() -> dict[Suite, Workload]:
    from velocia.workloads.jobs import TERA, TEXT
    from velocia.workloads.sql import TPCH

    return {TPCH.suite: TPCH, TERA.suite: TERA, TEXT.suite: TEXT}


def load_workloads() -> dict[Suite, Workload]:
    workloads = _builtin()
    for ep in entry_points(group="velocia.workloads"):
        try:
            suite = Suite(ep.name)
        except ValueError:
            continue  # not a suite this version of Velocia knows
        workloads.setdefault(suite, ep.load())
    return workloads


def get_workload(suite: Suite) -> Workload | None:
    return load_workloads().get(suite)


def select_units(
    workload: Workload, wanted: Literal["all"] | list[str], dataset: DatasetManifest | None = None
) -> list[Unit]:
    """The units an experiment asks for, in the workload's order."""
    available = workload.units(dataset)
    if wanted == "all":
        return available
    ids = {u.id for u in available}
    unknown = [u for u in wanted if u not in ids]
    if unknown:
        raise UnknownUnitError(
            f"unknown unit(s) for suite {workload.suite.value}: {', '.join(unknown)} "
            f"(available: {', '.join(u.id for u in available)})"
        )
    return [u for u in available if u.id in set(wanted)]
