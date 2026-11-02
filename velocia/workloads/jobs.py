"""Job workloads: units that run a driver job instead of a query (docs/03-spec.md §1.2)."""

from dataclasses import dataclass

from velocia.config.models import Suite, WorkloadKind
from velocia.datagen.manifest import DatasetManifest
from velocia.workloads import Unit


@dataclass(frozen=True)
class JobWorkload:
    suite: Suite
    label: str
    jobs: tuple[str, ...]
    kind: WorkloadKind = WorkloadKind.job

    def units(self, dataset: DatasetManifest | None = None) -> list[Unit]:
        return [Unit(id=job, kind=self.kind, job=job) for job in self.jobs]


TERA = JobWorkload(Suite.tera, "TeraSort-style sort (DataFrame API)", ("terasort",))
TEXT = JobWorkload(
    Suite.text, "Sort and WordCount on generated text (DataFrame API)", ("sort", "wordcount")
)
