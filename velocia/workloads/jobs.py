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
    job_prefix: str = ""  # unit "clean" runs driver job "etl_clean"

    def units(self, dataset: DatasetManifest | None = None) -> list[Unit]:
        # Driver job names are identifiers: unit "kmeans-train" runs job "kmeans_train".
        return [
            Unit(id=unit, kind=self.kind, job=self.job_prefix + unit.replace("-", "_"))
            for unit in self.jobs
        ]


TERA = JobWorkload(Suite.tera, "TeraSort-style sort (DataFrame API)", ("terasort",))
TEXT = JobWorkload(
    Suite.text, "Sort and WordCount on generated text (DataFrame API)", ("sort", "wordcount")
)
ETL = JobWorkload(
    Suite.etl,
    "ETL pipeline on generated data (read, clean, join, aggregate, write)",
    ("read", "clean", "join", "aggregate", "write"),
    job_prefix="etl_",
)
PAGERANK = JobWorkload(
    Suite.graph, "PageRank on a generated power-law graph (DataFrame API)", ("pagerank",)
)
KMEANS = JobWorkload(
    Suite.kmeans, "KMeans (MLlib) on generated clustered points", ("kmeans-train",)
)
