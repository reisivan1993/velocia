"""SQL workloads: one unit per ``<unit_id>.sql`` file."""

import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from velocia.config.models import Suite, WorkloadKind
from velocia.datagen.manifest import DatasetManifest
from velocia.workloads import Unit

QUERIES_DIR = Path(str(files("velocia.workloads.sql")))


def natural_key(unit_id: str) -> list[int | str]:
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", unit_id)]


def read_sql(path: Path) -> str:
    """Query text without the generated header comment."""
    lines = path.read_text("utf-8").splitlines()
    return "\n".join(line for line in lines if not line.startswith("-- ")).strip()


@dataclass(frozen=True)
class SqlWorkload:
    suite: Suite
    label: str
    directory: Path
    kind: WorkloadKind = WorkloadKind.sql

    def units(self, dataset: DatasetManifest | None = None) -> list[Unit]:
        paths = sorted(self.directory.glob("*.sql"), key=lambda p: natural_key(p.stem))
        return [Unit(id=p.stem, kind=self.kind, sql=read_sql(p)) for p in paths]


TPCH = SqlWorkload(Suite.tpch, "derived from TPC-H", QUERIES_DIR / "tpch")
TPCDS = SqlWorkload(Suite.tpcds, "derived from TPC-DS", QUERIES_DIR / "tpcds")
