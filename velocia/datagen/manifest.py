"""Dataset manifest (docs/03-spec.md §2, docs/04-interfaces.md §6).

A public format: a change here needs a spec update and a changelog entry.
"""

import hashlib
import json
from datetime import datetime
from typing import Literal

from pydantic import Field

from velocia.config.models import DataFormat, StrictModel, Suite

MANIFEST_FILE = "_dataset.json"  # leading underscore: Apache Spark skips it when reading the data


class DatasetFile(StrictModel):
    path: str  # relative to the dataset location
    bytes: int = Field(ge=0)


class DatasetTable(StrictModel):
    name: str
    rows: int = Field(ge=0)
    checksum: str  # content checksum, independent of file format and compression
    files: list[DatasetFile]


class DatasetManifest(StrictModel):
    format: Literal["velocia-dataset"] = "velocia-dataset"
    version: Literal[1] = 1
    dataset_id: str
    suite: Suite
    label: str  # e.g. "derived from TPC-H"; never an official TPC dataset
    generator: str
    generator_version: str
    generator_params: dict[str, float | int | str]
    seed: int | None
    scale: float
    data_format: DataFormat
    compression: str
    row_group_size: int = Field(gt=0)
    location: str
    tables: list[DatasetTable]
    files: int = Field(ge=0)
    bytes: int = Field(ge=0)
    checksum: str
    created_at: datetime
    velocia_version: str


def results_label(dataset_label: str) -> str:
    """How results on a dataset are labeled; TPC-derived data is never an official TPC result."""
    if "TPC" in dataset_label:
        return f"Data {dataset_label}; not an official TPC result."
    return f"Data: {dataset_label}."


def dataset_checksum(tables: list[DatasetTable]) -> str:
    """Checksum of the data content: same rows give the same checksum in any format."""
    canonical = json.dumps(
        sorted([t.name, t.rows, t.checksum] for t in tables), separators=(",", ":")
    )
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
