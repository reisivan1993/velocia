"""``bundle.json``: the bundle's self-description (docs/04-interfaces.md §7).

A public format: a change here needs a spec update and a changelog entry.
"""

from datetime import datetime
from typing import Literal

from velocia.config.models import StrictModel

BUNDLE_FILE = "bundle.json"
UNIT_TAG_FORMAT: Literal["velocia:<run_id>:<unit_id>:<rep>"] = "velocia:<run_id>:<unit_id>:<rep>"
PAIRING_COLUMNS = [
    "unit_id",
    "config_hash",
    "mode",
    "engine",
    "cluster",
    "hardware_manifest_hash",
    "run_id",
    "rep",
    "wall_ms",
    "is_reference",
]


class Producer(StrictModel):
    name: Literal["velocia"] = "velocia"
    version: str


class BundleManifest(StrictModel):
    format: Literal["benchmark-bundle"] = "benchmark-bundle"
    version: Literal[1] = 1
    created_at: datetime
    producer: Producer
    experiment_id: str
    reference_engine: str
    engines: list[str]
    hardware_manifest_hashes: list[str]
    cross_hardware: bool
    unit_tag_format: Literal["velocia:<run_id>:<unit_id>:<rep>"] = UNIT_TAG_FORMAT
    sql_stripped: bool = False
    label: str  # e.g. "Data derived from TPC-H; not an official TPC result."
