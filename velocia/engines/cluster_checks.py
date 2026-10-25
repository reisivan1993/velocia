"""The ``engines check --cluster`` gate (docs/03-spec.md §4, T43).

An experiment refuses an external cluster until the smoke test has passed there for the same
engine definition and the same cluster configuration. Passes are recorded in
``<results_dir>/_checks/cluster-checks.json``, keyed by a hash of both, so changing either
(image, Spark conf, master, executors, storage URIs, ...) requires a new check.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from velocia.config.models import Cluster
from velocia.engines.registry import Engine
from velocia.experiment.expander import stable_hash

RECORD = Path("_checks") / "cluster-checks.json"


def check_key(engine: Engine, cluster: Cluster) -> str:
    return stable_hash(
        {
            "engine": engine.definition.model_dump(mode="json"),
            "cluster": cluster.model_dump(mode="json"),
        }
    )


def _load(results_dir: Path) -> dict[str, Any]:
    path = results_dir / RECORD
    return json.loads(path.read_text("utf-8")) if path.is_file() else {}


def passed(results_dir: Path, engine: Engine, cluster: Cluster) -> bool:
    return check_key(engine, cluster) in _load(results_dir)


def record_pass(
    results_dir: Path, engine: Engine, cluster_name: str, cluster: Cluster, experiments: str
) -> None:
    records = _load(results_dir)
    records[check_key(engine, cluster)] = {
        "engine": engine.name,
        "cluster": cluster_name,
        "passed_at": datetime.now(UTC).isoformat(),
        "experiments": experiments,
    }
    path = results_dir / RECORD
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n", "utf-8")
