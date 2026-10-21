"""Expand an experiment into cells (docs/03-spec.md §3).

A cell is one engine on one cluster with one configuration and one mode; each repetition of a
cell is one run. Configurations are the cross product of every ``spark_conf`` (and, for the
synth suite, ``workload.sweep``) entry given as a list.

Two identifiers are derived from canonical JSON, so they don't depend on dict ordering, the
Python process or how a value was written in YAML (``true`` and ``"true"`` are the same Apache
Spark setting):

- ``config_hash`` identifies the workload and configuration only. It is the same for every
  engine and cluster, which is what pairs a candidate engine's cell with the reference's.
- ``cell_key`` identifies the whole cell: engine, cluster name and settings, config and mode.

Both are versioned (``v`` in the hashed payload); changing what goes into them is a results
format change.
"""

import hashlib
import itertools
import json
from dataclasses import dataclass
from typing import Any

from velocia.config.models import (
    Cluster,
    ConfScalar,
    DataFormat,
    Experiment,
    Mode,
    Workload,
)

HASH_VERSION = 1
HASH_HEX_CHARS = 16


@dataclass(frozen=True)
class Config:
    spark_conf: dict[str, str]  # every user setting, list entries resolved to one value
    sweep: dict[str, str]  # synth sweep point; empty for other suites
    varying: dict[str, str]  # the settings that differ between this experiment's configs
    config_hash: str


@dataclass(frozen=True)
class Cell:
    index: int  # 1-based position in the expansion order
    engine: str
    cluster_name: str
    cluster: Cluster
    config: Config
    data_format: DataFormat
    mode: Mode
    cell_key: str

    @property
    def config_hash(self) -> str:
        return self.config.config_hash

    @property
    def label(self) -> str:
        varying = " ".join(f"{k}={v}" for k, v in self.config.varying.items())
        return " ".join(filter(None, [self.engine, self.cluster_name, self.mode.value, varying]))


def spark_value(value: ConfScalar) -> str:
    """The string Apache Spark receives for a YAML scalar."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def stable_hash(payload: Any) -> str:
    canonical = json.dumps(
        {"v": HASH_VERSION, **payload}, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:HASH_HEX_CHARS]


def expand(experiment: Experiment) -> list[Cell]:
    """Cells in execution order: configs, then modes, then engines.

    Engines are innermost so that the cells being compared run close together in time.
    """
    clusters = experiment.named_clusters
    cells: list[Cell] = []
    for config in expand_configs(experiment):
        for mode in experiment.modes:
            for ref in experiment.engine_refs:
                cluster_name = ref.cluster or next(iter(clusters))
                cluster = clusters[cluster_name]
                cell_key = stable_hash(
                    {
                        "engine": ref.name,
                        "cluster_name": cluster_name,
                        "cluster": cluster.model_dump(mode="json", exclude_none=True),
                        "config_hash": config.config_hash,
                        "mode": mode.value,
                    }
                )
                cells.append(
                    Cell(
                        index=len(cells) + 1,
                        engine=ref.name,
                        cluster_name=cluster_name,
                        cluster=cluster,
                        config=config,
                        data_format=experiment.workload.format,
                        mode=mode,
                        cell_key=cell_key,
                    )
                )
    return cells


def expand_configs(experiment: Experiment) -> list[Config]:
    conf_axes = _axes(experiment.spark_conf)
    sweep_axes = _axes(experiment.workload.sweep or {})
    varying = {k for k, values in (conf_axes | sweep_axes).items() if len(values) > 1}
    workload = _workload_identity(experiment.workload)
    configs = []
    for conf_point in _product(conf_axes):
        for sweep_point in _product(sweep_axes):
            configs.append(
                Config(
                    spark_conf=conf_point,
                    sweep=sweep_point,
                    varying={k: v for k, v in (conf_point | sweep_point).items() if k in varying},
                    config_hash=stable_hash(
                        {"workload": workload, "spark_conf": conf_point, "sweep": sweep_point}
                    ),
                )
            )
    return configs


def _axes(settings: dict[str, Any]) -> dict[str, list[str]]:
    """Each setting as its list of Spark string values, keys sorted for a stable order."""
    return {
        key: [spark_value(v) for v in (value if isinstance(value, list) else [value])]
        for key, value in sorted(settings.items())
    }


def _product(axes: dict[str, list[str]]) -> list[dict[str, str]]:
    keys = list(axes)
    return [dict(zip(keys, values, strict=True)) for values in itertools.product(*axes.values())]


def _workload_identity(workload: Workload) -> dict[str, Any]:
    # Units don't change a config: results pair on (unit, config_hash). The sweep is replaced by
    # the sweep point of each config.
    return workload.model_dump(mode="json", exclude={"units", "sweep"}, exclude_none=True)
