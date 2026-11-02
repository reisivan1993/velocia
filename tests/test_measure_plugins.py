"""Python measure plugins: loaded by entry point, pure and offline, hashed into manifests."""

import socket
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest

from velocia.config import ConfigError
from velocia.config.models import HardwareProfile, LocalCluster
from velocia.measures import registry
from velocia.measures.builtins import ClusterInputs, UnitInputs
from velocia.measures.plugins import GROUP, NetworkBlocked, PluginError, no_network

CLUSTER = LocalCluster(runner="local", executors=1, executor_cores=4, executor_memory="4g")
APU = HardwareProfile(name="apu-node", accelerators_per_node=2)


def _entries(monkeypatch: pytest.MonkeyPatch, *objects: str) -> None:
    entries = [EntryPoint(o.lower(), f"measure_plugins:{o}", GROUP) for o in objects]
    monkeypatch.setattr(registry, "plugin_entries", lambda: entries)


def test_plugin_measure_loads_and_computes(monkeypatch: pytest.MonkeyPatch) -> None:
    _entries(monkeypatch, "ApuSeconds")
    measure = registry.load_measures(None)["apu_seconds"]
    assert measure.source == "plugin:measure_plugins:ApuSeconds"
    assert measure.definition_sha256 and measure.definition_sha256.startswith("sha256:")
    assert measure.describe()["sha256"] == measure.definition_sha256
    apu = ClusterInputs.of(CLUSTER, APU, 1.0)
    assert measure.compute(UnitInputs(wall_s=3.0), apu) == (6.0, None)
    plain = ClusterInputs.of(CLUSTER, None, 1.0)
    assert measure.compute(UnitInputs(wall_s=3.0), plain) == (
        None,
        "plugin apu_seconds returned no value",
    )
    assert measure.compute(UnitInputs(wall_s=None), apu) == (None, "unit did not finish")


def test_network_access_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    _entries(monkeypatch, "PhoneHome")
    measure = registry.load_measures(None)["phone_home"]
    with pytest.raises(NetworkBlocked):
        measure.compute(UnitInputs(wall_s=1.0), ClusterInputs.of(CLUSTER, None, 1.0))
    # Blocking is scoped to the plugin call.
    assert socket.create_connection is not None and socket.getaddrinfo("localhost", 80)


def test_bad_plugins_are_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _entries(monkeypatch, "Broken")
    with pytest.raises(ConfigError, match="direction"):
        registry.load_measures(None)
    _entries(monkeypatch, "ApuSeconds", "ApuSeconds")
    with pytest.raises(ConfigError, match="already defined"):
        registry.load_measures(None)


def test_no_network_blocks_lookups() -> None:
    with no_network(), pytest.raises(PluginError):
        socket.getaddrinfo("example.com", 80)


def test_network_access_fails_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from velocia.config import VelociaConfig
    from velocia.config.models import Experiment
    from velocia.experiment.expander import expand
    from velocia.orchestrator.run import Prepared, RunError, compute_results
    from velocia.store.catalog import Catalog
    from velocia.workloads import Unit

    _entries(monkeypatch, "PhoneHome")
    experiment = Experiment.model_validate(
        {
            "name": "t",
            "workload": {"kind": "sql", "suite": "tpch", "scale_factor": 0.1},
            "engines": ["spark-vanilla"],
            "reference_engine": "spark-vanilla",
            "cluster": {
                "runner": "local",
                "executors": 1,
                "executor_cores": 1,
                "executor_memory": "1g",
            },
            "measures": ["phone_home"],
        }
    )
    cells = expand(experiment)
    prepared = Prepared(
        experiment,
        "",
        "h",
        {},
        [Unit("q1", "sql")],
        None,  # type: ignore[arg-type]
        [registry.load_measures(None)["phone_home"]],
        {"default": None},
        [],
    )
    with Catalog(tmp_path) as cat:
        cat.insert("experiment", {"experiment_id": "e", "bootstrap_seed": 1})
        cell = cells[0]
        cat.insert(
            "cell", {"experiment_id": "e", "cell_key": cell.cell_key, "hardware_manifest_hash": "h"}
        )
        cat.insert("run", {"run_id": "r1", "experiment_id": "e", "cell_key": cell.cell_key})
        cat.insert_many(
            "unit_run",
            [
                {
                    "run_id": "r1",
                    "unit_id": "q1",
                    "rep": 1,
                    "wall_ms": 5.0,
                    "status": "ok",
                    "discarded": False,
                }
            ],
        )
        with pytest.raises(RunError, match="must not use the network"):
            compute_results(cat, "e", prepared, cells, VelociaConfig())
