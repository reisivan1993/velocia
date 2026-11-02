"""Python measure plugins: the ``velocia.measures`` entry point group (docs/04-interfaces.md §5).

For measures a formula can't express. A plugin is an object (or module) with ``name``,
``unit``, ``direction`` ("lower" or "higher") and ``compute(inputs) -> float | None``, where
``inputs`` are the same names formulas use (``unit.wall_s``, ``cluster.nodes``, ``hw.*``, ...).

Plugins must be pure and offline (CLAUDE.md: independence). ``compute`` runs with sockets
blocked; a plugin that tries the network, or raises, fails the run instead of producing a
number. The plugin's source code hash goes into every manifest like a formula's definition.
"""

import hashlib
import inspect
import socket
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from importlib.metadata import EntryPoint, entry_points
from typing import Any

from velocia.config.models import Direction
from velocia.measures.builtins import ClusterInputs, Measure, UnitInputs, Value

GROUP = "velocia.measures"


class PluginError(RuntimeError):
    pass


class NetworkBlocked(PluginError):
    pass


@contextmanager
def no_network() -> Iterator[None]:
    """Block outgoing connections and name lookups from this process while active."""

    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise NetworkBlocked("measure plugins must not use the network")

    saved = (socket.socket.connect, socket.socket.connect_ex, socket.create_connection)
    saved_lookup = socket.getaddrinfo
    socket.socket.connect = refuse  # type: ignore[method-assign]
    socket.socket.connect_ex = refuse  # type: ignore[method-assign]
    socket.create_connection = refuse
    socket.getaddrinfo = refuse
    try:
        yield
    finally:
        socket.socket.connect, socket.socket.connect_ex, socket.create_connection = saved  # type: ignore[method-assign]
        socket.getaddrinfo = saved_lookup


def _source_hash(obj: Any) -> str | None:
    try:
        source = inspect.getsource(inspect.getmodule(obj) or obj)
    except (OSError, TypeError):
        return None
    return "sha256:" + hashlib.sha256(source.encode()).hexdigest()


def plugin_measure(
    entry: EntryPoint, inputs_of: Callable[[UnitInputs, ClusterInputs], dict[str, float | None]]
) -> Measure:
    plugin = entry.load()
    try:
        name, unit = str(plugin.name), str(plugin.unit)
        direction = Direction(str(plugin.direction))
        compute_fn = plugin.compute
    except (AttributeError, ValueError) as exc:
        raise PluginError(
            f"measure plugin {entry.value}: needs name, unit, direction (lower|higher) and "
            f"compute(inputs): {exc}"
        ) from exc

    def compute(u: UnitInputs, c: ClusterInputs) -> Value:
        if u.wall_s is None:
            return None, "unit did not finish"
        try:
            with no_network():
                value = compute_fn(dict(inputs_of(u, c)))
        except PluginError:
            raise
        except Exception as exc:
            raise PluginError(f"measure plugin {name} failed: {exc!r}") from exc
        if value is None:
            return None, f"plugin {name} returned no value"
        return float(value), None

    return Measure(
        name=name,
        unit=unit,
        direction=direction,
        compute=compute,
        source=f"plugin:{entry.value}",
        definition_sha256=_source_hash(plugin),
    )


def plugin_entries() -> list[EntryPoint]:
    return sorted(entry_points(group=GROUP), key=lambda e: e.name)
