"""Find and load engine definitions (docs/04-interfaces.md §4).

Built-in definitions ship inside the package. A definition with the same name in the user's
engines directory replaces the built-in one, so users can pin a different image.
"""

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from velocia.config import ConfigError, EngineDef, load_model

BUILTIN_DIR = Path(str(files("velocia.engines") / "builtin"))


@dataclass(frozen=True)
class Engine:
    definition: EngineDef
    source: Path

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def builtin(self) -> bool:
        return self.source.parent == BUILTIN_DIR

    @property
    def build_context(self) -> Path | None:
        """Directory with this engine's Dockerfile, if it has one."""
        context = self.source.parent / self.name
        return context if (context / "Dockerfile").is_file() else None


def _load_dir(directory: Path) -> dict[str, Engine]:
    engines: dict[str, Engine] = {}
    if not directory.is_dir():
        return engines
    for path in sorted(directory.glob("*.yaml")):
        definition = load_model(path, EngineDef)
        if definition.name != path.stem:
            raise ConfigError(path, [("name", f"must match the file name {path.stem!r}")])
        engines[definition.name] = Engine(definition, path)
    return engines


def load_engines(user_dir: Path | None = None) -> dict[str, Engine]:
    engines = _load_dir(BUILTIN_DIR)
    if user_dir is not None:
        engines.update(_load_dir(user_dir))
    return dict(sorted(engines.items()))


def get_engine(name: str, user_dir: Path | None = None) -> Engine:
    engines = load_engines(user_dir)
    if name not in engines:
        known = ", ".join(engines) or "none"
        raise KeyError(f"unknown engine {name!r} (known: {known})")
    return engines[name]
