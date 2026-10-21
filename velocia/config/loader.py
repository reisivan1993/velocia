"""Load YAML files into config models with errors that name the file and field path."""

from collections.abc import Iterable
from pathlib import Path
from typing import TypeVar

import yaml
from pydantic import BaseModel, ValidationError

from velocia.config.models import UNION_TAGS, FieldError

M = TypeVar("M", bound=BaseModel)


class ConfigError(Exception):
    """A config or definition file is unreadable or invalid."""

    def __init__(self, path: Path, problems: list[tuple[str, str]]) -> None:
        self.path = path
        self.problems = problems
        lines = [f"{path}: {loc or '<root>'}: {msg}" for loc, msg in problems]
        super().__init__("\n".join(lines))


def load_model(path: Path, model: type[M]) -> M:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(path, [("", str(exc))]) from exc
    try:
        return model.model_validate(data if data is not None else {})
    except ValidationError as exc:
        raise ConfigError(path, problems(exc)) from exc


def problems(exc: ValidationError) -> list[tuple[str, str]]:
    """Turn a pydantic error into (field path, message) pairs such as ``engines[1].cluster``."""
    out = []
    for err in exc.errors():
        path = _join(p for p in err["loc"] if isinstance(p, int) or p not in UNION_TAGS)
        message = err["msg"]
        cause = err.get("ctx", {}).get("error")
        if isinstance(cause, FieldError):
            path = f"{path}.{cause.field}" if path else cause.field
            message = cause.message
        elif message.startswith("Value error, "):
            message = message.removeprefix("Value error, ")
        out.append((path, message))
    return out


def _join(parts: Iterable[int | str]) -> str:
    out = ""
    for part in parts:
        out += f"[{part}]" if isinstance(part, int) else (f".{part}" if out else str(part))
    return out
