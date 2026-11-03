"""Generate the reference pages of the user docs: ``python -m velocia.docs_reference``.

``docs/user/reference/cli.md`` comes from the CLI itself (every command, argument and option)
and ``docs/user/reference/files.md`` from the published JSON Schemas (every field of every
file format), so neither can drift from the code; a test checks the committed pages are current.
"""

import sys
from pathlib import Path
from typing import Any

import typer

from velocia.cli.app import app
from velocia.config.schema_export import SCHEMAS

OUT = Path("docs/user/reference")

FILES = {
    "velocia-config": ("velocia.yaml", "Optional settings in the working directory."),
    "experiment": ("Experiment files", "What to run: `velocia run <file>`."),
    "engine": ("Engine definitions", "`engines/<name>.yaml` in `engines_dir`."),
    "measure": ("Measure definitions", "`measures/<name>.yaml` in `measures_dir`."),
    "hardware-profile": ("Hardware profiles", "`hardware/<name>.yaml` in `hardware_dir`."),
    "synth": ("Synth specs", "`velocia datagen --suite synth --spec <file>` and `workload.synth`."),
    "dataset-manifest": ("Dataset manifests", "Written by `velocia datagen`; read-only."),
    "bundle": ("Benchmark bundles (`bundle.json`)", "Written by `velocia bundle`; read-only."),
}


def _cell(text: Any) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def _commands(group: Any, prefix: str) -> list[tuple[str, Any]]:
    # Typer ships its own copy of click, so the command tree is walked by shape, not class.
    out = []
    for name in sorted(group.commands):
        command = group.commands[name]
        path = f"{prefix} {name}"
        if hasattr(command, "commands"):
            out += _commands(command, path)
        else:
            out.append((path, command))
    return out


def cli_page() -> str:
    root = typer.main.get_command(app)
    lines = [
        "# CLI reference",
        "",
        "Generated from the CLI by `python -m velocia.docs_reference`; do not edit.",
        "",
    ]
    for path, command in _commands(root, "velocia"):
        lines += [f"## `{path}`", "", (command.help or "").strip().split("\n\n")[0], ""]
        params = [p for p in command.params if p.name != "help"]
        if not params:
            continue
        lines += ["| Argument or option | Default | Meaning |", "| --- | --- | --- |"]
        for p in params:
            if p.param_type_name == "argument":
                name = f"`{p.human_readable_name}`"
                default = "" if p.required or p.default is None else _cell(p.default)
                meaning = getattr(p, "help", None) or ""
            else:
                name = ", ".join(f"`{o}`" for o in p.opts)
                default = "" if p.default in (None, False) or p.is_flag else _cell(p.default)
                meaning = getattr(p, "help", None) or ""
            choices = getattr(p.type, "choices", None)
            if choices:
                meaning += f" One of: {', '.join(str(c) for c in choices)}."
            lines.append(f"| {name} | {default} | {_cell(meaning)} |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _type(prop: dict[str, Any]) -> str:
    if "$ref" in prop:
        name = prop["$ref"].rsplit("/", 1)[-1]
        return f"[{name}](#{name.lower()})"
    if "enum" in prop:
        return " \\| ".join(f"`{v}`" for v in prop["enum"])
    if "const" in prop:
        return f"`{prop['const']}`"
    for key in ("anyOf", "oneOf"):
        if key in prop:
            parts = [_type(p) for p in prop[key] if p.get("type") != "null"]
            optional = any(p.get("type") == "null" for p in prop[key])
            return " or ".join(dict.fromkeys(parts)) + (" (optional)" if optional else "")
    kind = prop.get("type", "any")
    if kind == "array":
        return f"list of {_type(prop.get('items', {}))}"
    if kind == "object" and "additionalProperties" in prop:
        values = prop["additionalProperties"]
        return f"map of {_type(values) if isinstance(values, dict) else 'any'}"
    return str(kind)


def _fields(title: str, schema: dict[str, Any], level: str) -> list[str]:
    props = schema.get("properties") or {}
    required = set(schema.get("required") or [])
    lines = [f"{level} {title}", ""]
    if schema.get("description"):
        lines += [" ".join(schema["description"].split()), ""]
    if not props:
        return lines
    lines += ["| Field | Type | Default | Meaning |", "| --- | --- | --- | --- |"]
    for name, prop in props.items():
        default = "required" if name in required else _cell(repr(prop.get("default", "")))
        meaning = _cell(prop.get("description", ""))
        lines.append(f"| `{name}` | {_type(prop)} | {default} | {meaning} |")
    return [*lines, ""]


def files_page() -> str:
    lines = [
        "# File formats",
        "",
        "Every field of every file Velocia reads or writes, generated from the published JSON",
        "Schemas in `schemas/` by `python -m velocia.docs_reference`; do not edit.",
        "",
    ]
    for key, (title, what) in FILES.items():
        schema = SCHEMAS[key].model_json_schema()
        lines += [f"## {title}", "", f"{what} Schema: `schemas/{key}.schema.json`.", ""]
        lines += _fields("Top level", schema, "###")[2:] if schema.get("properties") else []
        for name, definition in sorted((schema.get("$defs") or {}).items()):
            lines += _fields(name, definition, "###")
    return "\n".join(lines).rstrip() + "\n"


def pages() -> dict[str, str]:
    return {"cli.md": cli_page(), "files.md": files_page()}


def main(out: Path = OUT) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for name, text in pages().items():
        (out / name).write_text(text, "utf-8")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else OUT)
