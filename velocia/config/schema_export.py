"""Write the published JSON Schemas to ``schemas/``: ``python -m velocia.config.schema_export``."""

import json
import sys
from pathlib import Path

from pydantic import BaseModel

from velocia.config.models import EngineDef, Experiment, HardwareProfile, MeasureDef, VelociaConfig
from velocia.datagen.manifest import DatasetManifest

SCHEMAS: dict[str, type[BaseModel]] = {
    "velocia-config": VelociaConfig,
    "experiment": Experiment,
    "engine": EngineDef,
    "measure": MeasureDef,
    "hardware-profile": HardwareProfile,
    "dataset-manifest": DatasetManifest,
}


def render(model: type[BaseModel], name: str) -> str:
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"urn:velocia:schema:{name}:v1",
        **model.model_json_schema(),
    }
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def main(out_dir: Path = Path("schemas")) -> None:
    out_dir.mkdir(exist_ok=True)
    for name, model in SCHEMAS.items():
        (out_dir / f"{name}.schema.json").write_text(render(model, name), encoding="utf-8")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("schemas"))
