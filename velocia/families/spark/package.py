"""Package the driver program for shipping at submit time (ADR-3, docs/06-design.md §5.1).

The zip is byte-for-byte reproducible (sorted entries, fixed timestamps and permissions), so its
SHA-256 identifies the driver code exactly; the hash is in the file name and in every manifest.
"""

import hashlib
import io
import zipfile
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

DRIVER_SOURCE = Path(str(files("velocia.families.spark.driver")))
ZIP_PACKAGE = "velocia_driver"  # import name inside the zip
LAUNCHER_NAME = "velocia_launch.py"
LAUNCHER = "from velocia_driver.main import main\n\nmain()\n"
_FIXED_TIME = (1980, 1, 1, 0, 0, 0)


@dataclass(frozen=True)
class DriverPackage:
    zip_path: Path
    launcher_path: Path
    sha256: str


def driver_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(DRIVER_SOURCE.glob("*.py")):
            info = zipfile.ZipInfo(f"{ZIP_PACKAGE}/{path.name}", date_time=_FIXED_TIME)
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, path.read_bytes())
    return buffer.getvalue()


def build_driver(dest: Path) -> DriverPackage:
    """Write ``velocia-driver-<hash12>.zip`` and the launcher script into ``dest``."""
    data = driver_zip_bytes()
    digest = hashlib.sha256(data).hexdigest()
    dest.mkdir(parents=True, exist_ok=True)
    zip_path = dest / f"velocia-driver-{digest[:12]}.zip"
    zip_path.write_bytes(data)
    launcher = dest / LAUNCHER_NAME
    launcher.write_text(LAUNCHER, "utf-8")
    return DriverPackage(zip_path, launcher, f"sha256:{digest}")
