"""Thin wrapper over the Docker CLI for engine images."""

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


class DockerError(RuntimeError):
    pass


@dataclass(frozen=True)
class ImageInfo:
    image_id: str
    repo_digests: tuple[str, ...]
    architecture: str


def _docker(*args: str, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    if shutil.which("docker") is None:
        raise DockerError("docker CLI not found on PATH")
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )
    if result.returncode != 0:
        raise DockerError(f"docker {args[0]} failed: {result.stderr.strip()}")
    return result


def inspect_image(image: str) -> ImageInfo | None:
    try:
        result = _docker("image", "inspect", image, timeout=30)
    except DockerError:
        return None
    data = json.loads(result.stdout)[0]
    return ImageInfo(
        image_id=data["Id"],
        repo_digests=tuple(data.get("RepoDigests") or ()),
        architecture=data.get("Architecture", ""),
    )


def build_image(context: Path, tag: str) -> ImageInfo:
    _docker("build", "--tag", tag, str(context), timeout=3600)
    info = inspect_image(tag)
    if info is None:
        raise DockerError(f"image {tag} not found after build")
    return info


def run_in_image(image: str, *command: str, timeout: float = 300) -> str:
    """Run a command in a throwaway container and return stdout followed by stderr."""
    result = _docker("run", "--rm", image, *command, timeout=timeout)
    return result.stdout + result.stderr
