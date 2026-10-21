"""Built-in hardware probe for the ``local`` runner and the hardware manifest hash.

The local runner's node is this machine as Docker sees it: on macOS and Windows that is Docker
Desktop's VM (its CPUs and memory), on Linux the host. Both the host and the Docker view are
recorded; the hash uses the facts that decide performance and leaves out hostnames, timestamps
and free disk, so the same hardware hashes the same across days (design §5.6).
"""

import hashlib
import json
import os
import platform
import subprocess
import sys
from typing import Any

from velocia.config.models import HardwareProfile

GIB = 1024**3


def _run(*cmd: str) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def _host() -> dict[str, Any]:
    cpu_model = None
    cores = None
    memory = None
    if sys.platform == "darwin":
        cpu_model = _run("sysctl", "-n", "machdep.cpu.brand_string") or None
        cores = int(_run("sysctl", "-n", "hw.ncpu") or 0) or None
        memory = int(_run("sysctl", "-n", "hw.memsize") or 0) or None
    elif sys.platform == "linux":
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("model name"):
                        cpu_model = line.split(":", 1)[1].strip()
                        break
            with open("/proc/meminfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        memory = int(line.split()[1]) * 1024
                        break
        except OSError:
            pass
        cores = os.cpu_count()
    return {
        "os": platform.system(),
        "arch": platform.machine(),
        "cpu_model": cpu_model,
        "cores": cores,
        "memory_gib": round(memory / GIB) if memory else None,
    }


def _docker() -> dict[str, Any] | None:
    out = _run("docker", "info", "--format", "{{json .}}")
    if not out:
        return None
    info = json.loads(out)
    return {
        "cpus": info.get("NCPU"),
        "memory_gib": round(info["MemTotal"] / GIB) if info.get("MemTotal") else None,
        "os": info.get("OperatingSystem"),
        "arch": info.get("Architecture"),
        "server_version": info.get("ServerVersion"),
    }


def probe_local(profile: HardwareProfile | None = None) -> dict[str, Any]:
    """What the manifest records about the ``local`` runner's single node."""
    accelerators = None
    if profile is not None and profile.accelerators_per_node:
        accelerators = {
            "model": profile.accelerator_model,
            "count": profile.accelerators_per_node,
            "source": "declared",
        }
    return {
        "node_types": [
            {
                "name": "local",
                "count": 1,
                "host": _host(),
                "docker": _docker(),
                "disk_type": None,  # not probed in v0.1
                "accelerators": accelerators,
                "source": "detected",
            }
        ],
        "profile": profile.model_dump(mode="json") if profile is not None else None,
    }


def hardware_manifest_hash(hardware: dict[str, Any]) -> str:
    """SHA-256 of the performance-relevant facts only (design §5.6)."""
    facts = []
    for node in hardware["node_types"]:
        host = node.get("host") or {}
        docker = node.get("docker") or {}
        facts.append(
            {
                "count": node["count"],
                "cpu_model": host.get("cpu_model"),
                "arch": host.get("arch"),
                "cores": docker.get("cpus") or host.get("cores"),
                "memory_gib": docker.get("memory_gib") or host.get("memory_gib"),
                "disk_type": node.get("disk_type"),
                "accelerators": node.get("accelerators"),
                "source": node.get("source"),
            }
        )
    canonical = json.dumps(facts, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
