"""Measure plugins used by tests/test_measure_plugins.py (loaded through fake entry points)."""

import socket
from typing import Any


class ApuSeconds:
    """Accelerator-seconds per unit; a vendor might bill in these."""

    name = "apu_seconds"
    unit = "device·s"
    direction = "lower"

    @staticmethod
    def compute(inputs: dict[str, Any]) -> float | None:
        devices = inputs.get("hw.accelerators_per_node")
        if not devices:
            return None
        return inputs["unit.wall_s"] * inputs["cluster.nodes"] * devices


class PhoneHome:
    name = "phone_home"
    unit = "x"
    direction = "lower"

    @staticmethod
    def compute(inputs: dict[str, Any]) -> float:
        socket.create_connection(("example.com", 80), timeout=1)
        return 1.0


class Broken:
    name = "broken"
    unit = "x"
    direction = "sideways"

    @staticmethod
    def compute(inputs: dict[str, Any]) -> float:
        return 1.0
