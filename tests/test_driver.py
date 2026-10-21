import subprocess
import sys
import threading
import zipfile
from pathlib import Path
from typing import Any

from velocia.families.spark.driver.units import Unit, run_units, tag
from velocia.families.spark.package import ZIP_PACKAGE, build_driver, driver_zip_bytes

UNITS = [Unit("q1", "select 1"), Unit("q2", "select 2")]


def _run(execute: Any, warmup: bool = False, reps: list[int] | None = None,
         timeout_s: float = 5.0) -> tuple[list[dict[str, Any]], list[str]]:  # fmt: skip
    results: list[dict[str, Any]] = []
    cancelled: list[str] = []
    run_units(UNITS, "r1", reps or [1], warmup, timeout_s, execute, cancelled.append,
              results.append)  # fmt: skip
    return results, cancelled


def test_tags_and_order() -> None:
    seen: list[str] = []
    results, _ = _run(lambda unit, t: seen.append(t), reps=[1, 2])
    assert seen == ["velocia:r1:q1:1", "velocia:r1:q2:1", "velocia:r1:q1:2", "velocia:r1:q2:2"]
    assert [r["status"] for r in results] == ["ok"] * 4
    assert all(r["wall_ms"] >= 0 and not r["discarded"] for r in results)
    assert tag("r", "q", 3) == "velocia:r:q:3"


def test_warmup_is_rep_zero_and_discarded() -> None:
    results, _ = _run(lambda unit, t: None, warmup=True, reps=[1, 2, 3])
    assert [(r["rep"], r["discarded"]) for r in results[:2]] == [(0, True), (0, True)]
    assert [r["rep"] for r in results[2:]] == [1, 1, 2, 2, 3, 3]
    assert not any(r["discarded"] for r in results[2:])


def test_failed_unit_is_reported_and_loop_continues() -> None:
    def execute(unit: Unit, t: str) -> None:
        if unit.id == "q1":
            raise RuntimeError("boom")

    results, _ = _run(execute)
    assert [(r["unit_id"], r["status"]) for r in results] == [("q1", "failed"), ("q2", "ok")]
    assert results[0]["error"] == "RuntimeError: boom"
    assert results[0]["wall_ms"] is None


def test_timeout_cancels_the_unit_and_is_reported() -> None:
    def execute(unit: Unit, t: str) -> None:
        if unit.id == "q1":
            released = threading.Event()
            cancel_hook.append(released.set)
            if not released.wait(5):
                raise AssertionError("never cancelled")
            raise RuntimeError("job group cancelled")

    cancel_hook: list[Any] = []
    results: list[dict[str, Any]] = []
    cancelled: list[str] = []

    def cancel(t: str) -> None:
        cancelled.append(t)
        cancel_hook[0]()

    run_units(UNITS, "r1", [1], False, 0.2, execute, cancel, results.append)
    assert cancelled == ["velocia:r1:q1:1"]
    assert [r["status"] for r in results] == ["timeout", "ok"]
    assert results[0]["wall_ms"] is None


def test_driver_zip_is_reproducible_and_self_contained(tmp_path: Path) -> None:
    assert driver_zip_bytes() == driver_zip_bytes()
    package = build_driver(tmp_path)
    assert package.zip_path.name == f"velocia-driver-{package.sha256[7:19]}.zip"
    names = zipfile.ZipFile(package.zip_path).namelist()
    assert f"{ZIP_PACKAGE}/main.py" in names and f"{ZIP_PACKAGE}/units.py" in names
    # The zip imports on its own, without the rest of velocia on the path.
    out = subprocess.run(
        [sys.executable, "-I", "-c",
         "import sys; sys.path.insert(0, sys.argv[1]); import velocia_driver.main, "
         "velocia_driver.units; print('velocia' in sys.modules)", str(package.zip_path)],
        capture_output=True, text=True, check=True, cwd=tmp_path,
    )  # fmt: skip
    assert out.stdout.strip() == "False"


def test_driver_type_checks_for_python_310() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--python-version", "3.10",
         "velocia/families/spark/driver"],
        capture_output=True, text=True, cwd=Path(__file__).parent.parent,
    )  # fmt: skip
    assert result.returncode == 0, result.stdout
