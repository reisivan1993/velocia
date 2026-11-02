"""The viewer (T46/T47): localhost only, read-only, offline pages over the catalog."""

import re
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import pytest
from conftest import SECRET_MARKER, PlantedSecrets
from test_bundle import EXP, _results

from velocia.store.catalog import Catalog
from velocia.ui.server import UiError, check_host, make_server


@pytest.fixture
def viewer(tmp_path: Path, planted_secrets: PlantedSecrets) -> Iterator[str]:
    results = _results(tmp_path, planted_secrets)
    with Catalog(results) as catalog:
        catalog.update("experiment", {"experiment_id": EXP}, {"name": "e"})
    server = make_server(results, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def _get(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def test_localhost_only() -> None:
    for host in ("127.0.0.1", "localhost", "::1"):
        check_host(host)
    for host in ("0.0.0.0", "192.168.1.5", "example.com", "::"):
        with pytest.raises(UiError, match="localhost only"):
            check_host(host)


def test_pages(viewer: str) -> None:
    status, index = _get(viewer + "/")
    assert status == 200 and EXP in index and "/history/e" in index
    status, report = _get(f"{viewer}/e/{EXP}")
    assert status == 200 and "Velocia for Apache Spark" in report and EXP in report
    status, data = _get(f"{viewer}/e/{EXP}/report.json")
    assert status == 200 and '"format": "velocia-report"' in data
    status, history = _get(viewer + "/history/e")
    assert status == 200 and "over time" in history
    status, comparison = _get(f"{viewer}/compare?a={EXP}&b={EXP}")
    assert status == 200 and "reproduce" in comparison and "medians of the rerun" in comparison
    for page in (index, report, history, comparison):
        # Offline: nothing loads from anywhere else.
        assert re.search(r'<(script|link|img)[^>]+(src|href)="https?://', page) is None
        assert SECRET_MARKER not in page
    assert _get(viewer + "/e/nope")[0] == 404
    assert _get(viewer + "/nothing/here")[0] == 404


def test_read_only(viewer: str) -> None:
    request = urllib.request.Request(viewer + "/", data=b"x", method="POST")
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(request, timeout=10)
    assert exc.value.code == 405
