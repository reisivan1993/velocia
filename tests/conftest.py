"""Shared fixtures.

``planted_secrets`` is reused by every test of a writer (manifests, event logs, reports, bundles):
feed its inputs through the writer, then call ``assert_absent`` on everything written.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

SECRET_MARKER = "PLANTED-SECRET"


@dataclass(frozen=True)
class PlantedSecrets:
    """Spark configs, environment and an event-log line with secrets in every known hiding place."""

    spark_conf: dict[str, Any] = field(
        default_factory=lambda: {
            "spark.app.name": "tpch-q1",
            "spark.hadoop.fs.s3a.access.key": f"{SECRET_MARKER}-1",
            "spark.hadoop.fs.s3a.secret.key": f"{SECRET_MARKER}-2",
            "spark.hadoop.fs.azure.account.key.acct.blob.core.windows.net": f"{SECRET_MARKER}-3",
            "spark.ssl.keyPassword": f"{SECRET_MARKER}-4",
            "spark.authenticate.secret": f"{SECRET_MARKER}-5",
            "spark.kubernetes.authenticate.submission.oauthToken": f"{SECRET_MARKER}-6",
            "spark.hadoop.fs.s3a.aws.credentials.provider": f"{SECRET_MARKER}-7",
            "spark.driver.extraJavaOptions": (
                f"-Xss4m -Djavax.net.ssl.trustStorePassword={SECRET_MARKER}-8"
            ),
            "spark.eventLog.dir": f"s3a://AKIAXXXX:{SECRET_MARKER}-9/x@bucket/logs",
            "spark.velocia.nested": {"db": {"password": f"{SECRET_MARKER}-10", "user": "bench"}},
        }
    )
    env: dict[str, str] = field(
        default_factory=lambda: {
            "HOME": "/home/spark",
            "AWS_SECRET_ACCESS_KEY": f"{SECRET_MARKER}-11",
            "AWS_SESSION_TOKEN": f"{SECRET_MARKER}-12",
            "PGPASSWORD": f"{SECRET_MARKER}-13",
            "MYCORP_VAULT_ADDR": f"{SECRET_MARKER}-14",  # only caught by a user pattern
        }
    )
    user_patterns: tuple[str, ...] = ("*vault*",)
    # Apache Spark 3.5 SparkListenerEnvironmentUpdate, trimmed to the parts that carry secrets.
    event_log_line: str = json.dumps(
        {
            "Event": "SparkListenerEnvironmentUpdate",
            "Spark Properties": {"spark.hadoop.fs.s3a.secret.key": f"{SECRET_MARKER}-15"},
            "Hadoop Properties": {"fs.s3a.secret.key": f"{SECRET_MARKER}-16"},
            "System Properties": {
                "sun.java.command": "org.apache.spark.deploy.SparkSubmit "
                f"--conf spark.hadoop.fs.s3a.secret.key={SECRET_MARKER}-17 "
                f"--conf 'spark.ssl.keyStorePassword={SECRET_MARKER}-18' velocia_driver.py"
            },
            "Classpath Entries": {"/opt/spark/jars/spark-core_2.12-3.5.9.jar": "System Classpath"},
        }
    )

    @staticmethod
    def assert_absent(written: str | Path) -> None:
        """Fail if any planted secret appears in ``written`` (text, a file or a directory tree)."""
        if isinstance(written, Path):
            paths = [written]
            if written.is_dir():
                paths = [p for p in written.rglob("*") if p.is_file()]
            for path in paths:
                assert SECRET_MARKER.encode() not in path.read_bytes(), f"secret leaked in {path}"
        else:
            assert SECRET_MARKER not in written, f"secret leaked: {written!r}"


@pytest.fixture
def planted_secrets() -> PlantedSecrets:
    return PlantedSecrets()
