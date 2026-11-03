"""The ``aws`` runner: create an EKS cluster for one experiment, run it there, delete it (ADR-11).

Provisioning goes through the standard tools (``eksctl``, ``aws``, ``helm``, ``kubectl``,
``docker``), each call made through a ``Shell``. The real shell runs them; ``DryRunShell``
only records them, which is what ``velocia run --dry-run`` prints and what the tests check. The
cluster then runs the experiment with the ``k8s`` runner.

Everything created is tagged ``velocia:run=<name>`` and deleted in reverse order in a
``finally`` path, continuing past errors; ``velocia teardown`` finds leftovers by tag and name.
The budget is enforced twice: an estimate before anything is created, and a watchdog that
charges elapsed time at the cluster's hourly rate from the first command on.
"""

import json
import shlex
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from velocia.config.models import AwsCluster, K8sCluster

TAG = "velocia:run"
CONTROL_PLANE_USD_PER_HOUR = 0.10  # EKS standard support
NAT_GATEWAY_USD_PER_HOUR = 0.045  # eksctl's default VPC has one NAT gateway
PROVISION_HOURS = 0.75  # create (~20 min) plus delete (~15 min) plus image pushes
OPERATOR_CHART_VERSION = "2.5.2"
EFS_CSI_POLICY = "arn:aws:iam::aws:policy/service-role/AmazonEFSCSIDriverPolicy"
GRAVITON = ("a1.", "c6g", "c7g", "c8g", "m6g", "m7g", "m8g", "r6g", "r7g", "r8g", "t4g", "x2g")


class AwsError(RuntimeError):
    pass


class Shell(Protocol):
    def __call__(self, argv: list[str], stdin: str | None = None) -> str:
        """Run a command; its stdout. Raises AwsError when it fails."""
        ...


class RealShell:
    def __init__(self, echo: Callable[[str], None] = print) -> None:
        self.echo = echo

    def __call__(self, argv: list[str], stdin: str | None = None) -> str:
        result = subprocess.run(argv, input=stdin, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise AwsError(f"{shlex.join(argv[:3])} failed: {result.stderr.strip()[-2000:]}")
        return result.stdout


@dataclass
class DryRunShell:
    """Records commands; answers the lookups the plan needs with placeholders."""

    commands: list[str] = field(default_factory=list)
    answers: dict[str, str] = field(default_factory=dict)

    def __call__(self, argv: list[str], stdin: str | None = None) -> str:
        self.commands.append(shlex.join(argv))
        for marker, answer in self.answers.items():
            if marker in argv:
                return answer
        return "<" + (argv[1] if len(argv) > 1 else argv[0]) + " output>"


def instance_platform(node_type: str) -> str:
    return "linux/arm64" if node_type.startswith(GRAVITON) else "linux/amd64"


def hourly_usd(cluster: AwsCluster) -> float:
    return (
        cluster.executors * cluster.node_hour_usd
        + CONTROL_PLANE_USD_PER_HOUR
        + NAT_GATEWAY_USD_PER_HOUR
    )


def estimate_usd(cluster: AwsCluster, experiment_hours: float) -> float:
    """What a run is expected to cost: provisioning and teardown plus the experiment."""
    return round(hourly_usd(cluster) * (PROVISION_HOURS + experiment_hours), 2)


class BudgetWatchdog:
    """Charges elapsed time at the cluster's hourly rate; calls ``on_cap`` once at the cap."""

    def __init__(
        self,
        cluster: AwsCluster,
        on_cap: Callable[[], None],
        clock: Callable[[], float] = time.monotonic,
        interval_s: float = 30.0,
    ) -> None:
        self.cluster = cluster
        self.on_cap = on_cap
        self.clock = clock
        self.interval_s = interval_s
        self.started = clock()
        self.reached = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._watch, daemon=True)

    def spent_usd(self) -> float:
        return hourly_usd(self.cluster) * (self.clock() - self.started) / 3600

    def check(self) -> bool:
        """True (and ``on_cap`` called once) when the running cost has reached the cap."""
        if not self.reached.is_set() and self.spent_usd() >= self.cluster.budget_usd:
            self.reached.set()
            self.on_cap()
        return self.reached.is_set()

    def _watch(self) -> None:
        while not self._stop.wait(self.interval_s):
            if self.check():
                return

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()


@dataclass
class Provisioned:
    """What the experiment needs from a created cluster."""

    k8s: K8sCluster
    images: dict[str, str]  # local engine image -> ECR image


class AwsProvisioner:
    """Creates and deletes one EKS cluster with shared storage, images and the Spark Operator."""

    def __init__(
        self,
        cluster: AwsCluster,
        name: str,
        shell: Shell,
        work_dir: Path | None = None,
        echo: Callable[[str], None] = lambda _: None,
    ) -> None:
        self.cluster = cluster
        self.name = name  # also the tag value; at most 40 characters for eksctl stacks
        self.shell = shell
        self.echo = echo
        work = work_dir if work_dir is not None else Path(tempfile.gettempdir())
        self.kubeconfig = work / f"{name}.kubeconfig"
        self.created: list[str] = []  # what to delete, in creation order
        self.efs_id: str | None = None
        self.mount_targets: list[str] = []
        self.repositories: list[str] = []

    def _aws(self, *args: str) -> str:
        return self.shell(["aws", "--region", self.cluster.region, *args]).strip()

    def _kubectl(self, *args: str, stdin: str | None = None) -> str:
        return self.shell(["kubectl", "--kubeconfig", str(self.kubeconfig), *args], stdin)

    def provision(self, images: list[str]) -> Provisioned:
        c = self.cluster
        tags = f"{TAG}={self.name}"
        self.echo(f"Creating EKS cluster {self.name} in {c.region} ({c.executors} x {c.node_type})")
        self.created.append("cluster")
        self.shell(
            [
                "eksctl",
                "create",
                "cluster",
                "--name",
                self.name,
                "--region",
                c.region,
                "--version",
                c.kubernetes_version,
                "--nodegroup-name",
                "velocia",
                "--node-type",
                c.node_type,
                "--nodes",
                str(c.executors),
                "--nodes-min",
                str(c.executors),
                "--nodes-max",
                str(c.executors),
                *(["--spot"] if c.spot else []),
                "--managed",
                "--with-oidc",
                "--tags",
                tags,
                "--kubeconfig",
                str(self.kubeconfig),
            ]
        )
        self.echo("Adding shared storage (EFS) and the Spark Operator")
        role = f"{self.name}-efs-csi"[:64]
        self.shell(
            [
                "eksctl",
                "create",
                "iamserviceaccount",
                "--cluster",
                self.name,
                "--region",
                c.region,
                "--namespace",
                "kube-system",
                "--name",
                "efs-csi-controller-sa",
                "--attach-policy-arn",
                EFS_CSI_POLICY,
                "--role-name",
                role,
                "--role-only",
                "--approve",
            ]
        )
        account = self._aws("sts", "get-caller-identity", "--query", "Account", "--output", "text")
        self.shell(
            [
                "eksctl",
                "create",
                "addon",
                "--cluster",
                self.name,
                "--region",
                c.region,
                "--name",
                "aws-efs-csi-driver",
                "--service-account-role-arn",
                f"arn:aws:iam::{account}:role/{role}",
            ]
        )
        vpc = json.loads(
            self._aws(
                "eks",
                "describe-cluster",
                "--name",
                self.name,
                "--query",
                "cluster.resourcesVpcConfig",
                "--output",
                "json",
            )
        )
        self.efs_id = self._aws(
            "efs",
            "create-file-system",
            "--performance-mode",
            "generalPurpose",
            "--encrypted",
            "--tags",
            f"Key={TAG},Value={self.name}",
            "--query",
            "FileSystemId",
            "--output",
            "text",
        )
        self.created.append("efs")
        for subnet in vpc.get("subnetIds", []):
            self.mount_targets.append(
                self._aws(
                    "efs",
                    "create-mount-target",
                    "--file-system-id",
                    self.efs_id,
                    "--subnet-id",
                    subnet,
                    "--security-groups",
                    vpc.get("clusterSecurityGroupId", ""),
                    "--query",
                    "MountTargetId",
                    "--output",
                    "text",
                )
            )
        self.shell(
            [
                "helm",
                "install",
                "spark-operator",
                "spark-operator",
                "--repo",
                "https://kubeflow.github.io/spark-operator",
                "--version",
                OPERATOR_CHART_VERSION,
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                "spark-operator",
                "--create-namespace",
                "--set",
                "spark.jobNamespaces={default}",
                "--set",
                "spark.serviceAccount.name=spark",
                "--wait",
            ]
        )
        self._kubectl("apply", "-f", "-", stdin=self._storage_manifests())

        self.echo("Pushing engine images to ECR")
        registry = f"{account}.dkr.ecr.{c.region}.amazonaws.com"
        password = self._aws("ecr", "get-login-password")
        self.shell(["docker", "login", "--username", "AWS", "--password-stdin", registry], password)
        pushed = {}
        for image in images:
            repo = f"velocia/{self.name}/{image.split('/')[-1].split(':')[0]}"
            tag = image.split(":")[-1] if ":" in image.split("/")[-1] else "latest"
            self._aws(
                "ecr",
                "create-repository",
                "--repository-name",
                repo,
                "--tags",
                f"Key={TAG},Value={self.name}",
            )
            self.repositories.append(repo)
            remote = f"{registry}/{repo}:{tag}"
            self.shell(["docker", "tag", image, remote])
            self.shell(["docker", "push", remote])
            pushed[image] = remote
        k8s = K8sCluster(
            runner="k8s",
            namespace="default",
            executors=c.executors,
            executor_cores=c.executor_cores,
            executor_memory=c.executor_memory,
            driver_memory=c.driver_memory,
            hardware_profile=c.hardware_profile,
            volume_claim="velocia",
            service_account="spark",
            kubeconfig=str(self.kubeconfig),
        )
        return Provisioned(k8s=k8s, images=pushed)

    def _storage_manifests(self) -> str:
        storage_class = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": "velocia-efs"},
            "provisioner": "efs.csi.aws.com",
            "parameters": {
                "provisioningMode": "efs-ap",
                "fileSystemId": self.efs_id,
                "directoryPerms": "777",
            },
        }
        claim = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": "velocia", "namespace": "default"},
            "spec": {
                "accessModes": ["ReadWriteMany"],
                "storageClassName": "velocia-efs",
                "resources": {"requests": {"storage": "100Gi"}},
            },
        }
        return json.dumps({"apiVersion": "v1", "kind": "List", "items": [storage_class, claim]})

    def teardown(self) -> list[str]:
        """Delete everything in reverse order, past errors; what could not be deleted."""
        problems: list[str] = []

        def attempt(what: str, argv: list[str]) -> None:
            try:
                self.shell(argv)
            except AwsError as exc:
                problems.append(f"{what}: {exc}")

        region = self.cluster.region
        for repo in reversed(self.repositories):
            attempt(
                f"ECR repository {repo}",
                [
                    "aws",
                    "--region",
                    region,
                    "ecr",
                    "delete-repository",
                    "--repository-name",
                    repo,
                    "--force",
                ],
            )
        if "efs" in self.created and self.efs_id:
            for target in self.mount_targets:
                attempt(
                    f"EFS mount target {target}",
                    [
                        "aws",
                        "--region",
                        region,
                        "efs",
                        "delete-mount-target",
                        "--mount-target-id",
                        target,
                    ],
                )
            self._wait_mount_targets_gone()
            attempt(
                f"EFS file system {self.efs_id}",
                [
                    "aws",
                    "--region",
                    region,
                    "efs",
                    "delete-file-system",
                    "--file-system-id",
                    self.efs_id,
                ],
            )
        if "cluster" in self.created:
            attempt(
                f"EKS cluster {self.name}",
                [
                    "eksctl",
                    "delete",
                    "cluster",
                    "--name",
                    self.name,
                    "--region",
                    region,
                    "--wait",
                    "--disable-nodegroup-eviction",
                ],
            )
        return problems

    def _wait_mount_targets_gone(self, timeout_s: float = 300) -> None:
        if not self.mount_targets or isinstance(self.shell, DryRunShell):
            return
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                count = self._aws(
                    "efs",
                    "describe-mount-targets",
                    "--file-system-id",
                    str(self.efs_id),
                    "--query",
                    "length(MountTargets)",
                    "--output",
                    "text",
                )
            except AwsError:
                return
            if count.strip() in ("0", ""):
                return
            time.sleep(10)


def leftovers(region: str, shell: Shell) -> dict[str, list[str]]:
    """Velocia-created resources still in ``region``: clusters, EFS file systems, ECR repos."""
    clusters = [
        c["Name"]
        for c in json.loads(shell(["eksctl", "get", "cluster", "--region", region, "-o", "json"]))
        if str(c.get("Name", "")).startswith("velocia-")
    ]
    systems = json.loads(
        shell(
            [
                "aws",
                "--region",
                region,
                "efs",
                "describe-file-systems",
                "--query",
                f"FileSystems[?Tags[?Key=='{TAG}']].FileSystemId",
                "--output",
                "json",
            ]
        )
    )
    repos = json.loads(
        shell(
            [
                "aws",
                "--region",
                region,
                "ecr",
                "describe-repositories",
                "--query",
                "repositories[?starts_with(repositoryName, 'velocia/')].repositoryName",
                "--output",
                "json",
            ]
        )
    )
    return {"clusters": clusters, "efs": list(systems), "ecr": list(repos)}
