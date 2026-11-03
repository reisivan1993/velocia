"""The ``aws`` runner without AWS (ADR-11): every command recorded, nothing created."""

import json
import re
from pathlib import Path

import pytest

from velocia.config.models import AwsCluster
from velocia.runners.aws import (
    AwsError,
    AwsProvisioner,
    BudgetWatchdog,
    DryRunShell,
    estimate_usd,
    hourly_usd,
    instance_platform,
    leftovers,
)

CLUSTER = AwsCluster(
    runner="aws",
    region="us-east-1",
    node_type="m6i.4xlarge",
    node_hour_usd=0.768,
    budget_usd=50,
    executors=4,
    executor_cores=14,
    executor_memory="48g",
)
VPC = json.dumps({"subnetIds": ["subnet-a", "subnet-b"], "clusterSecurityGroupId": "sg-1"})


def _shell() -> DryRunShell:
    return DryRunShell(
        answers={
            "get-caller-identity": "123456789012",
            "describe-cluster": VPC,
            "create-file-system": "fs-1",
            "create-mount-target": "fsmt-x",
            "get-login-password": "pw",
        }
    )


def test_provision_plan(tmp_path: Path) -> None:
    shell = _shell()
    prov = AwsProvisioner(CLUSTER, "velocia-e1", shell, work_dir=tmp_path)
    out = prov.provision(["velocia/engine-spark-vanilla:3.5.9"])
    commands = shell.commands
    first = commands[0]
    assert first.startswith("eksctl create cluster --name velocia-e1 --region us-east-1")
    assert "--node-type m6i.4xlarge --nodes 4 --nodes-min 4 --nodes-max 4" in first
    assert "--tags velocia:run=velocia-e1" in first and "--spot" not in first
    assert any("aws-efs-csi-driver" in c and "123456789012:role/" in c for c in commands)
    assert sum("create-mount-target" in c for c in commands) == 2
    assert any(c.startswith("helm install spark-operator") and "2.5.2" in c for c in commands)
    assert any(
        "ecr create-repository --repository-name velocia/velocia-e1/engine-spark-vanilla" in c
        for c in commands
    )
    registry = "123456789012.dkr.ecr.us-east-1.amazonaws.com"
    assert f"docker push {registry}/velocia/velocia-e1/engine-spark-vanilla:3.5.9" in commands
    assert out.images == {
        "velocia/engine-spark-vanilla:3.5.9": (
            f"{registry}/velocia/velocia-e1/engine-spark-vanilla:3.5.9"
        )
    }
    assert out.k8s.kubeconfig == str(tmp_path / "velocia-e1.kubeconfig")
    assert out.k8s.volume_claim == "velocia" and out.k8s.executors == 4


def test_teardown_reverse_and_past_errors(tmp_path: Path) -> None:
    shell = _shell()
    prov = AwsProvisioner(CLUSTER, "velocia-e1", shell, work_dir=tmp_path)
    prov.provision(["img:1"])
    shell.commands.clear()

    def failing(argv: list[str], stdin: str | None = None) -> str:
        shell(argv, stdin)
        if "delete-file-system" in argv:
            raise AwsError("in use")
        return ""

    prov.shell = failing
    problems = prov.teardown()
    order = [c.split()[3] if c.startswith("aws") else c.split()[1] for c in shell.commands]
    # ECR, two mount targets, wait until they are gone, the file system, then the cluster.
    assert order == ["ecr", "efs", "efs", "efs", "efs", "delete"]
    assert "describe-mount-targets" in shell.commands[3]
    assert shell.commands[-1].startswith("eksctl delete cluster --name velocia-e1")
    assert problems == ["EFS file system fs-1: in use"]  # and the cluster was still deleted


def test_nothing_created_nothing_deleted(tmp_path: Path) -> None:
    shell = DryRunShell()
    assert AwsProvisioner(CLUSTER, "velocia-e2", shell, work_dir=tmp_path).teardown() == []
    assert shell.commands == []


def test_budget() -> None:
    assert hourly_usd(CLUSTER) == pytest.approx(4 * 0.768 + 0.10 + 0.045)
    assert estimate_usd(CLUSTER, 2.0) == round(hourly_usd(CLUSTER) * 2.75, 2)
    now = [0.0]
    capped: list[int] = []
    dog = BudgetWatchdog(CLUSTER, lambda: capped.append(1), clock=lambda: now[0])
    assert not dog.check()
    now[0] = 3600 * 50 / hourly_usd(CLUSTER) - 1
    assert not dog.check()
    now[0] += 2
    assert dog.check() and dog.check() and capped == [1]  # called once


def test_platforms_and_leftovers() -> None:
    assert instance_platform("m6i.4xlarge") == "linux/amd64"
    assert instance_platform("m7g.4xlarge") == "linux/arm64"
    shell = DryRunShell(
        answers={
            "get": json.dumps([{"Name": "velocia-e1"}, {"Name": "prod"}]),
            "describe-file-systems": '["fs-1"]',
            "describe-repositories": '["velocia/velocia-e1/engine-spark-vanilla"]',
        }
    )
    assert leftovers("us-east-1", shell) == {
        "clusters": ["velocia-e1"],
        "efs": ["fs-1"],
        "ecr": ["velocia/velocia-e1/engine-spark-vanilla"],
    }


def test_cost_uses_the_aws_node_price() -> None:
    from velocia.measures.builtins import BUILTINS, ClusterInputs, UnitInputs

    inputs = ClusterInputs.of(CLUSTER, None, 0.40)
    assert BUILTINS["cost_usd"].compute(UnitInputs(wall_s=3600.0), inputs) == (
        pytest.approx(4 * 0.768),
        None,
    )


def test_budget_refuses_before_anything_is_created(tmp_path: Path) -> None:
    from velocia.config.models import Experiment
    from velocia.engines.registry import load_engines
    from velocia.orchestrator.run import RunError, check_aws

    def experiment(budget: float, node_type: str = "m6i.4xlarge") -> Experiment:
        return Experiment.model_validate(
            {
                "name": "a",
                "workload": {"kind": "sql", "suite": "tpch", "scale_factor": 100},
                "engines": ["spark-vanilla"],
                "reference_engine": "spark-vanilla",
                "cluster": {**CLUSTER.model_dump(), "budget_usd": budget, "node_type": node_type},
            }
        )

    class Image:
        architecture = "arm64"  # built on Apple silicon

    known = load_engines(None)
    with pytest.raises(RunError, match="above budget_usd"):
        exp = experiment(1.0)
        check_aws(exp, known, exp.named_clusters, lambda image: None)
    exp = experiment(100_000.0)
    with pytest.raises(RunError, match=re.escape("is linux/arm64 but m6i.4xlarge nodes are")):
        check_aws(exp, known, exp.named_clusters, lambda image: Image())
    exp = experiment(100_000.0, "m7g.4xlarge")  # Graviton: arm64 nodes
    check_aws(exp, known, exp.named_clusters, lambda image: Image())


def test_dry_run_shows_the_plan_and_creates_nothing(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from velocia.cli.app import app

    exp = Path(__file__).parent.parent / "examples" / "aws" / "tpcds-sf100.yaml"
    result = CliRunner().invoke(app, ["run", str(exp), "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "budget_usd $100.00" in result.output and "ABOVE THE CAP" not in result.output
    assert "eksctl create cluster" in result.output and "eksctl delete cluster" in result.output
    assert result.output.index("Creates") < result.output.index("always deletes")


def test_failed_provisioning_is_still_torn_down(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from velocia.config import VelociaConfig
    from velocia.config.models import Experiment
    from velocia.engines.registry import load_engines
    from velocia.experiment.expander import expand
    from velocia.orchestrator.run import Orchestrator
    from velocia.runners.aws import AwsError

    experiment = Experiment.model_validate(
        {
            "name": "a",
            "workload": {"kind": "sql", "suite": "tpch", "scale_factor": 1},
            "engines": ["spark-vanilla"],
            "reference_engine": "spark-vanilla",
            "cluster": CLUSTER.model_dump(),
        }
    )
    prepared = SimpleNamespace(experiment=experiment, engines=load_engines(None))
    lines: list[str] = []
    orch = Orchestrator(prepared, VelociaConfig(), tmp_path, echo=lines.append)  # type: ignore[arg-type]
    shell = _shell()

    def fails_at_efs(argv: list[str], stdin: str | None = None) -> str:
        if "create-file-system" in argv:
            shell(argv, stdin)
            raise AwsError("EFS quota exceeded")
        return shell(argv, stdin)

    orch.shell = fails_at_efs  # type: ignore[assignment]
    with pytest.raises(AwsError, match="quota"):
        orch._cloud("a-20261103", expand(experiment)[0])
    orch._teardown_clouds()
    assert shell.commands[-1].startswith("eksctl delete cluster --name velocia-default-a-20261103")
    assert not any("delete-file-system" in c for c in shell.commands)  # never got an id
    assert any("AWS cluster default: about $" in line for line in lines)


def test_run_asks_before_spending() -> None:
    from typer.testing import CliRunner

    from velocia.cli.app import app

    exp = Path(__file__).parent.parent / "examples" / "aws" / "tpcds-sf100.yaml"
    result = CliRunner().invoke(app, ["run", str(exp)], input="n\n")
    assert result.exit_code == 1
    assert "This creates an EKS cluster (8 x m6i.4xlarge in us-east-1)" in result.output
    assert "not started" in result.output
