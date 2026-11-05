# AWS

The `aws` runner creates an EKS cluster for one experiment, runs it there with the
[Kubernetes](kubernetes.md) runner, and deletes everything afterwards. A hard budget cap is
checked before anything is created and enforced while it runs.

!!! warning "Not yet run against AWS"
    The runner is built and tested against recorded commands, not against a real AWS account.
    The first real run is still to come; check `--dry-run` and keep the budget cap small.

```yaml
cluster:
  runner: aws
  region: us-east-1
  node_type: m6i.4xlarge       # one executor per node
  node_hour_usd: 0.768         # from the AWS price list for your region
  budget_usd: 100              # hard cap
  spot: false
  executors: 8
  executor_cores: 14
  executor_memory: 44g
```

`examples/aws/tpcds-sf100.yaml` is a complete experiment.

## What you need

- An AWS account in your normal command-line setup (profile or environment variables).
  Velocia reads no credentials of its own.
- `eksctl`, `aws`, `helm`, `kubectl` and `docker` on this machine.
- Engine images built for the nodes' architecture: x86 instance types need `linux/amd64`
  images (on Apple silicon, build with `docker buildx --platform linux/amd64`); Graviton
  (`m7g`, `c7g`, ...) needs `linux/arm64`. Velocia refuses a mismatch before starting.
- A budget alarm on the AWS side as a second safety net.

`velocia doctor` shows whether the tools are installed. When a run starts, Velocia checks the
tools and your credentials (`aws sts get-caller-identity`) before it creates anything, so a
missing tool or an expired session costs nothing.

### Permissions

The AWS identity you run as needs:

- What `eksctl` needs to create and delete a cluster: its
  [minimum IAM policies](https://eksctl.io/usage/minimum-iam-policies/) (`AmazonEC2FullAccess`,
  `AWSCloudFormationFullAccess`, plus its `EksAllAccess` and `IamLimitedAccess` policies). The
  IAM part creates the role for the EFS storage driver.
- EFS: create and delete file systems and mount targets, tag them, describe them
  (`elasticfilesystem:CreateFileSystem`, `CreateMountTarget`, `DeleteMountTarget`,
  `DeleteFileSystem`, `Describe*`, `TagResource`).
- ECR: create, tag and delete repositories and push images (`ecr:CreateRepository`,
  `DeleteRepository`, `TagResource`, `GetAuthorizationToken`, `DescribeRepositories`, and the
  image upload actions `BatchCheckLayerAvailability`, `InitiateLayerUpload`,
  `UploadLayerPart`, `CompleteLayerUpload`, `PutImage`).
- `sts:GetCallerIdentity`.

A separate account (or one only for benchmarks) keeps this away from anything else.

## Before you spend anything

```bash
velocia run examples/aws/tpcds-sf100.yaml --dry-run
```

prints the estimated cost against the cap and every command it would run: what it creates
(cluster and node group with `eksctl`, EFS shared storage, ECR repositories for the engine
images, the Spark Operator with `helm`, the volume claim) and what it deletes afterwards.
`velocia run` then shows the estimate again and asks before creating anything (`--yes` skips
the question).

## The budget

The estimate is the nodes' hourly price plus the EKS control plane and a NAT gateway, over
about 45 minutes of creating and deleting plus the experiment's estimated duration. A run whose
estimate is above `budget_usd` does not start. While it runs, Velocia charges elapsed time at
the same rate from the first command on; at the cap it stops the running application, marks the
run "stopped: budget cap reached", and deletes everything. These are list-price estimates, not
your bill.

## Cleaning up

Everything is deleted when the run ends, also after errors and Ctrl+C. If something was left
behind (for example the machine lost power):

```bash
velocia teardown --region us-east-1          # lists what Velocia created and left
velocia teardown --region us-east-1 --yes    # deletes it
```

## Limits

- Datasets are copied from this machine into the cluster's storage with `kubectl cp`. That is
  fine for scale factors up to about 10; SF100 and above needs on-cluster generation with the
  official TPC kits (planned).
- `cost_usd` uses `node_hour_usd` unless the hardware profile sets a price.
