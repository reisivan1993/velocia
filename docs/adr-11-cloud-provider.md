# ADR-11: the first cloud runner is AWS (EKS + S3)

Status: **accepted** by the owner on 2026-11-03 (plan T50): option A. The runner is built and
tested without creating real AWS resources first; the owner is asked again before the first
real run.

## Context

v0.3 needs one cloud provider where Velocia creates a cluster, runs an experiment, and tears
everything down, with a hard budget cap (`docs/05-quality-and-release.md` §4: credentials only
from the environment, teardown in a `finally` path, a separate `velocia teardown`). The
acceptance run is TPC-DS-derived SF100 with CIs, correctness and cost.

What already exists and shapes the choice:

- The `k8s` runner (Spark Operator `SparkApplication`, one volume claim for data and outputs)
  works and is tested on kind.
- The `external` runner submits to any Spark cluster and handles S3 storage (`s3a`, tested
  against an S3 gateway).
- The key use case is vanilla Apache Spark on CPU vs Speedata's APU. Public information
  (November 2026): the APU is being deployed in Nebul's European sovereign cloud, and Speedata
  invites early access to a "Fast Spark Service for AWS" that keeps data in AWS and offloads
  Spark jobs to APUs. No AWS instance type or Marketplace listing was found.

## Options

| Option | How Velocia uses it | For | Against |
| --- | --- | --- | --- |
| **A. AWS EKS + S3** (recommended) | Cloud runner creates an EKS cluster (or node group) and runs the existing `k8s` runner; datasets and event logs on S3 | Reuses `k8s`; S3 path already tested; same region and storage as Speedata's announced AWS service, so a CPU-vs-APU run compares like with like | EKS control plane ~$0.10/h; cluster creation is slow (~10-15 min); IAM setup |
| B. GCP GKE + GCS | Same shape on GKE | One free zonal/Autopilot control plane per billing account; fast cluster creation | No announced APU presence; GCS needs the `gcs` connector instead of `s3a` |
| C. Azure AKS | Same shape on AKS | Free control-plane tier | No announced APU presence; ADLS connector |
| D. Managed Spark (EMR, Dataproc, Databricks) | `external`-style submission to a service-managed Spark | No Kubernetes to manage | The service's own Spark build and tuning, not the engine images: vanilla vs Comet/Gluten comparisons would not use Velocia's pinned images; each service is its own runner |
| E. Nebul | `external` runner against their Spark | Where the APU is confirmed | A specialist sovereign cloud: an access question for the APU pilot (T48), not a general cloud runner |

Control-plane prices are from 2026 comparison articles, not the providers' pages; check the
official pricing pages before deciding.

## Proposal

**Option A: AWS, EKS with managed node groups, S3 for data and event logs**, implemented as
"provision, then reuse `k8s`":

1. `velocia run` with `cluster: {runner: aws, region, node_type, nodes, spot?, budget_usd}`
   estimates the cost (nodes x hours x list price, plus control plane) and refuses to start
   above `budget_usd`.
2. It creates (or reuses, by tag) an EKS cluster and a node group, installs the Spark
   Operator, creates the volume claim (EFS for ReadWriteMany) or uses S3, then runs the cells
   with the `k8s` runner.
3. A watchdog compares the running cost with the cap and stops and tears down when it is hit.
   Everything Velocia creates carries a `velocia:run` tag; `velocia teardown` deletes all of
   it, and teardown also runs in a `finally` path.
4. Credentials come only from the standard AWS environment (profile or variables).

Rough cost of the SF100 acceptance run (to be re-estimated with current prices): 8 x 16-vCPU
on-demand nodes at about $0.77/h each, plus the control plane, is about $6.3/h; two engines x
99 queries x 5 repetitions plus data generation fits in a few hours, so a cap of $50-100.

## What the owner decides

1. The provider (A as proposed, or B/C/D).
2. An account to use, with a budget alarm on the provider side as a second safety net.
3. The budget cap for the acceptance run.
4. Whether spot instances are acceptable (cheaper; a reclaimed node fails a run, which is then
   reported, not hidden).

## Consequences

- New runner `aws` (provisioning and teardown only); execution stays in `k8s`.
- New optional dependency for the AWS API (`boto3`), installed only with an extra
  (`pip install velocia[aws]`): Velocia's core keeps no cloud dependency.
- CI cannot run the acceptance test; a nightly job with a small cap could, with a CI-only
  account (owner's call).

## Design (as built)

The `aws` runner provisions and tears down; the tested `k8s` runner executes. Provisioning uses
the standard command-line tools, like `docker` and `kubectl` elsewhere in Velocia, so Velocia
keeps no cloud SDK dependency:

| Step | Tool | Torn down by |
| --- | --- | --- |
| Cluster, VPC, node group (on-demand or spot), tagged `velocia:run=<id>` | `eksctl create cluster` | `eksctl delete cluster` |
| EFS CSI driver add-on | `eksctl create addon` | the cluster |
| EFS file system and mount targets (shared `/velocia` volume) | `aws efs` | `aws efs delete-*` |
| ECR repositories for the engine images; push | `aws ecr`, `docker push` | `aws ecr delete-repository` |
| Spark Operator | `helm install` | the cluster |
| StorageClass and the `velocia` volume claim | `kubectl apply` | the cluster |

The steps are data (a plan of commands), so `velocia run --dry-run` shows every command and the
cost estimate without touching AWS, and the tests run them against fake `eksctl`, `aws`,
`helm`, `kubectl` and `docker` executables that record calls.

**Budget:** the cluster states `node_hour_usd` (from the AWS price list; no stale built-in
table) and `budget_usd`. Before anything is created, the estimate (nodes x node price plus the
control plane, over the provisioning time plus the experiment's estimated duration) must be
below the cap, or the run refuses to start. During the run a watchdog charges elapsed time at
the same hourly rate from the moment provisioning starts; at the cap it stops the run, marks it
"stopped: budget cap reached", and tears down.

**Teardown:** always in a `finally` path, in reverse order, continuing past errors and listing
what could not be deleted; `velocia teardown` finds leftovers by their `velocia:run` tag.

## Not done yet

- **No run against real AWS yet**: everything is tested against recorded commands. The first
  real run needs the owner's go-ahead and a small cap.
- **SF100 data**: datasets are copied in from the workstation with `kubectl cp`, which does not
  scale to SF100; on-cluster generation with the official kits (T49) is the next step.
- IAM permissions are listed in `docs/user/aws.md` from the tools' documentation, not yet
  confirmed by a real run.
