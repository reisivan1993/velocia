# Kubernetes

The `k8s` runner runs each Spark application on a Kubernetes cluster through the
[Spark Operator](https://github.com/kubeflow/spark-operator) (`SparkApplication`, cluster
mode): driver and executors are pods.

```yaml
cluster:
  runner: k8s
  namespace: bench
  executors: 4
  executor_cores: 4
  executor_memory: 8g
  node_selector: {pool: cpu}
  hardware_profile: cpu-node
  volume_claim: velocia          # PersistentVolumeClaim, mounted at /velocia
  service_account: spark         # the driver's account (creates executor pods)
```

## What the cluster needs

- The Spark Operator, watching the namespace (`helm install spark-operator
  spark-operator/spark-operator --set "spark.jobNamespaces={bench}"`), and a service account
  for the driver (the chart creates one).
- A PersistentVolumeClaim named by `volume_claim`. Datasets, each run's inputs, outputs and
  event logs live on it. Use `ReadWriteMany` storage (NFS, EFS, CephFS, ...) when executors
  can land on more than one node.
- The engine images, pullable by the nodes (push them to a registry, or `kind load
  docker-image` on kind).
- `kubectl` on this machine, with access to the namespace (`context` picks a kubeconfig
  context).

## What happens in a run

1. Velocia starts a small helper pod with the volume mounted, copies the dataset in once
   (verified by file sizes and marked, so later runs skip the copy), and copies the driver
   program and the run's `run.json` in.
2. It applies one `SparkApplication` per repetition (cold mode) or per series (warm mode) and
   waits for it to finish, within the experiment's timeouts.
3. It copies the outputs and the event log back, deletes the application, the run's files on
   the volume, and the helper pod. Nothing stays running.

The manifest records the nodes the pods may run on (architecture, CPUs, memory, from the
Kubernetes API) plus the hardware profile. The page cache of the nodes is not dropped, so cold
runs are labeled "cold (cache not dropped)". Accelerators: `resources: {vendor.com/apu: 1}`
requests one device per executor (one device type).

Tested in CI on kind with Spark Operator 2.5.2: the same experiment on Kubernetes and on the
local runner gives the same answers.
