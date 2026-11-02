# Engines

An engine is a container image plus Spark configuration. Built in:

| Engine | Image |
| --- | --- |
| `spark-vanilla` | the official Apache Spark 3.5.9 image (Java 17, Python 3), pinned by digest |
| `spark-comet` | the same base with Apache DataFusion Comet 1.1.0 and its Spark configuration |
| `spark-gluten` | the same base with Apache Gluten 1.7.0 (Velox backend); **x86 (linux/amd64) only** |

`velocia engines list` shows them; `velocia engines build <name>` builds an image locally.
Velocia's driver program is shipped with each run, not baked into images, so any image with
Apache Spark 3.5 and Python 3 works.

Gluten's official build has native code for x86 only. On Apple silicon (arm64 Docker) Velocia
refuses it before starting, because under emulation it crashes; run it on an x86 machine or
cluster. An engine file declares this with `platforms: [linux/amd64]`.

## Adding an engine

Create `engines/<name>.yaml` (the directory is `engines_dir` in `velocia.yaml`):

```yaml
name: spark-myaccel
family: spark
image: registry.example.com/spark-myaccel@sha256:...   # pin by digest
spark_versions: ["3.5"]
spark_conf:
  spark.plugins: com.example.AcceleratorPlugin
  spark.memory.offHeap.enabled: "true"
  spark.memory.offHeap.size: 2g
platforms: [linux/amd64]          # optional: where the image runs natively
notes: "Vendor accelerator X, driver 1.2"
```

A file with the name of a built-in engine replaces it, for example to pin a different image.
Then check it:

```bash
velocia engines check spark-myaccel --spark 3.5
```

This runs TPC-H-derived q1, q3, q6 at scale factor 0.01 and a 100,000-row TeraSort against
`spark-vanilla`, and fails unless every result matches.

Hardware accelerators that run on their own servers (for example an APU appliance exposing the
regular Spark API) use the `external` runner: see [External clusters](external.md).
