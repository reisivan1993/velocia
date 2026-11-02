# Measures

Every measure is computed per unit and repetition and goes through the same statistics (median,
p95, CV, ratio with CI). `velocia measures list` shows them all.

## Built-in

| Measure | Unit | Better | Source |
| --- | --- | --- | --- |
| `wall_time` | s | lower | driver timing |
| `task_cpu_time` | s | lower | event log (see note) |
| `task_run_time` | s | lower | event log |
| `gc_time` | s | lower | event log |
| `shuffle_bytes` | bytes | lower | event log |
| `spill_bytes` | bytes | lower | event log |
| `throughput_rows` | rows/s | higher | input rows / wall time |
| `throughput_bytes` | bytes/s | higher | input bytes / wall time |
| `node_hours` | node·h | lower | wall time x nodes |
| `accelerator_hours` | device·h | lower | node-hours x accelerators per node (hardware profile) |
| `cost_usd` | USD | lower | node-hours x price (hardware profile, else `pricing`) |
| `energy_kwh` | kWh | lower | node-hours x watts (hardware profile; an estimate) |

!!! note "CPU time and accelerators"
    Spark's task CPU time counts the task thread only. Engines that run native code on other
    threads, such as Comet, report less CPU time than they use; the report flags this.

A measure whose input is missing is `null` with a reason, never a guess.

## Your own unit

Put a formula in `measures/<name>.yaml`:

```yaml
name: vendor_cu
unit: CU
direction: lower
description: "Vendor compute units (definition supplied by the vendor)"
formula: "unit.wall_s / 3600 * cluster.nodes * hw.cu_per_node_hour"
```

and the coefficient in the cluster's hardware profile, `hardware/<profile>.yaml`:

```yaml
name: apu-box
price_per_node_hour_usd: 6.10
accelerators_per_node: 2
accelerator_model: "declared-model"
custom: {cu_per_node_hour: 1.0}
```

Formulas are parsed, never executed: numbers, `+ - * / // % **`, `min`, `max`, `abs`, and inputs

- `unit.`: `wall_s`, `task_cpu_s`, `task_run_s`, `gc_s`, `input_rows`, `input_bytes`,
  `output_rows`, `output_bytes`, `shuffle_read_bytes`, `shuffle_write_bytes`, `shuffle_bytes`,
  `spill_bytes`, `stages`, `tasks`, `plan_transitions`, `plan_accelerated_share`,
  `fallback_share` (share of task time in stages without accelerated operators)
- `cluster.`: `nodes`, `executors`, `executor_cores`, `executor_memory_gb`
- `hw.`: `price_per_node_hour_usd`, `watts_per_node`, `accelerators_per_node` and every `custom`
  coefficient of the hardware profile

`velocia measures check measures/vendor_cu.yaml` validates a file. Anything else, an unknown
input, attribute access or a function call, is rejected before a run starts.

## Python plugins

For a measure a formula can't express, install a Python package that registers an object in
the `velocia.measures` entry point group:

```toml
# the plugin package's pyproject.toml
[project.entry-points."velocia.measures"]
apu_seconds = "my_measures:ApuSeconds"
```

```python
class ApuSeconds:
    name = "apu_seconds"
    unit = "device·s"
    direction = "lower"

    @staticmethod
    def compute(inputs):  # the same names formulas use
        devices = inputs.get("hw.accelerators_per_node")
        if not devices:
            return None  # missing input: null with a reason, never a guess
        return inputs["unit.wall_s"] * inputs["cluster.nodes"] * devices
```

Plugins must be pure and offline. `compute` runs with network access blocked; a plugin that
tries the network, or raises, fails the run. Manifests record the plugin and a hash of its
source code.
