# Troubleshooting

Start with `velocia doctor`: it checks Docker, its CPUs and memory, the engine images, free
disk, and DuckDB's TPC-H extension.

**"dataset ... not found"**: generate it first with the command the message shows, for example
`velocia datagen --suite tpch --sf 1`.

**"image ... not found"**: `velocia engines build <engine>`.

**Applications fail with out-of-memory errors**: give Docker more memory (Docker Desktop:
Settings → Resources) or lower `executor_memory`. In local mode the driver gets
`executors x executor_memory`; Comet adds 2 GB of off-heap memory by default.

**"quiet check flagged"**: another heavy program or other containers were busy. Results still
come out, flagged; close other work for steadier numbers.

**"host slept ... timings unreliable"**: the computer went to sleep during a run (for example the
lid was closed). Rerun the experiment.

**High variation flags (CV > 10%)**: add repetitions, close other programs, or use larger data so
that each unit runs longer than a few hundred milliseconds.

**An interrupted run**: `velocia run exp.yaml --resume` continues from the first unfinished cell.
Containers of an interrupted run are removed automatically.

**DuckDB downloads an extension**: the first TPC-H data generation downloads DuckDB's `tpch`
extension once. This is the only network access besides pulling images and your own clusters;
Velocia sends no telemetry.
