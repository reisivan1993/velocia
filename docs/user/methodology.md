# Methodology

## Cells, runs and units

An experiment expands into **cells**: one engine on one cluster with one configuration and one
mode. Each cell runs `repetitions` times; each repetition runs every **unit** (a query, or a job
step such as TeraSort). Lists in `spark_conf` expand into one configuration per value.
`velocia run --dry-run` shows the cells before anything starts.

Cells of different engines with the same configuration and mode share a `config_hash`; that is
how a candidate engine's results are paired with the reference engine's.

## Timing

- Each unit is tagged with a Spark job description (`velocia:<run_id>:<unit_id>:<rep>`), run
  into Spark's `noop` sink (no results are collected to the driver), and timed in the driver with
  a monotonic clock, including planning.
- **Cold** mode starts a fresh Spark application for each repetition. Where Velocia can, it also
  drops the operating system's page cache (Linux hosts); elsewhere, for example on macOS, the
  report says **cold (cache not dropped)**.
- **Warm** mode runs one discarded warm-up pass, then all repetitions in the same application.
- Each unit has a timeout (`timeout_per_unit`); a unit that hits it is cancelled and reported as a
  timeout, never dropped silently.

## Statistics

For every cell, unit and measure Velocia reports the median, p95 and coefficient of variation
(CV) over repetitions; a CV above 10% is flagged. Against the reference engine it reports the
**ratio of medians**, oriented so that **> 1 always means better than the reference** (reference
/ candidate for lower-is-better measures such as time; candidate / reference for higher-is-better
ones such as throughput). For wall time that ratio is the speedup.

The 95% confidence interval comes from a percentile bootstrap (10,000 resamples by default):
candidate and reference repetitions are resampled independently, the ratio of medians is
recomputed each time, and the 2.5th and 97.5th percentiles are reported. The seed is stored with
the results, so a report is reproducible bit for bit. With fewer than 5 repetitions the interval
is still computed, but it is wide, and the report says so.

The **total** row sums all units of a repetition (only repetitions where every unit finished) and
is the headline comparison.

## Correctness

After the timed runs, each cell runs one untimed pass:

- **SQL units:** row count and an order-independent checksum of the result
  (`sum(xxhash64(...))`, floating and decimal columns rounded to 4 decimals because engines add
  floats in different orders). It must equal the reference engine's.
- **Job units:** the job's validator. TeraSort's checks that every output file is sorted, that
  files follow each other in key order, and that the output has the same rows as the input.

A mismatch marks the unit **FAILED**: its ratio is never shown, and the total fails too.

## Fair comparisons

- Results are compared only within one **hardware manifest** (CPU model, cores, memory,
  accelerators; not hostnames or free disk). Comparing engines on different hardware, such as
  vanilla Spark on CPU nodes against a hardware accelerator, needs
  `comparison.allow_cross_hardware: true` and at least one normalized measure (cost,
  node-hours, accelerator-hours or a custom unit) next to time.
- Before each cell a **quiet check** looks for other Velocia containers and a busy CPU, and flags
  the cell if the machine isn't quiet. On macOS, `velocia run` also keeps the machine from idle
  sleep; if it sleeps anyway, the affected runs are flagged as unreliable.

## Reproducibility

`velocia compare <first> <rerun>` checks whether a rerun reproduces an experiment: for at least
90% of cells the rerun's median falls inside a 95% interval around the first run's median, and no
engine's ratio interval moves from better to worse than the reference.
