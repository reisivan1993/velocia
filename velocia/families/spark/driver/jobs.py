"""Job units, written with the DataFrame API only (plan D-11).

DataFrame jobs run entirely in Spark's engine (Python only builds the plan), so accelerators
that replace Spark's physical operators, such as Comet, Gluten or a hardware accelerator,
are measured; an RDD job would bypass them.

TeraSort follows the classic benchmark's data shape: 100-byte records, a 10-byte binary key
and a 90-byte binary value, generated deterministically from a seed.
"""

from typing import Any

from .fs import Fs, join

KEY_BYTES = 10
VALUE_BYTES = 90


def teragen(spark: Any, rows: int, seed: int, partitions: int, output: str) -> int:
    """Write ``rows`` records as Parquet; the same arguments always give the same rows."""
    from pyspark.sql import functions as F

    h1 = F.sha2(F.concat_ws(":", F.lit(str(seed)), F.lit("k"), F.col("id").cast("string")), 256)
    h2 = F.sha2(F.concat_ws(":", F.lit(str(seed)), F.lit("v"), F.col("id").cast("string")), 512)
    h3 = F.sha2(F.concat_ws(":", F.lit(str(seed)), F.lit("w"), F.col("id").cast("string")), 256)
    df = spark.range(0, rows, 1, partitions).select(
        F.unhex(F.substring(h1, 1, KEY_BYTES * 2)).alias("key"),
        F.unhex(F.substring(F.concat(h2, h3), 1, VALUE_BYTES * 2)).alias("value"),
    )
    df.write.mode("overwrite").parquet(output)
    return rows


def terasort(spark: Any, tables: dict[str, str], output: str) -> None:
    """The timed unit: read, sort by key, write."""
    spark.read.parquet(tables["tera"]).orderBy("key").write.mode("overwrite").parquet(output)


def _part_files(spark: Any, output: str) -> list[str]:
    """Output part files in name order; ``output`` may be on any storage the cluster reads."""
    names = [
        name
        for name, _, is_dir in Fs(spark).list(output)
        if not is_dir and name.startswith("part-") and name.endswith(".parquet")
    ]
    return [join(output, n) for n in names]


def validate_terasort(spark: Any, tables: dict[str, str], output: str) -> dict[str, Any]:
    """TeraValidate: every output file sorted, files in key order, same rows as the input.

    Returns rows and an order-independent checksum (compared with the reference engine) plus
    ``problem`` when the output is not a valid sort.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    def digest(df: Any) -> tuple[int, str]:
        row = df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.xxhash64("key", "value").cast("decimal(38,0)")).alias("checksum"),
        ).first()
        return int(row["rows"]), str(row["checksum"] or 0)

    in_rows, in_sum = digest(spark.read.parquet(tables["tera"]))
    out_rows, out_sum = digest(spark.read.parquet(output))
    result: dict[str, Any] = {"rows": out_rows, "checksum": out_sum, "problem": None}
    if (in_rows, in_sum) != (out_rows, out_sum):
        result["problem"] = f"output rows/checksum differ from input ({out_rows} vs {in_rows})"
        return result

    previous_max = None
    for path in _part_files(spark, output):
        # coalesce(1) concatenates the file's splits in order, so the row index is file order.
        df = spark.read.parquet(path).coalesce(1).withColumn("_i", F.monotonically_increasing_id())
        ordered = Window.orderBy("_i")
        unsorted = (
            df.withColumn("_prev", F.lag("key").over(ordered))
            .where(F.col("_prev") > F.col("key"))
            .count()
        )
        if unsorted:
            result["problem"] = f"{path.rsplit('/', 1)[-1]}: {unsorted} keys out of order"
            return result
        bounds = df.agg(F.min("key").alias("lo"), F.max("key").alias("hi")).first()
        if bounds["lo"] is None:
            continue  # empty file
        if previous_max is not None and bytes(bounds["lo"]) < previous_max:
            result["problem"] = f"{path.rsplit('/', 1)[-1]} starts below the previous file's end"
            return result
        previous_max = bytes(bounds["hi"])
    return result


JOBS = {"terasort": terasort}
VALIDATORS = {"terasort": validate_terasort}
