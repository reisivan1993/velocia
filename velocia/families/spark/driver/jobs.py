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


def _digest(df: Any, cols: tuple[str, ...]) -> tuple[int, str]:
    from pyspark.sql import functions as F

    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("checksum"),
    ).first()
    return int(row["rows"]), str(row["checksum"] or 0)


def validate_sorted(
    spark: Any, input_path: str, output: str, key: str, cols: tuple[str, ...]
) -> dict[str, Any]:
    """TeraValidate: every output file sorted by ``key``, files in key order, same rows as the
    input.

    Returns rows and an order-independent checksum (compared with the reference engine) plus
    ``problem`` when the output is not a valid sort.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    in_rows, in_sum = _digest(spark.read.parquet(input_path), cols)
    out_rows, out_sum = _digest(spark.read.parquet(output), cols)
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
            df.withColumn("_prev", F.lag(key).over(ordered))
            .where(F.col("_prev") > F.col(key))
            .count()
        )
        if unsorted:
            result["problem"] = f"{path.rsplit('/', 1)[-1]}: {unsorted} keys out of order"
            return result
        bounds = df.agg(F.min(key).alias("lo"), F.max(key).alias("hi")).first()
        if bounds["lo"] is None:
            continue  # empty file
        if previous_max is not None and bounds["lo"] < previous_max:
            result["problem"] = f"{path.rsplit('/', 1)[-1]} starts below the previous file's end"
            return result
        previous_max = bounds["hi"]
    return result


def validate_terasort(spark: Any, tables: dict[str, str], output: str) -> dict[str, Any]:
    return validate_sorted(spark, tables["tera"], output, "key", ("key", "value"))


# --- text: Sort and WordCount ---------------------------------------------------------------

VOCABULARY = 50_000
WORDS_PER_LINE = 10
SKEW = 3.0  # word index = VOCABULARY * u**SKEW: a few words are very common, like real text


def textgen(spark: Any, rows: int, seed: int, partitions: int, output: str) -> int:
    """Write ``rows`` lines of ``WORDS_PER_LINE`` words; the same arguments give the same lines."""
    from pyspark.sql import functions as F

    def word(position: int) -> Any:
        h = F.xxhash64(F.lit(seed), F.col("id"), F.lit(position))
        u = (F.abs(h % 1_000_000_007) / F.lit(1_000_000_007.0)).cast("double")
        index = F.floor(F.pow(u, F.lit(SKEW)) * F.lit(VOCABULARY)).cast("long")
        return F.concat(F.lit("w"), index.cast("string"))

    df = spark.range(0, rows, 1, partitions).select(
        F.concat_ws(" ", *[word(i) for i in range(WORDS_PER_LINE)]).alias("line")
    )
    df.write.mode("overwrite").parquet(output)
    return rows


def sort_text(spark: Any, tables: dict[str, str], output: str) -> None:
    """The timed unit: read lines, sort them, write."""
    spark.read.parquet(tables["text"]).orderBy("line").write.mode("overwrite").parquet(output)


def _words(df: Any) -> Any:
    from pyspark.sql import functions as F

    return df.select(F.explode(F.split(F.col("line"), " ")).alias("word"))


def wordcount(spark: Any, tables: dict[str, str], output: str) -> None:
    """The timed unit: split lines into words, count each word, write (word, count)."""
    _words(spark.read.parquet(tables["text"])).groupBy("word").count().write.mode(
        "overwrite"
    ).parquet(output)


def validate_sort(spark: Any, tables: dict[str, str], output: str) -> dict[str, Any]:
    return validate_sorted(spark, tables["text"], output, "line", ("line",))


def validate_wordcount(spark: Any, tables: dict[str, str], output: str) -> dict[str, Any]:
    """Counts add up to the number of words in the input; each word appears once.

    Rows and checksum of the (word, count) pairs are compared with the reference engine.
    """
    from pyspark.sql import functions as F

    counts = spark.read.parquet(output)
    rows, checksum = _digest(counts, ("word", "count"))
    result: dict[str, Any] = {"rows": rows, "checksum": checksum, "problem": None}
    words = _words(spark.read.parquet(tables["text"])).count()
    totals = counts.agg(
        F.sum("count").alias("total"), F.countDistinct("word").alias("distinct")
    ).first()
    if int(totals["total"] or 0) != words:
        result["problem"] = f"counts add up to {totals['total']}, the input has {words} words"
    elif int(totals["distinct"]) != rows:
        result["problem"] = f"{rows - int(totals['distinct'])} words appear more than once"
    return result


JOBS = {"terasort": terasort, "sort": sort_text, "wordcount": wordcount}
VALIDATORS = {
    "terasort": validate_terasort,
    "sort": validate_sort,
    "wordcount": validate_wordcount,
}
# --- synth: seeded edge-case data ----------------------------------------------------------

UNIFORM_BUCKETS = 1_000_000_007  # prime: hash % P / P is a uniform number in [0, 1)


def _uniform(seed: int, tag: str) -> Any:
    from pyspark.sql import functions as F

    h = F.xxhash64(F.lit(seed), F.lit(tag), F.col("id"))
    return (F.abs(h % UNIFORM_BUCKETS) / F.lit(float(UNIFORM_BUCKETS))).cast("double")


def synth_key(spec: dict[str, Any]) -> Any:
    """The fact table's key: 0 .. keys-1, uniform, Zipf(s) or with one hot key.

    Zipf ranks come from the inverse CDF of a density proportional to x^-s on
    [0.5, keys + 0.5), rounded, so rank k has probability close to k^-s / H(keys, s).
    """
    from pyspark.sql import functions as F

    keys, seed = int(spec["keys"]), int(spec["seed"])
    u = _uniform(seed, "key")
    if spec["key_dist"] == "zipf" and float(spec["zipf_s"]) > 0:
        s = float(spec["zipf_s"])
        lo, hi = 0.5, keys + 0.5
        if abs(s - 1.0) < 1e-9:
            x = F.lit(lo) * F.pow(F.lit(hi / lo), u)
        else:
            a, b = lo ** (1 - s), hi ** (1 - s)
            x = F.pow(F.lit(a) + u * F.lit(b - a), F.lit(1 / (1 - s)))
        rank = F.least(F.greatest(F.round(x), F.lit(1.0)), F.lit(float(keys)))
        return (rank - 1).cast("long")
    uniform_key = F.floor(u * keys).cast("long")
    if spec["key_dist"] == "hotkey":
        hot = _uniform(seed, "hot") < F.lit(float(spec["hot_fraction"]))
        return F.when(hot, F.lit(0).cast("long")).otherwise(uniform_key)
    return uniform_key


def _string(seed: int, mean_length: int) -> Any:
    """Hex text of length mean_length/2 .. 3*mean_length/2, seeded per row."""
    from pyspark.sql import functions as F

    if mean_length == 0:
        return F.lit("")
    length = F.floor(_uniform(seed, "len") * mean_length).cast("int") + F.lit(mean_length // 2)
    digest = F.sha2(F.concat_ws(":", F.lit(seed), F.col("id")), 256)  # 64 hex characters
    return F.repeat(digest, mean_length // 64 + 2).substr(F.lit(1), length)


def synthgen(spark: Any, spec: dict[str, Any], output: str) -> dict[str, int]:
    """Write ``fact`` and ``dim`` under ``output``; the same spec always gives the same rows."""
    from pyspark.sql import functions as F

    seed, rows, keys, files = (int(spec[k]) for k in ("seed", "rows", "keys", "files"))
    payload = []
    for i in range(int(spec["payload_columns"])):
        value = F.xxhash64(F.lit(seed), F.lit(f"p{i}"), F.col("id"))
        null = _uniform(seed, f"null{i}") < F.lit(float(spec["null_fraction"]))
        payload.append(F.when(null, F.lit(None).cast("long")).otherwise(value).alias(f"p{i}"))
    fact = spark.range(0, rows, 1, files).select(
        F.col("id"),
        synth_key(spec).alias("key"),
        *payload,
        _string(seed, int(spec["string_length"])).alias("s"),
    )
    fact.write.mode("overwrite").parquet(f"{output}/fact")
    dim = spark.range(0, keys, 1, max(1, min(files, keys // 1000 + 1))).select(
        F.col("id").alias("key"),
        F.concat(F.lit("k"), F.col("id").cast("string")).alias("name"),
        (F.col("id") % 100).alias("grp"),
    )
    dim.write.mode("overwrite").parquet(f"{output}/dim")
    return {"fact": rows, "dim": keys}


GENERATORS = {"tera": teragen, "text": textgen}
