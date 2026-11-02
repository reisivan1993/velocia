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


# --- synth: seeded edge-case data ----------------------------------------------------------

# Two to four bytes per character in UTF-8, and one emoji outside the Basic Multilingual Plane.
MULTIBYTE_WORDS = ["café", "naïve", "日本語", "데이터", "Ελλάδα", "мир", "שלום", "😀"]
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
    words = F.array(*[F.lit(w) for w in MULTIBYTE_WORDS])
    picks = [
        F.element_at(
            words,
            (F.floor(_uniform(seed, f"u{i}") * len(MULTIBYTE_WORDS)) + 1).cast("int"),
        )
        for i in range(3)
    ]
    fanout = int(spec["nest_fanout"])
    fact = spark.range(0, rows, 1, files).select(
        F.col("id"),
        synth_key(spec).alias("key"),
        *payload,
        _string(seed, int(spec["string_length"])).alias("s"),
        (F.xxhash64(F.lit(seed), F.lit("d"), F.col("id")) % 10**12 / 10**4)
        .cast("decimal(38,10)")
        .alias("d"),
        F.concat_ws(" ", *picks).alias("u"),
        F.struct(
            (F.col("id") % 1000).alias("a"),
            F.expr(
                f"transform(sequence(1, {fanout}), x -> x * (id % 97))"
                if fanout
                else "cast(array() as array<bigint>)"
            ).alias("arr"),
        ).alias("nest"),
    )
    fact.write.mode("overwrite").parquet(f"{output}/fact")
    dim = spark.range(0, keys, 1, max(1, min(files, keys // 1000 + 1))).select(
        F.col("id").alias("key"),
        F.concat(F.lit("k"), F.col("id").cast("string")).alias("name"),
        (F.col("id") % 100).alias("grp"),
    )
    dim.write.mode("overwrite").parquet(f"{output}/dim")
    return {"fact": rows, "dim": keys}


# --- etl: a five-step pipeline, one unit per step ---------------------------------------------
# Each step reads the previous step's output from the scratch directory, so the units run in
# order within a repetition (the driver always runs units in workload order).

ETL_STEPS = ["read", "clean", "join", "aggregate", "write"]


def _previous(output: str, step: str) -> str:
    return join(output.rsplit("/", 1)[0], ETL_STEPS[ETL_STEPS.index(step) - 1])


def etl_read(spark: Any, tables: dict[str, str], output: str) -> None:
    """Land the raw events: project and type the columns the pipeline uses."""
    from pyspark.sql import functions as F

    spark.read.parquet(tables["fact"]).select(
        "id", "key", "p0", "p1", F.col("d").alias("amount"), F.col("u").alias("label")
    ).write.mode("overwrite").parquet(output)


def etl_clean(spark: Any, tables: dict[str, str], output: str) -> None:
    """Drop incomplete rows and duplicate ids, normalize text."""
    from pyspark.sql import functions as F

    raw = spark.read.parquet(_previous(output, "clean"))
    raw.where(F.col("p0").isNotNull() & F.col("p1").isNotNull()).dropDuplicates(["id"]).select(
        "id", "key", "p0", "p1", "amount", F.lower(F.trim(F.col("label"))).alias("label")
    ).write.mode("overwrite").parquet(output)


def etl_join(spark: Any, tables: dict[str, str], output: str) -> None:
    """Enrich events with their dimension row."""
    clean = spark.read.parquet(_previous(output, "join"))
    dim = spark.read.parquet(tables["dim"])
    clean.join(dim, "key").write.mode("overwrite").parquet(output)


def etl_aggregate(spark: Any, tables: dict[str, str], output: str) -> None:
    """Totals per group and name."""
    from pyspark.sql import functions as F

    spark.read.parquet(_previous(output, "aggregate")).groupBy("grp", "name").agg(
        F.count(F.lit(1)).alias("events"),
        F.sum("amount").alias("amount"),
        F.sum(F.col("p0") % 1000).alias("score"),
        F.countDistinct("label").alias("labels"),
    ).write.mode("overwrite").parquet(output)


def etl_write(spark: Any, tables: dict[str, str], output: str) -> None:
    """Publish the result partitioned by group, as a downstream table would be."""
    spark.read.parquet(_previous(output, "write")).write.mode("overwrite").partitionBy(
        "grp"
    ).parquet(output)


def validate_output(spark: Any, tables: dict[str, str], output: str) -> dict[str, Any]:
    """Rows and checksum of a step's output, compared with the reference engine."""
    df = spark.read.parquet(output)
    rows, checksum = _digest(df, tuple(sorted(df.columns)))
    return {"rows": rows, "checksum": checksum, "problem": None if rows else "empty output"}


# --- graph: PageRank -------------------------------------------------------------------------

EDGES_PER_VERTEX = 10
PAGERANK_ITERATIONS = 10
DAMPING = 0.85
ITERATIVE_TOLERANCE = 1e-6  # relative; engines sum floats in different orders
RANKS_CHECKED = 50


def graphgen(spark: Any, rows: int, seed: int, partitions: int, output: str) -> int:
    """``rows`` directed edges over rows / 10 vertices; targets follow a power law (Zipf 1.0),
    so a few vertices are very popular, like links on the web."""
    from pyspark.sql import functions as F

    vertices = max(rows // EDGES_PER_VERTEX, 2)
    spec = {"keys": vertices, "seed": seed, "key_dist": "zipf", "zipf_s": 1.0}
    spark.range(0, rows, 1, partitions).select(
        F.floor(_uniform(seed, "src") * vertices).cast("long").alias("src"),
        synth_key(spec).alias("dst"),
    ).write.mode("overwrite").parquet(output)
    return rows


def pagerank(spark: Any, tables: dict[str, str], output: str) -> None:
    """The timed unit: 10 PageRank iterations with the DataFrame API, ranks written out."""
    from pyspark.sql import functions as F

    edges = spark.read.parquet(tables["graph"])
    out_degree = edges.groupBy("src").agg(F.count(F.lit(1)).alias("degree"))
    links = edges.join(out_degree, "src")
    vertices = (
        edges.select(F.col("src").alias("id"))
        .union(edges.select(F.col("dst").alias("id")))
        .distinct()
        .localCheckpoint()
    )
    ranks = vertices.withColumn("rank", F.lit(1.0))
    for _ in range(PAGERANK_ITERATIONS):
        contributions = (
            links.join(ranks, links.src == ranks.id)
            .groupBy("dst")
            .agg(F.sum(F.col("rank") / F.col("degree")).alias("incoming"))
        )
        ranks = (
            vertices.join(contributions, vertices.id == contributions.dst, "left")
            .select(
                "id",
                (
                    F.lit(1 - DAMPING) + F.lit(DAMPING) * F.coalesce(F.col("incoming"), F.lit(0.0))
                ).alias("rank"),
            )
            .localCheckpoint()  # keeps the plan from growing with every iteration
        )
    ranks.write.mode("overwrite").parquet(output)


def validate_pagerank(spark: Any, tables: dict[str, str], output: str) -> dict[str, Any]:
    """Exact: the vertex set. Within a tolerance: total rank and the first 50 vertices' ranks."""
    from pyspark.sql import functions as F

    ranks = spark.read.parquet(output)
    rows, checksum = _digest(ranks, ("id",))
    total = ranks.agg(F.sum("rank")).first()[0]
    first = [r["rank"] for r in ranks.orderBy("id").limit(RANKS_CHECKED).collect()]
    problem = None if rows else "no ranks"
    if rows and any(r is None or r <= 0 for r in first):
        problem = "ranks must be positive"
    return {
        "rows": rows,
        "checksum": checksum,
        "values": [float(total or 0.0), *[float(r or 0.0) for r in first]],
        "tolerance": ITERATIVE_TOLERANCE,
        "problem": problem,
    }


# --- kmeans: MLlib KMeans ----------------------------------------------------------------------

KMEANS_DIMS = 10
KMEANS_TRUE_CENTERS = 8
KMEANS_K = 8
KMEANS_MAX_ITER = 20


def kmeansgen(spark: Any, rows: int, seed: int, partitions: int, output: str) -> int:
    """``rows`` points in 10 dimensions around 8 seeded centers (spread +/- 5 per dimension)."""
    from pyspark.sql import functions as F

    center = F.floor(_uniform(seed, "center") * KMEANS_TRUE_CENTERS)
    columns = []
    for d in range(KMEANS_DIMS):
        base = (F.xxhash64(F.lit(seed), F.lit(f"c{d}"), center) % 100).cast("double")
        noise = (_uniform(seed, f"n{d}a") + _uniform(seed, f"n{d}b") - 1.0) * 5.0
        columns.append((base + noise).alias(f"f{d}"))
    spark.range(0, rows, 1, partitions).select(*columns).write.mode("overwrite").parquet(output)
    return rows


def kmeans_train(spark: Any, tables: dict[str, str], output: str) -> None:
    """The timed unit: MLlib KMeans (k-means||, seed 42) through the JVM API.

    Calling MLlib on the JVM keeps the job free of numpy, which the official Apache Spark image
    doesn't ship and vendors' images may not either.
    """
    jvm = spark.sparkContext._jvm
    gateway = spark.sparkContext._gateway
    names = gateway.new_array(jvm.java.lang.String, KMEANS_DIMS)
    for d in range(KMEANS_DIMS):
        names[d] = f"f{d}"
    assembler = (
        jvm.org.apache.spark.ml.feature.VectorAssembler()
        .setInputCols(names)
        .setOutputCol("features")
    )
    points = assembler.transform(spark.read.parquet(tables["kmeans"])._jdf)
    model = (
        jvm.org.apache.spark.ml.clustering.KMeans()
        .setK(KMEANS_K)
        .setSeed(42)
        .setMaxIter(KMEANS_MAX_ITER)
        .setFeaturesCol("features")
        .fit(points)
    )
    summary = model.summary()
    centers = [list(c.toArray()) for c in model.clusterCenters()]
    rows = [
        (i, float(summary.trainingCost()), int(summary.numIter()), *center)
        for i, center in enumerate(sorted(centers))
    ]
    columns = ["center", "cost", "iterations", *[f"f{d}" for d in range(KMEANS_DIMS)]]
    spark.createDataFrame(rows, columns).coalesce(1).write.mode("overwrite").parquet(output)


def validate_kmeans(spark: Any, tables: dict[str, str], output: str) -> dict[str, Any]:
    """Exact: k and the iteration count. Within a tolerance: cost and the sorted centers."""
    model = spark.read.parquet(output).orderBy("center").collect()
    values = [float(model[0]["cost"])] if model else []
    for row in model:
        values += [float(row[f"f{d}"]) for d in range(KMEANS_DIMS)]
    return {
        "rows": len(model),
        "checksum": f"iterations={model[0]['iterations']}" if model else "",
        "values": values,
        "tolerance": ITERATIVE_TOLERANCE,
        "problem": None if len(model) == KMEANS_K else f"{len(model)} centers, expected {KMEANS_K}",
    }


GENERATORS = {"tera": teragen, "text": textgen, "graph": graphgen, "kmeans": kmeansgen}


JOBS = {
    "terasort": terasort,
    "sort": sort_text,
    "wordcount": wordcount,
    "etl_read": etl_read,
    "etl_clean": etl_clean,
    "etl_join": etl_join,
    "etl_aggregate": etl_aggregate,
    "etl_write": etl_write,
    "pagerank": pagerank,
    "kmeans_train": kmeans_train,
}
VALIDATORS = {
    "terasort": validate_terasort,
    "sort": validate_sort,
    "wordcount": validate_wordcount,
    **{f"etl_{step}": validate_output for step in ETL_STEPS},
    "pagerank": validate_pagerank,
    "kmeans_train": validate_kmeans,
}
