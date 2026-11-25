"""Scenario & Scale: E-commerce clickstream join (100M clicks, 40% GUEST_000).

Quantifies key skew with `spark_partition_id()` and eliminates 99%-stuck task
stragglers using two-stage salting when AQE skew join cannot handle the key.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Executing a raw SortMergeJoin on a heavily skewed join key
# (e.g. NULL, `GUEST_000`, `-1`) with no salting, filtering, or broadcast.
# Otherwise: Hash partitioning sends all 40M `GUEST_000` rows to a single
# reducer task—causing multi-GB shuffle spill to disk, GC thrashing, and
# Executor OOM (exit code 137) while 199 other tasks sit idle at 99% progress.
def bad_unmitigated_skew_join(
    df_clicks: DataFrame,
    df_users: DataFrame,
) -> DataFrame:
    return df_clicks.join(df_users, on="customer_id", how="inner")


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Diagnose partition skew using `F.spark_partition_id()`,
# isolate or salt only hot keys (preserving normal keys un-exploded), and join
# with exploded dimension salts (or broadcast small dimensions directly).
def profile_partition_skew(df: DataFrame) -> DataFrame:
    """Quantify row distribution per partition via pure Spark SQL."""
    return (
        df.withColumn("pid", F.spark_partition_id())
        .groupBy("pid")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .orderBy(F.col("row_count").desc())
    )


def salted_skew_join(
    df_fact: DataFrame,
    df_dim: DataFrame,
    join_key: str = "customer_id",
    hot_key: str = "GUEST_000",
    salt_factor: int = 8,
) -> DataFrame:
    """Distribute a skewed hot key evenly across `salt_factor` reducers."""
    salts = F.sequence(F.lit(0), F.lit(salt_factor - 1))

    df_fact_salted = df_fact.withColumn(
        "_salt",
        F.when(
            F.col(join_key) == F.lit(hot_key),
            F.floor(F.rand(seed=42) * F.lit(salt_factor)).cast("int"),
        ).otherwise(F.lit(0)),
    ).withColumn(
        "_salted_key",
        F.concat(F.col(join_key), F.lit("#"), F.col("_salt").cast("string")),
    )

    df_dim_hot = (
        df_dim.filter(F.col(join_key) == F.lit(hot_key))
        .withColumn("_salt", F.explode(salts))
    )
    df_dim_cold = df_dim.filter(F.col(join_key) != F.lit(hot_key)).withColumn(
        "_salt", F.lit(0)
    )
    df_dim_salted = df_dim_hot.unionByName(df_dim_cold).withColumn(
        "_salted_key",
        F.concat(F.col(join_key), F.lit("#"), F.col("_salt").cast("string")),
    )

    return df_fact_salted.join(
        df_dim_salted.drop(join_key, "_salt"),
        on="_salted_key",
        how="inner",
    ).drop("_salt", "_salted_key")


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_balanced.explain("formatted") shows:
#   SortMergeJoin [_salted_key], [_salted_key], Inner
#   :- Sort -> Exchange hashpartitioning(_salted_key)
#   +- Sort -> Exchange hashpartitioning(_salted_key)
# Spark UI -> Stages tab -> Summary Metrics:
#   - Skew signature: Median task duration = 2s, 75th = 4s, Max = 42m;
#     Max Shuffle Read Size = 18 GB vs Median = 90 MB; Spill (Disk) > 0.
#   - Post-salting: Max duration drops in line with 75th percentile and disk
#     spill drops to 0.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. If the dimension table fits in executor/driver memory (< ~100 MB), use
#    `F.broadcast(df_dim)` first—broadcast joins have zero shuffle and are
#    immune to fact-side key skew!
# 2. AQE `skewJoin` only handles SortMergeJoin/ShuffledHashJoin when partitions
#    exceed `skewedPartitionThresholdInBytes` (256 MB) and does NOT fix skew in
#    `groupBy` or `Window.partitionBy(skewed_col)`—for those, use two-stage
#    salted aggregation (`groupBy(key, salt)` -> `groupBy(key)`).
# 3. Note NULL join keys: standard inner joins drop NULLs before shuffling, but
#    LEFT OUTER joins retain NULLs—coalesce or salt NULLs explicitly if needed.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("076_DataSkewSalting")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    df_clicks = spark.range(0, 10_000, 1, 4).select(
        F.when(F.col("id") < 8000, F.lit("GUEST_000"))
        .otherwise(F.concat(F.lit("CUST_"), (F.col("id") % 500).cast("string")))
        .alias("customer_id"),
        F.col("id").cast("double").alias("amount"),
    )
    df_users = spark.createDataFrame(
        [("GUEST_000", "Guest"), ("CUST_1", "Standard")],
        "customer_id STRING, tier STRING",
    )
    df_joined = salted_skew_join(df_clicks, df_users, salt_factor=4)
    profile_partition_skew(df_joined).show()
