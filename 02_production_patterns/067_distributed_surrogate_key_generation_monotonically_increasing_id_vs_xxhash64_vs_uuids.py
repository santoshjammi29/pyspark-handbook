"""Distributed Surrogate Keys: monotonically_increasing_id vs xxhash64 vs UUIDs.

Scenario & Scale:
    A lakehouse dimensional pipeline generates surrogate keys for 200M+
    customer and account records across distributed partitions. Keys must be
    100% deterministic across pipeline retries and safe for Star Schema joins.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Using `monotonically_increasing_id()` or `uuid()` as durable
# dimensional surrogate keys for incremental/replayable pipelines.
# Otherwise: `monotonically_increasing_id()` encodes partition IDs and shifts
# whenever input partitioning changes; `uuid()` breaks idempotency and bloats
# joins with 36-byte random strings.
def transform_surrogate_keys_bad(df: DataFrame) -> DataFrame:
    return df.withColumn("cust_sk", F.monotonically_increasing_id()).withColumn(
        "alt_sk", F.expr("uuid()")
    )


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Generate deterministic 64-bit integer keys via `F.xxhash64`
# (or 256-bit `F.sha2` where collision resistance > 2^64 is mandated) using
# null-safe delimiter concatenation (`concat_ws` + `coalesce`).
def transform_add_deterministic_surrogate_keys(
    df: DataFrame, key_cols: list[str]
) -> DataFrame:
    """Attach deterministic `xxhash64` and `sha2` surrogate keys without shuffles."""
    normalized_cols = [F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in key_cols]
    composite_key = F.concat_ws("||", *normalized_cols)

    return (
        df.withColumn("customer_sk_xx64", F.xxhash64(*normalized_cols))
        .withColumn("customer_hk_sha256", F.sha2(composite_key, 256))
        .withColumn("_partition_id", F.spark_partition_id())
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` confirms `xxhash64` and `sha2` execute purely inside
#   a map-side `Project` stage with ZERO `Exchange` (shuffle) or `Window` overhead.
# - Unlike `row_number().over(Window.orderBy(...))`, which forces a single-partition
#   global sort bottleneck, hash surrogate keys scale linearly across all cores.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `xxhash64` ignores `NULL` arguments if passed raw; wrap nullable columns in
#    `F.coalesce(col, F.lit("__NULL__"))` and separate fields with a delimiter
#    so `("ab", "c")` never collides with `("a", "bc")`.
# 2. Birthday-paradox collision risk: 64-bit `xxhash64` reaches ~1% collision
#    probability around ~6e8 distinct rows; use `sha2(..., 256)` or Delta
#    `GENERATED ALWAYS AS IDENTITY` for multi-billion-row dimensions.
# 3. Avoid legacy RDD partition inspection in Spark Connect / Spark 4.0; inspect
#    partition spread via `df.groupBy(F.spark_partition_id()).count()`.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("067_surrogate_keys")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    df_cust = spark.createDataFrame(
        [("NAT-1001", "Alice", "2025-01-10"), ("NAT-1002", "Bob", "2025-02-15")],
        "natural_key STRING, name STRING, created_at STRING",
    ).repartition(2)
    df_keys = transform_add_deterministic_surrogate_keys(df_cust, ["natural_key", "created_at"])
    df_keys.show(truncate=False)
