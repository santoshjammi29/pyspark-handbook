"""Scenario & Scale: ML feature pipeline caching 50 GB intermediate datasets.

Demonstrates StorageLevel selection (MEMORY_AND_DISK vs _SER), lazy caching,
iterative reuse gains, and mandatory `unpersist()` to prevent GC thrashing.
"""

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Calling `.cache()` on a massive DataFrame used only once,
# caching before selective filters/projections, or forgetting `.unpersist()`.
# Otherwise: Single-use caching wastes CPU/RAM serializing blocks never re-read,
# evicts execution shuffle memory (causing disk spills), and triggers severe
# JVM Garbage Collection pauses (>20% GC time) or Executor OOMs.
def bad_single_use_cache(df_raw: DataFrame) -> int:
    # Wasted cache: accessed only once and never unpersisted
    return df_raw.cache().filter(F.col("id") > F.lit(1000)).count()


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Filter and project columns FIRST, persist ONLY DataFrames
# reused across 2+ actions or iterative loops with explicit StorageLevel,
# materialize once, and call `unpersist(blocking=True)` immediately after use.
def run_iterative_cached_metrics(df_raw: DataFrame) -> dict:
    """Persist pruned features across multiple actions, then release memory."""
    df_features = df_raw.select(
        F.col("id"),
        (F.col("id") * F.lit(2.5)).alias("feat_a"),
        F.sin(F.col("id").cast("double")).alias("feat_b"),
    )
    df_features.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        total_rows = df_features.count()  # Materializes cache lazily
        high_val_rows = df_features.filter(F.col("feat_a") > F.lit(1000.0)).count()
        avg_b = df_features.agg(F.avg("feat_b")).first()[0]
        return {
            "total_rows": total_rows,
            "high_val_rows": high_val_rows,
            "avg_b": float(avg_b or 0.0),
        }
    finally:
        df_features.unpersist(blocking=True)


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_features.filter(...).explain("formatted") after materialization shows:
#   InMemoryTableScan [feat_a#...]
#   +- InMemoryRelation [id#..., feat_a#..., feat_b#...],
#      StorageLevel(disk, memory, deserialized, 1 replicas)
# Spark UI -> Storage tab & Executors tab:
#   - Storage tab displays `Fraction Cached`, `Size in Memory`, and `Size on Disk`.
#     If Fraction Cached < 100%, Spark recomputes missing partitions on every action!
#   - Executors tab: watch `Task Time` vs `GC Time`; GC > 10% signals oversized
#     deserialized caches—switch to `MEMORY_AND_DISK` (serialized in Spark SQL)
#     or `DISK_ONLY`, or write a checkpoint/staging Parquet table instead.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. `df.cache()` on a DataFrame is equivalent to `persist(StorageLevel.MEMORY_AND_DISK)`
#    in PySpark (historically MEMORY_ONLY for RDDs; DataFrames use
#    MEMORY_AND_DISK_DESER by default in Spark SQL).
# 2. Caching breaks lazy Catalyst pushdown across the cache boundary: always
#    apply `filter()` and `select()` BEFORE `.persist()`, never after.
# 3. `df.cache()` is lazy—unless an action runs, nothing is cached; additionally,
#    actions like `.take(1)` or `.show(5)` only cache the touched partition(s)!


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("079_CachingAndPersistence")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    df = spark.range(1, 50_000, 1, 4)
    print(run_iterative_cached_metrics(df))
