"""Scenario & Scale: Daily 5 MB aggregated report writing 200 tiny output files.

Compares `coalesce(n)` (zero-shuffle local partition merging) vs `repartition(n)`
(full network shuffle) using pure DataFrame / Spark SQL APIs.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Calling `repartition(1)` to compact small outputs (causing a
# full-cluster shuffle into 1 task) OR calling `coalesce(n)` to increase
# partitions or immediately after a heavy upstream filter without checking skew.
# Otherwise: `repartition(1)` bottlenecks the entire job on a single executor
# after a full network shuffle; `coalesce(200)` on a 4-partition DF silently
# stays at 4 partitions, and `coalesce(1)` upstream pushes the entire DAG onto
# 1 core before filtering!
def bad_repartition_one_or_coalesce_up(df: DataFrame) -> DataFrame:
    # Full shuffle of all data across network into 1 single task
    return df.repartition(1)


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Use `coalesce(n)` strictly to shrink partitions before I/O
# (zero Exchange shuffle), and use `repartition(n)` / `repartition(n, key)`
# when increasing parallelism or balancing skewed data evenly across workers.
def compact_for_write(df: DataFrame, target_files: int = 2) -> DataFrame:
    """Reduce partition count locally without triggering a network shuffle."""
    return df.coalesce(target_files)


def balance_partitions(
    df: DataFrame,
    num_partitions: int,
    partition_col: str = "",
) -> DataFrame:
    """Increase or evenly rebalance partitions via full hash/round-robin shuffle."""
    if partition_col:
        return df.repartition(num_partitions, F.col(partition_col))
    return df.repartition(num_partitions)


def inspect_partition_counts(df: DataFrame) -> DataFrame:
    """Inspect row distribution across partitions using pure Spark SQL."""
    return (
        df.withColumn("pid", F.spark_partition_id())
        .groupBy("pid")
        .agg(F.count(F.lit(1)).alias("rows"))
        .orderBy("pid")
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# compact_for_write(df, 2).explain("formatted"):
#   Coalesce 2                  <-- NO Exchange node! Merges parent partitions locally.
#   +- * Range (1, 40001, step=1, splits=8)
# balance_partitions(df, 2).explain("formatted"):
#   Exchange RoundRobinPartitioning(2), REPARTITION_BY_NUM
#   +- * Range (1, 40001, step=1, splits=8)
# Spark UI -> Stages tab:
#   - `coalesce(2)` executes in the same stage with 0 Shuffle Read/Write bytes,
#     but watch upstream tasks—if upstream was `8` partitions and coalesced to
#     `2`, upstream concurrency drops to 2 tasks unless a stage boundary exists!


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. `coalesce(n)` pushes down into the stage above it! Calling `df.filter(...).coalesce(1)`
#    forces the entire filter/scan to run in 1 task unless a shuffle separates
#    them; use `repartition` or AQE coalescing if upstream parallelism matters.
# 2. `coalesce(n)` can NEVER increase partitions (`coalesce(10)` on 2 partitions
#    stays 2); only `repartition(n)` can scale out.
# 3. In Spark 4.0 / Spark Connect, low-level RDD partition APIs are
#    unsupported; use `F.spark_partition_id()` or `explain("formatted")` instead.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("080_RepartitionVsCoalesce")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    df_src = spark.range(1, 40_001, 1, 8)
    df_coal = compact_for_write(df_src, target_files=2)
    df_repart = balance_partitions(df_src, num_partitions=2)
    df_coal.explain("formatted")
    df_repart.explain("formatted")
    inspect_partition_counts(df_coal).show()
