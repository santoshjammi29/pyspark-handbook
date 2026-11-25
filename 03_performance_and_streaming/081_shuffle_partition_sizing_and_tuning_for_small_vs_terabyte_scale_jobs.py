"""Scenario & Scale: Tuning shuffles from 100 MB jobs up to 1 TB+ joins.

Replaces arbitrary default `spark.sql.shuffle.partitions = 200` with data-size
math (100–200 MB/partition) and AQE `advisoryPartitionSizeInBytes` auto-tuning.
"""

import math

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Leaving `spark.sql.shuffle.partitions = 200` static across
# both 50 MB microservice jobs and 2 TB warehouse joins with AQE disabled.
# Otherwise: On 50 MB jobs, 200 tasks process ~250 KB each (task scheduling
# dominates latency and emits 200 tiny files); on 2 TB jobs, 200 tasks process
# 10 GB each, causing severe Shuffle Spill (Disk) and Executor OOMs.
def bad_fixed_shuffle_partitions(spark: SparkSession, df: DataFrame) -> DataFrame:
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    return df.groupBy(F.col("id") % F.lit(10)).count()


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Size initial shuffle partitions using `shuffle_mb / 150 MB`
# (snapping up for high-volume jobs) and enable AQE `advisoryPartitionSizeInBytes`
# (64–128 MB) so Spark dynamically merges under-filled post-shuffle partitions.
def compute_optimal_shuffle_partitions(
    estimated_shuffle_mb: float,
    target_mb_per_partition: float = 150.0,
    min_partitions: int = 4,
) -> int:
    """Calculate target shuffle partition count targeting ~150 MB per task."""
    raw = math.ceil(estimated_shuffle_mb / target_mb_per_partition)
    return max(min_partitions, raw)


def configure_dynamic_shuffle_tuning(
    spark: SparkSession,
    estimated_shuffle_mb: float,
    advisory_mb: int = 64,
) -> int:
    """Configure initial shuffle partitions and AQE dynamic coalescing."""
    partitions = compute_optimal_shuffle_partitions(estimated_shuffle_mb)
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        str(advisory_mb * 1024 * 1024),
    )
    return partitions


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df.explain("formatted") shows `Exchange hashpartitioning(..., <N>)` inside
# Initial Plan and `AQEShuffleRead coalesced` in Final Plan once executed.
# Spark UI -> Stages tab:
#   - Check `Shuffle Write Size / Records` of the map stage and `Shuffle Read
#     Size` per task in the reduce stage.
#   - Ideal task Shuffle Read = 100–200 MB uncompressed / compressed-in-flight,
#     task duration 1–10s, and `Spill (Disk) = 0 B`.
#   - Remember AQE can COALESCEDOWN from `shuffle.partitions`, NOT split up
#     (unless skewJoin triggers), so set `shuffle.partitions` high enough for
#     your peak stage and let AQE shrink small stages!


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. AQE coalesces partitions downward (`N -> M` where `M <= N`) but does NOT
#    increase `spark.sql.shuffle.partitions` beyond its configured ceiling—for
#    1 TB+ joins, set `shuffle.partitions` to 2000–4000 rather than 200.
# 2. Align partition count to a multiple of total cluster CPU cores (e.g., 64
#    cores -> 64, 128, 256 partitions) to avoid tail-wave straggler cycles.
# 3. `advisoryPartitionSizeInBytes` governs post-shuffle coalescing target size
#    (default 64 MB), not input file split size (`spark.sql.files.maxPartitionBytes`,
#    default 128 MB).


if __name__ == "__main__":
    spark = SparkSession.builder.appName("081_ShufflePartitionSizing").getOrCreate()
    configured = configure_dynamic_shuffle_tuning(
        spark, estimated_shuffle_mb=500.0, advisory_mb=64
    )
    print(f"Configured shuffle partitions: {configured}")
    df_agg = (
        spark.range(1, 50_000, 1, 4)
        .groupBy((F.col("id") % F.lit(10)).alias("grp"))
        .count()
    )
    df_agg.collect()
    df_agg.explain("formatted")
