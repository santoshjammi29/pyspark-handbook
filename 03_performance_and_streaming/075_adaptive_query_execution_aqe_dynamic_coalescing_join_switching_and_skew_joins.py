"""Scenario & Scale: 2 TB batch ETL pipeline running with fixed shuffle settings.

Demonstrates Adaptive Query Execution (AQE): dynamic partition coalescing,
runtime SortMergeJoin -> BroadcastHashJoin switching, and dynamic skew splitting.
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Disabling AQE (`spark.sql.adaptive.enabled = false`) and
# hardcoding static `spark.sql.shuffle.partitions = 200` for all stages.
# Otherwise: Selective filters that shrink 2 TB -> 50 MB still launch 200 tiny
# tasks (high scheduler overhead & small output files), stale table stats force
# slow SortMergeJoins instead of BroadcastHashJoins, and skewed keys stall at 99%.
def bad_static_plan_execution(spark: SparkSession, df: DataFrame) -> DataFrame:
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    return df.filter(F.col("id") < F.lit(100)).groupBy(F.col("id") % F.lit(5)).count()


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Enable AQE with partition coalescing, runtime broadcast
# conversion, and dynamic skew join optimization tuned to cluster memory.
def configure_aqe(
    spark: SparkSession,
    advisory_partition_bytes: int = 64 * 1024 * 1024,
    skew_factor: int = 5,
    skew_threshold_bytes: int = 256 * 1024 * 1024,
) -> Dict[str, str]:
    """Enable and inspect core AQE runtime optimization settings."""
    settings = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(
            advisory_partition_bytes
        ),
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": str(skew_factor),
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(
            skew_threshold_bytes
        ),
    }
    for key, val in settings.items():
        spark.conf.set(key, val)
    return settings


def run_aqe_coalesce_demo(spark: SparkSession) -> DataFrame:
    """Trigger an AQE query whose post-filter data shrinks dramatically."""
    spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "20")
    return (
        spark.range(1, 100_000, 1, 8)
        .filter(F.col("id") < F.lit(100))
        .groupBy((F.col("id") % F.lit(5)).alias("bucket"))
        .count()
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df.explain("formatted") BEFORE execution shows `AdaptiveSparkPlan isFinalPlan=false`.
# After action execution (e.g. Spark UI -> SQL tab -> Details):
#   AdaptiveSparkPlan isFinalPlan=true
#   +- == Final Plan ==
#      * HashAggregate
#      +- AQEShuffleRead coalesced          <-- 20 shuffle partitions merged to 1!
#         +- ShuffleQueryStage 0
#   +- == Initial Plan ==
#      Exchange hashpartitioning(..., 20)
# For joins, watch `SortMergeJoin` in Initial Plan flip to `BroadcastHashJoin`
# in Final Plan, or `AQEShuffleRead skewed` splitting straggler partitions.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. AQE only re-optimizes at shuffle/broadcast stage boundaries (QueryStage)
#    and does NOT apply to continuous Structured Streaming micro-batches
#    (except stateless or per-batch inside `foreachBatch`).
# 2. AQE dynamic skew join triggers ONLY when BOTH conditions hold:
#    partitionSize > skewedPartitionFactor * medianSize AND
#    partitionSize > skewedPartitionThresholdInBytes (default 256 MB).
# 3. `coalescePartitions.enabled` can merge tiny partitions down to 1, which is
#    great for downstream compute—unless you immediately write and want N files;
#    set `minPartitionSize` / `advisoryPartitionSizeInBytes` accordingly.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("075_AQEOptimization")
        .config("spark.sql.shuffle.partitions", "20")
        .getOrCreate()
    )
    configure_aqe(spark)
    df_res = run_aqe_coalesce_demo(spark)
    df_res.collect()
    df_res.explain("formatted")
