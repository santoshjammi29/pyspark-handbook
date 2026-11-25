"""Scenario & Scale: Nightly reconciliation job degrading 3x on weekends.

Provides Spark 4.0+ query tagging (`spark.addTag`), EventLog / Spark UI metrics
triage for task skew, shuffle spill (Memory vs Disk), and GC heap thrashing.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Blindly doubling cluster size when a job slows 3x, or
# pulling full datasets with `.count()` / `.collect()` just for skew metrics.
# Otherwise: Adding more nodes does nothing for a single-task skew bottleneck or
# driver planning stall (burning 2x cloud spend), and un-tagged queries make
# Spark UI / EventLog forensic correlation across stages nearly impossible.
def bad_untagged_blind_scale(spark: SparkSession, df: DataFrame) -> int:
    # Forces full driver collection & lacks query tags for Spark UI triage
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    return len(df.collect())


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Tag queries via `spark.addTag()` (Spark 3.5+/4.0 Connect-safe)
# for Spark UI / EventLog correlation, inspect partition skew & execution plans
# programmatically, and tune shuffle partitions + AQE to eliminate disk spill.
def run_tagged_reconciliation(
    spark: SparkSession,
    df_events: DataFrame,
    job_tag: str = "nightly_reconciliation",
) -> DataFrame:
    """Execute tagged aggregation with AQE and spill-safe partition sizing."""
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.addTag(job_tag)
    try:
        return (
            df_events.groupBy((F.col("id") % F.lit(10)).alias("acct_bucket"))
            .agg(
                F.count(F.lit(1)).alias("txn_count"),
                F.sum(F.col("id").cast("double")).alias("total_amt"),
            )
        )
    finally:
        spark.removeTag(job_tag)


def compute_skew_ratio(df: DataFrame, group_col: str) -> DataFrame:
    """Quantify key skew (max / median) before a heavy shuffle."""
    counts = df.groupBy(group_col).agg(F.count(F.lit(1)).alias("key_rows"))
    return counts.agg(
        F.min("key_rows").alias("min_rows"),
        F.percentile_approx("key_rows", 0.5).alias("median_rows"),
        F.percentile_approx("key_rows", 0.75).alias("p75_rows"),
        F.max("key_rows").alias("max_rows"),
        F.round(
            F.try_divide(
                F.max("key_rows").cast("double"),
                F.percentile_approx("key_rows", 0.5).cast("double"),
            ),
            2,
        ).alias("max_to_median_ratio"),
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# Spark UI / EventLog Forensics Matrix (Jobs -> Stages -> Event Timeline):
#   1. Task Skew: Stages Summary shows `Max Duration >> 75th / Median` (e.g.
#      p75=3s, Max=45m) and `Max Shuffle Read >> Median`. Fix: AQE skewJoin / salt.
#   2. Shuffle Spill: `Spill (Memory)` = uncompressed RAM bytes evicted;
#      `Spill (Disk)` = compressed bytes written to executor scratch SSD.
#      Any `Spill (Disk) > 0` indicates partitions exceed execution memory ->
#      increase `spark.sql.shuffle.partitions` or executor RAM/core ratio.
#   3. GC Thrashing: `Task GC Time > 10%` of Task Execution Time in Stages/Executors
#      tab -> reduce cached objects, avoid collect_list/UDFs, or use off-heap.
#   4. Scheduler / Deserialization Delay: large cyan/orange bars in Event
#      Timeline indicate driver task-serialization or Python worker startup lag.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. `Spill (Memory)` is ALWAYS larger than `Spill (Disk)` for the same spill
#    event because Spark tracks deserialized memory size before serializing and
#    compressing to disk (`Spill (Disk)`).
# 2. `spark.addTag()` attaches searchable tags visible in Spark UI SQL/Jobs
#    tabs and Spark Connect server logs, replacing legacy `setJobGroup()`.
# 3. High `Shuffle Write` + low CPU + long `Fetch Wait Time` points to network
#    or disk I/O contention rather than CPU starvation—adding cores worsens it.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("083_SparkUIForensics")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    df_src = spark.range(1, 100_000, 1, 4).withColumn(
        "acct_key",
        F.when(F.col("id") < 70_000, F.lit("HOT_ACCT")).otherwise(
            (F.col("id") % F.lit(100)).cast("string")
        ),
    )
    compute_skew_ratio(df_src, "acct_key").show()
    df_out = run_tagged_reconciliation(spark, df_src)
    df_out.explain("formatted")
    df_out.show()
