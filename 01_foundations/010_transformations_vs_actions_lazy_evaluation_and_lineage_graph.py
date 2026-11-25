"""Scenario & Scale: Diagnosing lazy DAG execution & shuffle stage boundaries (500M rows/job).

Business Context:
Engineers often misattribute 40-minute job failures to a final `.count()` action when
the actual bottleneck lies in upstream wide shuffles or repeated uncached evaluations
of an expensive lineage graph.
"""

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_spark(app_name: str = "010_Lazy_Evaluation_DAG_Lineage") -> SparkSession:
    """Create or reuse a portable SparkSession with ANSI mode enabled."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice:
# - Calling `.count()`, `.show()`, and `.write` sequentially on the same unpersisted
#   wide DataFrame, or calling `df.rdd.toDebugString()` in Spark Connect workloads.
# Otherwise:
# - Each action re-executes the entire upstream DAG from source storage (3x I/O & compute).
# - Calling `.rdd` or `sparkContext` raises `PySparkNotImplementedError` on Spark Connect.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Separate narrow/wide plan construction, inspect via `explain("formatted")`,
# and reuse results with `.persist()` or single-pass aggregations.
def build_narrow_and_wide_plan(spark: SparkSession) -> DataFrame:
    """Construct a lazy DAG combining narrow pipelined filters and a wide groupBy shuffle."""
    df_nums = spark.range(1, 100001, 1, 4)

    # Narrow transformations: stay inside the same partition (no Exchange)
    df_narrow = df_nums.filter(F.col("id") % 2 == 0).withColumn(
        "squared", F.col("id") * F.col("id")
    )

    # Wide transformation: introduces a shuffle Exchange boundary
    return df_narrow.groupBy((F.col("id") % 10).alias("bucket")).agg(
        F.sum("squared").alias("sum_squares")
    )


def main() -> None:
    spark = build_spark()

    # Zero computation happens while building the logical plan
    df_wide = build_narrow_and_wide_plan(spark)

    # Inspect logical + physical DAG cleanly on both Classic & Spark Connect
    df_wide.explain(mode="formatted")

    start = time.monotonic()
    df_persisted = df_wide.persist()
    result_count = df_persisted.count()
    rows = df_persisted.collect()
    df_persisted.unpersist()
    elapsed = time.monotonic() - start

    assert result_count == 5
    assert len(rows) == 5
    assert elapsed >= 0.0


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - In `explain("formatted")`, narrow operations (`Filter`, `Project`) fuse inside a
#   single `WholeStageCodegen` block; every `Exchange` operator marks a Stage boundary.
# - In Spark UI (Jobs / SQL tabs), verify `.persist()` turns the second action's stage
#   into `InMemoryTableScan` instead of re-reading source partitions.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `count()`, `collect()`, `take()`, `first()`, `show()`, and `write` are actions;
#    `filter()`, `select()`, `groupBy()`, and `join()` only build Catalyst logical plans.
# 2. Spark 4.0+ Spark Connect forbids `.rdd` and `spark.sparkContext` calls; rely on
#    `df.explain("formatted")` and the Spark UI SQL/DAG tabs instead of `toDebugString()`.
# 3. Always pair `.persist()` with `.unpersist()` in a `try/finally` block on long-lived
#    sessions so cached blocks do not starve downstream executor memory.

if __name__ == "__main__":
    main()
