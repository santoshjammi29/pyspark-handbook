"""Scenario & Scale: Sales quota ranking & Top-N leaderboards (50M rep scores/quarter).

Business Context:
Rank sales reps within geographic territories while handling ties deterministically
(`row_number`, `rank`, `dense_rank`, `percent_rank`) and pruning Top-N per partition.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "026_Ranking_Window_Functions") -> SparkSession:
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
#   Window.orderBy(F.col("quota_pct").desc())  # missing .partitionBy("region")
# Otherwise:
# - An unpartitioned `Window.orderBy(...)` forces Spark to coalesce 100% of rows into
#   a single partition on one executor (`Exchange SinglePartition`), causing severe
#   single-node bottleneck and OutOfMemoryError on multi-GB tables.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Partitioned WindowSpec + tie-breaker ordering + Top-N pruning.
REPS_SCHEMA = T.StructType(
    [
        T.StructField("region", T.StringType(), nullable=False),
        T.StructField("rep_name", T.StringType(), nullable=False),
        T.StructField("quota_pct", T.DoubleType(), nullable=False),
    ]
)


def compute_quota_rankings(df: DataFrame) -> DataFrame:
    """Apply all four ranking functions partitioned by region with deterministic tie-breaking."""
    tie_aware_window = Window.partitionBy("region").orderBy(
        F.col("quota_pct").desc(), F.col("rep_name").asc()
    )
    score_window = Window.partitionBy("region").orderBy(F.col("quota_pct").desc())

    return df.withColumns(
        {
            "row_num": F.row_number().over(tie_aware_window),
            "rank": F.rank().over(score_window),
            "dense_rank": F.dense_rank().over(score_window),
            "percent_rank": F.round(F.percent_rank().over(score_window), 2),
        }
    )


def top_n_by_dense_rank(df_ranked: DataFrame, n: int = 2) -> DataFrame:
    """Return Top-N performers per region while keeping all tied runners-up."""
    return df_ranked.filter(F.col("dense_rank") <= n).select(
        "region", "rep_name", "quota_pct", "dense_rank"
    )


def main() -> None:
    spark = build_spark()

    reps_data = [
        ("East", "Alice", 125.0),
        ("East", "Bob", 110.0),
        ("East", "Charlie", 110.0),
        ("East", "David", 95.0),
        ("West", "Eve", 140.0),
        ("West", "Frank", 130.0),
        ("West", "Grace", 130.0),
        ("West", "Heidi", 100.0),
    ]
    df_reps = spark.createDataFrame(reps_data, schema=REPS_SCHEMA)

    df_ranked = compute_quota_rankings(df_reps)
    df_top2 = top_n_by_dense_rank(df_ranked, n=2)

    david = df_ranked.filter(F.col("rep_name") == "David").first()
    assert david["rank"] == 4 and david["dense_rank"] == 3
    assert df_top2.filter(F.col("region") == "East").count() == 3  # Alice, Bob, Charlie


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_top2.explain("formatted")` leverages Catalyst's `WindowGroupLimit` operator
#   for `dense_rank <= 2` / `row_number <= N`, performing partial Top-N pruning BEFORE
#   `Exchange hashpartitioning(region)` to shrink shuffle I/O dramatically.
# - Look out for `Exchange SinglePartition` in `explain("formatted")`; it indicates a
#   missing `partitionBy(...)` clause on a Window specification.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `row_number()` assigns `1,2,3,4` (always add a unique tie-breaker column for
#    determinism); `rank()` skips gaps (`1,2,2,4`); `dense_rank()` has no gaps (`1,2,2,3`).
# 2. `percent_rank()` computes `(rank - 1) / (N - 1)` in `[0.0, 1.0]` and returns `0.0`
#    for a 1-row partition; `ntile(k)` splits rows into `k` roughly equal buckets.
# 3. If filtering by `row_number() <= k`, Spark pushes `WindowGroupLimit` down only
#    when the filter sits immediately above the `Window` without intervening UDFs.

if __name__ == "__main__":
    main()
