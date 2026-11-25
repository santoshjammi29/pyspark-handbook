"""Project Telecom: Customer Churn Feature Engineering & Rolling Windows.

Scenario & Scale:
    A mobile carrier processes 100M+ daily subscriber usage logs to compute
    point-in-time rolling 30-day behavioral features (data drop ratio, support
    call spikes) for churn ML training and Feature Store serving.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Using `rowsBetween(-29, 0)` on irregular daily telemetry or
# computing global aggregates across the full dataset before training split.
# Otherwise: Missing days shift `rowsBetween` to cover 45-90 calendar days,
# while global aggregates leak future subscriber behavior into historical labels.
def transform_churn_features_leaky_bad(df_usage: DataFrame) -> DataFrame:
    win = Window.partitionBy("subscriber_id").orderBy("usage_date").rowsBetween(-29, 0)
    return df_usage.withColumn("avg_data_30d", F.avg("data_gb").over(win))  # Wrong on sparse dates


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Convert dates to epoch days (`unix_date` / date cast to INT)
# and use `rangeBetween(-29, 0)` for exact 30-calendar-day rolling windows,
# guarding division with `F.try_divide` under ANSI mode.
def transform_build_churn_features(df_usage: DataFrame) -> DataFrame:
    """Compute point-in-time 30-day calendar rolling churn features per subscriber."""
    df_dated = (
        df_usage.withColumn("usage_dt", F.try_to_date(F.col("usage_date"), "yyyy-MM-dd"))
        .withColumn("data_gb_num", F.try_cast(F.col("data_gb"), T.DoubleType()))
        .filter(F.col("usage_dt").isNotNull())
        .withColumn("day_epoch", F.unix_date(F.col("usage_dt")))
    )

    win_30d = Window.partitionBy("subscriber_id").orderBy("day_epoch").rangeBetween(-29, 0)

    return (
        df_dated.withColumn("avg_data_gb_30d", F.round(F.avg("data_gb_num").over(win_30d), 2))
        .withColumn("total_support_calls_30d", F.sum("support_calls").over(win_30d))
        .withColumn("max_daily_call_mins_30d", F.max("call_mins").over(win_30d))
        .withColumn(
            "data_usage_drop_ratio",
            F.round(
                F.coalesce(F.try_divide(F.col("data_gb_num"), F.col("avg_data_gb_30d")), F.lit(1.0)),
                2,
            ),
        )
        .withColumn(
            "is_high_churn_risk",
            F.when(
                (F.col("data_usage_drop_ratio") < F.lit(0.4))
                & (F.col("total_support_calls_30d") >= F.lit(2)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .select(
            "subscriber_id",
            "usage_dt",
            "avg_data_gb_30d",
            "total_support_calls_30d",
            "max_daily_call_mins_30d",
            "data_usage_drop_ratio",
            "is_high_churn_risk",
        )
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` displays `Window` with `RANGE BETWEEN 29 PRECEDING
#   AND CURRENT ROW` after `Exchange hashpartitioning(subscriber_id)` and `Sort`.
# - Monitor Spark UI spill metrics on `WindowExec`; partition upstream Delta
#   usage tables by `usage_dt` so daily incremental runs only read a 30-day tail.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `rangeBetween(-29, 0)` requires an integral/numeric `orderBy` expression:
#    use `F.unix_date(usage_dt)` (days since 1970-01-01) for calendar days or
#    `cast("long")` on timestamps for seconds-based windows.
# 2. Point-in-time joins (`ASOF` / `timestamp <= label_ts`) prevent data leakage
#    between offline Feature Store training sets and online real-time serving.
# 3. `F.try_divide` avoids `DIVIDE_BY_ZERO` crashes in ANSI mode when a new
#    subscriber has `0.0` average data usage over the rolling window.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("099_telecom_churn_features")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    usage_rows = [
        ("SUB-1", "2025-01-01", 1.5, 45, 0),
        ("SUB-1", "2025-01-05", 2.0, 30, 0),
        ("SUB-1", "2025-01-15", 0.5, 10, 1),
        ("SUB-1", "2025-01-20", 0.1, 5, 2),
        ("SUB-2", "2025-01-20", 3.2, 55, 0),
    ]
    df_in = spark.createDataFrame(
        usage_rows, "subscriber_id STRING, usage_date STRING, data_gb DOUBLE, call_mins INT, support_calls INT"
    )
    transform_build_churn_features(df_in).show(truncate=False)
