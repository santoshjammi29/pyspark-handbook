"""Project Finance: Credit Card Fraud Velocity Detection & Customer 360.

Scenario & Scale:
    A tier-1 retail bank processes 50M credit card swipes daily. Risk pipelines
    must flag impossible geo-velocity (< 30 min across distinct cities) and
    high-value anomalies in Silver, then publish a Gold Customer 360 scorecard.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Self-joining 50M swipes to themselves to compare consecutive
# events, or casting timestamps/amounts without ANSI `try_*` guards.
# Otherwise: O(N^2) self-join explodes shuffle spill and misses SLA, while one
# corrupt swipe timestamp aborts the entire risk-scoring batch under ANSI mode.
def transform_fraud_self_join_bad(df_swipes: DataFrame) -> DataFrame:
    return df_swipes.alias("a").join(df_swipes.alias("b"), on="card_id")  # Cartesian blowup


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Single-pass `Window.partitionBy("card_id").orderBy("ts")`
# with `F.lag()`, `F.try_to_timestamp()`, and `F.try_cast()` -> Gold scorecard.
def transform_silver_fraud_scoring(df_bronze: DataFrame) -> DataFrame:
    """Parse swipe events safely and score geo-velocity & high-value risk."""
    df_typed = (
        df_bronze.withColumn("ts", F.try_to_timestamp(F.col("swipe_time"), F.lit("yyyy-MM-dd HH:mm:ss")))
        .withColumn("amount_dec", F.try_cast(F.col("amount"), T.DecimalType(14, 2)))
        .filter(F.col("ts").isNotNull() & F.col("amount_dec").isNotNull())
    )

    card_win = Window.partitionBy("card_id").orderBy("ts")
    return (
        df_typed.withColumn("prev_city", F.lag("city").over(card_win))
        .withColumn("prev_ts", F.lag("ts").over(card_win))
        .withColumn(
            "elapsed_minutes",
            F.try_divide(F.col("ts").cast("long") - F.col("prev_ts").cast("long"), F.lit(60.0)),
        )
        .withColumn(
            "is_geo_velocity_fraud",
            F.col("prev_city").isNotNull()
            & (F.col("city") != F.col("prev_city"))
            & (F.col("elapsed_minutes") < F.lit(30.0)),
        )
        .withColumn("is_high_value_alert", F.col("amount_dec") >= F.lit(5000))
        .withColumn(
            "fraud_risk_score",
            F.when(F.col("is_geo_velocity_fraud"), F.lit(95))
            .when(F.col("is_high_value_alert"), F.lit(75))
            .otherwise(F.lit(10)),
        )
    )


def transform_gold_card_360(df_silver_fraud: DataFrame) -> DataFrame:
    """Aggregate card-level risk posture for automated card-hold actions."""
    return (
        df_silver_fraud.groupBy("card_id")
        .agg(
            F.count("swipe_id").alias("total_swipes"),
            F.round(F.sum("amount_dec"), 2).alias("total_spend"),
            F.max("fraud_risk_score").alias("peak_risk_score"),
            F.sum(F.when(F.col("fraud_risk_score") >= 75, F.lit(1)).otherwise(F.lit(0))).alias(
                "suspicious_events_count"
            ),
        )
        .withColumn(
            "account_status",
            F.when(F.col("peak_risk_score") >= 90, F.lit("LOCKED_FRAUD_HOLD"))
            .when(F.col("peak_risk_score") >= 70, F.lit("FLAGGED_FOR_REVIEW"))
            .otherwise(F.lit("GOOD_STANDING")),
        )
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows a single `Exchange hashpartitioning(card_id)`
#   -> `Sort` -> `Window` (O(N log N) per card) instead of an O(N^2) join.
# - Watch for skewed `card_id` values (corporate/fleet cards with millions of
#   swipes) in Spark UI Tasks tab; pre-aggregate or salt synthetic card pools.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. In Structured Streaming, `lag()` over an unbounded `Window` is unsupported;
#    use Spark 4.0 `transformWithState` (`StatefulProcessor` + `ValueState` + TTL)
#    to store each card's last `(city, ts)` across micro-batches.
# 2. Always filter `ts IS NOT NULL` before `Window.orderBy("ts")` so corrupt
#    null timestamps don't pollute `lag()` intervals.
# 3. Use `DecimalType(14, 2)` for currency amounts to avoid rounding drift.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("096_finance_fraud_360")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    swipes = [
        ("SW-01", "CARD-101", 85.50, "New York", "2025-01-20 12:00:00"),
        ("SW-02", "CARD-101", 1200.00, "London", "2025-01-20 12:05:00"),
        ("SW-03", "CARD-102", 45.00, "Chicago", "2025-01-20 12:00:00"),
        ("SW-05", "CARD-103", 9500.00, "Miami", "2025-01-20 12:10:00"),
    ]
    df_in = spark.createDataFrame(
        swipes, "swipe_id STRING, card_id STRING, amount DOUBLE, city STRING, swipe_time STRING"
    )
    df_silver = transform_silver_fraud_scoring(df_in)
    df_silver.select("swipe_id", "card_id", "city", "prev_city", "fraud_risk_score").show()
    transform_gold_card_360(df_silver).show(truncate=False)
