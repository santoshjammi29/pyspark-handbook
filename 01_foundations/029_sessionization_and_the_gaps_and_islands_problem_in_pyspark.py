"""Scenario & Scale: Clickstream inactivity sessionization (500M clicks/day, 15m SLA).

Business Context:
Solve the Gaps & Islands problem in distributed PySpark: split user page-view events
into distinct sessions whenever inactivity exceeds 30 minutes, then aggregate metrics.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "029_Sessionization_Gaps_Islands") -> SparkSession:
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
# - Collecting all user events to the driver or using a stateful Python UDF / `mapInPandas`
#   to loop chronologically over every user's click list in Python memory.
# Otherwise:
# - Pulling millions of clicks per power-user/bot into a single Python object causes
#   executor/driver OutOfMemoryError and forfeits Catalyst WholeStageCodegen.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Two-phase JVM Window pattern: `lag()` boundary flag + cumulative `sum()`.
CLICKS_SCHEMA = T.StructType(
    [
        T.StructField("user_id", T.StringType(), nullable=False),
        T.StructField("event_time", T.StringType(), nullable=False),
        T.StructField("page", T.StringType(), nullable=False),
    ]
)


def sessionize_clicks(df: DataFrame, gap_minutes: float = 30.0) -> DataFrame:
    """Assign deterministic session IDs using a 30-minute inactivity timeout."""
    df_ts = df.withColumn("ts", F.try_to_timestamp(F.col("event_time")))
    user_window = Window.partitionBy("user_id").orderBy("ts", "page")
    cum_window = user_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    prev_ts = F.lag("ts").over(user_window)
    inactivity_min = F.coalesce(
        F.try_divide(F.col("ts").cast(T.LongType()) - prev_ts.cast(T.LongType()), F.lit(60.0)),
        F.lit(0.0),
    )

    df_flagged = df_ts.withColumns(
        {
            "inactivity_minutes": inactivity_min,
            "is_new_session": F.when(
                (inactivity_min > gap_minutes) | prev_ts.isNull(), 1
            ).otherwise(0),
        }
    )

    return df_flagged.withColumn(
        "session_num", F.sum("is_new_session").over(cum_window)
    ).withColumn(
        "session_id", F.concat(F.col("user_id"), F.lit("_SESS_"), F.col("session_num"))
    )


def summarize_sessions(df_sessionized: DataFrame) -> DataFrame:
    """Aggregate session start, end, page view count, and duration in minutes."""
    return df_sessionized.groupBy("session_id", "user_id").agg(
        F.min("ts").alias("session_start"),
        F.max("ts").alias("session_end"),
        F.count("page").alias("page_views"),
        F.round(
            F.try_divide(
                F.max("ts").cast(T.LongType()) - F.min("ts").cast(T.LongType()),
                F.lit(60.0),
            ),
            1,
        ).alias("session_duration_min"),
    )


def main() -> None:
    spark = build_spark()

    clicks = [
        ("U-01", "2023-11-20 10:00:00", "/home"),
        ("U-01", "2023-11-20 10:12:00", "/catalog"),
        ("U-01", "2023-11-20 10:25:00", "/product/42"),
        ("U-01", "2023-11-20 11:30:00", "/home"),
        ("U-01", "2023-11-20 11:40:00", "/cart"),
        ("U-02", "2023-11-20 09:00:00", "/login"),
        ("U-02", "2023-11-20 09:15:00", "/dashboard"),
    ]
    df_raw = spark.createDataFrame(clicks, schema=CLICKS_SCHEMA)
    df_sessionized = sessionize_clicks(df_raw, gap_minutes=30.0)
    df_summary = summarize_sessions(df_sessionized)

    assert df_summary.count() == 3
    u01_s1 = df_summary.filter(F.col("session_id") == "U-01_SESS_1").first()
    u01_s2 = df_summary.filter(F.col("session_id") == "U-01_SESS_2").first()
    assert u01_s1["page_views"] == 3 and u01_s1["session_duration_min"] == 25.0
    assert u01_s2["page_views"] == 2 and u01_s2["session_duration_min"] == 10.0


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Because `user_window` and `cum_window` share `(partitionBy=user_id, orderBy=ts,page)`,
#   Catalyst executes ONE `Exchange hashpartitioning(user_id)` + `Sort` across both
#   window stages, followed by one `HashAggregate` for `summarize_sessions`.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always parse timestamp strings via `try_to_timestamp` before sorting/subtracting;
#    string subtraction fails and non-ISO dates sort lexicographically wrong.
# 2. Keep a deterministic secondary sort key (`page` or `event_id`) inside `orderBy`
#    when multiple clicks occur within the exact same second.
# 3. In Structured Streaming, use native `F.session_window("ts", "30 minutes")` with
#    watermarking rather than batch `lag()` + cumulative `sum()`.

if __name__ == "__main__":
    main()
