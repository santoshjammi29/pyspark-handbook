"""Scenario & Scale: Digital clickstream session pathing (250M events/day, 12m SLA).

Business Context:
Aggregate per-session page trajectories into both duplicate-preserving ordered lists
(`collect_list`) and unique page sets (`collect_set`) without relying on non-guaranteed
pre-shuffle sort order.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "022_Collect_List_Set_Sorted") -> SparkSession:
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
#   df.orderBy("event_time").groupBy("session_id").agg(F.collect_list("page_url"))
# Otherwise:
# - `groupBy()` introduces a hash shuffle (`Exchange`) that destroys upstream row
#   ordering; `collect_list` arrives in arbitrary task/partition completion order.
# - Calling `collect_list` on unpartitioned or bot-skewed sessions (millions of clicks
#   under one `session_id`) exhausts executor heap (`OutOfMemoryError`).


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Bundle `(sort_key, value)` in a `struct`, collect, `array_sort`, and project.
CLICKSTREAM_SCHEMA = T.StructType(
    [
        T.StructField("session_id", T.StringType(), nullable=False),
        T.StructField("event_time", T.StringType(), nullable=False),
        T.StructField("page_url", T.StringType(), nullable=False),
    ]
)


def aggregate_session_journeys(df: DataFrame) -> DataFrame:
    """Compute unique page sets and chronologically ordered page sequences per session."""
    df_typed = df.withColumn("event_ts", F.try_to_timestamp(F.col("event_time")))

    return (
        df_typed.groupBy("session_id")
        .agg(
            F.array_sort(F.collect_list(F.struct("event_ts", "page_url"))).alias("sorted_events"),
            F.collect_set("page_url").alias("unique_pages_set"),
        )
        .withColumn(
            "ordered_page_sequence",
            F.transform(F.col("sorted_events"), lambda x: x.getField("page_url")),
        )
        .drop("sorted_events")
    )


def main() -> None:
    spark = build_spark()

    clickstream = [
        ("SESS-1", "2023-11-20 10:00:00", "/home"),
        ("SESS-1", "2023-11-20 10:05:00", "/products"),
        ("SESS-1", "2023-11-20 10:02:00", "/products"),  # Out-of-order arrival
        ("SESS-1", "2023-11-20 10:10:00", "/checkout"),
        ("SESS-2", "2023-11-20 11:00:00", "/home"),
        ("SESS-2", "2023-11-20 11:04:00", "/pricing"),
        ("SESS-2", "2023-11-20 11:08:00", "/home"),
    ]
    df_clicks = spark.createDataFrame(clickstream, schema=CLICKSTREAM_SCHEMA)
    df_journeys = aggregate_session_journeys(df_clicks)

    s1 = df_journeys.filter(F.col("session_id") == "SESS-1").first()
    s2 = df_journeys.filter(F.col("session_id") == "SESS-2").first()
    assert s1["ordered_page_sequence"] == ["/home", "/products", "/products", "/checkout"]
    assert set(s2["unique_pages_set"]) == {"/home", "/pricing"}


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_journeys.explain("formatted")` shows `ObjectHashAggregate` (unlike `HashAggregate`
#   for primitive sums) because `collect_list`/`collect_set` buffer JVM array objects.
# - Monitor Spark UI Stage `Peak Execution Memory` and `Shuffle Spill (Memory/Disk)` for
#   outlier keys when collecting unbounded lists.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `array_sort` orders structs lexicographically starting from the first field; always
#    place `event_ts` (or sequence ID) as the FIRST field in `F.struct("event_ts", ...)`.
# 2. Both `collect_list` and `collect_set` ignore `NULL` values automatically.
# 3. Cap bot/crawler sessions (e.g. `F.slice(..., 1, 500)`) or filter bot IDs before
#    `collect_list` to prevent a single skewed key from crashing an executor with OOM.

if __name__ == "__main__":
    main()
