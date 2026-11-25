"""Scenario & Scale: E-commerce webhook CDC deduplication (150M events/day, 5m SLA).

Business Context:
Webhook retries deliver exact duplicate payloads as well as out-of-order state
updates (`CREATED` -> `PAID` -> `CANCELLED`). Silver tables must deterministically keep
only the latest valid event per `order_id`.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "009_Deterministic_Deduplication") -> SparkSession:
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
#   df.orderBy(F.desc("updated_at")).dropDuplicates(["order_id"])
# Otherwise:
# - Catalyst strips or ignores upstream ordering across the `HashAggregate` / shuffle
#   boundary emitted by `dropDuplicates(["order_id"])`, yielding non-deterministic
#   winners (`CREATED` overwriting `PAID`) across runs.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: `Window.partitionBy(pk).orderBy(ts.desc(), tie_breaker)` + `row_number() == 1`.
WEBHOOK_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), nullable=False),
        T.StructField("order_status", T.StringType(), nullable=False),
        T.StructField("updated_at", T.StringType(), nullable=False),
        T.StructField("total_amount", T.DoubleType(), nullable=False),
    ]
)


def deduplicate_latest_orders(df: DataFrame) -> DataFrame:
    """Retain the latest state per order_id deterministically via Window row_number."""
    df_with_ts = df.withColumn("updated_ts", F.try_to_timestamp(F.col("updated_at")))
    window_spec = Window.partitionBy("order_id").orderBy(
        F.col("updated_ts").desc_nulls_last(),
        F.col("order_status").desc(),
    )
    return (
        df_with_ts.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn", "updated_ts")
    )


def main() -> None:
    spark = build_spark()

    webhook_events = [
        ("ORD-101", "CREATED", "2023-11-20 09:00:00", 120.00),
        ("ORD-101", "PAID", "2023-11-20 09:05:00", 120.00),
        ("ORD-101", "PAID", "2023-11-20 09:05:00", 120.00),
        ("ORD-102", "CREATED", "2023-11-20 09:10:00", 85.50),
        ("ORD-102", "CANCELLED", "2023-11-20 09:12:00", 85.50),
        ("ORD-103", "CREATED", "2023-11-20 09:15:00", 430.00),
    ]
    df_webhooks = spark.createDataFrame(webhook_events, schema=WEBHOOK_SCHEMA)
    df_dedup = deduplicate_latest_orders(df_webhooks)

    rows = {r["order_id"]: r["order_status"] for r in df_dedup.collect()}
    assert rows == {"ORD-101": "PAID", "ORD-102": "CANCELLED", "ORD-103": "CREATED"}


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_dedup.explain("formatted")` uses Catalyst's `WindowGroupLimit` optimization
#   (Spark 3.4+/4.0) when filtering `row_number() == 1`, pushing Top-1 heap pruning
#   before and after `Exchange hashpartitioning(order_id)` to cut shuffle spill.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `distinct()` hashes every column; `dropDuplicates(["pk"])` hashes by key and picks
#    an arbitrary first row per partition—use it ONLY when all duplicates are identical.
# 2. Always include a deterministic tie-breaker column (e.g. `ingest_seq` or status) in
#    `orderBy(...)` when two CDC updates can share the exact same millisecond timestamp.
# 3. On massive append tables, replace full-table `row_number()` with incremental
#    `MERGE INTO` keyed on `order_id` and guarded by `target.updated_at < source.updated_at`.

if __name__ == "__main__":
    main()
