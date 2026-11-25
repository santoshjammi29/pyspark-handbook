"""Scenario & Scale: Point-of-Sale fact-to-store dimension enrichment (2B txns/day).

Business Context:
Eliminate multi-gigabyte `SortMergeJoin` shuffles when joining massive POS facts with
small 2MB store lookup tables using `F.broadcast()` and auto-broadcast threshold tuning.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "031_Broadcast_Hash_Join_Tuning") -> SparkSession:
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
# - Disabling AQE or leaving `autoBroadcastJoinThreshold` too low when joining a small
#   computed dimension (causing a 2B-row `SortMergeJoin`), OR blindly wrapping a 4GB
#   table in `F.broadcast(df_large)`.
# Otherwise:
# - Missing broadcast triggers two massive `Exchange` shuffles and external disk sort.
# - Over-broadcasting a multi-GB table forces the Driver to collect the entire table
#   into JVM heap, triggering `OutOfMemoryError` or `BroadcastExchange` timeout.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Explicit `F.broadcast()` on verified small dimensions + AQE threshold awareness.
STORE_SCHEMA = T.StructType(
    [
        T.StructField("store_id", T.LongType(), nullable=False),
        T.StructField("store_name", T.StringType(), nullable=False),
        T.StructField("city", T.StringType(), nullable=False),
    ]
)


def enrich_transactions_with_stores(df_txns: DataFrame, df_stores: DataFrame) -> DataFrame:
    """Join large POS fact stream with small store lookup via BroadcastHashJoin."""
    return df_txns.join(F.broadcast(df_stores), on="store_id", how="inner")


def main() -> None:
    spark = build_spark()

    df_transactions = spark.range(1, 20001, 1, 4).select(
        F.col("id").alias("txn_id"),
        (F.col("id") % 10 + 1).alias("store_id"),
        F.round(F.rand(seed=42) * 200 + 5, 2).alias("amount"),
    )

    stores = [
        (1, "Downtown Flagship", "New York"),
        (2, "Airport Kiosk", "Chicago"),
        (3, "Suburban Mall", "Dallas"),
        (4, "Tech Center", "San Francisco"),
        (5, "Metro Hub", "Atlanta"),
        (6, "Harbor Outlet", "Boston"),
        (7, "Plaza Central", "Seattle"),
        (8, "Galleria", "Houston"),
        (9, "Old Town", "Denver"),
        (10, "Express Mart", "Miami"),
    ]
    df_stores = spark.createDataFrame(stores, schema=STORE_SCHEMA)

    df_joined = enrich_transactions_with_stores(df_transactions, df_stores)
    df_joined.explain(mode="formatted")

    threshold_bytes = int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
    assert threshold_bytes > 0
    assert df_joined.count() == 20000


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_joined.explain("formatted")` displays `BroadcastHashJoin` with `BroadcastExchange`
#   ONLY on the `df_stores` build side and ZERO `Exchange` on the 20,000-row fact side.
# - In Spark UI (SQL tab), verify `data size` of the `BroadcastExchange` node stays well
#   under `spark.driver.maxResultSize` and executor memory limits.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Default `spark.sql.autoBroadcastJoinThreshold` is `10MB`; with AQE enabled, Spark
#    dynamically converts `SortMergeJoin` to `BroadcastHashJoin` at runtime if post-filter
#    shuffle statistics fall below the threshold.
# 2. A small Parquet/Delta table on disk (e.g. 8MB Zstd-compressed) can expand 10x-20x
#    when materialized as uncompressed JVM hash-map rows in memory—causing Driver OOM!
# 3. Explicit `F.broadcast(df)` overrides `autoBroadcastJoinThreshold`, but you cannot
#    broadcast the left side of a `left_outer` join or the right side of a `right_outer` join.

if __name__ == "__main__":
    main()
