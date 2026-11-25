"""Scenario & Scale: Real-time card swipe fraud enrichment against customer SCD.

Enriches 20k+ auth swipes/sec with a bounded customer risk dimension table
without maintaining streaming state or watermarks on the lookup side.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Caching the static dimension table once at stream startup
# (`df_dim.cache()`) or attempting a RIGHT OUTER JOIN (stream on right).
# Otherwise: Cached static dimensions go stale permanently until cluster restart,
# and Spark rejects right-outer stream-static joins (cannot emit unmatched
# static rows continuously with unbounded streaming semantics).
def bad_stale_cached_dim_join(
    df_stream: DataFrame,
    df_static_dim: DataFrame,
) -> DataFrame:
    df_static_dim.cache()  # Freezes dimension snapshot for entire stream life
    return df_stream.join(df_static_dim, on="customer_id", how="right")


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Perform an inner or left-outer stream-static join against an
# uncached table/path DataFrame so each micro-batch reads the latest committed
# snapshot, and broadcast small dimension tables to avoid stream shuffles.
def enrich_swipes_with_customer_dim(
    df_swipes_stream: DataFrame,
    df_customers_static: DataFrame,
    broadcast_dim: bool = True,
) -> DataFrame:
    """Enrich streaming card swipes with current customer credit limits."""
    dim = F.broadcast(df_customers_static) if broadcast_dim else df_customers_static
    return (
        df_swipes_stream.join(dim, on="customer_id", how="left")
        .withColumn(
            "is_limit_breached",
            F.when(F.col("credit_limit").isNull(), F.lit(False)).otherwise(
                F.col("amount") > F.col("credit_limit")
            ),
        )
        .withColumn(
            "fraud_alert",
            F.when(F.col("customer_name").isNull(), F.lit("UNKNOWN_CUSTOMER"))
            .when(F.col("is_limit_breached"), F.lit("CREDIT_LIMIT_EXCEEDED"))
            .otherwise(F.lit("APPROVED")),
        )
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_enriched.explain("formatted") shows:
#   BroadcastHashJoin [customer_id], LeftOuter, BuildRight
#   :- StreamingRelation / Filter
#   +- BroadcastExchange -> BatchScan / FileScan (refreshed per micro-batch)
# Spark UI -> SQL / Stages tab:
#   - Zero StateStoreSave / StateStoreRestore operators appear because
#     stream-static joins are completely stateless.
#   - BroadcastExchange eliminates wide network shuffles on the high-volume
#     streaming side; watch driver memory if dimension table grows > 100 MB.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Stream-static joins do NOT require watermarks and support inner + left-outer
#    (stream on left), whereas right-outer and full-outer are unsupported.
# 2. Reading a Delta/Iceberg static table inside the join causes Spark to pick
#    up newly committed dimension versions automatically on each micro-batch.
# 3. If a Parquet static table is overwritten in-place without ACID snapshots,
#    concurrent micro-batches can fail with FileNotFoundException; always use
#    an ACID table format (Delta/Iceberg) for mutable static dimensions.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("057_StreamStaticJoins")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    df_dim = spark.createDataFrame(
        [
            ("C-101", "Alice Smith", 25000.0),
            ("C-102", "Bob Jones", 10000.0),
        ],
        "customer_id STRING, customer_name STRING, credit_limit DOUBLE",
    )
    df_txns = spark.createDataFrame(
        [
            ("TXN-01", "C-101", 450.0),
            ("TXN-02", "C-102", 12000.0),
            ("TXN-03", "C-999", 50.0),
        ],
        "txn_id STRING, customer_id STRING, amount DOUBLE",
    )
    enrich_swipes_with_customer_dim(df_txns, df_dim).show(truncate=False)
