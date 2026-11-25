"""Scenario & Scale: Retail checkout matching order events with payment streams.

Joins two unbounded Kafka-scale event streams arriving up to 1 hour apart,
enforcing dual watermarks and interval bounds to prevent RocksDB state OOM.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Joining two streaming DataFrames solely on equality keys
# without dual watermarks and an explicit event-time range predicate.
# Otherwise: For inner joins without time bounds, Spark must buffer every
# unmatched order and payment in state forever until executor OOM; outer joins
# fail outright at query analysis time.
def bad_unbounded_stream_join(
    df_orders_stream: DataFrame,
    df_payments_stream: DataFrame,
) -> DataFrame:
    return df_orders_stream.join(
        df_payments_stream,
        on="order_id",
        how="left",  # Fails AnalysisException without dual watermark + interval
    )


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Define watermarks on BOTH streams and constrain the join
# predicate with an explicit event-time interval so Spark can evict expired
# buffered rows symmetrically from both left and right state stores.
def join_order_and_payment_streams(
    df_orders: DataFrame,
    df_payments: DataFrame,
    orders_watermark: str = "2 hours",
    payments_watermark: str = "3 hours",
    how: str = "inner",
) -> DataFrame:
    """Join order and payment streams within a bounded 1-hour event window."""
    orders_wm = df_orders.withWatermark("order_time", orders_watermark).alias(
        "ord"
    )
    payments_wm = df_payments.withWatermark(
        "payment_time", payments_watermark
    ).alias("pay")

    join_cond = (
        (F.col("ord.order_id") == F.col("pay.order_id"))
        & (F.col("pay.payment_time") >= F.col("ord.order_time"))
        & (
            F.col("pay.payment_time")
            <= F.col("ord.order_time") + F.expr("INTERVAL 1 HOUR")
        )
    )
    return orders_wm.join(payments_wm, on=join_cond, how=how).select(
        F.col("ord.order_id").alias("order_id"),
        F.col("ord.amount").alias("amount"),
        F.col("ord.order_time").alias("order_time"),
        F.col("pay.payment_id").alias("payment_id"),
        F.col("pay.payment_time").alias("payment_time"),
        F.round(
            F.try_divide(
                F.col("pay.payment_time").cast("long")
                - F.col("ord.order_time").cast("long"),
                F.lit(60.0),
            ),
            1,
        ).alias("payment_delay_minutes"),
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_joined.explain("formatted") shows:
#   StreamingSymmetricHashJoin [order_id], [order_id], Inner,
#     condition = [payment_time >= order_time AND payment_time <= order_time + 1h]
# Spark UI -> Structured Streaming tab -> State Operators:
#   - StreamingSymmetricHashJoin maintains FOUR internal state stores (two key
#     stores + two key-to-numValues stores per side).
#   - Verify `numRowsTotal` drops periodically as global watermark advances past
#     order_time + 1 hour; runaway growth indicates missing interval bounds.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Left/right/full outer stream-stream joins MUST specify both watermarks
#    and time interval bounds; unmatched rows are emitted ONLY after the global
#    watermark exceeds the upper interval bound (order_time + 1h + watermark).
# 2. The global watermark defaults to min(left_wm, right_wm), so an idle stream
#    freezes state eviction on BOTH sides.
# 3. Always enable RocksDBStateStoreProvider for multi-hour stream-stream joins
#    so state lives off-heap instead of exhausting JVM GC.


if __name__ == "__main__":
    from datetime import datetime, timedelta

    spark = (
        SparkSession.builder.appName("058_StreamStreamDualWatermarkJoins")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    base_ts = datetime(2025, 1, 15, 10, 0, 0)
    df_ord = spark.createDataFrame(
        [
            ("ORD-1", 120.0, base_ts),
            ("ORD-2", 450.0, base_ts + timedelta(minutes=5)),
        ],
        "order_id STRING, amount DOUBLE, order_time TIMESTAMP",
    )
    df_pay = spark.createDataFrame(
        [
            ("ORD-1", "PAY-901", base_ts + timedelta(minutes=2)),
            ("ORD-2", "PAY-902", base_ts + timedelta(minutes=45)),
            ("ORD-99", "PAY-999", base_ts + timedelta(minutes=15)),
        ],
        "order_id STRING, payment_id STRING, payment_time TIMESTAMP",
    )
    join_order_and_payment_streams(df_ord, df_pay).show(truncate=False)
