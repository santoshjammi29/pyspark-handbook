"""Scenario & Scale: Mobile gaming telemetry aggregating 10-minute score windows.

Handles up to 15-minute airplane-mode client delays across millions of active
players while bounding RocksDB state store memory via event-time watermarks.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Running stateful window aggregations without withWatermark()
# or attempting outputMode("append") on a windowed groupBy without a watermark.
# Otherwise: Spark throws AnalysisException in append mode, or in update/complete
# mode retains every historical window in RocksDB/JVM heap forever until OOM.
def bad_unbounded_window_agg(df_stream: DataFrame) -> DataFrame:
    return df_stream.groupBy(
        F.window("event_time", "10 minutes"),
        "player",
    ).agg(F.sum("score").alias("total_score"))


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Pair withWatermark(event_time, delay) BEFORE groupBy(window)
# so Spark computes Watermark = Max(event_time) - delay and safely evicts
# expired windows from state memory once window.end <= Watermark.
def aggregate_windowed_scores(
    df_events: DataFrame,
    watermark_delay: str = "15 minutes",
    window_duration: str = "10 minutes",
) -> DataFrame:
    """Compute tumbling window score totals with bounded watermark state."""
    return (
        df_events.withWatermark("event_time", watermark_delay)
        .groupBy(F.window("event_time", window_duration), F.col("player"))
        .agg(
            F.sum(F.col("score").cast("bigint")).alias("total_score"),
            F.count(F.lit(1)).alias("events_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("player"),
            F.col("total_score"),
            F.col("events_count"),
        )
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_windowed.explain("formatted") shows:
#   EventTimeWatermark event_time#..., 15 minutes
#   -> HashAggregate (partial)
#   -> Exchange hashpartitioning(window, player)
#   -> StateStoreRestore -> HashAggregate -> StateStoreSave
# Spark UI -> Structured Streaming tab -> State Operators:
#   - Monitor `numRowsTotal` and `memoryUsedBytes`: steady-state plateau confirms
#     windows older than (max_event_time - watermark_delay) are being evicted.
#   - Check `numRowsDroppedByWatermark` to track events exceeding the 15m SLA.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Append mode holds window output until window.end <= watermark, adding
#    watermark_delay + window_duration latency; use Update mode for low latency.
# 2. Watermarks advance ONLY when new data arrives—idle Kafka partitions hold
#    back the global minimum watermark unless spark.sql.streaming.multipleWatermarkPolicy
#    or idle-partition timeouts are understood.
# 3. Any event with event_time < current_watermark is silently dropped before
#    updating aggregation state; route ultra-late events to a side/DLQ pipeline
#    if strict financial completeness is required.


if __name__ == "__main__":
    from datetime import datetime, timedelta

    spark = (
        SparkSession.builder.appName("056_StreamingWindowsWatermarking")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    base_ts = datetime(2025, 1, 15, 12, 0, 0)
    sample = [
        (base_ts, "Alice", 100),
        (base_ts + timedelta(minutes=2), "Bob", 150),
        (base_ts + timedelta(minutes=15), "Charlie", 300),
        (base_ts + timedelta(minutes=3), "Alice", 50),  # Late within 15m delay
    ]
    df = spark.createDataFrame(
        sample, "event_time TIMESTAMP, player STRING, score INT"
    )
    aggregate_windowed_scores(df).orderBy("window_start", "player").show(
        truncate=False
    )
