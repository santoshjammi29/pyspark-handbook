"""Scenario & Scale: IoT gateway ingesting connected-vehicle cellular telemetry.

Eliminates duplicate sensor events caused by tunnel reconnects within a rolling
20-minute SLA window while capping RocksDB key state memory.
"""

from pyspark.sql import DataFrame, SparkSession


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Calling dropDuplicates(["device_id"]) without a watermark or
# omitting the watermarked timestamp column from the deduplication key list.
# Otherwise: Spark buffers every distinct device_id ever observed in state
# memory indefinitely, leading to unrecoverable RocksDB disk/RAM exhaustion.
def bad_unbounded_dedup(df_stream: DataFrame) -> DataFrame:
    return df_stream.dropDuplicates(["device_id"])


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Combine withWatermark("event_time", window) with
# dropDuplicates(["device_id", "event_time"]) (or dropDuplicatesWithinWatermark
# in Spark 3.5+/4.0 when exact timestamp differs across retries).
def deduplicate_telemetry_stream(
    df_stream: DataFrame,
    watermark_delay: str = "20 minutes",
    within_watermark_only: bool = False,
) -> DataFrame:
    """Deduplicate streaming sensor events with bounded watermark eviction."""
    watermarked = df_stream.withWatermark("event_time", watermark_delay)
    if within_watermark_only:
        # Spark 3.5+/4.0: deduplicates by device_id even if retry timestamp shifts
        return watermarked.dropDuplicatesWithinWatermark(["device_id"])
    return watermarked.dropDuplicates(["device_id", "event_time"])


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_deduped.explain("formatted") shows:
#   EventTimeWatermark event_time#..., 20 minutes
#   -> Exchange hashpartitioning(device_id, event_time)
#   -> StreamingDeduplicate [device_id, event_time]
# Spark UI -> Structured Streaming tab -> State Operators:
#   - Track `numRowsTotal`: keys whose event_time < (max_event_time - 20 min)
#     are evicted after each micro-batch commit.
#   - Monitor `numRowsDroppedByWatermark` vs duplicates dropped internally by
#     StreamingDeduplicate (`numRowsUpdated` stays 0, duplicates discarded).


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Standard dropDuplicates() requires the watermark column in the subset keys
#    to enable state eviction; omit it and Spark keeps all keys forever.
# 2. dropDuplicatesWithinWatermark(["device_id"]) removes that requirement and
#    evicts keys purely by arrival/event watermark, ideal when retries assign new
#    timestamps—but duplicates arriving after the watermark window pass through.
# 3. Size the watermark conservatively above p99 network reconnect latency so
#    legitimate late retries are still caught before key eviction.


if __name__ == "__main__":
    from datetime import datetime, timedelta

    spark = (
        SparkSession.builder.appName("059_StreamingDeduplicationWatermark")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    base_ts = datetime(2025, 1, 15, 15, 0, 0)
    telemetry = [
        ("DEV-01", base_ts, 22.5),
        ("DEV-02", base_ts, 25.0),
        ("DEV-01", base_ts, 22.5),  # Exact duplicate -> dropped
        ("DEV-01", base_ts + timedelta(minutes=5), 23.0),
        ("DEV-03", base_ts + timedelta(minutes=10), 19.5),
    ]
    df = spark.createDataFrame(
        telemetry, "device_id STRING, event_time TIMESTAMP, temp DOUBLE"
    )
    df_out = deduplicate_telemetry_stream(df)
    assert df_out.count() == 4
    df_out.orderBy("device_id", "event_time").show(truncate=False)
