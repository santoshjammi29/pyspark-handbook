"""Scenario & Scale: Payments platform ingesting live card swipes globally.

Processes 50k+ events/sec continuously via Structured Streaming without
dropping micro-batches, leveraging WAL offsets and RocksDB state tracking.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Pulling micro-batch rows to the driver via .collect() /
# .toPandas() or omitting checkpointLocation and outputMode guarantees.
# Otherwise: Driver OOM crashes under burst traffic, unbounded WAL loss on
# restart, and duplicate or dropped payment transactions across failures.
def bad_ingest_stream(spark: SparkSession) -> None:
    df_raw = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    # Triggering unbounded driver collection or console sink without checkpoint
    df_raw.writeStream.format("console").start()


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Declarative streaming DataFrame transformations with explicit
# schemas, ANSI-safe casting/division, checkpointLocation, and query telemetry.
def build_transaction_stream(
    spark: SparkSession,
    rows_per_second: int = 10,
) -> DataFrame:
    """Build a normalized synthetic payments stream using the rate source."""
    df_raw = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", rows_per_second)
        .option("numPartitions", 2)
        .load()
    )
    return df_raw.select(
        F.col("timestamp").alias("event_time"),
        F.concat(F.lit("TXN-"), F.col("value").cast("string")).alias(
            "transaction_id"
        ),
        F.round(
            F.try_cast(F.col("value") % F.lit(500), "double") + F.lit(10.50),
            2,
        ).alias("amount"),
        F.when(F.col("value") % F.lit(10) == 0, F.lit("HIGH_RISK"))
        .otherwise(F.lit("STANDARD"))
        .alias("risk_category"),
    )


def start_stream_with_checkpoint(
    df_stream: DataFrame,
    sink_uri: str,
    checkpoint_uri: str,
    query_name: str = "live_transactions",
) -> StreamingQuery:
    """Start an append-mode Parquet stream with durable WAL checkpointing."""
    return (
        df_stream.writeStream.format("parquet")
        .queryName(query_name)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_uri)
        .start(sink_uri)
    )


def inspect_query_progress(query: StreamingQuery) -> dict:
    """Return key Structured Streaming throughput and latency telemetry."""
    progress = query.lastProgress or {}
    return {
        "batch_id": progress.get("batchId"),
        "num_input_rows": progress.get("numInputRows", 0),
        "input_rows_per_sec": progress.get("inputRowsPerSecond", 0.0),
        "processed_rows_per_sec": progress.get("processedRowsPerSecond", 0.0),
        "duration_ms": progress.get("durationMs", {}),
    }


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_stream.explain("formatted") shows StreamingRelationV2 -> Project.
# Spark UI -> Structured Streaming tab:
#   - Compare Input Rate (rows/s) vs Process Rate (rows/s); sustained
#     Process Rate < Input Rate signals growing micro-batch backlog.
#   - Inspect Batch Duration breakdown: triggerExecution = getBatch +
#     queryPlanning + addBatch + walCommit + commitOffsets.
#   - Verify Offset Log and Commit Log advance monotonically per batchId.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Exactly-once fault tolerance requires BOTH a replayable source (Kafka,
#    cloudFiles, file stream) AND an idempotent sink + durable checkpointLocation.
# 2. Append mode emits rows only once; Update emits changed keys; Complete
#    re-emits the full aggregation table every micro-batch (only for small tables).
# 3. Never mutate checkpointLocation schema incompatibly without draining the
#    backlog or provisioning a fresh checkpoint directory.


if __name__ == "__main__":
    import tempfile
    import time

    spark = (
        SparkSession.builder.appName("054_StructuredStreamingFoundations")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        df_txns = build_transaction_stream(spark, rows_per_second=5)
        query = start_stream_with_checkpoint(
            df_txns,
            sink_uri=f"{tmp_dir}/sink",
            checkpoint_uri=f"{tmp_dir}/chk",
        )
        time.sleep(2)
        print(inspect_query_progress(query))
        query.stop()
