"""Scenario & Scale: FinOps optimization for hourly-arriving batch streams.

Replaces $50k/month 24/7 idle streaming clusters with scheduled incremental
batches processing 100GB+ backlogs safely via Trigger.AvailableNow().
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Using deprecated Trigger.Once() for large backlogs or running
# 24/7 Trigger.ProcessingTime() on hourly file drops.
# Otherwise: Trigger.Once() ignores rate-limit options (maxFilesPerTrigger /
# maxOffsetsPerTrigger) and loads the entire backlog into one giant micro-batch,
# triggering Executor OOM; 24/7 clusters burn 90% idle compute spend.
def bad_trigger_once(
    df_stream: DataFrame,
    sink_uri: str,
    checkpoint_uri: str,
) -> StreamingQuery:
    return (
        df_stream.writeStream.format("parquet")
        .option("checkpointLocation", checkpoint_uri)
        .trigger(once=True)  # Ignores maxFilesPerTrigger; OOM risk on backlog
        .start(sink_uri)
    )


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Use Trigger.AvailableNow() paired with rate-limiting options
# (maxFilesPerTrigger / maxBytesPerTrigger) to drain backlogs across multiple
# bounded micro-batches and shut down cleanly for transient job clusters.
def read_rate_limited_file_stream(
    spark: SparkSession,
    source_uri: str,
    schema_ddl: str,
    max_files_per_trigger: int = 1000,
) -> DataFrame:
    """Create a bounded-batch file stream that respects trigger rate limits."""
    return (
        spark.readStream.schema(schema_ddl)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .json(source_uri)
    )


def run_available_now_batch(
    df_stream: DataFrame,
    sink_uri: str,
    checkpoint_uri: str,
) -> None:
    """Process all available offsets across bounded micro-batches, then stop."""
    query = (
        df_stream.writeStream.format("parquet")
        .option("checkpointLocation", checkpoint_uri)
        .outputMode("append")
        .trigger(availableNow=True)
        .start(sink_uri)
    )
    query.awaitTermination()


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_stream.explain("formatted") displays FileStreamScan[id, val] with
# StreamingFileIndex.
# Spark UI -> Structured Streaming tab:
#   - With Trigger.AvailableNow(), observe multiple sequential batchIds (0, 1,
#     2...) each capped at maxFilesPerTrigger, rather than one massive batchId 0.
#   - Check Jobs tab: cluster exits automatically once all available offsets
#     are committed, enabling ephemeral job-cluster orchestration.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Trigger.AvailableNow() snapshots the available files/offsets at query
#    start; new files arriving mid-run wait for the next scheduled invocation.
# 2. Streaming state (watermarks, deduplication, aggregations) persists in the
#    checkpoint directory between AvailableNow() runs—it is NOT wiped on stop.
# 3. Never switch from Trigger.Once() to AvailableNow() without ensuring
#    maxFilesPerTrigger / maxOffsetsPerTrigger are tuned to executor memory.


if __name__ == "__main__":
    import json
    import tempfile
    from pathlib import Path

    spark = (
        SparkSession.builder.appName("055_StreamingTriggersAvailableNow")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        src = Path(tmp_dir) / "src"
        src.mkdir(parents=True, exist_ok=True)
        (src / "events_1.json").write_text(
            "\n".join([
                json.dumps({"id": 1, "val": "A"}),
                json.dumps({"id": 2, "val": "B"}),
            ])
        )
        df = read_rate_limited_file_stream(
            spark, str(src), "id INT, val STRING", max_files_per_trigger=1
        )
        run_available_now_batch(df, f"{tmp_dir}/sink", f"{tmp_dir}/chk")
        spark.read.parquet(f"{tmp_dir}/sink").show()
