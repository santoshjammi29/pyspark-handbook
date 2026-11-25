"""Scenario & Scale: Fintech streaming SLA enforcement (< 60s end-to-end lag).

Captures asynchronous micro-batch throughput, watermark lag, and query failures
via StreamingQueryListener without adding overhead to executor tasks.
"""

from collections.abc import Callable

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Polling `query.lastProgress` in a tight `while True:` driver
# loop with `time.sleep(1)` or computing counts inside `foreachBatch` just for
# monitoring.
# Otherwise: Polling blocks driver threads, misses transient micro-batch spikes,
# and extra `.count()` calls inside `foreachBatch` double-execute the DAG.
def bad_polling_monitor(query) -> None:
    import time

    while query.isActive:
        p = query.lastProgress  # Misses intermediate batches under fast triggers
        if p and p.get("numInputRows", 0) == 0:
            print("idle")
        time.sleep(5)


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Subclass StreamingQueryListener and register asynchronously
# via `spark.streams.addListener()`—zero task overhead, zero JVM driver context
# coupling, and Spark Connect compatible.
class PipelineSLAListener(StreamingQueryListener):
    """Asynchronous observability listener detecting streaming backpressure."""

    def __init__(
        self,
        lag_ratio_threshold: float = 0.8,
        alert_sink: Callable[[str], None] | None = None,
    ) -> None:
        self.lag_ratio_threshold = lag_ratio_threshold
        self.alert_sink = alert_sink or print

    def onQueryStarted(self, event) -> None:
        self.alert_sink(
            f"[SLA_START] id={event.id} name={event.name} runId={event.runId}"
        )

    def onQueryProgress(self, event) -> None:
        progress = event.progress
        input_rate = float(progress.inputRowsPerSecond or 0.0)
        proc_rate = float(progress.processedRowsPerSecond or 0.0)
        batch_id = progress.batchId
        num_rows = progress.numInputRows

        status = "HEALTHY"
        if input_rate > 0.0 and proc_rate < (input_rate * self.lag_ratio_threshold):
            status = "WARNING_PROCESSING_LAG"

        self.alert_sink(
            f"[SLA_METRIC] batch={batch_id} rows={num_rows} "
            f"in_rate={input_rate:.1f}/s proc_rate={proc_rate:.1f}/s status={status}"
        )

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            self.alert_sink(
                f"[SLA_CRITICAL] id={event.id} crashed: {event.exception}"
            )
        else:
            self.alert_sink(f"[SLA_STOP] id={event.id} terminated cleanly")


def register_sla_listener(
    spark: SparkSession,
    lag_ratio_threshold: float = 0.8,
) -> PipelineSLAListener:
    """Attach SLA listener to SparkSession cleanly via spark.streams."""
    listener = PipelineSLAListener(lag_ratio_threshold=lag_ratio_threshold)
    spark.streams.addListener(listener)
    return listener


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# StreamingQueryListener hooks into the driver's event bus asynchronously
# (identical telemetry source backing the Spark UI -> Structured Streaming tab).
# Key durationMs fields to track in `event.progress.durationMs`:
#   - `triggerExecution`: total micro-batch wall time (must stay < trigger interval).
#   - `getBatch`: source offset fetch time (spikes indicate Kafka/S3 listing lag).
#   - `addBatch`: actual Spark execution time (inspect Stages tab if elevated).


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Listener callbacks execute on the driver's listener bus thread—keep
#    `onQueryProgress` fast and non-blocking (async HTTP/metrics queue), or it
#    will drop/delay subsequent listener events.
# 2. A single listener attached to `spark.streams` receives events from ALL
#    active streaming queries in that SparkSession; filter by `event.progress.name`.
# 3. Always detach listeners in tests/notebooks via `spark.streams.removeListener`
#    to avoid duplicate alerts across repeated runs in a long-lived session.


if __name__ == "__main__":
    import tempfile
    import time

    spark = (
        SparkSession.builder.appName("063_StreamingObservabilityListener")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    listener = register_sla_listener(spark)
    with tempfile.TemporaryDirectory() as tmp_dir:
        df_rate = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
        query = (
            df_rate.writeStream.format("parquet")
            .queryName("sla_telemetry_demo")
            .option("checkpointLocation", f"{tmp_dir}/chk")
            .start(f"{tmp_dir}/sink")
        )
        time.sleep(2)
        query.stop()
    spark.streams.removeListener(listener)
