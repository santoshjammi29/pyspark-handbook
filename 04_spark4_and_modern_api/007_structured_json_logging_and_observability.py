"""
Module 04 / 007: Spark 4.0 Structured JSON Logging & Query Observability

Scenario & Scale:
    A 24/7 Structured Streaming & batch lakehouse platform operates 60+ shared
    Spark 4.0 clusters shipping logs to OpenTelemetry / Datadog / CloudWatch.
    Legacy pipelines use `print()`, regex grok parsers over multi-line JVM logs,
    and `SparkContext.setJobGroup()`—all of which break under Spark Connect
    and Serverless runtimes.

Goal:
    Enable Spark 4.0 native Structured JSON Logging (`MDC` correlation fields),
    correlate jobs cleanly with `spark.addTag()` (zero `sparkContext` usage), and
    capture real-time streaming metrics via `StreamingQueryListener`.
"""

from __future__ import annotations

import json
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
)


def build_spark() -> SparkSession:
    """Build a Spark 4.0+ session with native structured JSON logging enabled."""
    return (
        SparkSession.builder.appName("007_Structured_JSON_Observability")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.log.structuredLogging.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: `SparkContext.setJobGroup` + unstructured `print()` polling
# ---------------------------------------------------------------------------
def anti_pattern_legacy_observability(df: DataFrame) -> int:
    # ❌ Bad Practice: Calling `SparkContext.setJobGroup(...)` / `setLogLevel()`,
    # printing unstructured strings, and polling `query.lastProgress` in a sleep loop:
    #     SparkContext.setJobGroup(sc, "grp_1", "legacy_job")  # Fails on Spark Connect
    # Otherwise: `SparkContext` crashes under Spark Connect & Serverless compute,
    # multi-line stack traces fracture regex log shippers, and busy driver polling loops
    # miss micro-batch spikes or delay termination alerts.
    print("Starting batch count for legacy_job")
    return df.count()


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Structured JSON Logs + Tags + Listener
# ---------------------------------------------------------------------------
class JsonTelemetryListener(StreamingQueryListener):
    """Emit structured JSON metrics on every streaming micro-batch without polling."""

    def __init__(self, logger: logging.Logger) -> None:
        super().__init__()
        self.logger = logger

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        self.logger.info(
            json.dumps(
                {
                    "event": "query_started",
                    "query_id": str(event.id),
                    "run_id": str(event.runId),
                    "name": event.name,
                }
            )
        )

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        p = event.progress
        self.logger.info(
            json.dumps(
                {
                    "event": "query_progress",
                    "query_id": str(p.id),
                    "batch_id": p.batchId,
                    "num_input_rows": p.numInputRows,
                    "input_rows_per_sec": p.inputRowsPerSecond,
                    "processed_rows_per_sec": p.processedRowsPerSecond,
                }
            )
        )

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        self.logger.info(json.dumps({"event": "query_idle", "query_id": str(event.id)}))

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        self.logger.warning(
            json.dumps(
                {
                    "event": "query_terminated",
                    "query_id": str(event.id),
                    "exception": event.exception,
                }
            )
        )


def good_practice_observable_pipeline(spark: SparkSession) -> DataFrame:
    # ✅ Good Practice: Use `spark.addTag` (Spark Connect + Classic compatible) for
    # correlation/cancellation and register `StreamingQueryListener` for push telemetry.
    logger = logging.getLogger("spark_observability")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    listener = JsonTelemetryListener(logger)
    spark.streams.addListener(listener)

    spark.addTag("domain=payments")
    spark.addTag("sla=tier1")
    try:
        stream_df = (
            spark.readStream.format("rate")
            .option("rowsPerSecond", 5)
            .load()
            .withColumn("bucket", F.col("value") % F.lit(4))
        )
        query = (
            stream_df.writeStream.format("memory")
            .queryName("payments_rate_obs")
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        return spark.table("payments_rate_obs")
    finally:
        spark.removeTag("domain=payments")
        spark.removeTag("sla=tier1")
        spark.streams.removeListener(listener)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - With `spark.log.structuredLogging.enabled=true` (Spark 4.0 default JSON Log4j2
#   layout), Spark emits single-line JSON log records containing `ts`, `level`, `msg`,
#   and `context` MDC keys (`task_name`, `stage_id`, `job_id`, `query_id`, `tag`).
# - Calling `explain("formatted")` alongside `spark.addTag(...)` lets platform engineers
#   match physical plan operators in the Spark UI SQL/Streaming tabs directly against
#   JSON log lines filtered by `context.tag = "domain=payments"`.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Tag Constraints: `spark.addTag` forbids commas (`,`) and enforces a 128-character
#    limit per tag; always remove tags in `try ... finally` to avoid polluting shared
#    multi-tenant sessions.
# 2. Keep `StreamingQueryListener` Non-Blocking: Listener callbacks execute sequentially on
#    Spark's listener event thread—never run Spark SQL actions or synchronous slow HTTP
#    calls inside `onQueryProgress`, or you will stall metric delivery and checkpointing.
# 3. `trigger(availableNow=True)` vs `once=True`: Always use `availableNow=True` in Spark
#    3.3+/4.0+ for bounded incremental runs; `once=True` is deprecated and ignores
#    `maxFilesPerTrigger` / `maxOffsetsPerTrigger` backpressure limits.
# 4. Zero `sparkContext` Dependency: `spark.addTag`, `spark.interruptTag`, `spark.streams`,
#    and `spark.conf` all work seamlessly across both Classic Spark and Spark Connect.


if __name__ == "__main__":
    spark = build_spark()
    try:
        good_practice_observable_pipeline(spark).show(5, truncate=False)
    finally:
        spark.stop()
