"""
048 — Databricks Auto Loader (cloudFiles): Directory Listing vs File Notification & Checkpointing.

Scenario & Scale:
    An IoT telemetry landing bucket receives 10,000+ JSON/CSV files per hour
    (100M+ files over a year). Standard batch globbing (`spark.read.json("...")`)
    triggers full S3/ADLS/GCS directory listings on every run, causing driver OOMs,
    hours of listing latency, and duplicate ingestion without durable state.
"""

from __future__ import annotations

import tempfile
from pathlib import Path


# ==============================================================================
# 2. Anti-Pattern: Full-Bucket Batch Listing & Ephemeral Local Checkpoints
# ==============================================================================
# ❌ Bad Practice:
#   files = [f.path for f in dbutils.fs.ls("s3://iot-landing/raw/")]
#   df = spark.read.json("s3://iot-landing/raw/*.json")
#   df.writeStream.format("delta").option("checkpointLocation", "file:/local/chk").start("file:/local/bronze")
#
# Otherwise:
#   Listing millions of objects via `dbutils.fs.ls` or `*.json` explodes cloud API
#   LIST costs and driver memory. Storing checkpoints or schema locations on local
#   ephemeral disk wipes state on cluster restart, causing duplicate ingestion.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Auto Loader cloudFiles + Volume Checkpointing
# ==============================================================================
# ✅ Good Practice: Parameterize governed UC Volume / cloud checkpoint URIs and
#                   use `cloudFiles` with `availableNow=True` for cost-bounded runs.
def build_autoloader_options(
    source_format: str,
    schema_location: str,
    use_file_notifications: bool = False,
) -> dict[str, str]:
    """Return production Databricks Auto Loader options for cloudFiles."""
    options = {
        "cloudFiles.format": source_format,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.includeExistingFiles": "true",
    }
    if use_file_notifications:
        # Recommended above ~1M files/day using AWS SQS/SNS or Azure Event Grid
        options["cloudFiles.useNotifications"] = "true"
    return options


def run_autoloader_stream(
    spark,
    landing_uri: str,
    schema_uri: str,
    checkpoint_uri: str,
    target_table: str,
    use_notifications: bool = False,
) -> None:
    """Execute an incremental Auto Loader stream to a Unity Catalog Delta table."""
    spark.conf.set("spark.sql.ansi.enabled", "true")
    options = build_autoloader_options("json", schema_uri, use_file_notifications=use_notifications)

    df_stream = spark.readStream.format("cloudFiles").options(**options).load(landing_uri)

    query = (
        df_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_uri)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()


def simulate_incremental_file_stream_locally(
    spark,
    landing_dir: Path,
    checkpoint_dir: Path,
    output_dir: Path,
):
    """Portable OSS PySpark 4.0+ micro-batch simulation using `availableNow=True`."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    spark.conf.set("spark.sql.ansi.enabled", "true")
    explicit_schema = T.StructType([
        T.StructField("device_id", T.StringType(), nullable=False),
        T.StructField("raw_temp", T.StringType(), nullable=True),
        T.StructField("reading_ts", T.StringType(), nullable=True),
    ])

    df_stream = (
        spark.readStream
        .schema(explicit_schema)
        .option("maxFilesPerTrigger", 100)
        .json(str(landing_dir))
        .withColumn("temp_c", F.expr("try_cast(raw_temp AS DOUBLE)"))
        .withColumn("event_ts", F.expr("try_cast(reading_ts AS TIMESTAMP)"))
    )

    query = (
        df_stream.writeStream
        .format("parquet")
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start(str(output_dir))
    )
    query.awaitTermination()
    return spark.read.parquet(str(output_dir))


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In Spark UI -> Structured Streaming tab, monitor `inputRowsPerSecond`,
#   `processedRowsPerSecond`, and RocksDB state store memory under `State Operators`
#   (Auto Loader persists discovered file paths in RocksDB inside `checkpointLocation`).
# - In `df.explain("formatted")` on the micro-batch, confirm `FileScan` reads only
#   the newly discovered file paths from the batch offset log, not the full bucket.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Directory Listing mode issues incremental prefix listings (best for
#   <1M files or lexicographically ordered paths); File Notification mode subscribes
#   to SQS/Event Grid/PubSub for constant-time discovery on tens of millions of files.
# - Gotcha 2: `cloudFiles.schemaLocation` is mandatory when schema inference is on;
#   deleting it forces re-inference and can break downstream Delta merge compatibility.
# - Gotcha 3: `trigger(availableNow=True)` replaces deprecated `trigger(once=True)` by
#   splitting large backlogs into bounded micro-batches respecting `maxFilesPerTrigger`.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_048_AutoLoader").getOrCreate()
    with tempfile.TemporaryDirectory() as tmp_dir:
        base = Path(tmp_dir)
        landing = base / "landing"
        checkpoint = base / "checkpoint"
        output = base / "bronze_out"
        landing.mkdir(parents=True, exist_ok=True)
        (landing / "batch_001.json").write_text(
            '{"device_id":"IOT-1","raw_temp":"24.5","reading_ts":"2026-01-15T10:00:00Z"}\n'
            '{"device_id":"IOT-2","raw_temp":"BAD","reading_ts":"2026-01-15T10:01:00Z"}\n',
            encoding="utf-8",
        )
        df_out = simulate_incremental_file_stream_locally(spark, landing, checkpoint, output)
        df_out.show(truncate=False)
