"""Incremental Batch File Discovery via Watermarking & Modification Timestamps.

Scenario & Scale:
    An hourly batch pipeline ingests partner CSV/Parquet drops from an object
    store bucket containing 500k+ historical files. Full directory re-scans
    waste 45 minutes per run; incremental discovery must finish in < 3 minutes.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Listing and reading the entire landing prefix on every hourly
# batch, or advancing the high-watermark to `now()` without an overlap buffer.
# Otherwise: O(N) file listing latency degrades SLAs exponentially, while clock
# skew or in-flight multipart uploads cause silent missed files.
def read_full_bucket_bad(spark: SparkSession, landing_uri: str, schema: str) -> DataFrame:
    return spark.read.format("csv").schema(schema).option("header", "true").load(landing_uri)


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Bound file discovery with `modifiedAfter` & `modifiedBefore`
# (leaving a clock-skew overlap window) + deterministic key deduplication.
def compute_watermark_window(
    last_watermark: datetime,
    now_utc: datetime | None = None,
    overlap_minutes: int = 2,
    settle_seconds: int = 30,
) -> tuple[str, str]:
    """Compute safe ISO-8601 UTC [modifiedAfter, modifiedBefore] bounds."""
    current = now_utc or datetime.now(timezone.utc)
    after_ts = (last_watermark - timedelta(minutes=overlap_minutes)).strftime("%Y-%m-%dT%H:%M:%S")
    before_ts = (current - timedelta(seconds=settle_seconds)).strftime("%Y-%m-%dT%H:%M:%S")
    return after_ts, before_ts


def read_incremental_files(
    spark: SparkSession,
    landing_uri: str,
    schema_ddl: str,
    modified_after: str,
    modified_before: str,
) -> DataFrame:
    """Read only objects modified within the bounded watermark interval."""
    return (
        spark.read.format("csv")
        .schema(schema_ddl)
        .option("header", "true")
        .option("modifiedAfter", modified_after)
        .option("modifiedBefore", modified_before)
        .load(landing_uri)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_mod_ts", F.col("_metadata.file_modification_time"))
    )


def transform_dedup_overlap_window(df: DataFrame, key_col: str) -> DataFrame:
    """Deduplicate records re-read due to the safety overlap buffer."""
    win = Window.partitionBy(key_col).orderBy(F.col("_file_mod_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("record_id", F.try_cast(F.col(key_col), T.LongType()))
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Spark evaluates `modifiedAfter`/`modifiedBefore` in the driver's
#   `InMemoryFileIndex` listing phase BEFORE launching executor tasks.
# - In Spark UI Jobs tab, watch the driver listing duration; once a prefix exceeds
#   ~100k files, migrate to date-partitioned prefixes or event-driven ingestion.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `modifiedAfter` / `modifiedBefore` still requires `LIST` API calls on the
#    prefix—for millions of files, use date-hierarchical paths (`YYYY/MM/DD/`)
#    or streaming file-notification queues (SQS/Event Grid).
# 2. Always persist high-watermark state in an ACID table AFTER the target write
#    succeeds; advancing watermarks before commit causes unrecoverable data loss.
# 3. Leave a 30-60s `modifiedBefore` settle buffer so files currently undergoing
#    eventual-consistency visibility or multipart flush are caught on next run.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("051_incremental_watermark_discovery")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        Path(tmp_dir, "batch_1.csv").write_text("id,val\n1,A\n2,B\n", encoding="utf-8")
        after_s, before_s = compute_watermark_window(
            datetime.now(timezone.utc) - timedelta(minutes=10),
            settle_seconds=0,
        )
        df_inc = read_incremental_files(spark, tmp_dir, "id STRING, val STRING", after_s, before_s)
        transform_dedup_overlap_window(df_inc, "id").show(truncate=False)
