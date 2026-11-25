"""
Module 05 / 038: Delta OPTIMIZE, File Compaction & Auto-Compaction Strategies

Scenario & Scale:
    A 24/7 Structured Streaming pipeline appends micro-batches every 30 seconds,
    generating 1,000+ tiny 50 KB Parquet files per hour. Analytical query latency
    degrades from 2s to 45s due to S3/ADLS LIST/GET metadata storm and driver
    file-planning overhead.

Goal:
    Compact fragmented small files into optimal ~128 MB–1 GB targets using
    `OPTIMIZE`, tune `delta.targetFileSize`, and enable `optimizeWrite` +
    `autoCompact` to prevent small-file sprawl at write time.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from pyspark.sql import DataFrame, SparkSession


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("038_Delta_OPTIMIZE_Compaction")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: High-frequency micro-batch appends without compaction
# ---------------------------------------------------------------------------
def anti_pattern_small_file_sprawl(df: DataFrame, delta_uri: str) -> None:
    # ❌ Bad Practice: Appending 200 Spark shuffle partitions every 30 seconds with
    # `autoCompact=false` and `optimizeWrite=false`, never scheduling `OPTIMIZE`.
    # Otherwise: Cloud storage accumulates millions of KB-sized Parquet files, driver
    # log replay slows to minutes, and vectorized Parquet readers spend 90% of wall
    # time opening/closing HTTP streams instead of scanning columnar row groups.
    df.repartition(200).write.format("delta").mode("append").save(delta_uri)


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: OPTIMIZE Bin-Packing + Write-Time Tuning
# ---------------------------------------------------------------------------
def good_practice_compaction_strategy(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Enable `optimizeWrite` (pre-write adaptive shuffle coalesce) and
    # `autoCompact` (post-commit lightweight bin-packing), and schedule `OPTIMIZE` with
    # partition predicates for bounded incremental compaction.
    target_uri = str(delta_dir)

    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(128 * 1024 * 1024))

    for i in range(12):
        batch = [(f"ITEM-{i}-{j}", f"desc_{j}", round(10.0 * j, 2)) for j in range(5)]
        spark.createDataFrame(batch, "item_id STRING, desc STRING, val DOUBLE").write.format(
            "delta"
        ).mode("append").save(target_uri)

    files_before = len(list(delta_dir.glob("*.parquet")))
    opt_metrics = spark.sql(f"OPTIMIZE delta.`{target_uri}`")
    opt_metrics.select(
        "path", "metrics.numFilesAdded", "metrics.numFilesRemoved"
    ).show(truncate=False)

    files_after = len(list(delta_dir.glob("*.parquet")))
    print(f"Parquet files on disk: before={files_before}, after_optimize={files_after}")


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - After `OPTIMIZE`, `spark.read.format("delta").load(target_uri).explain("formatted")`
#   still shows `FileScan parquet`, but the Spark UI SQL tab `number of files read`
#   drops from thousands to a handful of ~128 MB–1 GB files, slashing `Scan` task
#   launch overhead and boosting Parquet dictionary/RLE compression ratios.
# - Inspect `DESCRIBE HISTORY` `operation == 'OPTIMIZE'` `operationMetrics`
#   (`numAddedFiles`, `numRemovedFiles`, `p50FileSize`) to verify target file sizing.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `OPTIMIZE` Is Safe for Concurrent Readers/Writers: Readers remain isolated on the
#    prior snapshot; `OPTIMIZE` commits `remove` (old small files) + `add` (compacted
#    files) atomically without blocking concurrent `APPEND` streams.
# 2. Target File Size Tradeoff: Use 64–128 MB for tables with frequent row-level `MERGE`
#    (copy-on-write blast radius) and 512 MB–1 GB for append-mostly Gold analytics tables.
# 3. `optimizeWrite` vs `autoCompact`: `optimizeWrite` adds an adaptive shuffle before
#    writing so each partition emits larger files; `autoCompact` runs a mini-`OPTIMIZE`
#    immediately after small commits (useful when `optimizeWrite` still leaves <64 MB files).
# 4. Pair `OPTIMIZE` with `VACUUM`: `OPTIMIZE` creates new compacted files while keeping
#    old small files for Time Travel—schedule `VACUUM` after retention to reclaim disk.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_compaction_strategy(spark, Path(tmp_dir) / "delta_optimize")
    finally:
        spark.stop()
