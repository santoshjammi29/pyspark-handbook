"""
Module 05 / 034: ACID CRUD Operations on Delta: Insert, Update & Delete Mechanics

Scenario & Scale:
    A GDPR/CCPA privacy compliance pipeline processes 50K daily right-to-be-forgotten
    deletes and consent updates against a 1.5B-row customer profile Delta table
    without taking downstream BI queries offline.

Goal:
    Execute atomic row-level `UPDATE` and `DELETE` via `DeltaTable` / Spark SQL,
    leverage Delta Deletion Vectors (Merge-on-Read) to avoid full Parquet file
    rewrites, and verify commit metrics in `history()`.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("034_Delta_ACID_CRUD_Mechanics")
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
# (2) Anti-Pattern: Full-partition overwrite on raw Parquet for single-row edits
# ---------------------------------------------------------------------------
def anti_pattern_partition_rewrite(spark: SparkSession, parquet_dir: str, user_id: str) -> None:
    # ❌ Bad Practice: Reading an entire Parquet partition, filtering out 1 GDPR user,
    # and writing back with `mode("overwrite")` (`partitionOverwriteMode=dynamic`).
    # Otherwise: Deleting 10 rows across 500 partitions rewrites terabytes of cold
    # Parquet data, creates race conditions for concurrent readers, and leaves no
    # atomic rollback or commit history.
    spark.read.parquet(parquet_dir).filter(F.col("user_id") != F.lit(user_id)).write.mode(
        "overwrite"
    ).parquet(parquet_dir)


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Row-Level Delta CRUD + Deletion Vectors
# ---------------------------------------------------------------------------
def good_practice_acid_crud(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Enable Deletion Vectors (`delta.enableDeletionVectors=true`)
    # and run atomic `DeltaTable.update()` / `delete()`, writing tiny bitmap tombstones
    # instead of rewriting untouched rows in 1 GB Parquet files.
    target_uri = str(delta_dir)
    privacy_data = [
        ("U-01", "alice@corp.com", True, "2026-01-10"),
        ("U-02", "bob@spam.com", True, "2026-02-15"),
        ("U-03", "charlie@privacy.org", True, "2026-03-20"),
    ]
    df = spark.createDataFrame(
        privacy_data, "user_id STRING, email STRING, marketing_consent BOOLEAN, created_date STRING"
    ).withColumn("created_date", F.try_cast(F.col("created_date"), "DATE"))

    (
        df.write.format("delta")
        .option("delta.enableDeletionVectors", "true")
        .mode("overwrite")
        .save(target_uri)
    )

    delta_table = DeltaTable.forPath(spark, target_uri)
    delta_table.update(
        condition=F.col("email") == F.lit("bob@spam.com"),
        set={"marketing_consent": F.lit(False)},
    )
    delta_table.delete(condition=F.col("user_id") == F.lit("U-03"))

    delta_table.toDF().orderBy("user_id").show(truncate=False)
    delta_table.history().select("version", "operation", "operationMetrics").show(truncate=False)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - When Deletion Vectors (DVs) are enabled, `DELETE` / `UPDATE` physical plans scan
#   only files matching the predicate (`PushedFilters` + data skipping) and emit small
#   RoaringBitmap DV files instead of rewriting entire multi-hundred-MB Parquet files.
# - On subsequent reads, `explain("formatted")` shows a `FileScan parquet` with DV
#   metadata applied at scan time; inspect `numDeletionVectorsAdded` and
#   `numCopiedRows` in `DESCRIBE HISTORY` `operationMetrics` to verify MoR efficiency.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Copy-on-Write (CoW) vs Deletion Vectors (MoR): Classic Delta CoW rewrites every
#    file containing >=1 matched row (high write amplification for sparse deletes);
#    Delta 3.x/4.0 Deletion Vectors defer physical compaction until `OPTIMIZE`.
# 2. Materializing Deletes for GDPR Compliance: DVs hide deleted rows logically at read
#    time—always run `REORG TABLE ... APPLY (PURGE)` followed by `VACUUM` after the
#    retention window so PII bytes are physically erased from storage.
# 3. Conflict Detection on Concurrent Writes: Two concurrent `UPDATE` jobs touching the
#    same file without row-level concurrency fail with `ConcurrentAppendException`;
#    enable Deletion Vectors + row-level concurrency to isolate disjoint row edits.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_acid_crud(spark, Path(tmp_dir) / "delta_privacy")
    finally:
        spark.stop()
